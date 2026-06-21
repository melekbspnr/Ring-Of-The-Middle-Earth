// Package engine — turn_processor.go implements the 13-step turn processing loop.
// Section 6 of the spec. Called by TurnProcessor.Run() goroutine.
// All steps operate on config fields — no unit ID string literals.
package engine

import (
	"encoding/json"
	"log"
	"sync"
	"time"

	"ring-of-the-middle-earth/internal/config"
	"ring-of-the-middle-earth/internal/game"
	"ring-of-the-middle-earth/internal/shared"
)

// Producer captures the event publishing methods the engine needs.
type Producer interface {
	Produce(topic, key string, value []byte) error
}

// ExactlyOnceFunc publishes a single message with stronger delivery guarantees.
type ExactlyOnceFunc func(brokers, topic, key string, value []byte) error

// TurnProcessor owns the 13-step turn-end processing pipeline.
type TurnProcessor struct {
	gameCfg          *config.GameConfig
	mapCfg           *config.MapConfig
	graph            *game.Graph
	cache            *game.WorldStateCache
	producer         Producer
	exactOnce        ExactlyOnceFunc
	brokers          string
	leaderInstanceID string

	mu            sync.Mutex
	currentTurn   int
	pendingOrders []ParsedOrder
	gameOverSent  bool
}

// ParsedOrder is a validated, parsed game order.
type ParsedOrder struct {
	OrderType    string
	PlayerID     string
	UnitID       string
	Turn         int
	PathIDs      []string // AssignRoute / RedirectUnit
	TargetPath   string   // BlockPath / SearchPath / MaiaAbility
	TargetRegion string   // AttackRegion / ReinforceRegion / DeployNazgul
}

// NewTurnProcessor creates a TurnProcessor.
func NewTurnProcessor(
	gameCfg *config.GameConfig,
	mapCfg *config.MapConfig,
	graph *game.Graph,
	cache *game.WorldStateCache,
	producer Producer,
	exactOnce ExactlyOnceFunc,
	brokers string,
	leaderInstanceID string,
) *TurnProcessor {
	return &TurnProcessor{
		gameCfg:          gameCfg,
		mapCfg:           mapCfg,
		graph:            graph,
		cache:            cache,
		producer:         producer,
		exactOnce:        exactOnce,
		brokers:          brokers,
		leaderInstanceID: leaderInstanceID,
		currentTurn:      1,
	}
}

// Reset clears buffered orders and turn counter (new game, leader only).
func (tp *TurnProcessor) Reset() {
	tp.mu.Lock()
	defer tp.mu.Unlock()
	tp.pendingOrders = nil
	tp.currentTurn = 1
	tp.gameOverSent = false
}

func (tp *TurnProcessor) syncTurnLocked(cacheTurn int) {
	if cacheTurn <= 0 || cacheTurn == tp.currentTurn {
		return
	}
	if cacheTurn < tp.currentTurn {
		// A lower turn means a new game/session reset was observed via Kafka.
		tp.pendingOrders = nil
	}
	tp.currentTurn = cacheTurn
}

// Run reads validated orders from engineCh and buffers them.
// AdvanceTurn() triggers the 13-step processing.
func (tp *TurnProcessor) Run(engineCh <-chan shared.Event, doneCh <-chan struct{}) {
	for {
		select {
		case <-doneCh:
			return
		case ev, ok := <-engineCh:
			if !ok {
				return
			}
			tp.bufferOrder(ev)
		}
	}
}

// AdvanceTurn executes the full 13-step turn-end sequence from Section 6.
func (tp *TurnProcessor) AdvanceTurn() {
	snap := tp.cache.Snapshot()
	initialSnap := snap
	rbState := tp.cache.RingBearerSnapshot()

	tp.mu.Lock()
	tp.syncTurnLocked(snap.Turn)
	if tp.gameOverSent || snap.Session.GameOver {
		tp.mu.Unlock()
		return
	}
	orders := tp.pendingOrders
	tp.pendingOrders = nil
	turn := tp.currentTurn
	tp.mu.Unlock()

	log.Printf("[turn %d] processing %d orders", turn, len(orders))

	// Step 1: Collect all validated orders for this turn.
	// (already done — orders slice)

	// Step 2: Process AssignRoute and RedirectUnit (sync RingBearer route into rbState)
	snap, rbState = tp.stepRouteOrders(snap, rbState, orders)

	// Step 3: Process BlockPath and SearchPath
	snap, rbState = tp.stepBlockAndSearch(snap, rbState, orders, turn)

	// Step 4: Process ReinforceRegion and DeployNazgul
	snap = tp.stepReinforceAndDeploy(snap, orders)

	// Step 5: Process FortifyRegion
	snap = tp.stepFortify(snap, orders)

	// Step 6: Process MaiaAbility
	snap = tp.stepMaiaAbilities(snap, orders)

	// Step 7: Auto-advance all units with routes
	snap, rbState = tp.stepAutoAdvance(snap, rbState, turn)

	// Step 8: Process AttackRegion
	snap = tp.stepCombat(snap, orders)

	// Step 9: Decrement TEMPORARILY_OPEN timers
	snap = tp.stepTempOpenTimers(snap)

	// Step 10: Decrement fortification timers
	snap = tp.stepFortifyTimers(snap)

	// Step 11: Decrement respawn and cooldown counters
	snap = tp.stepRespawnAndCooldown(snap)

	// Step 12: Run detection check
	detection := RunDetection(turn, tp.gameCfg.HiddenUntilTurn, rbState, snap.Units, tp.gameCfg.Units, tp.graph)
	if detection.Exposed {
		rbState.Exposed = true
		rbState.LastDetectedTurn = turn
		rbState.LastDetectedRegion = detection.TrueRegion
		tp.produceDetectionEvents(detection, turn)
	}

	tp.produceStateDeltaEvents(initialSnap, snap, turn)

	// Step 13: Evaluate win conditions
	winner, cause := tp.evaluateWinConditions(snap, rbState, orders, turn)
	if winner != "" {
		tp.produceGameOver(winner, cause, turn)
		tp.applyGameOverToSnapshot(&snap, winner, cause, turn)
		snap.Turn = turn
		tp.syncLightView(&snap, rbState)
		tp.cache.Update(snap, rbState)
		return
	}

	if turn >= tp.gameCfg.MaxTurns {
		tp.produceGameOver("DRAW", "MAX_TURNS_REACHED", turn)
		tp.applyGameOverToSnapshot(&snap, "DRAW", "MAX_TURNS_REACHED", turn)
		snap.Turn = turn
		tp.syncLightView(&snap, rbState)
		tp.cache.Update(snap, rbState)
		return
	}

	// Emit WorldStateSnapshot — `turn+1` is the active turn for new orders / UI
	tp.produceWorldState(snap, rbState, turn+1)

	// Reset exposed flag (end of turn)
	rbState.Exposed = false

	snap.Turn = turn
	tp.syncLightView(&snap, rbState)
	tp.cache.Update(snap, rbState)

	tp.mu.Lock()
	tp.currentTurn++
	tp.mu.Unlock()
}

func (tp *TurnProcessor) produceStateDeltaEvents(before, after game.WorldStateCache, turn int) {
	for unitID, afterUnit := range after.Units {
		if beforeUnit, ok := before.Units[unitID]; ok {
			cfg, cfgOK := tp.gameCfg.Units[unitID]
			if cfgOK && cfg.Class == "RingBearer" {
				continue
			}
			if beforeUnit.CurrentRegion != afterUnit.CurrentRegion ||
				beforeUnit.Strength != afterUnit.Strength ||
				beforeUnit.Status != afterUnit.Status ||
				beforeUnit.Cooldown != afterUnit.Cooldown {
				tp.produceUnitMoved(unitID, beforeUnit, afterUnit, turn)
			}
		}
	}

	for regionID, afterRegion := range after.Regions {
		if beforeRegion, ok := before.Regions[regionID]; ok && beforeRegion.ControlledBy != afterRegion.ControlledBy {
			tp.produceRegionControlChanged(regionID, afterRegion.ControlledBy, turn)
		}
	}

	for pathID, afterPath := range after.Paths {
		if beforePath, ok := before.Paths[pathID]; ok {
			if beforePath.Status != afterPath.Status ||
				beforePath.SurveillanceLevel != afterPath.SurveillanceLevel ||
				beforePath.TempOpenTurns != afterPath.TempOpenTurns {
				tp.producePathStatusChanged(pathID, afterPath, turn)
			}
		}
	}
}

// ─── Step implementations ─────────────────────────────────────────────────────

func (tp *TurnProcessor) stepRouteOrders(snap game.WorldStateCache, rbState game.RingBearerState, orders []ParsedOrder) (game.WorldStateCache, game.RingBearerState) {
	for _, o := range orders {
		switch o.OrderType {
		case "ASSIGN_ROUTE":
			if u, ok := snap.Units[o.UnitID]; ok && u.Status == game.StatusActive {
				u.Route = append([]string(nil), o.PathIDs...)
				u.RouteIdx = 0
				u.TravelPathID = ""
				u.TravelTurnsRemaining = 0
				snap.Units[o.UnitID] = u
				cfg := tp.gameCfg.Units[o.UnitID]
				if cfg.Class == "RingBearer" {
					rbState.Route = append([]string(nil), o.PathIDs...)
					rbState.RouteIdx = 0
					rbState.TravelPathID = ""
					rbState.TravelTurnsRemaining = 0
				}
			}
		case "REDIRECT_UNIT":
			if u, ok := snap.Units[o.UnitID]; ok && u.Status == game.StatusActive {
				u.Route = append([]string(nil), o.PathIDs...)
				u.RouteIdx = 0
				u.TravelPathID = ""
				u.TravelTurnsRemaining = 0
				snap.Units[o.UnitID] = u
				cfg := tp.gameCfg.Units[o.UnitID]
				if cfg.Class == "RingBearer" {
					rbState.Route = append([]string(nil), o.PathIDs...)
					rbState.RouteIdx = 0
					rbState.TravelPathID = ""
					rbState.TravelTurnsRemaining = 0
				}
			}
		}
	}
	return snap, rbState
}

func pathStatusFromSurveillance(path game.PathState) game.PathStatus {
	if path.SurveillanceLevel > 0 {
		return game.PathThreatened
	}
	return game.PathOpen
}

func (tp *TurnProcessor) pathGuardedByEnemy(pathCfg config.PathConfig, blockerSide config.Side, snap game.WorldStateCache) bool {
	for unitID, unit := range snap.Units {
		if unit.Status != game.StatusActive {
			continue
		}
		cfg, ok := tp.gameCfg.Units[unitID]
		if !ok {
			continue
		}
		if cfg.Side == blockerSide || cfg.Class != "FellowshipGuard" {
			continue
		}
		if unit.CurrentRegion == pathCfg.From || unit.CurrentRegion == pathCfg.To {
			return true
		}
	}
	return false
}

func (tp *TurnProcessor) stepBlockAndSearch(snap game.WorldStateCache, rbState game.RingBearerState, orders []ParsedOrder, turn int) (game.WorldStateCache, game.RingBearerState) {
	for _, o := range orders {
		switch o.OrderType {
		case "BLOCK_PATH":
			if p, ok := snap.Paths[o.TargetPath]; ok {
				pathCfg := tp.mapCfg.Paths[o.TargetPath]
				u := snap.Units[o.UnitID]
				// Unit must be at one of the path's endpoints
				if (u.CurrentRegion == pathCfg.From || u.CurrentRegion == pathCfg.To) &&
					!tp.pathGuardedByEnemy(pathCfg, tp.gameCfg.Units[o.UnitID].Side, snap) {
					p.Status = game.PathBlocked
					p.BlockedByUnitID = o.UnitID
					snap.Paths[o.TargetPath] = p
					// Check if Ring Bearer's current route includes this path
					if snap.IsInRingBearerRoute(rbState, o.TargetPath) {
						log.Printf("[turn %d] RouteCompromised: ring-bearer route blocked on %s", turn, o.TargetPath)
					}
				}
			}

		case "SEARCH_PATH":
			if p, ok := snap.Paths[o.TargetPath]; ok {
				if p.SurveillanceLevel < 3 {
					p.SurveillanceLevel++
				}
				if p.Status == game.PathOpen && p.SurveillanceLevel > 0 {
					p.Status = game.PathThreatened
				}
				snap.Paths[o.TargetPath] = p
			}
		}
	}
	// Revert BLOCKED paths whose blocking unit has moved away
	for pathID, p := range snap.Paths {
		if p.Status == game.PathBlocked && p.BlockedByUnitID != "" {
			u, ok := snap.Units[p.BlockedByUnitID]
			pathCfg := tp.mapCfg.Paths[pathID]
			if !ok || (u.CurrentRegion != pathCfg.From && u.CurrentRegion != pathCfg.To) {
				p.Status = pathStatusFromSurveillance(p)
				p.BlockedByUnitID = ""
				snap.Paths[pathID] = p
			}
		}
	}
	return snap, rbState
}

func (tp *TurnProcessor) stepReinforceAndDeploy(snap game.WorldStateCache, orders []ParsedOrder) game.WorldStateCache {
	for _, o := range orders {
		switch o.OrderType {
		case "REINFORCE_REGION":
			if u, ok := snap.Units[o.UnitID]; ok {
				cfg := tp.gameCfg.Units[o.UnitID]
				// Move to adjacent region (basic adjacency check via graph)
				edges := tp.graph.Neighbours(u.CurrentRegion)
				for _, e := range edges {
					if e.To == o.TargetRegion {
						u.CurrentRegion = o.TargetRegion
						u.TravelPathID = ""
						u.TravelTurnsRemaining = 0
						snap.Units[o.UnitID] = u
						_ = cfg
						break
					}
				}
			}
		case "DEPLOY_NAZGUL":
			if u, ok := snap.Units[o.UnitID]; ok {
				cfg := tp.gameCfg.Units[o.UnitID]
				if cfg.DetectionRange > 0 { // config-driven Nazgul identification
					u.CurrentRegion = o.TargetRegion
					u.TravelPathID = ""
					u.TravelTurnsRemaining = 0
					snap.Units[o.UnitID] = u
				}
			}
		}
	}
	return snap
}

func (tp *TurnProcessor) stepFortify(snap game.WorldStateCache, orders []ParsedOrder) game.WorldStateCache {
	for _, o := range orders {
		if o.OrderType == "FORTIFY_REGION" {
			cfg := tp.gameCfg.Units[o.UnitID]
			if !cfg.CanFortify { // config-driven: only GondorArmy can fortify
				continue
			}
			u := snap.Units[o.UnitID]
			if r, ok := snap.Regions[u.CurrentRegion]; ok {
				r.Fortified = true
				r.FortifyTurns = 2
				snap.Regions[u.CurrentRegion] = r
			}
		}
	}
	return snap
}

func (tp *TurnProcessor) stepMaiaAbilities(snap game.WorldStateCache, orders []ParsedOrder) game.WorldStateCache {
	for _, o := range orders {
		if o.OrderType != "MAIA_ABILITY" {
			continue
		}
		unitSnap, ok := snap.Units[o.UnitID]
		if !ok {
			continue
		}
		cfg := tp.gameCfg.Units[o.UnitID]
		pathState, pathOk := snap.Paths[o.TargetPath]
		pathCfg := tp.mapCfg.Paths[o.TargetPath]
		if !pathOk {
			continue
		}

		// Check whether this CorruptPath-capable Maia was disabled by Isengard falling.
		maiaDisabled := snap.IsCorruptPathMaiaDisabled(o.UnitID, tp.gameCfg)

		result, err := DispatchMaiaAbility(unitSnap, cfg, o.TargetPath, pathState, pathCfg, maiaDisabled)
		if err != nil {
			log.Printf("[maia] %s ability failed: %v", o.UnitID, err)
			continue
		}

		// Apply result
		switch result.Effect {
		case "OPEN_PATH":
			pathState.Status = game.PathTemporarilyOpen
			pathState.TempOpenTurns = 2
		case "CORRUPT_PATH":
			pathState.SurveillanceLevel = 3
		}
		snap.Paths[o.TargetPath] = pathState

		// Set cooldown on the unit
		unitSnap.Cooldown = cfg.Cooldown
		snap.Units[o.UnitID] = unitSnap

		log.Printf("[maia] %s applied %s on %s", o.UnitID, result.Effect, o.TargetPath)
	}
	return snap
}

func (tp *TurnProcessor) stepAutoAdvance(snap game.WorldStateCache, rbState game.RingBearerState, turn int) (game.WorldStateCache, game.RingBearerState) {
	// Advance all units with assigned routes
	for id, u := range snap.Units {
		if u.Status != game.StatusActive || len(u.Route) == 0 || u.RouteIdx >= len(u.Route) {
			continue
		}
		nextPathID := u.Route[u.RouteIdx]
		pathState, ok := snap.Paths[nextPathID]
		if !ok {
			continue
		}

		if u.TravelPathID == nextPathID && u.TravelTurnsRemaining > 0 {
			u.TravelTurnsRemaining--
			if u.TravelTurnsRemaining == 0 {
				pathCfg := tp.mapCfg.Paths[nextPathID]
				dest := pathCfg.To
				if u.CurrentRegion == pathCfg.To {
					dest = pathCfg.From
				}
				u.CurrentRegion = dest
				u.RouteIdx++
				u.TravelPathID = ""
			}
			snap.Units[id] = u
			continue
		}

		switch pathState.Status {
		case game.PathBlocked:
			log.Printf("[advance] unit %s route blocked on %s", id, nextPathID)
			// emit RouteBlocked
			continue
		case game.PathOpen, game.PathThreatened, game.PathTemporarilyOpen:
			pathCfg := tp.mapCfg.Paths[nextPathID]
			cost := pathCfg.Cost
			if cost <= 1 {
				dest := pathCfg.To
				if u.CurrentRegion == pathCfg.To {
					dest = pathCfg.From
				}
				u.CurrentRegion = dest
				u.RouteIdx++
			} else {
				u.TravelPathID = nextPathID
				u.TravelTurnsRemaining = cost - 1
			}
			snap.Units[id] = u
		}

		if u.RouteIdx >= len(u.Route) {
			log.Printf("[advance] unit %s route complete at %s", id, u.CurrentRegion)
		}
	}

	// Advance Ring Bearer separately (hidden)
	if len(rbState.Route) > 0 && rbState.RouteIdx < len(rbState.Route) {
		nextPathID := rbState.Route[rbState.RouteIdx]
		pathState, ok := snap.Paths[nextPathID]
		if !ok {
			return snap, rbState
		}

		if rbState.TravelPathID == nextPathID && rbState.TravelTurnsRemaining > 0 {
			rbState.TravelTurnsRemaining--
			if rbState.TravelTurnsRemaining == 0 {
				pathCfg := tp.mapCfg.Paths[nextPathID]
				dest := pathCfg.To
				if rbState.TrueRegion == pathCfg.To {
					dest = pathCfg.From
				}
				prev := rbState.TrueRegion
				rbState.TrueRegion = dest
				rbState.RouteIdx++
				rbState.TravelPathID = ""
				if IsExposedBySurveillance(pathState, turn, tp.gameCfg.HiddenUntilTurn) {
					rbState.Exposed = true
					tp.produceRingBearerSpotted(nextPathID, turn)
					log.Printf("[advance] ring-bearer spotted on %s (surveillance level %d)", nextPathID, pathState.SurveillanceLevel)
				}
				tp.produceRingBearerMoved(dest, turn)
				log.Printf("[advance] ring-bearer moved from %s to %s (turn %d)", prev, dest, turn)
			}
			return snap, rbState
		}
		switch pathState.Status {
		case game.PathBlocked:
			log.Printf("[advance] ring-bearer route blocked on %s", nextPathID)
		case game.PathOpen, game.PathThreatened, game.PathTemporarilyOpen:
			pathCfg := tp.mapCfg.Paths[nextPathID]
			cost := pathCfg.Cost
			if cost <= 1 {
				dest := pathCfg.To
				if rbState.TrueRegion == pathCfg.To {
					dest = pathCfg.From
				}
				prev := rbState.TrueRegion
				rbState.TrueRegion = dest
				rbState.RouteIdx++

				// Surveillance exposure check (Step 7)
				if IsExposedBySurveillance(pathState, turn, tp.gameCfg.HiddenUntilTurn) {
					rbState.Exposed = true
					tp.produceRingBearerSpotted(nextPathID, turn)
					log.Printf("[advance] ring-bearer spotted on %s (surveillance level %d)", nextPathID, pathState.SurveillanceLevel)
				}
				tp.produceRingBearerMoved(dest, turn)
				log.Printf("[advance] ring-bearer moved from %s to %s (turn %d)", prev, dest, turn)
			} else {
				rbState.TravelPathID = nextPathID
				rbState.TravelTurnsRemaining = cost - 1
			}
		}
	}
	return snap, rbState
}

func (tp *TurnProcessor) stepCombat(snap game.WorldStateCache, orders []ParsedOrder) game.WorldStateCache {
	for _, o := range orders {
		if o.OrderType != "ATTACK_REGION" {
			continue
		}
		targetRegion, ok := snap.Regions[o.TargetRegion]
		if !ok {
			continue
		}
		regionCfg := tp.mapCfg.Regions[o.TargetRegion]

		// Gather attackers (units in the attacker's current region, same side)
		attackerUnit := snap.Units[o.UnitID]
		attackerCfg := tp.gameCfg.Units[o.UnitID]

		var attackers []game.UnitSnapshot
		var attackerCfgs []config.UnitConfig
		var defenders []game.UnitSnapshot
		var defenderCfgs []config.UnitConfig

		for uid, u := range snap.Units {
			if u.Status != game.StatusActive {
				continue
			}
			cfg := tp.gameCfg.Units[uid]
			if u.CurrentRegion == attackerUnit.CurrentRegion && cfg.Side == attackerCfg.Side {
				attackers = append(attackers, u)
				attackerCfgs = append(attackerCfgs, cfg)
			}
			if u.CurrentRegion == o.TargetRegion && cfg.Side != attackerCfg.Side {
				defenders = append(defenders, u)
				defenderCfgs = append(defenderCfgs, cfg)
			}
		}

		result := ResolveAttack(attackers, attackerCfgs, defenders, defenderCfgs, targetRegion, regionCfg)

		if result.AttackerWon {
			// Move all attackers into target region
			for _, a := range attackers {
				u := snap.Units[a.ID]
				u.CurrentRegion = o.TargetRegion
				u.TravelPathID = ""
				u.TravelTurnsRemaining = 0
				snap.Units[a.ID] = u
			}
			// Update region control
			targetRegion.ControlledBy = game.Controller(attackerCfg.Side)
			// Apply damage to defenders
			for i, d := range defenders {
				updated := ApplyDamage(snap.Units[d.ID], defenderCfgs[i], result.Damage)
				snap.Units[d.ID] = updated
			}
			// If Isengard falls, permanently disable CorruptPath-capable Maia units.
			if o.TargetRegion == "isengard" && attackerCfg.Side == config.SideFreePeoples {
				snap.DisableCorruptPathMaias(tp.gameCfg)
			}
		} else {
			// Each attacker loses 1 strength
			for i, a := range attackers {
				updated := ApplyDamage(snap.Units[a.ID], attackerCfgs[i], 1)
				snap.Units[a.ID] = updated
			}
		}
		snap.Regions[o.TargetRegion] = targetRegion
	}
	return snap
}

func (tp *TurnProcessor) stepTempOpenTimers(snap game.WorldStateCache) game.WorldStateCache {
	for id, p := range snap.Paths {
		if p.Status != game.PathTemporarilyOpen {
			continue
		}
		p.TempOpenTurns--
		if p.TempOpenTurns <= 0 {
			if p.BlockedByUnitID != "" {
				p.Status = game.PathBlocked
			} else {
				p.Status = pathStatusFromSurveillance(p)
			}
			p.TempOpenTurns = 0
		}
		snap.Paths[id] = p
	}
	return snap
}

func (tp *TurnProcessor) stepFortifyTimers(snap game.WorldStateCache) game.WorldStateCache {
	for id, r := range snap.Regions {
		if r.Fortified {
			r.FortifyTurns--
			if r.FortifyTurns <= 0 {
				r.Fortified = false
			}
			snap.Regions[id] = r
		}
	}
	return snap
}

func (tp *TurnProcessor) stepRespawnAndCooldown(snap game.WorldStateCache) game.WorldStateCache {
	for id, u := range snap.Units {
		cfg := tp.gameCfg.Units[id]

		// Respawn counter
		if u.Status == game.StatusRespawning {
			u.RespawnTurns--
			if u.RespawnTurns <= 0 {
				u.Status = game.StatusActive
				u.Strength = cfg.Strength // restore full strength
				u.CurrentRegion = cfg.StartRegion
				u.RespawnTurns = 0
				u.TravelPathID = ""
				u.TravelTurnsRemaining = 0
			}
		}

		// Cooldown counter
		if u.Cooldown > 0 {
			u.Cooldown--
		}

		snap.Units[id] = u
	}
	return snap
}

// ─── Win condition evaluation ─────────────────────────────────────────────────

func (tp *TurnProcessor) evaluateWinConditions(snap game.WorldStateCache, rbState game.RingBearerState, orders []ParsedOrder, turn int) (winner, cause string) {
	// Light Side wins: RB at mount-doom + DestroyRing order + no Dark Side unit at mount-doom
	destroyRequested := false
	for _, o := range orders {
		if o.OrderType == "DESTROY_RING" {
			destroyRequested = true
		}
	}
	if rbState.TrueRegion == "mount-doom" && destroyRequested {
		darkAtMountDoom := false
		for id, u := range snap.Units {
			cfg := tp.gameCfg.Units[id]
			if u.CurrentRegion == "mount-doom" && cfg.Side == config.SideShadow && u.Status == game.StatusActive {
				darkAtMountDoom = true
				break
			}
		}
		if !darkAtMountDoom {
			return "FREE_PEOPLES", "RING_DESTROYED"
		}
	}

	// Dark Side wins: Nazgul same region as Ring Bearer AND exposed
	if rbState.Exposed {
		for id, u := range snap.Units {
			cfg := tp.gameCfg.Units[id]
			if cfg.DetectionRange > 0 && u.Status == game.StatusActive && u.CurrentRegion == rbState.TrueRegion {
				_ = id
				return "SHADOW", "RING_BEARER_CAUGHT"
			}
		}
	}

	return "", ""
}

// ─── Kafka event production ───────────────────────────────────────────────────

func (tp *TurnProcessor) produceDetectionEvents(d DetectionResult, turn int) {
	log.Printf("[detection] turn %d: ring-bearer DETECTED at %s by %s", turn, d.TrueRegion, d.DetectedByUnitID)
	b, err := json.Marshal(map[string]interface{}{
		"regionId":  d.TrueRegion,
		"turn":      turn,
		"timestamp": time.Now().UnixMilli(),
		"kind":      "DETECTED",
	})
	if err != nil {
		return
	}
	_ = tp.producer.Produce("game.ring.detection", "player-dark", b)
}

func (tp *TurnProcessor) produceRingBearerMoved(trueRegion string, turn int) {
	rbID := game.RingBearerID(tp.gameCfg)
	b, err := json.Marshal(map[string]interface{}{
		"trueRegion": trueRegion,
		"turn":       turn,
		"timestamp":  time.Now().UnixMilli(),
	})
	if err != nil {
		return
	}
	_ = tp.producer.Produce("game.ring.position", rbID, b)
}

func (tp *TurnProcessor) produceRingBearerSpotted(pathID string, turn int) {
	b, err := json.Marshal(map[string]interface{}{
		"pathId":    pathID,
		"turn":      turn,
		"timestamp": time.Now().UnixMilli(),
		"kind":      "SPOTTED",
	})
	if err != nil {
		return
	}
	_ = tp.producer.Produce("game.ring.detection", "player-dark", b)
}

func (tp *TurnProcessor) produceUnitMoved(unitID string, before, after game.UnitSnapshot, turn int) {
	b, err := json.Marshal(map[string]interface{}{
		"unitId":    unitID,
		"from":      before.CurrentRegion,
		"to":        after.CurrentRegion,
		"strength":  after.Strength,
		"status":    string(after.Status),
		"cooldown":  after.Cooldown,
		"turn":      turn,
		"timestamp": time.Now().UnixMilli(),
	})
	if err != nil {
		return
	}
	_ = tp.producer.Produce("game.events.unit", unitID, b)
}

func (tp *TurnProcessor) produceRegionControlChanged(regionID string, controller game.Controller, turn int) {
	b, err := json.Marshal(map[string]interface{}{
		"regionId":      regionID,
		"newController": string(controller),
		"turn":          turn,
		"timestamp":     time.Now().UnixMilli(),
	})
	if err != nil {
		return
	}
	_ = tp.producer.Produce("game.events.region", regionID, b)
}

func (tp *TurnProcessor) producePathStatusChanged(pathID string, pathState game.PathState, turn int) {
	b, err := json.Marshal(map[string]interface{}{
		"pathId":            pathID,
		"newStatus":         string(pathState.Status),
		"surveillanceLevel": pathState.SurveillanceLevel,
		"tempOpenTurns":     pathState.TempOpenTurns,
		"turn":              turn,
		"timestamp":         time.Now().UnixMilli(),
	})
	if err != nil {
		return
	}
	_ = tp.producer.Produce("game.events.path", pathID, b)
}

func (tp *TurnProcessor) produceWorldState(snap game.WorldStateCache, rbState game.RingBearerState, turn int) {
	log.Printf("[world] turn %d: emitting WorldStateSnapshot", turn)
	b, err := game.MarshalWorldBroadcast(snap, rbState, turn, tp.mapCfg, tp.gameCfg)
	if err != nil {
		log.Printf("[world] marshal: %v", err)
		return
	}
	if err := tp.producer.Produce("game.broadcast", "world", b); err != nil {
		log.Printf("[world] produce: %v", err)
	}
	tp.produceSessionTurn(turn)
}

func (tp *TurnProcessor) produceSessionTurn(nextTurn int) {
	now := time.Now().UnixMilli()
	session := tp.cache.Snapshot().Session
	b, err := json.Marshal(map[string]interface{}{
		"turn":              nextTurn,
		"leaderId":          tp.leaderInstanceID,
		"leaderHeartbeatTs": now,
		"epoch":             session.Epoch,
		"gameOver":          session.GameOver,
		"gameOverWinner":    session.GameOverWinner,
		"gameOverCause":     session.GameOverCause,
		"gameOverTurn":      session.GameOverTurn,
	})
	if err != nil {
		return
	}
	_ = tp.producer.Produce("game.session", "session", b)
}

func (tp *TurnProcessor) produceGameOver(winner, cause string, turn int) {
	tp.mu.Lock()
	if tp.gameOverSent {
		tp.mu.Unlock()
		return
	}
	tp.gameOverSent = true
	tp.mu.Unlock()
	log.Printf("[game] GAME OVER — winner=%s cause=%s turn=%d", winner, cause, turn)
	b, err := json.Marshal(map[string]interface{}{
		"winner":    winner,
		"cause":     cause,
		"turn":      turn,
		"timestamp": time.Now().UnixMilli(),
	})
	if err != nil {
		return
	}
	if tp.exactOnce != nil {
		log.Println("[TEST] SLEEPING 15 SECONDS BEFORE GAMEOVER TRANSACTION... KILL ME NOW!")
		time.Sleep(15 * time.Second)
		if err := tp.exactOnce(tp.brokers, "game.broadcast", "game-over", b); err == nil {
			tp.produceGameOverSession(winner, cause, turn)
			return
		} else {
			log.Printf("[game] gameover exactly-once: %v", err)
		}
	}
	if err := tp.producer.Produce("game.broadcast", "game-over", b); err != nil {
		log.Printf("[game] gameover exactly-once: %v", err)
	}
	tp.produceGameOverSession(winner, cause, turn)
}

func (tp *TurnProcessor) produceGameOverSession(winner, cause string, turn int) {
	session := tp.cache.Snapshot().Session
	now := time.Now().UnixMilli()
	b, err := json.Marshal(map[string]interface{}{
		"turn":              turn,
		"leaderId":          tp.leaderInstanceID,
		"leaderHeartbeatTs": now,
		"epoch":             session.Epoch,
		"gameOver":          true,
		"gameOverWinner":    winner,
		"gameOverCause":     cause,
		"gameOverTurn":      turn,
	})
	if err != nil {
		return
	}
	_ = tp.producer.Produce("game.session", "session", b)
}

func (tp *TurnProcessor) applyGameOverToSnapshot(snap *game.WorldStateCache, winner, cause string, turn int) {
	snap.Session.GameOver = true
	snap.Session.GameOverWinner = winner
	snap.Session.GameOverCause = cause
	snap.Session.GameOverTurn = turn
}

func (tp *TurnProcessor) syncLightView(snap *game.WorldStateCache, rbState game.RingBearerState) {
	snap.LightView.RingBearerRegion = rbState.TrueRegion
	snap.LightView.AssignedRoute = append([]string(nil), rbState.Route...)
	snap.LightView.RouteIdx = rbState.RouteIdx
}

func (tp *TurnProcessor) bufferOrder(ev shared.Event) {
	po, ok := parseValidatedOrder(ev.Payload)
	if !ok {
		return
	}
	tp.mu.Lock()
	tp.syncTurnLocked(tp.cache.Snapshot().Turn)
	defer tp.mu.Unlock()
	if po.Turn != tp.currentTurn {
		return
	}
	for _, ex := range tp.pendingOrders {
		if ex.UnitID == po.UnitID {
			log.Printf("[turn] duplicate order dropped for unit %s", po.UnitID)
			return
		}
	}
	tp.pendingOrders = append(tp.pendingOrders, po)
}

func parseValidatedOrder(payload []byte) (ParsedOrder, bool) {
	var validated struct {
		PlayerID       string `json:"playerId"`
		UnitID         string `json:"unitId"`
		OrderType      string `json:"orderType"`
		Turn           int    `json:"turn"`
		Payload        []byte `json:"payload"`
		RouteRiskScore *int   `json:"routeRiskScore"`
	}
	if err := json.Unmarshal(payload, &validated); err != nil {
		return ParsedOrder{}, false
	}
	if (validated.OrderType == "ASSIGN_ROUTE" || validated.OrderType == "REDIRECT_UNIT") && validated.RouteRiskScore == nil {
		return ParsedOrder{}, false
	}

	orderPayload := validated.Payload
	if len(orderPayload) == 0 {
		orderPayload = payload
	}

	var body struct {
		OrderType    string   `json:"orderType"`
		PlayerID     string   `json:"playerId"`
		UnitID       string   `json:"unitId"`
		Turn         int      `json:"turn"`
		PathIDs      []string `json:"pathIds"`
		NewPathIDs   []string `json:"newPathIds"`
		TargetPathID string   `json:"targetPathId"`
		TargetRegion string   `json:"targetRegion"`
	}
	if err := json.Unmarshal(orderPayload, &body); err != nil {
		return ParsedOrder{}, false
	}
	if validated.PlayerID != "" {
		body.PlayerID = validated.PlayerID
	}
	if validated.UnitID != "" {
		body.UnitID = validated.UnitID
	}
	if validated.OrderType != "" {
		body.OrderType = validated.OrderType
	}
	if validated.Turn != 0 {
		body.Turn = validated.Turn
	}

	pathIDs := body.PathIDs
	if body.OrderType == "REDIRECT_UNIT" {
		pathIDs = body.NewPathIDs
	}
	return ParsedOrder{
		OrderType:    body.OrderType,
		PlayerID:     body.PlayerID,
		UnitID:       body.UnitID,
		Turn:         body.Turn,
		PathIDs:      append([]string(nil), pathIDs...),
		TargetPath:   body.TargetPathID,
		TargetRegion: body.TargetRegion,
	}, true
}
