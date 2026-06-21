// Package kafkaclient — order_ingest: raw orders -> validate -> game.orders.validated / game.dlq.
package kafkaclient

import (
	"encoding/json"
	"log"
	"strings"
	"time"

	"ring-of-the-middle-earth/internal/config"
	"ring-of-the-middle-earth/internal/game"
	"ring-of-the-middle-earth/internal/pipeline"
	"ring-of-the-middle-earth/internal/shared"
)

// RawOrderBody matches POST /order JSON (plus optional timestamp).
type RawOrderBody struct {
	OrderType    string   `json:"orderType"`
	PlayerID     string   `json:"playerId"`
	UnitID       string   `json:"unitId"`
	Turn         int      `json:"turn"`
	PathIDs      []string `json:"pathIds"`
	NewPathIDs   []string `json:"newPathIds"`
	TargetPathID string   `json:"targetPathId"`
	TargetRegion string   `json:"targetRegion"`
	Timestamp    int64    `json:"timestamp"`
}

// ValidatedOrderBody is the JSON payload emitted to game.orders.validated.
// It mirrors the assignment's OrderValidated schema, including Topology 2
// enrichment fields for route-related orders.
type ValidatedOrderBody struct {
	PlayerID        string   `json:"playerId"`
	UnitID          string   `json:"unitId"`
	OrderType       string   `json:"orderType"`
	Payload         []byte   `json:"payload"`
	Turn            int      `json:"turn"`
	Timestamp       int64    `json:"timestamp"`
	RouteRiskScore  *int     `json:"routeRiskScore"`
	ThreatenedPaths []string `json:"threatenedPaths"`
	BlockedPaths    []string `json:"blockedPaths"`
}

func playerFaction(playerID string) config.Side {
	if strings.HasPrefix(playerID, "player-dark") {
		return config.SideShadow
	}
	return config.SideFreePeoples
}

// RunOrderIngest reads raw order events (payload = POST body JSON), validates, produces.
func RunOrderIngest(
	rawCh <-chan shared.Event,
	cache *game.WorldStateCache,
	gameCfg *config.GameConfig,
	mapCfg *config.MapConfig,
	graph *game.Graph,
	producer *Producer,
	doneCh <-chan struct{},
) {
	seenByTurn := map[int]map[string]struct{}{}

	for {
		select {
		case <-doneCh:
			return
		case ev, ok := <-rawCh:
			if !ok {
				return
			}
			if ev.Topic != "game.orders.raw" {
				continue
			}

			code, msg := ValidateRawOrder(ev.Payload, cache, gameCfg, mapCfg, graph)
			if code != "" {
				produceDLQ(producer, code, msg, ev.Payload)
				continue
			}

			var body RawOrderBody
			if err := json.Unmarshal(ev.Payload, &body); err != nil {
				produceDLQ(producer, "INVALID_JSON", err.Error(), ev.Payload)
				continue
			}

			pruneSeenOrders(seenByTurn, cache.Snapshot().Turn)
			if _, ok := seenByTurn[body.Turn]; !ok {
				seenByTurn[body.Turn] = map[string]struct{}{}
			}
			if _, exists := seenByTurn[body.Turn][body.UnitID]; exists {
				produceDLQ(producer, "DUPLICATE_UNIT_ORDER", "same unit already submitted an order this turn", ev.Payload)
				continue
			}
			seenByTurn[body.Turn][body.UnitID] = struct{}{}

			validatedPayload, err := buildValidatedOrder(ev.Payload, body, cache, gameCfg, mapCfg, graph)
			if err != nil {
				produceDLQ(producer, "INVALID_JSON", err.Error(), ev.Payload)
				continue
			}

			key := body.UnitID
			if key == "" {
				key = "unknown"
			}
			if err := producer.Produce("game.orders.validated", key, validatedPayload); err != nil {
				log.Printf("[order-ingest] validated produce: %v", err)
			}
		}
	}
}

func pruneSeenOrders(seenByTurn map[int]map[string]struct{}, currentTurn int) {
	for turn := range seenByTurn {
		if turn < currentTurn {
			delete(seenByTurn, turn)
		}
	}
}

func produceDLQ(producer *Producer, code, msg string, rawPayload []byte) {
	dlq := map[string]interface{}{
		"originalTopic": "game.orders.raw",
		"partition":     -1,
		"offset":        -1,
		"errorCode":     code,
		"errorMessage":  msg,
		"rawPayload":    string(rawPayload),
		"timestamp":     time.Now().UnixMilli(),
	}
	b, _ := json.Marshal(dlq)
	if err := producer.Produce("game.dlq", code, b); err != nil {
		log.Printf("[order-ingest] dlq produce: %v", err)
	}
}

func buildValidatedOrder(
	rawPayload []byte,
	body RawOrderBody,
	cache *game.WorldStateCache,
	gameCfg *config.GameConfig,
	mapCfg *config.MapConfig,
	graph *game.Graph,
) ([]byte, error) {
	validated := ValidatedOrderBody{
		PlayerID:        body.PlayerID,
		UnitID:          body.UnitID,
		OrderType:       body.OrderType,
		Payload:         rawPayload,
		Turn:            body.Turn,
		Timestamp:       body.Timestamp,
		ThreatenedPaths: []string{},
		BlockedPaths:    []string{},
	}

	pathIDs := body.PathIDs
	if body.OrderType == "REDIRECT_UNIT" {
		pathIDs = body.NewPathIDs
	}
	if body.OrderType == "ASSIGN_ROUTE" || body.OrderType == "REDIRECT_UNIT" {
		snap := cache.Snapshot()
		startRegion := submittedRouteStartRegion(body.UnitID, snap, gameCfg)
		score := pipeline.ComputeRouteRiskForPathIDs(startRegion, pathIDs, snap, gameCfg, graph, mapCfg)
		validated.RouteRiskScore = &score.RiskScore
		validated.ThreatenedPaths = append(validated.ThreatenedPaths, score.ThreatPaths...)
		validated.BlockedPaths = append(validated.BlockedPaths, score.BlockedPaths...)
	}

	return json.Marshal(validated)
}

func submittedRouteStartRegion(
	unitID string,
	snap game.WorldStateCache,
	gameCfg *config.GameConfig,
) string {
	cfg, ok := gameCfg.Units[unitID]
	if ok && cfg.Class == "RingBearer" {
		return snap.LightView.RingBearerRegion
	}
	if unit, ok := snap.Units[unitID]; ok {
		return unit.CurrentRegion
	}
	if ok {
		return cfg.StartRegion
	}
	return ""
}

// ValidateRawOrder validates a raw order against all 8 rules (K4 criterion).
// Returns empty errorCode if the order is valid.
func ValidateRawOrder(
	payload []byte,
	cache *game.WorldStateCache,
	gameCfg *config.GameConfig,
	mapCfg *config.MapConfig,
	graph *game.Graph,
) (errorCode string, errorMsg string) {
	var body RawOrderBody
	if err := json.Unmarshal(payload, &body); err != nil {
		return "INVALID_JSON", err.Error()
	}
	if body.OrderType == "" || body.PlayerID == "" || body.UnitID == "" {
		return "INVALID_JSON", "missing orderType, playerId, or unitId"
	}

	snap := cache.Snapshot()

	cfg, ok := gameCfg.Units[body.UnitID]
	if !ok {
		return "NOT_YOUR_UNIT", "unknown unit"
	}

	if body.Turn != snap.Turn {
		return "WRONG_TURN", "order turn does not match game turn"
	}

	if cfg.Side != playerFaction(body.PlayerID) {
		return "NOT_YOUR_UNIT", "unit not on player's side"
	}

	u, uok := snap.Units[body.UnitID]
	if !uok || u.Status != game.StatusActive {
		return "INVALID_TARGET", "unit not active"
	}

	switch body.OrderType {
	case "ASSIGN_ROUTE", "REDIRECT_UNIT":
		paths := body.PathIDs
		if body.OrderType == "REDIRECT_UNIT" {
			paths = body.NewPathIDs
		}
		if len(paths) == 0 {
			return "INVALID_PATH", "empty route"
		}
		currentRegion := u.CurrentRegion
		if cfg.Class == "RingBearer" {
			currentRegion = snap.LightView.RingBearerRegion
		}
		for i, pathID := range paths {
			pc, pok := mapCfg.Paths[pathID]
			if !pok {
				return "INVALID_PATH", "unknown path"
			}
			if i == 0 && cfg.Class == "RingBearer" {
				if p, ok := snap.Paths[pathID]; ok && p.Status == game.PathBlocked {
					return "PATH_BLOCKED", "next path blocked"
				}
			}
			switch currentRegion {
			case pc.From:
				currentRegion = pc.To
			case pc.To:
				currentRegion = pc.From
			default:
				return "INVALID_PATH", "route path is not connected to the unit position"
			}
		}
	case "BLOCK_PATH", "SEARCH_PATH":
		pc, ok := mapCfg.Paths[body.TargetPathID]
		if !ok {
			return "INVALID_TARGET", "unknown path"
		}
		reg := u.CurrentRegion
		if reg != pc.From && reg != pc.To {
			return "UNIT_NOT_ADJACENT", "unit not at path endpoint"
		}
		if body.OrderType == "SEARCH_PATH" && cfg.Side != config.SideShadow {
			return "NOT_YOUR_UNIT", "search path is shadow-only"
		}
		if body.OrderType == "BLOCK_PATH" && guardedByEnemyFellowship(snap, gameCfg, pc, cfg.Side) {
			return "INVALID_TARGET", "path endpoint guarded by fellowship"
		}

	case "ATTACK_REGION":
		if !regionIsEnemyAdjacent(graph, snap, u.CurrentRegion, body.TargetRegion, cfg.Side) {
			return "INVALID_TARGET", "attack target not adjacent or not enemy-held"
		}

	case "MAIA_ABILITY":
		if u.Cooldown > 0 {
			return "ABILITY_ON_COOLDOWN", "maia on cooldown"
		}
		if _, ok := mapCfg.Paths[body.TargetPathID]; !ok {
			return "INVALID_TARGET", "unknown path"
		}

	case "DEPLOY_NAZGUL":
		if cfg.Side != config.SideShadow || cfg.DetectionRange <= 0 {
			return "NOT_YOUR_UNIT", "deploy nazgul shadow-only"
		}

	case "FORTIFY_REGION":
		if !cfg.CanFortify {
			return "NOT_YOUR_UNIT", "cannot fortify"
		}

	case "DESTROY_RING":
		if cfg.Class != "RingBearer" {
			return "NOT_YOUR_UNIT", "only ring bearer can destroy the ring"
		}
		if snap.LightView.RingBearerRegion != "mount-doom" {
			return "DESTROY_CONDITION_NOT_MET", "ring bearer is not at mount-doom"
		}
		if darkSideActiveAtRegion(snap, gameCfg, "mount-doom") {
			return "DESTROY_CONDITION_NOT_MET", "shadow occupies mount-doom"
		}

	case "REINFORCE_REGION":
		// Reinforce: loose check — engine will no-op if invalid.
	}

	return "", ""
}

func guardedByEnemyFellowship(
	snap game.WorldStateCache,
	gameCfg *config.GameConfig,
	path config.PathConfig,
	blockerSide config.Side,
) bool {
	for unitID, unit := range snap.Units {
		if unit.Status != game.StatusActive {
			continue
		}
		cfg, ok := gameCfg.Units[unitID]
		if !ok || cfg.Side == blockerSide || cfg.Class != "FellowshipGuard" {
			continue
		}
		if unit.CurrentRegion == path.From || unit.CurrentRegion == path.To {
			return true
		}
	}
	return false
}

func darkSideActiveAtRegion(snap game.WorldStateCache, gameCfg *config.GameConfig, regionID string) bool {
	for unitID, unit := range snap.Units {
		if unit.Status != game.StatusActive || unit.CurrentRegion != regionID {
			continue
		}
		cfg, ok := gameCfg.Units[unitID]
		if ok && cfg.Side == config.SideShadow {
			return true
		}
	}
	return false
}

func regionIsEnemyAdjacent(graph *game.Graph, snap game.WorldStateCache, fromRegion, targetRegion string, side config.Side) bool {
	if targetRegion == "" {
		return false
	}
	tgt, ok := snap.Regions[targetRegion]
	if !ok {
		return false
	}
	if tgt.ControlledBy == game.ControlNeutral || tgt.ControlledBy == game.Controller(side) {
		return false
	}
	for _, e := range graph.Neighbours(fromRegion) {
		if e.To == targetRegion {
			return true
		}
	}
	return false
}
