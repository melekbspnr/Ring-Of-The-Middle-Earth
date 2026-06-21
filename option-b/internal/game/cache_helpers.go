// Package game — cache_helpers.go provides methods on WorldStateCache
// used by TurnProcessor and other engine components.
package game

import (
	"ring-of-the-middle-earth/internal/config"
	"sync"
)

// NewWorldStateCache initialises the cache from config (startup state).
func NewWorldStateCache(gameCfg *config.GameConfig, mapCfg *config.MapConfig) *WorldStateCache {
	rbID := RingBearerID(gameCfg)
	c := &WorldStateCache{
		Turn:             1,
		Session:          SessionState{},
		RingBearerUnitID: rbID,
		Units:            make(map[string]UnitSnapshot),
		Regions:          make(map[string]RegionState),
		Paths:            make(map[string]PathState),
	}

	// Initialise units from config
	for id, cfg := range gameCfg.Units {
		c.Units[id] = UnitSnapshot{
			ID:            id,
			CurrentRegion: cfg.StartRegion,
			Strength:      cfg.Strength,
			Status:        StatusActive,
			Cooldown:      0,
		}
	}

	// Ring Bearer's public CurrentRegion is always ""
	if rb, ok := c.Units[rbID]; ok {
		rb.CurrentRegion = ""
		c.Units[rbID] = rb
	}

	// Initialise regions from map config
	for id, r := range mapCfg.Regions {
		c.Regions[id] = RegionState{
			ID:           id,
			ControlledBy: Controller(r.StartControl),
			ThreatLevel:  r.StartThreat,
		}
	}

	// Initialise paths from map config (all OPEN at start)
	for id, p := range mapCfg.Paths {
		c.Paths[id] = PathState{
			ID:                id,
			Status:            PathOpen,
			SurveillanceLevel: 0,
			TempOpenTurns:     0,
			BlockedByUnitID:   "",
		}
		_ = p
	}

	return c
}

// rbMu protects the RingBearer authoritative state (separate from main cache).
var rbMu sync.RWMutex
var rbStateStore RingBearerState

// InitRingBearerState seeds the RingBearer state at game start.
func (c *WorldStateCache) InitRingBearerState(startRegion string) {
	rbMu.Lock()
	defer rbMu.Unlock()
	rbStateStore = RingBearerState{
		TrueRegion: startRegion,
		Exposed:    false,
		Route:      nil,
		RouteIdx:   0,
	}
	c.mu.Lock()
	c.LightView.RingBearerRegion = startRegion
	c.mu.Unlock()
}

// ResetFromConfig rebuilds the entire cache from static config (new game).
func (c *WorldStateCache) ResetFromConfig(gameCfg *config.GameConfig, mapCfg *config.MapConfig) {
	c.mu.Lock()
	defer c.mu.Unlock()
	rbID := RingBearerID(gameCfg)
	c.Turn = 1
	c.Session = SessionState{}
	c.RingBearerUnitID = rbID
	c.Units = make(map[string]UnitSnapshot)
	c.Regions = make(map[string]RegionState)
	c.Paths = make(map[string]PathState)
	c.LightView = LightSideView{}
	c.DarkView = DarkSideView{RingBearerRegion: ""}

	for id, cfg := range gameCfg.Units {
		c.Units[id] = UnitSnapshot{
			ID:            id,
			CurrentRegion: cfg.StartRegion,
			Strength:      cfg.Strength,
			Status:        StatusActive,
			Cooldown:      0,
		}
	}
	if rb, ok := c.Units[rbID]; ok {
		rb.CurrentRegion = ""
		c.Units[rbID] = rb
	}
	for id, r := range mapCfg.Regions {
		c.Regions[id] = RegionState{
			ID:           id,
			ControlledBy: Controller(r.StartControl),
			ThreatLevel:  r.StartThreat,
		}
	}
	for id := range mapCfg.Paths {
		c.Paths[id] = PathState{
			ID:                id,
			Status:            PathOpen,
			SurveillanceLevel: 0,
			TempOpenTurns:     0,
			BlockedByUnitID:   "",
		}
	}
	ResetCorruptPathMaiaDisabled()
}

// ResetCorruptPathMaiaDisabled clears corrupt-path Maia disable flags.
func ResetCorruptPathMaiaDisabled() {
	corruptPathMaiaDisabledMu.Lock()
	defer corruptPathMaiaDisabledMu.Unlock()
	corruptPathMaiaDisabledIDs = map[string]bool{}
}

// RingBearerSnapshot returns a copy of the Ring Bearer's authoritative state.
func (c *WorldStateCache) RingBearerSnapshot() RingBearerState {
	rbMu.RLock()
	defer rbMu.RUnlock()
	s := rbStateStore
	s.Route = copyRoute(rbStateStore.Route)
	return s
}

// Update writes a new snapshot into the cache (called after turn processing).
func (c *WorldStateCache) Update(snap WorldStateCache, rbState RingBearerState) {
	c.mu.Lock()
	c.Turn = snap.Turn + 1
	c.Session = snap.Session
	if snap.RingBearerUnitID != "" {
		c.RingBearerUnitID = snap.RingBearerUnitID
	}
	c.Units = snap.Units
	c.Regions = snap.Regions
	c.Paths = snap.Paths
	c.LightView = snap.LightView
	// DarkView.RingBearerRegion stays ""
	c.DarkView = DarkSideView{
		RingBearerRegion:   "",
		LastDetectedRegion: snap.DarkView.LastDetectedRegion,
		LastDetectedTurn:   snap.DarkView.LastDetectedTurn,
	}
	c.mu.Unlock()

	rbMu.Lock()
	rbStateStore = rbState
	rbMu.Unlock()
}

// IsInRingBearerRoute checks if a path ID is in the Ring Bearer's current route.
func (snap *WorldStateCache) IsInRingBearerRoute(rbState RingBearerState, pathID string) bool {
	for _, p := range rbState.Route {
		if p == pathID {
			return true
		}
	}
	return false
}

// Corrupt-path Maia disabled flag, set when Isengard falls.
var corruptPathMaiaDisabledMu sync.RWMutex
var corruptPathMaiaDisabledIDs = map[string]bool{}

// DisableCorruptPathMaias permanently disables all Maia units with CorruptPath
// ability (config.MaiaAbilityPaths non-empty) when Isengard falls.
// Config-driven: no hardcoded unit IDs.
func (snap WorldStateCache) DisableCorruptPathMaias(gameCfg *config.GameConfig) {
	corruptPathMaiaDisabledMu.Lock()
	defer corruptPathMaiaDisabledMu.Unlock()
	for id, cfg := range gameCfg.Units {
		if cfg.Maia && len(cfg.MaiaAbilityPaths) > 0 {
			corruptPathMaiaDisabledIDs[id] = true
		}
	}
}

// IsCorruptPathMaiaDisabled checks if a specific CorruptPath Maia has been
// permanently disabled.
func (snap WorldStateCache) IsCorruptPathMaiaDisabled(unitID string, gameCfg *config.GameConfig) bool {
	cfg, ok := gameCfg.Units[unitID]
	if !ok {
		return false
	}
	// Only applies to Maia units with configured CorruptPath targets.
	if !cfg.Maia || len(cfg.MaiaAbilityPaths) == 0 {
		return false
	}
	corruptPathMaiaDisabledMu.RLock()
	defer corruptPathMaiaDisabledMu.RUnlock()
	return corruptPathMaiaDisabledIDs[unitID]
}
