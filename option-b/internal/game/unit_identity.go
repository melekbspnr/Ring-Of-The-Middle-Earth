package game

import "ring-of-the-middle-earth/internal/config"

// RingBearerID resolves the Ring Bearer unit ID from config instead of
// hardcoding it in game logic.
func RingBearerID(gameCfg *config.GameConfig) string {
	if gameCfg == nil {
		return ""
	}
	for id, cfg := range gameCfg.Units {
		if cfg.Class == "RingBearer" {
			return id
		}
	}
	return ""
}

// RingBearerStartRegion resolves the Ring Bearer's configured start region.
func RingBearerStartRegion(gameCfg *config.GameConfig) string {
	if gameCfg == nil {
		return ""
	}
	rbID := RingBearerID(gameCfg)
	if cfg, ok := gameCfg.Units[rbID]; ok {
		return cfg.StartRegion
	}
	return ""
}

// IsRingBearerID checks whether the provided unit ID is the configured
// Ring Bearer for this cache snapshot.
func (c *WorldStateCache) IsRingBearerID(unitID string) bool {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.isRingBearerIDUnlocked(unitID)
}

func (c *WorldStateCache) isRingBearerIDUnlocked(unitID string) bool {
	if unitID == "" {
		return false
	}
	if c.RingBearerUnitID != "" {
		return c.RingBearerUnitID == unitID
	}
	return false
}
