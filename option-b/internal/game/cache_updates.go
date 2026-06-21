// Package game — cache_updates.go provides fine-grained cache mutation methods.
// Called by CacheManager. These are the only write paths to the cache
// outside of TurnProcessor.Update().
package game

func (c *WorldStateCache) ensureMaps() {
	if c.Units == nil {
		c.Units = make(map[string]UnitSnapshot)
	}
	if c.Regions == nil {
		c.Regions = make(map[string]RegionState)
	}
	if c.Paths == nil {
		c.Paths = make(map[string]PathState)
	}
}

// ApplySnapshot replaces cache state with a full WorldStateSnapshot.
func (c *WorldStateCache) ApplySnapshot(snap WorldStateSnapshot) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.ensureMaps()
	c.Turn = snap.Turn
	for _, u := range snap.Units {
		existing := c.Units[u.ID]
		if c.isRingBearerIDUnlocked(u.ID) {
			existing.CurrentRegion = ""
		} else {
			existing.CurrentRegion = u.CurrentRegion
		}
		existing.Strength = u.Strength
		existing.Status = UnitStatus(u.Status)
		c.Units[u.ID] = existing
	}
	for _, r := range snap.Regions {
		existing := c.Regions[r.ID]
		existing.ControlledBy = Controller(r.ControlledBy)
		existing.ThreatLevel = r.ThreatLevel
		existing.Fortified = r.Fortified
		c.Regions[r.ID] = existing
	}
	for _, p := range snap.Paths {
		existing := c.Paths[p.ID]
		existing.Status = PathStatus(p.NewStatus)
		existing.SurveillanceLevel = p.SurveillanceLevel
		existing.TempOpenTurns = p.TempOpenTurns
		c.Paths[p.ID] = existing
	}
	if snap.RingBearerTrueRegion != "" {
		c.LightView.RingBearerRegion = snap.RingBearerTrueRegion
	}
}

// UpdateUnit applies a UnitMoved event to the cache.
func (c *WorldStateCache) UpdateUnit(unitID, newRegion string, strength int, status UnitStatus, cooldown *int) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.ensureMaps()
	u := c.Units[unitID]
	// Ring Bearer's public region is always "" — enforced here
	if c.isRingBearerIDUnlocked(unitID) {
		u.CurrentRegion = ""
	} else {
		u.CurrentRegion = newRegion
	}
	if strength > 0 {
		u.Strength = strength
	}
	if status != "" {
		u.Status = status
	}
	if cooldown != nil {
		u.Cooldown = *cooldown
	}
	c.Units[unitID] = u
}

// UpdateRegionControl applies a RegionControlChanged event.
func (c *WorldStateCache) UpdateRegionControl(regionID string, controller Controller) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.ensureMaps()
	r := c.Regions[regionID]
	r.ControlledBy = controller
	c.Regions[regionID] = r
}

// UpdatePath applies a PathStatusChanged event.
func (c *WorldStateCache) UpdatePath(pathID string, status PathStatus, surveillance, tempOpenTurns int) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.ensureMaps()
	p := c.Paths[pathID]
	p.Status = status
	p.SurveillanceLevel = surveillance
	p.TempOpenTurns = tempOpenTurns
	c.Paths[pathID] = p
}

// UpdateLightView updates the Light Side's Ring Bearer position view.
// Called only from game.ring.position events — never from Dark Side channels.
func (c *WorldStateCache) UpdateLightView(trueRegion string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.LightView.RingBearerRegion = trueRegion
}

// UpdateDarkView updates the Dark Side's last-detected position.
// DarkView.RingBearerRegion is ALWAYS "" — never set here.
func (c *WorldStateCache) UpdateDarkView(lastDetectedRegion string, turn int) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.DarkView.RingBearerRegion = "" // enforced: always empty
	c.DarkView.LastDetectedRegion = lastDetectedRegion
	c.DarkView.LastDetectedTurn = turn
}

// SetTurn updates the current turn counter.
func (c *WorldStateCache) SetTurn(turn int) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.Turn = turn
}

// UpdateSession applies the compacted game.session state.
func (c *WorldStateCache) UpdateSession(
	turn int,
	leaderID *string,
	leaderHeartbeatTs *int64,
	epoch *int64,
	gameOver *bool,
	gameOverWinner *string,
	gameOverCause *string,
	gameOverTurn *int,
) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.Turn = turn
	if leaderID != nil {
		c.Session.LeaderID = *leaderID
	}
	if leaderHeartbeatTs != nil {
		c.Session.LeaderHeartbeatTs = *leaderHeartbeatTs
	}
	if epoch != nil {
		c.Session.Epoch = *epoch
	}
	if gameOver != nil {
		c.Session.GameOver = *gameOver
	}
	if gameOverWinner != nil {
		c.Session.GameOverWinner = *gameOverWinner
	}
	if gameOverCause != nil {
		c.Session.GameOverCause = *gameOverCause
	}
	if gameOverTurn != nil {
		c.Session.GameOverTurn = *gameOverTurn
	}
}
