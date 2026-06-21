// Package api — cache_manager.go owns the WorldStateCache update loop.
// Runs as a dedicated goroutine. Receives events from cacheUpdateCh and
// applies them to the cache. Sends value copies to workers — never pointers.
package api

import (
	"encoding/json"
	"log"

	"ring-of-the-middle-earth/internal/game"
)

// RunCacheManager reads cache update events and applies them to the WorldStateCache.
// This is the only goroutine that writes to the cache (except TurnProcessor via Update()).
func RunCacheManager(cache *game.WorldStateCache, updateCh <-chan Event, doneCh <-chan struct{}) {
	for {
		select {
		case <-doneCh:
			return
		case ev, ok := <-updateCh:
			if !ok {
				return
			}
			applyEvent(cache, ev)
		}
	}
}

// applyEvent updates the cache based on the incoming Kafka event topic.
func applyEvent(cache *game.WorldStateCache, ev Event) {
	switch ev.Topic {
	case "game.broadcast":
		applyWorldStateSnapshot(cache, ev.Payload)

	case "game.events.unit":
		applyUnitEvent(cache, ev.Payload)

	case "game.events.region":
		applyRegionEvent(cache, ev.Payload)

	case "game.events.path":
		applyPathEvent(cache, ev.Payload)

	case "game.ring.position":
		applyRingBearerMoved(cache, ev.Payload)

	case "game.ring.detection":
		applyRingBearerDetected(cache, ev.Payload)

	case "game.session":
		applySessionEvent(cache, ev.Payload)
	}
}

// ─── Event appliers ───────────────────────────────────────────────────────────

func applyWorldStateSnapshot(cache *game.WorldStateCache, payload []byte) {
	var maybeGameOver struct {
		Winner string `json:"winner"`
		Cause  string `json:"cause"`
	}
	if err := json.Unmarshal(payload, &maybeGameOver); err == nil && maybeGameOver.Winner != "" && maybeGameOver.Cause != "" {
		return
	}
	var snap game.WorldStateSnapshot
	if err := json.Unmarshal(payload, &snap); err != nil {
		log.Printf("[cache] bad WorldStateSnapshot: %v", err)
		return
	}
	cache.ApplySnapshot(snap)
}

func applyUnitEvent(cache *game.WorldStateCache, payload []byte) {
	var ev struct {
		UnitID   string `json:"unitId"`
		From     string `json:"from"`
		To       string `json:"to"`
		Strength int    `json:"strength"`
		Status   string `json:"status"`
		Cooldown *int   `json:"cooldown,omitempty"`
		Turn     int    `json:"turn"`
	}
	if err := json.Unmarshal(payload, &ev); err != nil {
		log.Printf("[cache] bad UnitMoved event: %v", err)
		return
	}
	cache.UpdateUnit(ev.UnitID, ev.To, ev.Strength, game.UnitStatus(ev.Status), ev.Cooldown)
}

func applyRegionEvent(cache *game.WorldStateCache, payload []byte) {
	var ev struct {
		RegionID      string `json:"regionId"`
		NewController string `json:"newController"`
		Turn          int    `json:"turn"`
	}
	if err := json.Unmarshal(payload, &ev); err != nil {
		log.Printf("[cache] bad RegionControlChanged event: %v", err)
		return
	}
	cache.UpdateRegionControl(ev.RegionID, game.Controller(ev.NewController))
}

func applyPathEvent(cache *game.WorldStateCache, payload []byte) {
	var ev struct {
		PathID            string `json:"pathId"`
		NewStatus         string `json:"newStatus"`
		SurveillanceLevel int    `json:"surveillanceLevel"`
		TempOpenTurns     int    `json:"tempOpenTurns"`
		Turn              int    `json:"turn"`
	}
	if err := json.Unmarshal(payload, &ev); err != nil {
		log.Printf("[cache] bad PathStatusChanged event: %v", err)
		return
	}
	cache.UpdatePath(ev.PathID, game.PathStatus(ev.NewStatus), ev.SurveillanceLevel, ev.TempOpenTurns)
}

func applyRingBearerMoved(cache *game.WorldStateCache, payload []byte) {
	var ev struct {
		TrueRegion string `json:"trueRegion"`
		Turn       int    `json:"turn"`
	}
	if err := json.Unmarshal(payload, &ev); err != nil {
		log.Printf("[cache] bad RingBearerMoved event: %v", err)
		return
	}
	// Update Light Side view only — Dark Side never sees this
	cache.UpdateLightView(ev.TrueRegion)
}

func applyRingBearerDetected(cache *game.WorldStateCache, payload []byte) {
	var ev struct {
		RegionID string `json:"regionId"`
		Turn     int    `json:"turn"`
	}
	if err := json.Unmarshal(payload, &ev); err != nil {
		log.Printf("[cache] bad RingBearerDetected event: %v", err)
		return
	}
	// Update Dark Side view with last known detection (not true region)
	cache.UpdateDarkView(ev.RegionID, ev.Turn)
}

func applySessionEvent(cache *game.WorldStateCache, payload []byte) {
	var ev struct {
		Turn              int     `json:"turn"`
		LeaderID          *string `json:"leaderId,omitempty"`
		LeaderHeartbeatTs *int64  `json:"leaderHeartbeatTs,omitempty"`
		Epoch             *int64  `json:"epoch,omitempty"`
		GameOver          *bool   `json:"gameOver,omitempty"`
		GameOverWinner    *string `json:"gameOverWinner,omitempty"`
		GameOverCause     *string `json:"gameOverCause,omitempty"`
		GameOverTurn      *int    `json:"gameOverTurn,omitempty"`
	}
	if err := json.Unmarshal(payload, &ev); err != nil {
		return
	}
	cache.UpdateSession(
		ev.Turn,
		ev.LeaderID,
		ev.LeaderHeartbeatTs,
		ev.Epoch,
		ev.GameOver,
		ev.GameOverWinner,
		ev.GameOverCause,
		ev.GameOverTurn,
	)
}
