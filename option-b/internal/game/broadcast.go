// Package game — JSON payloads for game.broadcast (WorldStateSnapshot wire format).
package game

import (
	"encoding/json"
	"time"

	"ring-of-the-middle-earth/internal/config"
)

// BroadcastPath is a path row in the world snapshot JSON.
type BroadcastPath struct {
	ID                string `json:"id"`
	NewStatus         string `json:"newStatus"`
	SurveillanceLevel int    `json:"surveillanceLevel"`
	TempOpenTurns     int    `json:"tempOpenTurns"`
}

// MarshalWorldBroadcast builds the JSON body for game.broadcast SSE / cache replay.
func MarshalWorldBroadcast(
	snap WorldStateCache,
	rb RingBearerState,
	turn int,
	mapCfg *config.MapConfig,
	gameCfg *config.GameConfig,
) ([]byte, error) {
	var units []UnitPublic
	for id, u := range snap.Units {
		cfg := gameCfg.Units[id]
		pub := UnitPublic{
			ID:            id,
			Strength:      u.Strength,
			Status:        string(u.Status),
			Side:          string(cfg.Side),
			CurrentRegion: u.CurrentRegion,
		}
		if cfg.Class == "RingBearer" {
			pub.CurrentRegion = rb.TrueRegion
		}
		units = append(units, pub)
	}

	var regions []RegionSnapshot
	for id, r := range snap.Regions {
		rc := mapCfg.Regions[id]
		regions = append(regions, RegionSnapshot{
			ID:           id,
			ControlledBy: string(r.ControlledBy),
			ThreatLevel:  r.ThreatLevel,
			Fortified:    r.Fortified,
			Terrain:      rc.Terrain,
		})
	}

	var paths []BroadcastPath
	for id, p := range snap.Paths {
		paths = append(paths, BroadcastPath{
			ID:                id,
			NewStatus:         string(p.Status),
			SurveillanceLevel: p.SurveillanceLevel,
			TempOpenTurns:     p.TempOpenTurns,
		})
	}

	ws := struct {
		Turn                 int              `json:"turn"`
		Regions              []RegionSnapshot `json:"regions"`
		Units                []UnitPublic     `json:"units"`
		Paths                []BroadcastPath  `json:"paths"`
		RingBearerTrueRegion string           `json:"ringBearerTrueRegion,omitempty"`
		Timestamp            int64            `json:"timestamp"`
	}{
		Turn:                 turn,
		Regions:              regions,
		Units:                units,
		Paths:                paths,
		RingBearerTrueRegion: rb.TrueRegion,
		Timestamp:            time.Now().UnixMilli(),
	}
	return json.Marshal(ws)
}
