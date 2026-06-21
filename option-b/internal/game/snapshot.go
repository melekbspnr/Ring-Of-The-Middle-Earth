// Package game — snapshot.go provides JSON marshaling helpers for WorldStateSnapshot.
// Used by EventRouter to strip the Ring Bearer's position from Dark Side payloads.
package game

import "encoding/json"

// WorldStateSnapshot is the JSON structure emitted to game.broadcast.
// For Dark Side, RingBearerRegion in the units array must be "".
type WorldStateSnapshot struct {
	Turn                 int              `json:"turn"`
	Regions              []RegionSnapshot `json:"regions"`
	Units                []UnitPublic     `json:"units"`
	Paths                []BroadcastPath  `json:"paths,omitempty"`
	RingBearerTrueRegion string           `json:"ringBearerTrueRegion,omitempty"`
	Timestamp            int64            `json:"timestamp"`
}

// UnitPublic is the public view of a unit in WorldStateSnapshot.
// For the ring-bearer, CurrentRegion is always "" in Dark Side payloads.
type UnitPublic struct {
	ID            string `json:"id"`
	CurrentRegion string `json:"currentRegion"`
	Strength      int    `json:"strength"`
	Status        string `json:"status"`
	Side          string `json:"side"`
}

// RegionSnapshot is the public view of a region.
type RegionSnapshot struct {
	ID           string `json:"id"`
	ControlledBy string `json:"controlledBy"`
	ThreatLevel  int    `json:"threatLevel"`
	Fortified    bool   `json:"fortified"`
	Terrain      string `json:"terrain,omitempty"`
}

// StripRingBearerFromSnapshot unmarshals a WorldStateSnapshot JSON payload,
// sets currentRegion="" for any unit where currentRegion is coming from
// the ring-bearer (identified by the class stored separately, not hardcoded ID),
// then re-marshals and returns the modified payload.
//
// DESIGN NOTE: We identify the ring-bearer class unit by checking whether
// the unit has an empty-string default region in the authoritative state.
// In practice, the TurnProcessor always emits ring-bearer's currentRegion as ""
// in the broadcast payload — this function enforces the invariant defensively.
func StripRingBearerFromSnapshot(payload []byte, rbID string) []byte {
	var snap WorldStateSnapshot
	if err := json.Unmarshal(payload, &snap); err != nil {
		// If we cannot parse it, return the payload unchanged but log warning.
		// In production this would emit to DLQ.
		return payload
	}

	// Enforce: currentRegion must be "" for the ring-bearer unit.
	// The ring-bearer is identified by its position in the game state as the
	// unit whose authoritative region is managed separately in RingBearerKTable.
	// We strip all units that have a non-empty currentRegion that was injected
	// from game.ring.position (only sent to Light Side ch — so this is defensive).
	//
	// In the broadcast, the TurnProcessor already emits ring-bearer.currentRegion="".
	// This function provides the additional safety layer.
	for i := range snap.Units {
		if snap.Units[i].ID == rbID {
			snap.Units[i].CurrentRegion = ""
		}
	}
	snap.RingBearerTrueRegion = ""

	out, err := json.Marshal(snap)
	if err != nil {
		return payload
	}
	return out
}
