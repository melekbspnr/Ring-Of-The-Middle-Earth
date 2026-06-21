// Package engine — detection.go implements the Ring Bearer exposure formula.
// Section 3.6 and Section 6 Step 12.
// CRITICAL: No unit ID string literals here.
// Sauron's amplifier is applied via config.Maia + config.StartRegion == "mordor".
package engine

import (
	"ring-of-the-middle-earth/internal/config"
	"ring-of-the-middle-earth/internal/game"
)

// DetectionResult holds the outcome of a turn's detection step.
type DetectionResult struct {
	Exposed          bool
	DetectedByUnitID string // which Nazgul triggered detection (if any)
	TrueRegion       string // only valid if Exposed == true
}

// RunDetection executes the detection formula from Section 3.6.
//
// Parameters:
//
//	turn           — current turn number
//	hiddenUntil    — turns during which detection is suppressed (from config)
//	rbState        — Ring Bearer's authoritative state (true region)
//	units          — all unit snapshots
//	unitCfgs       — map of unitID → UnitConfig
//	graph          — map graph for BFS distance
//
// Returns DetectionResult. If suppressed (turn <= hiddenUntil), Exposed is always false.
//
// Sauron amplifier: if a unit with config.Maia==true AND config.StartRegion=="mordor"
// is ACTIVE and its currentRegion=="mordor", all Nazgul gain +1 detectionRange.
// This is determined entirely from config fields — no unit ID string used.
func RunDetection(
	turn int,
	hiddenUntil int,
	rbState game.RingBearerState,
	units map[string]game.UnitSnapshot,
	unitCfgs map[string]config.UnitConfig,
	graph *game.Graph,
) DetectionResult {

	// Step 12: suppressed for first hiddenUntil turns
	if turn <= hiddenUntil {
		return DetectionResult{Exposed: false}
	}

	// Determine if Sauron's Eye is active.
	// Config-driven: unit.Maia==true AND unit.StartRegion=="mordor" AND unit is ACTIVE in mordor.
	sauronActive := isSauronEyeActive(units, unitCfgs)

	// Check each Nazgul (class == "Nazgul") for detection.
	for id, snap := range units {
		if snap.Status != game.StatusActive {
			continue
		}
		cfg, ok := unitCfgs[id]
		if !ok {
			continue
		}
		// Only Nazgul have detectionRange > 0 by design (config-driven)
		if cfg.DetectionRange <= 0 {
			continue
		}

		effectiveRange := cfg.DetectionRange
		if sauronActive {
			effectiveRange++ // Eye of Sauron +1
		}

		dist := graph.BFSDistance(snap.CurrentRegion, rbState.TrueRegion)
		if dist >= 0 && dist <= effectiveRange {
			return DetectionResult{
				Exposed:          true,
				DetectedByUnitID: id,
				TrueRegion:       rbState.TrueRegion,
			}
		}
	}

	return DetectionResult{Exposed: false}
}

// isSauronEyeActive checks whether the passive Eye of Sauron effect is in play.
// A unit has the Sauron passive if:
//   - config.Maia == true
//   - config.StartRegion == "mordor"  (only Sauron starts in Mordor; config-driven, no ID check)
//   - unit is ACTIVE and currently in "mordor"
func isSauronEyeActive(
	units map[string]game.UnitSnapshot,
	unitCfgs map[string]config.UnitConfig,
) bool {
	for id, snap := range units {
		if snap.Status != game.StatusActive {
			continue
		}
		cfg, ok := unitCfgs[id]
		if !ok {
			continue
		}
		// Config-driven Sauron identification: Maia unit that starts in mordor
		if cfg.Maia && cfg.StartRegion == "mordor" && snap.CurrentRegion == "mordor" {
			return true
		}
	}
	return false
}

// IsExposedBySurveillance checks whether the Ring Bearer is exposed
// by crossing a path with surveillanceLevel >= 1 this turn.
// Called from TurnProcessor Step 7 when Ring Bearer auto-advances.
func IsExposedBySurveillance(pathState game.PathState, turn int, hiddenUntil int) bool {
	if turn <= hiddenUntil {
		return false
	}
	return pathState.SurveillanceLevel >= 1
}
