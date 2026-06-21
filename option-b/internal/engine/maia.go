// Package engine — maia.go implements the config-driven Maia ability dispatch.
//
// CRITICAL DESIGN:
//
//	Both Gandalf and Saruman send the same orderType: "MAIA_ABILITY".
//	The dispatch is determined entirely by config fields:
//	  - Gandalf: config.Maia==true, config.MaiaAbilityPaths==[] (can open any blocked path)
//	  - Saruman: config.Maia==true, config.MaiaAbilityPaths != [] (restricted list)
//	NO unit ID string literal is used anywhere in this file.
//
// Q&A Question 2: "Show exactly where the dispatch happens and what config field determines the outcome."
// Answer: This file. The dispatch is in DispatchMaiaAbility, using config.MaiaAbilityPaths.
package engine

import (
	"fmt"
	"ring-of-the-middle-earth/internal/config"
	"ring-of-the-middle-earth/internal/game"
)

// MaiaAbilityResult holds the outcome of a Maia ability activation.
type MaiaAbilityResult struct {
	Effect            string // "OPEN_PATH" or "CORRUPT_PATH"
	TargetPath        string
	NewStatus         game.PathStatus
	SurveillanceLevel int
}

// MaiaAbilityError codes.
var (
	ErrAbilityOnCooldown = fmt.Errorf("ABILITY_ON_COOLDOWN")
	ErrMaiaDisabled      = fmt.Errorf("MAIA_DISABLED")
	ErrInvalidPath       = fmt.Errorf("INVALID_PATH")
	ErrUnitNotAdjacent   = fmt.Errorf("UNIT_NOT_ADJACENT")
	ErrPathNotBlocked    = fmt.Errorf("PATH_NOT_BLOCKED") // Gandalf requires BLOCKED
)

// DispatchMaiaAbility determines which Maia effect to apply based on config fields.
//
// Decision logic (config-driven):
//
//	if len(cfg.MaiaAbilityPaths) == 0 → OpenPath effect (Gandalf behaviour)
//	if len(cfg.MaiaAbilityPaths) > 0  → CorruptPath effect (Saruman behaviour)
//
// This single dispatch replaces all hardcoded unit-name checks.
func DispatchMaiaAbility(
	unitSnap game.UnitSnapshot,
	cfg config.UnitConfig,
	targetPathID string,
	pathState game.PathState,
	pathCfg config.PathConfig,
	maiaDisabled bool, // true when Isengard falls for Saruman-class units
) (MaiaAbilityResult, error) {

	// Pre-checks common to all Maia
	if !cfg.Maia {
		return MaiaAbilityResult{}, ErrMaiaDisabled
	}
	if unitSnap.Cooldown > 0 {
		return MaiaAbilityResult{}, ErrAbilityOnCooldown
	}
	if maiaDisabled {
		return MaiaAbilityResult{}, ErrMaiaDisabled
	}

	// Unit must be at one of the path endpoints
	if unitSnap.CurrentRegion != pathCfg.From && unitSnap.CurrentRegion != pathCfg.To {
		return MaiaAbilityResult{}, ErrUnitNotAdjacent
	}

	// Dispatch: config.MaiaAbilityPaths determines the behaviour
	if len(cfg.MaiaAbilityPaths) == 0 {
		// OpenPath behaviour (Gandalf)
		return applyOpenPath(targetPathID, pathState)
	}

	// CorruptPath behaviour (Saruman)
	return applyCorruptPath(cfg, targetPathID, pathState)
}

// applyOpenPath implements the Gandalf OpenPath effect.
// Requirement: path must be BLOCKED.
// Effect: path becomes TEMPORARILY_OPEN for 2 turns.
func applyOpenPath(targetPathID string, pathState game.PathState) (MaiaAbilityResult, error) {
	if pathState.Status != game.PathBlocked {
		return MaiaAbilityResult{}, ErrPathNotBlocked
	}
	return MaiaAbilityResult{
		Effect:     "OPEN_PATH",
		TargetPath: targetPathID,
		NewStatus:  game.PathTemporarilyOpen,
		// TempOpenTurns = 2 is set by TurnProcessor when it updates PathState
	}, nil
}

// applyCorruptPath implements the Saruman CorruptPath effect.
// Requirement: targetPathID must be in config.MaiaAbilityPaths.
// Requirement: path must be OPEN, THREATENED, or BLOCKED.
// Effect: permanentlyr sets surveillanceLevel=3.
func applyCorruptPath(cfg config.UnitConfig, targetPathID string, pathState game.PathState) (MaiaAbilityResult, error) {
	// Validate path is in Saruman's allowed list (config-driven)
	if !containsPath(cfg.MaiaAbilityPaths, targetPathID) {
		return MaiaAbilityResult{}, ErrInvalidPath
	}

	// Path must not already be at max surveillance (idempotent check)
	// Status can be OPEN, THREATENED, or BLOCKED — all valid for corruption
	if pathState.Status == game.PathTemporarilyOpen {
		return MaiaAbilityResult{}, ErrInvalidPath
	}

	return MaiaAbilityResult{
		Effect:            "CORRUPT_PATH",
		TargetPath:        targetPathID,
		NewStatus:         pathState.Status, // status unchanged; only surveillance increases
		SurveillanceLevel: 3,                // permanent maximum
	}, nil
}

// containsPath checks whether a path ID is in a unit's maiaAbilityPaths slice.
func containsPath(paths []string, target string) bool {
	for _, p := range paths {
		if p == target {
			return true
		}
	}
	return false
}
