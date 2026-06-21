// Package tests — maia_test.go
// B5 criterion: Maia dispatch: same order type → different effect by config.
// Run with: go test ./tests/... -v -race
package tests

import (
	"testing"

	"ring-of-the-middle-earth/internal/config"
	"ring-of-the-middle-earth/internal/engine"
	"ring-of-the-middle-earth/internal/game"
)

// Case 1: Gandalf (MaiaAbilityPaths=[]) on a BLOCKED path → OPEN_PATH effect
func TestMaia_GandalfOpenPath(t *testing.T) {
	unitSnap := game.UnitSnapshot{
		ID:            "gandalf",
		CurrentRegion: "bree",
		Cooldown:      0,
		Status:        game.StatusActive,
	}
	cfg := config.UnitConfig{
		Maia:             true,
		MaiaAbilityPaths: nil, // empty → Gandalf behaviour (open any blocked path)
	}
	pathState := game.PathState{Status: game.PathBlocked}
	pathCfg := config.PathConfig{From: "bree", To: "weathertop"}

	result, err := engine.DispatchMaiaAbility(unitSnap, cfg, "bree-to-weathertop", pathState, pathCfg, false)
	if err != nil {
		t.Fatalf("Case 1: unexpected error: %v", err)
	}
	if result.Effect != "OPEN_PATH" {
		t.Errorf("Case 1: expected OPEN_PATH, got %s", result.Effect)
	}
}

// Case 2: Saruman (MaiaAbilityPaths=["fords-of-isen-to-edoras"]) on allowed path → CORRUPT_PATH
func TestMaia_SarumanCorruptPath(t *testing.T) {
	unitSnap := game.UnitSnapshot{
		ID:            "saruman",
		CurrentRegion: "fords-of-isen",
		Cooldown:      0,
		Status:        game.StatusActive,
	}
	cfg := config.UnitConfig{
		Maia:             true,
		MaiaAbilityPaths: []string{"fords-of-isen-to-edoras"}, // non-empty → Saruman behaviour
	}
	pathState := game.PathState{Status: game.PathOpen}
	pathCfg := config.PathConfig{From: "fords-of-isen", To: "edoras"}

	result, err := engine.DispatchMaiaAbility(unitSnap, cfg, "fords-of-isen-to-edoras", pathState, pathCfg, false)
	if err != nil {
		t.Fatalf("Case 2: unexpected error: %v", err)
	}
	if result.Effect != "CORRUPT_PATH" {
		t.Errorf("Case 2: expected CORRUPT_PATH, got %s", result.Effect)
	}
}

// Case 3: Same order type, different config → different effect
// Gandalf on OPEN path → error (PathNotBlocked)
func TestMaia_GandalfRequiresBlockedPath(t *testing.T) {
	unitSnap := game.UnitSnapshot{
		ID:            "gandalf",
		CurrentRegion: "bree",
		Cooldown:      0,
	}
	cfg := config.UnitConfig{
		Maia:             true,
		MaiaAbilityPaths: nil, // Gandalf
	}
	pathState := game.PathState{Status: game.PathOpen} // NOT blocked
	pathCfg := config.PathConfig{From: "bree", To: "weathertop"}

	_, err := engine.DispatchMaiaAbility(unitSnap, cfg, "bree-to-weathertop", pathState, pathCfg, false)
	if err != engine.ErrPathNotBlocked {
		t.Errorf("Case 3: expected ErrPathNotBlocked, got %v", err)
	}
}

// Case 4: Maia on cooldown → ABILITY_ON_COOLDOWN
func TestMaia_CooldownReject(t *testing.T) {
	unitSnap := game.UnitSnapshot{
		ID:            "gandalf",
		CurrentRegion: "bree",
		Cooldown:      2, // on cooldown
	}
	cfg := config.UnitConfig{
		Maia:             true,
		MaiaAbilityPaths: nil,
	}
	pathState := game.PathState{Status: game.PathBlocked}
	pathCfg := config.PathConfig{From: "bree", To: "weathertop"}

	_, err := engine.DispatchMaiaAbility(unitSnap, cfg, "bree-to-weathertop", pathState, pathCfg, false)
	if err != engine.ErrAbilityOnCooldown {
		t.Errorf("Case 4: expected ErrAbilityOnCooldown, got %v", err)
	}
}

// Case 5: Maia disabled (Isengard fell) → MAIA_DISABLED
func TestMaia_DisabledAfterIsengardFalls(t *testing.T) {
	unitSnap := game.UnitSnapshot{
		ID:            "saruman",
		CurrentRegion: "isengard",
		Cooldown:      0,
	}
	cfg := config.UnitConfig{
		Maia:             true,
		MaiaAbilityPaths: []string{"fords-of-isen-to-edoras"},
	}
	pathState := game.PathState{Status: game.PathOpen}
	pathCfg := config.PathConfig{From: "fords-of-isen", To: "edoras"}

	_, err := engine.DispatchMaiaAbility(unitSnap, cfg, "fords-of-isen-to-edoras", pathState, pathCfg, true) // maiaDisabled=true
	if err != engine.ErrMaiaDisabled {
		t.Errorf("Case 5: expected ErrMaiaDisabled, got %v", err)
	}
}
