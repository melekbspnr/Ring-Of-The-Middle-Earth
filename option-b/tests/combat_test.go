// Package tests — combat_test.go
// Section 35: Required Unit Tests — 8 cases for combat formula (B3 criterion).
// Run with: go test ./tests/... -v -race
// No Docker or Kafka required.
package tests

import (
	"testing"

	"ring-of-the-middle-earth/internal/config"
	"ring-of-the-middle-earth/internal/engine"
	"ring-of-the-middle-earth/internal/game"
)

// ─── Helpers ──────────────────────────────────────────────────────────────────

func makeUnit(id string, strength int) game.UnitSnapshot {
	return game.UnitSnapshot{
		ID:       id,
		Strength: strength,
		Status:   game.StatusActive,
	}
}

func makeCfg(side config.Side, ignoresFortress, leadership bool, leadershipBonus int) config.UnitConfig {
	return config.UnitConfig{
		Side:            side,
		IgnoresFortress: ignoresFortress,
		Leadership:      leadership,
		LeadershipBonus: leadershipBonus,
		Indestructible:  false,
		Respawns:        false,
	}
}

func plainRegion() game.RegionState     { return game.RegionState{Fortified: false} }
func fortifiedRegion() game.RegionState { return game.RegionState{Fortified: true} }

// ─── Test Cases ───────────────────────────────────────────────────────────────

// Case 1: Attacker(5) vs Defender(5, PLAINS) → tie — attacker repelled
func TestCombat_TiePlains(t *testing.T) {
	attackers := []game.UnitSnapshot{makeUnit("a", 5)}
	aCfgs := []config.UnitConfig{makeCfg(config.SideShadow, false, false, 0)}
	defenders := []game.UnitSnapshot{makeUnit("d", 5)}
	dCfgs := []config.UnitConfig{makeCfg(config.SideFreePeoples, false, false, 0)}
	region := plainRegion()
	regionCfg := config.RegionConfig{Terrain: "PLAINS"}

	result := engine.ResolveAttack(attackers, aCfgs, defenders, dCfgs, region, regionCfg)

	if result.AttackerWon {
		t.Errorf("Case 1: expected attacker repelled (tie), got AttackerWon=true")
	}
	if result.AttackerPower != 5 || result.DefenderPower != 5 {
		t.Errorf("Case 1: expected 5v5, got %dv%d", result.AttackerPower, result.DefenderPower)
	}
}

// Case 2: Attacker(5) vs Defender(5, FORTRESS) → defender wins (5 vs 7)
func TestCombat_FortressTerrain(t *testing.T) {
	attackers := []game.UnitSnapshot{makeUnit("a", 5)}
	aCfgs := []config.UnitConfig{makeCfg(config.SideShadow, false, false, 0)}
	defenders := []game.UnitSnapshot{makeUnit("d", 5)}
	dCfgs := []config.UnitConfig{makeCfg(config.SideFreePeoples, false, false, 0)}
	region := plainRegion()
	regionCfg := config.RegionConfig{Terrain: "FORTRESS"}

	result := engine.ResolveAttack(attackers, aCfgs, defenders, dCfgs, region, regionCfg)

	if result.AttackerWon {
		t.Errorf("Case 2: expected defender wins (fortress +2), got AttackerWon=true")
	}
	if result.DefenderPower != 7 {
		t.Errorf("Case 2: expected DefenderPower=7 (5+2), got %d", result.DefenderPower)
	}
}

// Case 3: UrukHai(5, ignoresFortress) vs Defender(5, FORTRESS) → tie (5 vs 5)
// ignoresFortress skips terrain bonus but NOT fortification bonus
func TestCombat_UrukHaiIgnoresFortress(t *testing.T) {
	attackers := []game.UnitSnapshot{makeUnit("uruk-hai-legion", 5)}
	aCfgs := []config.UnitConfig{makeCfg(config.SideShadow, true, false, 0)} // ignoresFortress=true
	defenders := []game.UnitSnapshot{makeUnit("d", 5)}
	dCfgs := []config.UnitConfig{makeCfg(config.SideFreePeoples, false, false, 0)}
	region := plainRegion() // NOT fortified — only terrain bonus is skipped
	regionCfg := config.RegionConfig{Terrain: "FORTRESS"}

	result := engine.ResolveAttack(attackers, aCfgs, defenders, dCfgs, region, regionCfg)

	if result.AttackerWon {
		t.Errorf("Case 3: expected tie (5v5 with ignoresFortress), got AttackerWon=true")
	}
	if result.DefenderPower != 5 {
		t.Errorf("Case 3: expected DefenderPower=5 (terrain skipped), got %d", result.DefenderPower)
	}
}

// Case 4: UrukHai(5) vs Defender(5, FORTRESS, fortified) → defender wins (5 vs 7)
// fortification bonus still applies even when ignoresFortress=true
func TestCombat_UrukHaiVsFortified(t *testing.T) {
	attackers := []game.UnitSnapshot{makeUnit("uruk-hai-legion", 5)}
	aCfgs := []config.UnitConfig{makeCfg(config.SideShadow, true, false, 0)} // ignoresFortress=true
	defenders := []game.UnitSnapshot{makeUnit("gondor-army", 5)}
	dCfgs := []config.UnitConfig{makeCfg(config.SideFreePeoples, false, false, 0)}
	region := fortifiedRegion() // fortification bonus = +2
	regionCfg := config.RegionConfig{Terrain: "FORTRESS"}

	result := engine.ResolveAttack(attackers, aCfgs, defenders, dCfgs, region, regionCfg)

	if result.AttackerWon {
		t.Errorf("Case 4: expected defender wins (fortified, +2), got AttackerWon=true")
	}
	// ignoresFortress skips terrain (+2) but fortification (+2) still applies
	// DefenderPower = 5 + 0 (terrain skipped) + 2 (fortification) = 7
	if result.DefenderPower != 7 {
		t.Errorf("Case 4: expected DefenderPower=7 (fortification applies), got %d", result.DefenderPower)
	}
}

// Case 5: Leadership bonus applied to co-located allies
// Aragorn(5, leader +1) + Gimli(3) attack → Gimli effective=4; 5+4=9 vs 5
func TestCombat_LeadershipBonus(t *testing.T) {
	aragorn := makeUnit("aragorn", 5)
	gimli := makeUnit("gimli", 3)
	attackers := []game.UnitSnapshot{aragorn, gimli}
	aCfgs := []config.UnitConfig{
		{Side: config.SideFreePeoples, Leadership: true, LeadershipBonus: 1},  // Aragorn
		{Side: config.SideFreePeoples, Leadership: false, LeadershipBonus: 0}, // Gimli
	}
	defenders := []game.UnitSnapshot{makeUnit("uruk-hai-legion", 5)}
	dCfgs := []config.UnitConfig{makeCfg(config.SideShadow, false, false, 0)}
	region := plainRegion()
	regionCfg := config.RegionConfig{Terrain: "PLAINS"}

	result := engine.ResolveAttack(attackers, aCfgs, defenders, dCfgs, region, regionCfg)

	if !result.AttackerWon {
		t.Errorf("Case 5: expected attacker wins (9 vs 5), got AttackerWon=false")
	}
	// Aragorn=5 (no self-bonus), Gimli=3+1=4 → total=9
	if result.AttackerPower != 9 {
		t.Errorf("Case 5: expected AttackerPower=9 (Aragorn5+Gimli4), got %d", result.AttackerPower)
	}
}

// Case 6: Indestructible unit takes fatal damage → strength floors at 1, stays ACTIVE
func TestCombat_IndestructibleFloorsAtOne(t *testing.T) {
	snap := makeUnit("witch-king", 5)
	cfg := config.UnitConfig{Indestructible: true}

	// Deal damage that would normally destroy (damage=10 >> strength=5)
	updated := engine.ApplyDamage(snap, cfg, 10)

	if updated.Status != game.StatusActive {
		t.Errorf("Case 6: expected ACTIVE, got %s", updated.Status)
	}
	if updated.Strength != 1 {
		t.Errorf("Case 6: expected Strength=1 (floor), got %d", updated.Strength)
	}
}

// Case 7: Respawn unit takes fatal damage → RESPAWNING with configured respawnTurns
func TestCombat_RespawnMechanics(t *testing.T) {
	snap := makeUnit("gondor-cavalry", 3)
	cfg := config.UnitConfig{
		Respawns:     true,
		RespawnTurns: 3,
	}

	// Deal lethal damage
	updated := engine.ApplyDamage(snap, cfg, 5)

	if updated.Status != game.StatusRespawning {
		t.Errorf("Case 7: expected RESPAWNING, got %s", updated.Status)
	}
	if updated.Strength != 0 {
		t.Errorf("Case 7: expected Strength=0, got %d", updated.Strength)
	}
	if updated.RespawnTurns != 3 {
		t.Errorf("Case 7: expected RespawnTurns=3 (from config), got %d", updated.RespawnTurns)
	}
}

// Case 8: Mountains terrain bonus (+1 defender power)
func TestCombat_MountainsTerrain(t *testing.T) {
	attackers := []game.UnitSnapshot{makeUnit("a", 5)}
	aCfgs := []config.UnitConfig{makeCfg(config.SideShadow, false, false, 0)}
	defenders := []game.UnitSnapshot{makeUnit("d", 5)}
	dCfgs := []config.UnitConfig{makeCfg(config.SideFreePeoples, false, false, 0)}
	region := plainRegion()
	regionCfg := config.RegionConfig{Terrain: "MOUNTAINS"}

	result := engine.ResolveAttack(attackers, aCfgs, defenders, dCfgs, region, regionCfg)

	if result.AttackerWon {
		t.Errorf("Case 8: expected defender wins (mountains +1), got AttackerWon=true")
	}
	// Defender power = 5 (strength) + 1 (mountains terrain) = 6
	if result.DefenderPower != 6 {
		t.Errorf("Case 8: expected DefenderPower=6 (5+1 mountains), got %d", result.DefenderPower)
	}
	if result.AttackerPower != 5 {
		t.Errorf("Case 8: expected AttackerPower=5, got %d", result.AttackerPower)
	}
}
