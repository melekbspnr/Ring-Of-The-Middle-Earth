// Package engine — combat.go implements the config-driven combat formula.
// CRITICAL: No unit ID string literals appear here.
// All modifiers (ignoresFortress, leadership, indestructible) are read from UnitConfig.
package engine

import (
	"ring-of-the-middle-earth/internal/config"
	"ring-of-the-middle-earth/internal/game"
)

// CombatResult holds the outcome of a battle.
type CombatResult struct {
	AttackerWon   bool
	Damage        int // damage dealt to defender if attacker won
	AttackerPower int
	DefenderPower int
}

// TerrainBonus returns the defensive terrain bonus for a region's terrain type.
// FORTRESS → +2, MOUNTAINS → +1, others → 0.
func TerrainBonus(terrain string) int {
	switch terrain {
	case "FORTRESS":
		return 2
	case "MOUNTAINS":
		return 1
	default:
		return 0
	}
}

// ResolveAttack executes the combat formula from Section 4.
//
// Parameters:
//
//	attackers      — list of attacking unit snapshots
//	attackerCfgs   — corresponding UnitConfig for each attacker (same index)
//	defenders      — list of defending unit snapshots
//	defenderCfgs   — corresponding UnitConfig for each defender (same index)
//	defenderRegion — the region being contested (for terrain and fortification)
//
// All behaviour is driven by config fields — no unit ID checks.
func ResolveAttack(
	attackers []game.UnitSnapshot,
	attackerCfgs []config.UnitConfig,
	defenders []game.UnitSnapshot,
	defenderCfgs []config.UnitConfig,
	defenderRegion game.RegionState,
	regionCfg config.RegionConfig,
) CombatResult {

	// ── attacker power ────────────────────────────────────────────────────────
	attackerPower := 0
	for i, a := range attackers {
		eff := effectiveStrength(a, attackerCfgs[i], attackers, attackerCfgs)
		attackerPower += eff
	}

	// ── defender power ────────────────────────────────────────────────────────
	defenderPower := 0
	for i, d := range defenders {
		eff := effectiveStrength(d, defenderCfgs[i], defenders, defenderCfgs)
		defenderPower += eff
	}

	// terrain bonus — skipped if ALL attackers have ignoresFortress
	// (per spec: ignoresFortress means terrain_bonus NOT added to defender power,
	//  but only when attacking a FORTRESS terrain; fortification_bonus still applies)
	anyIgnoresFortress := false
	for _, cfg := range attackerCfgs {
		if cfg.IgnoresFortress {
			anyIgnoresFortress = true
			break
		}
	}

	if !anyIgnoresFortress {
		defenderPower += TerrainBonus(regionCfg.Terrain)
	}

	// fortification bonus (always applies, even for ignoresFortress attackers)
	if defenderRegion.Fortified {
		defenderPower += 2
	}

	// ── resolve ───────────────────────────────────────────────────────────────
	if attackerPower > defenderPower {
		return CombatResult{
			AttackerWon:   true,
			Damage:        attackerPower - defenderPower,
			AttackerPower: attackerPower,
			DefenderPower: defenderPower,
		}
	}
	// attacker repelled — each attacker loses 1 strength (handled by caller)
	return CombatResult{
		AttackerWon:   false,
		Damage:        0,
		AttackerPower: attackerPower,
		DefenderPower: defenderPower,
	}
}

// effectiveStrength calculates a unit's strength including leadership bonus.
// Leadership bonus: each ally co-located with a leader receives +leadershipBonus.
// The leader itself does NOT receive its own bonus.
func effectiveStrength(
	unit game.UnitSnapshot,
	unitCfg config.UnitConfig,
	alliedUnits []game.UnitSnapshot,
	alliedCfgs []config.UnitConfig,
) int {

	base := unit.Strength
	bonus := 0

	// Find the highest leadership bonus from co-located leaders (same side, not self)
	for i, ally := range alliedUnits {
		if ally.ID == unit.ID {
			continue // not self
		}
		cfg := alliedCfgs[i]
		if cfg.Leadership && cfg.Side == unitCfg.Side {
			if cfg.LeadershipBonus > bonus {
				bonus = cfg.LeadershipBonus
			}
		}
	}

	return base + bonus
}

// ApplyDamage applies damage to a unit using config-driven rules.
// Returns the updated snapshot.
// indestructible: strength floors at 1, never DESTROYED or RESPAWNING.
// respawns:       on zero strength → RESPAWNING (returns to home after respawnTurns).
// others:         on zero strength → DESTROYED.
func ApplyDamage(snap game.UnitSnapshot, cfg config.UnitConfig, damage int) game.UnitSnapshot {
	raw := snap.Strength - damage
	if cfg.Indestructible {
		if raw < 1 {
			raw = 1
		}
		snap.Strength = raw
		snap.Status = game.StatusActive
		return snap
	}

	if raw <= 0 {
		snap.Strength = 0
		if cfg.Respawns {
			snap.Status = game.StatusRespawning
			snap.RespawnTurns = cfg.RespawnTurns
			snap.CurrentRegion = ""
		} else {
			snap.Status = game.StatusDestroyed
		}
	} else {
		snap.Strength = raw
	}
	return snap
}
