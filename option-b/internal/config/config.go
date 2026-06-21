// Package config loads and holds all static game configuration.
// IMPORTANT: No unit ID string literals appear anywhere in game logic.
// All behaviour is driven by config fields (e.g., config.Indestructible, config.DetectionRange).
package config

import (
	"bufio"
	"fmt"
	"os"
	"strconv"
	"strings"
)

// Side represents which faction a unit belongs to.
type Side string

const (
	SideFreePeoples Side = "FREE_PEOPLES"
	SideShadow      Side = "SHADOW"
)

// UnitConfig holds the full static configuration for a unit.
// No game logic may reference a unit by its string ID — only these fields.
type UnitConfig struct {
	ID               string
	Name             string
	Class            string
	Side             Side
	StartRegion      string
	Strength         int
	Leadership       bool
	LeadershipBonus  int
	Indestructible   bool
	DetectionRange   int
	Respawns         bool
	RespawnTurns     int
	Maia             bool
	MaiaAbilityPaths []string
	IgnoresFortress  bool
	CanFortify       bool
	Cooldown         int
}

// GameConfig holds global game settings and all unit configs.
type GameConfig struct {
	HiddenUntilTurn     int
	MaxTurns            int
	TurnDurationSeconds int
	Units               map[string]UnitConfig // keyed by unit ID
}

// RegionConfig holds the static definition of a map region.
type RegionConfig struct {
	ID           string
	Name         string
	Terrain      string
	SpecialRole  string
	StartControl string
	StartThreat  int
}

// PathConfig holds the static definition of a map path (edge).
type PathConfig struct {
	ID   string
	From string
	To   string
	Cost int
}

// MapConfig holds all region and path definitions.
type MapConfig struct {
	Regions map[string]RegionConfig // keyed by region ID
	Paths   map[string]PathConfig   // keyed by path ID
}

// LoadGameConfig parses config/units.conf in a simple line-by-line manner.
func LoadGameConfig(path string) (*GameConfig, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("open units config: %w", err)
	}
	defer f.Close()

	cfg := &GameConfig{
		Units: make(map[string]UnitConfig),
	}

	scanner := bufio.NewScanner(f)
	var currentUnit *UnitConfig
	inUnitBlock := false

	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())

		// skip comments and empty lines
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}

		// Global settings
		if kv := parseKV(line); kv != nil {
			switch kv[0] {
			case "hidden-until-turn":
				cfg.HiddenUntilTurn, _ = strconv.Atoi(kv[1])
			case "max-turns":
				cfg.MaxTurns, _ = strconv.Atoi(kv[1])
			case "turn-duration-seconds":
				cfg.TurnDurationSeconds, _ = strconv.Atoi(kv[1])
			}
		}

		// Start of a unit block
		if strings.HasPrefix(line, "{") {
			inUnitBlock = true
			currentUnit = &UnitConfig{}
		}

		// End of unit block
		if inUnitBlock && strings.Contains(line, "}") {
			if currentUnit != nil && currentUnit.ID != "" {
				cfg.Units[currentUnit.ID] = *currentUnit
			}
			inUnitBlock = false
			currentUnit = nil
		}

		if inUnitBlock && currentUnit != nil {
			parseUnitField(line, currentUnit)
		}
	}

	if err := scanner.Err(); err != nil {
		return nil, fmt.Errorf("scan units config: %w", err)
	}

	return cfg, nil
}

// LoadMapConfig parses config/map.conf.
func LoadMapConfig(path string) (*MapConfig, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("open map config: %w", err)
	}
	defer f.Close()

	mc := &MapConfig{
		Regions: make(map[string]RegionConfig),
		Paths:   make(map[string]PathConfig),
	}

	scanner := bufio.NewScanner(f)
	inRegions := false
	inPaths := false

	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}

		if strings.HasPrefix(line, "regions") {
			inRegions = true
			inPaths = false
			continue
		}
		if strings.HasPrefix(line, "paths") {
			inPaths = true
			inRegions = false
			continue
		}
		if line == "]" {
			inRegions = false
			inPaths = false
			continue
		}

		if inRegions && strings.HasPrefix(line, "{") {
			r := parseRegionLine(line)
			if r.ID != "" {
				mc.Regions[r.ID] = r
			}
		}
		if inPaths && strings.HasPrefix(line, "{") {
			p := parsePathLine(line)
			if p.ID != "" {
				mc.Paths[p.ID] = p
			}
		}
	}

	return mc, scanner.Err()
}

// ─── helpers ──────────────────────────────────────────────────────────────────

// parseKV splits "key = value" into ["key","value"].
func parseKV(line string) []string {
	parts := strings.SplitN(line, "=", 2)
	if len(parts) != 2 {
		return nil
	}
	return []string{strings.TrimSpace(parts[0]), strings.Trim(strings.TrimSpace(parts[1]), `"`)}
}

// parseUnitField populates a single field in a UnitConfig from a conf line.
func parseUnitField(line string, u *UnitConfig) {
	// Each field appears as  key=value  or  key="value"
	// We split on comma to handle multiple fields on same line
	parts := splitFields(line)
	for _, part := range parts {
		kv := strings.SplitN(strings.TrimSpace(part), "=", 2)
		if len(kv) != 2 {
			continue
		}
		key := strings.TrimSpace(kv[0])
		val := strings.TrimSpace(strings.Trim(kv[1], `"`))

		switch key {
		case "id":
			u.ID = val
		case "name":
			u.Name = val
		case "class":
			u.Class = val
		case "side":
			u.Side = Side(val)
		case "start":
			u.StartRegion = val
		case "strength":
			u.Strength, _ = strconv.Atoi(val)
		case "leadership":
			u.Leadership = val == "true"
		case "leadershipBonus":
			u.LeadershipBonus, _ = strconv.Atoi(val)
		case "indestructible":
			u.Indestructible = val == "true"
		case "detectionRange":
			u.DetectionRange, _ = strconv.Atoi(val)
		case "respawns":
			u.Respawns = val == "true"
		case "respawnTurns":
			u.RespawnTurns, _ = strconv.Atoi(val)
		case "maia":
			u.Maia = val == "true"
		case "maiaAbilityPaths":
			u.MaiaAbilityPaths = parseStringList(val)
		case "ignoresFortress":
			u.IgnoresFortress = val == "true"
		case "canFortify":
			u.CanFortify = val == "true"
		case "cooldown":
			u.Cooldown, _ = strconv.Atoi(val)
		}
	}
}

// splitFields splits a conf line by comma, but not commas inside brackets.
func splitFields(line string) []string {
	line = strings.TrimSpace(line)
	line = strings.TrimPrefix(line, "{")
	line = strings.TrimSuffix(line, ",")
	line = strings.TrimSuffix(line, "}")
	line = strings.TrimSpace(line)

	var result []string
	depth := 0
	start := 0
	for i, ch := range line {
		switch ch {
		case '[':
			depth++
		case ']':
			depth--
		case ',':
			if depth == 0 {
				result = append(result, line[start:i])
				start = i + 1
			}
		}
	}
	result = append(result, line[start:])
	return result
}

// parseStringList parses ["a","b","c"] → []string{"a","b","c"}.
func parseStringList(val string) []string {
	val = strings.Trim(val, "[]")
	if val == "" {
		return nil
	}
	parts := strings.Split(val, ",")
	var out []string
	for _, p := range parts {
		s := strings.Trim(strings.TrimSpace(p), `"`)
		if s != "" {
			out = append(out, s)
		}
	}
	return out
}

// parseRegionLine parses a single region entry from map.conf.
func parseRegionLine(line string) RegionConfig {
	r := RegionConfig{}
	parts := splitFields(line)
	for _, part := range parts {
		kv := strings.SplitN(strings.TrimSpace(part), "=", 2)
		if len(kv) != 2 {
			continue
		}
		key := strings.TrimSpace(kv[0])
		val := strings.TrimSpace(strings.Trim(kv[1], `"`))
		switch key {
		case "id":
			r.ID = val
		case "name":
			r.Name = val
		case "terrain":
			r.Terrain = val
		case "specialRole":
			r.SpecialRole = val
		case "startControl":
			r.StartControl = val
		case "startThreat":
			r.StartThreat, _ = strconv.Atoi(val)
		}
	}
	return r
}

// parsePathLine parses a single path entry from map.conf.
func parsePathLine(line string) PathConfig {
	p := PathConfig{}
	parts := splitFields(line)
	for _, part := range parts {
		kv := strings.SplitN(strings.TrimSpace(part), "=", 2)
		if len(kv) != 2 {
			continue
		}
		key := strings.TrimSpace(kv[0])
		val := strings.TrimSpace(strings.Trim(kv[1], `"`))
		switch key {
		case "id":
			p.ID = val
		case "from":
			p.From = val
		case "to":
			p.To = val
		case "cost":
			p.Cost, _ = strconv.Atoi(val)
		}
	}
	return p
}
