// Package tests — detection_test.go
// B4 criterion: Detection formula + Sauron amplifier + hidden-until-turn.
// Run with: go test ./tests/... -v -race
package tests

import (
	"testing"

	"ring-of-the-middle-earth/internal/config"
	"ring-of-the-middle-earth/internal/engine"
	"ring-of-the-middle-earth/internal/game"
)

func buildDetectionGraph() *game.Graph {
	g := game.NewGraph()
	g.AddPath("p1", "the-shire", "bree", 1)
	g.AddPath("p2", "bree", "weathertop", 1)
	g.AddPath("p3", "weathertop", "rivendell", 1)
	g.AddPath("p4", "minas-morgul", "mordor", 1)
	g.AddPath("p5", "mordor", "mount-doom", 1)
	return g
}

// Case 1: Detection suppressed during hidden-until-turn
func TestDetection_HiddenUntilTurn(t *testing.T) {
	graph := buildDetectionGraph()
	units := map[string]game.UnitSnapshot{
		"nazgul-1": {ID: "nazgul-1", CurrentRegion: "the-shire", Status: game.StatusActive},
	}
	cfgs := map[string]config.UnitConfig{
		"nazgul-1": {DetectionRange: 2},
	}
	rbState := game.RingBearerState{TrueRegion: "the-shire"} // same region!

	// Turn 1, hiddenUntil=3 → should NOT be detected
	result := engine.RunDetection(1, 3, rbState, units, cfgs, graph)
	if result.Exposed {
		t.Errorf("Case 1: expected NOT exposed (turn 1 <= hiddenUntil 3), got Exposed=true")
	}
}

// Case 2: Nazgul detects Ring Bearer within range
func TestDetection_NazgulInRange(t *testing.T) {
	graph := buildDetectionGraph()
	units := map[string]game.UnitSnapshot{
		"nazgul-1": {ID: "nazgul-1", CurrentRegion: "bree", Status: game.StatusActive},
	}
	cfgs := map[string]config.UnitConfig{
		"nazgul-1": {DetectionRange: 2},
	}
	rbState := game.RingBearerState{TrueRegion: "the-shire"} // 1 hop from bree

	// Turn 5 > hiddenUntil 3, Nazgul range=2 >= distance=1
	result := engine.RunDetection(5, 3, rbState, units, cfgs, graph)
	if !result.Exposed {
		t.Errorf("Case 2: expected EXPOSED (nazgul 1 hop away, range 2), got Exposed=false")
	}
	if result.TrueRegion != "the-shire" {
		t.Errorf("Case 2: expected TrueRegion='the-shire', got '%s'", result.TrueRegion)
	}
}

// Case 3: Sauron amplifier adds +1 range
func TestDetection_SauronAmplifier(t *testing.T) {
	graph := buildDetectionGraph()
	units := map[string]game.UnitSnapshot{
		// Nazgul at weathertop: 3 hops from the-shire (weathertop->bree->the-shire)
		// Base range=2, so can't reach. But with Sauron +1, range becomes 3.
		"nazgul-1": {ID: "nazgul-1", CurrentRegion: "rivendell", Status: game.StatusActive},
		// Sauron: Maia at mordor (start region = mordor)
		"sauron": {ID: "sauron", CurrentRegion: "mordor", Status: game.StatusActive},
	}
	cfgs := map[string]config.UnitConfig{
		"nazgul-1": {DetectionRange: 2},
		"sauron":   {Maia: true, StartRegion: "mordor"},
	}
	// Ring Bearer at bree: 2 hops from rivendell
	rbState := game.RingBearerState{TrueRegion: "bree"}

	// Without Sauron: distance=2, range=2 → detected (just in range)
	// With Sauron: range becomes 3 → definitely detected
	result := engine.RunDetection(5, 3, rbState, units, cfgs, graph)
	if !result.Exposed {
		t.Errorf("Case 3: expected EXPOSED with Sauron amplifier, got Exposed=false")
	}
}

// Case 4: Surveillance exposure on path crossing
func TestDetection_SurveillanceExposure(t *testing.T) {
	// Path with surveillance level 1 → Ring Bearer exposed
	path := game.PathState{SurveillanceLevel: 1}
	exposed := engine.IsExposedBySurveillance(path, 5, 3)
	if !exposed {
		t.Errorf("Case 4a: expected exposed (surveillance=1, turn 5 > hidden 3)")
	}

	// Same path but turn <= hiddenUntil → NOT exposed
	exposedHidden := engine.IsExposedBySurveillance(path, 2, 3)
	if exposedHidden {
		t.Errorf("Case 4b: expected NOT exposed (turn 2 <= hidden 3)")
	}

	// Path with no surveillance → NOT exposed
	cleanPath := game.PathState{SurveillanceLevel: 0}
	exposedClean := engine.IsExposedBySurveillance(cleanPath, 5, 3)
	if exposedClean {
		t.Errorf("Case 4c: expected NOT exposed (surveillance=0)")
	}
}
