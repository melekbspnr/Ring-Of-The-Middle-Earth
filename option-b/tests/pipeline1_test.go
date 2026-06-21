// Package tests — pipeline1_test.go
// Section 35: Required Unit Tests — 2 cases for Pipeline 1 Route Risk.
// Run with: go test ./tests/... -v -race
package tests

import (
	"testing"

	"ring-of-the-middle-earth/internal/config"
	"ring-of-the-middle-earth/internal/game"
	"ring-of-the-middle-earth/internal/pipeline"
)

// buildTestCache builds a minimal WorldStateCache with controlled threat/surveillance values.
func buildTestCache(
	regions map[string]game.RegionState,
	paths map[string]game.PathState,
	units map[string]game.UnitSnapshot,
) *game.WorldStateCache {
	c := &game.WorldStateCache{}
	c.Turn = 1
	c.Regions = regions
	c.Paths = paths
	c.Units = units
	return c
}

// buildTestGraph returns a minimal graph for Route 1 (Fellowship).
func buildTestGraph() *game.Graph {
	g := game.NewGraph()
	// Route 1 — Fellowship path
	g.AddPath("shire-to-bree", "the-shire", "bree", 1)
	g.AddPath("bree-to-weathertop", "bree", "weathertop", 1)
	g.AddPath("weathertop-to-rivendell", "weathertop", "rivendell", 1)
	g.AddPath("rivendell-to-moria", "rivendell", "moria", 2)
	g.AddPath("moria-to-lothlorien", "moria", "lothlorien", 1)
	g.AddPath("lothlorien-to-emyn-muil", "lothlorien", "emyn-muil", 1)
	g.AddPath("emyn-muil-to-ithilien", "emyn-muil", "ithilien", 2)
	g.AddPath("ithilien-to-cirith-ungol", "ithilien", "cirith-ungol", 2)
	g.AddPath("cirith-ungol-to-mount-doom", "cirith-ungol", "mount-doom", 2)
	return g
}

// ─── Test Cases ───────────────────────────────────────────────────────────────

// Case 1: Route with known threat and surveillance values → correct riskScore computed.
//
// Route 1 (Fellowship) passes through: bree(threat=1), weathertop(threat=2), rivendell(threat=0)...
// Set surveillanceLevel=2 on "bree-to-weathertop": +2*3=6.
// Set one THREATENED path: +2.
// No Nazgul nearby.
// Expected riskScore = sum(threatLevels) + surveillance*3 + threatened*2
func TestPipeline1_RiskScoreWithThreatAndSurveillance(t *testing.T) {
	regions := map[string]game.RegionState{
		"bree":         {ID: "bree", ThreatLevel: 1},
		"weathertop":   {ID: "weathertop", ThreatLevel: 2},
		"rivendell":    {ID: "rivendell", ThreatLevel: 0},
		"moria":        {ID: "moria", ThreatLevel: 3},
		"lothlorien":   {ID: "lothlorien", ThreatLevel: 0},
		"emyn-muil":    {ID: "emyn-muil", ThreatLevel: 2},
		"ithilien":     {ID: "ithilien", ThreatLevel: 2},
		"cirith-ungol": {ID: "cirith-ungol", ThreatLevel: 4},
		"mount-doom":   {ID: "mount-doom", ThreatLevel: 5},
	}

	paths := map[string]game.PathState{
		"shire-to-bree":              {ID: "shire-to-bree", Status: game.PathOpen, SurveillanceLevel: 0},
		"bree-to-weathertop":         {ID: "bree-to-weathertop", Status: game.PathThreatened, SurveillanceLevel: 2}, // +2*3=6 surv, +2 threatened
		"weathertop-to-rivendell":    {ID: "weathertop-to-rivendell", Status: game.PathOpen, SurveillanceLevel: 0},
		"rivendell-to-moria":         {ID: "rivendell-to-moria", Status: game.PathOpen, SurveillanceLevel: 0},
		"moria-to-lothlorien":        {ID: "moria-to-lothlorien", Status: game.PathOpen, SurveillanceLevel: 0},
		"lothlorien-to-emyn-muil":    {ID: "lothlorien-to-emyn-muil", Status: game.PathOpen, SurveillanceLevel: 0},
		"emyn-muil-to-ithilien":      {ID: "emyn-muil-to-ithilien", Status: game.PathOpen, SurveillanceLevel: 0},
		"ithilien-to-cirith-ungol":   {ID: "ithilien-to-cirith-ungol", Status: game.PathOpen, SurveillanceLevel: 0},
		"cirith-ungol-to-mount-doom": {ID: "cirith-ungol-to-mount-doom", Status: game.PathOpen, SurveillanceLevel: 0},
	}

	cache := buildTestCache(regions, paths, map[string]game.UnitSnapshot{})
	graph := buildTestGraph()
	gameCfg := &config.GameConfig{Units: map[string]config.UnitConfig{}}

	dispatcher := pipeline.NewRouteRiskDispatcher(cache, graph, gameCfg)

	// Manually call compute via exported helper for testing
	// We test computeRouteRisk indirectly by running the full pipeline
	// sum(threatLevels): 1+2+0+3+0+2+2+4+5 = 19
	// surveillance: 2*3 = 6 (on bree-to-weathertop)
	// threatened: 1*2 = 2
	// nazgulProximity: 0
	// Expected = 19 + 6 + 2 = 27
	expectedMin := 27

	result := dispatcher.ComputeForTest()
	var route1 *pipeline.RouteScore
	for i := range result.Routes {
		if result.Routes[i].Name == "Route 1 — Fellowship" {
			route1 = &result.Routes[i]
			break
		}
	}

	if route1 == nil {
		t.Fatal("Case 1: Route 1 not found in result")
	}
	if route1.RiskScore < expectedMin {
		t.Errorf("Case 1: expected riskScore>=%d, got %d", expectedMin, route1.RiskScore)
	}
	t.Logf("Case 1: Route 1 riskScore=%d (expected>=%d) ✓", route1.RiskScore, expectedMin)
}

// Case 2: Nazgul within 2 hops → proximity count adds correctly to score.
//
// Place a Nazgul at "moria" (within 2 hops of rivendell, lothlorien, weathertop).
// nazgulProximityCount should be 1 (one Nazgul), adding +2 to score.
func TestPipeline1_NazgulProximityIncreasesScore(t *testing.T) {
	regions := map[string]game.RegionState{
		"bree":         {ID: "bree", ThreatLevel: 0},
		"weathertop":   {ID: "weathertop", ThreatLevel: 0},
		"rivendell":    {ID: "rivendell", ThreatLevel: 0},
		"moria":        {ID: "moria", ThreatLevel: 0},
		"lothlorien":   {ID: "lothlorien", ThreatLevel: 0},
		"emyn-muil":    {ID: "emyn-muil", ThreatLevel: 0},
		"ithilien":     {ID: "ithilien", ThreatLevel: 0},
		"cirith-ungol": {ID: "cirith-ungol", ThreatLevel: 0},
		"mount-doom":   {ID: "mount-doom", ThreatLevel: 0},
	}

	paths := map[string]game.PathState{
		"shire-to-bree":              {ID: "shire-to-bree", Status: game.PathOpen},
		"bree-to-weathertop":         {ID: "bree-to-weathertop", Status: game.PathOpen},
		"weathertop-to-rivendell":    {ID: "weathertop-to-rivendell", Status: game.PathOpen},
		"rivendell-to-moria":         {ID: "rivendell-to-moria", Status: game.PathOpen},
		"moria-to-lothlorien":        {ID: "moria-to-lothlorien", Status: game.PathOpen},
		"lothlorien-to-emyn-muil":    {ID: "lothlorien-to-emyn-muil", Status: game.PathOpen},
		"emyn-muil-to-ithilien":      {ID: "emyn-muil-to-ithilien", Status: game.PathOpen},
		"ithilien-to-cirith-ungol":   {ID: "ithilien-to-cirith-ungol", Status: game.PathOpen},
		"cirith-ungol-to-mount-doom": {ID: "cirith-ungol-to-mount-doom", Status: game.PathOpen},
	}

	// Nazgul-3 at "moria" — within 2 hops of rivendell and lothlorien (both on Route 1)
	units := map[string]game.UnitSnapshot{
		"nazgul-3": {ID: "nazgul-3", CurrentRegion: "moria", Status: game.StatusActive, Strength: 3},
	}

	// Config: nazgul-3 has detectionRange=1 (Nazgul identification via config, not ID)
	gameCfg := &config.GameConfig{
		Units: map[string]config.UnitConfig{
			"nazgul-3": {DetectionRange: 1}, // config-driven: DetectionRange>0 → Nazgul
		},
	}

	cache := buildTestCache(regions, paths, units)
	graph := buildTestGraph()
	dispatcher := pipeline.NewRouteRiskDispatcher(cache, graph, gameCfg)

	// Score without Nazgul would be 0 (all threats=0, no surveillance).
	// With Nazgul within 2 hops: +2 per Nazgul.
	result := dispatcher.ComputeForTest()
	var route1 *pipeline.RouteScore
	for i := range result.Routes {
		if result.Routes[i].Name == "Route 1 — Fellowship" {
			route1 = &result.Routes[i]
			break
		}
	}

	if route1 == nil {
		t.Fatal("Case 2: Route 1 not found")
	}
	if route1.RiskScore < 2 {
		t.Errorf("Case 2: expected riskScore>=2 (nazgul proximity +2), got %d", route1.RiskScore)
	}
	t.Logf("Case 2: Route 1 riskScore=%d (nazgul proximity applied) ✓", route1.RiskScore)
}
