// Package pipeline — route_risk.go implements Pipeline 1 (Route Risk — Light Side).
// Section 32 of the spec.
//
// Architecture:
//
//	Dispatcher → buffered ch (cap=20) → 4 workers → unbuffered ch → Aggregator → Deliverer
//
// Cancellation: context.Context + or-done pattern
// Shutdown:     sync.WaitGroup at every stage boundary
// Timeout:      2 seconds → return partial result
package pipeline

import (
	"context"
	"log"
	"sync"
	"time"

	"ring-of-the-middle-earth/internal/config"
	"ring-of-the-middle-earth/internal/game"
	"ring-of-the-middle-earth/internal/shared"
)

const (
	routeRiskBufferCap = 20
	routeRiskWorkers   = 4
	routeRiskTimeout   = 2 * time.Second
)

// RouteRiskDispatcher manages the Pipeline 1 goroutines.
type RouteRiskDispatcher struct {
	cache     *game.WorldStateCache
	graph     *game.Graph
	gameCfg   *config.GameConfig
	triggerCh chan shared.AnalysisRequest
	resultCh  chan RankedRouteList
}

// RankedRouteList is the output of Pipeline 1.
type RankedRouteList struct {
	Routes      []RouteScore `json:"routes"`
	Recommended string       `json:"recommended"`
	Warnings    []string     `json:"warnings"`
}

// RouteScore holds a canonical route and its computed risk score.
type RouteScore struct {
	Name         string   `json:"name"`
	Regions      []string `json:"regions"`
	RiskScore    int      `json:"riskScore"`
	ThreatPaths  []string `json:"threatenedPaths"`
	BlockedPaths []string `json:"blockedPaths"`
}

// routeWork is the unit of work per worker (one canonical route per job).
type routeWork struct {
	routeName string
	regions   []string // destination regions in route (not including start)
	pathIDs   []string // path IDs in route
}

// ComputeRouteRiskForPathIDs applies the Route Risk formula to an arbitrary
// submitted route. This mirrors Topology 2 for ASSIGN_ROUTE / REDIRECT_UNIT.
func ComputeRouteRiskForPathIDs(
	startRegion string,
	pathIDs []string,
	snap game.WorldStateCache,
	gameCfg *config.GameConfig,
	graph *game.Graph,
	mapCfg *config.MapConfig,
) RouteScore {
	work := routeWork{
		routeName: "Submitted Route",
		regions:   resolveRouteRegions(startRegion, pathIDs, mapCfg),
		pathIDs:   append([]string(nil), pathIDs...),
	}
	return computeRouteRisk(work, snap, gameCfg, graph)
}

// NewRouteRiskDispatcher creates the dispatcher.
func NewRouteRiskDispatcher(cache *game.WorldStateCache, graph *game.Graph, gameCfg *config.GameConfig) *RouteRiskDispatcher {
	return &RouteRiskDispatcher{
		cache:     cache,
		graph:     graph,
		gameCfg:   gameCfg,
		triggerCh: make(chan shared.AnalysisRequest, 5),
		resultCh:  make(chan RankedRouteList, 1),
	}
}

// Run starts the pipeline and waits for triggers.
func (d *RouteRiskDispatcher) Run(doneCh <-chan struct{}) {
	for {
		select {
		case <-doneCh:
			return
		case req := <-d.triggerCh:
			result := d.compute(req)
			// Non-blocking send — drop if nobody's listening
			select {
			case d.resultCh <- result:
			default:
			}
		}
	}
}

// Trigger sends an analysis request to the pipeline.
func (d *RouteRiskDispatcher) Trigger(req shared.AnalysisRequest) {
	select {
	case d.triggerCh <- req:
	default:
		log.Println("[pipeline1] trigger channel full, dropping")
	}
}

// Result returns the last computed result (non-blocking).
func (d *RouteRiskDispatcher) Result() (RankedRouteList, bool) {
	select {
	case r := <-d.resultCh:
		return r, true
	default:
		return RankedRouteList{}, false
	}
}

// compute runs the fan-out/fan-in pipeline.
func (d *RouteRiskDispatcher) compute(req shared.AnalysisRequest) RankedRouteList {
	snap := d.cache.Snapshot()

	// The 4 canonical routes from Section 2.3
	routes := canonicalRoutes()

	ctx, cancel := context.WithTimeout(context.Background(), routeRiskTimeout)
	defer cancel()

	// Stage 1: Dispatcher → buffered work channel
	workCh := make(chan routeWork, routeRiskBufferCap)
	go func() {
		defer close(workCh)
		for _, r := range routes {
			select {
			case <-ctx.Done():
				return
			case workCh <- r:
			}
		}
	}()

	// Stage 2: 4 Workers → unbuffered result channel
	resultCh := make(chan RouteScore)
	var wg sync.WaitGroup
	for i := 0; i < routeRiskWorkers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for work := range orDone(ctx, workCh) {
				score := computeRouteRisk(work, snap, d.gameCfg, d.graph)
				select {
				case <-ctx.Done():
					return
				case resultCh <- score:
				}
			}
		}()
	}

	// Close resultCh when all workers finish
	go func() {
		wg.Wait()
		close(resultCh)
	}()

	// Stage 3: Aggregator
	var scores []RouteScore
	for score := range resultCh {
		scores = append(scores, score)
	}

	return aggregate(scores)
}

// computeRouteRisk applies the risk formula from Section 32.
//
// riskScore =
//
//	  sum(region.threatLevel for each destination region)
//	+ sum(path.surveillanceLevel for each path) * 3
//	+ count(BLOCKED paths)    * 5
//	+ count(THREATENED paths) * 2
//	+ nazgulProximityCount    * 2
func computeRouteRisk(work routeWork, snap game.WorldStateCache, gameCfg *config.GameConfig, graph *game.Graph) RouteScore {
	score := 0
	var threatened []string
	var blocked []string

	// Sum region threat levels
	for _, regionID := range work.regions {
		if r, ok := snap.Regions[regionID]; ok {
			score += r.ThreatLevel
		}
	}

	// Sum path surveillance levels + count blocked/threatened
	for _, pathID := range work.pathIDs {
		p, ok := snap.Paths[pathID]
		if !ok {
			continue
		}
		score += p.SurveillanceLevel * 3

		switch p.Status {
		case game.PathBlocked:
			score += 5
			blocked = append(blocked, pathID)
		case game.PathThreatened:
			score += 2
			threatened = append(threatened, pathID)
		}
	}

	// Nazgul proximity count: Nazgul within 2 hops of any route region
	proximity := computeNazgulProximity(work.regions, snap, gameCfg, graph)
	score += proximity * 2

	return RouteScore{
		Name:         work.routeName,
		Regions:      work.regions,
		RiskScore:    score,
		ThreatPaths:  threatened,
		BlockedPaths: blocked,
	}
}

// computeNazgulProximity counts Nazgul units (config.DetectionRange > 0) within
// 2 graph hops of any region in the route.
// Config-driven Nazgul identification — no unit ID hardcoding.
func computeNazgulProximity(routeRegions []string, snap game.WorldStateCache, gameCfg *config.GameConfig, graph *game.Graph) int {
	count := 0
	for uid, u := range snap.Units {
		if u.Status != game.StatusActive {
			continue
		}
		cfg := gameCfg.Units[uid]
		if cfg.DetectionRange <= 0 { // config-driven: only Nazgul have detectionRange > 0
			continue
		}
		for _, regionID := range routeRegions {
			dist := graph.BFSDistance(u.CurrentRegion, regionID)
			if dist >= 0 && dist <= 2 {
				count++
				break // count this Nazgul once per route
			}
		}
	}
	return count
}

// aggregate sorts routes by risk score and picks a recommendation.
func aggregate(scores []RouteScore) RankedRouteList {
	// Simple insertion sort (4 routes max)
	for i := 1; i < len(scores); i++ {
		for j := i; j > 0 && scores[j].RiskScore < scores[j-1].RiskScore; j-- {
			scores[j], scores[j-1] = scores[j-1], scores[j]
		}
	}

	var warnings []string
	recommended := ""
	if len(scores) > 0 {
		recommended = scores[0].Name
		if scores[0].RiskScore > 20 {
			warnings = append(warnings, "All routes are heavily contested")
		}
		if len(scores[0].BlockedPaths) > 0 {
			warnings = append(warnings, "Recommended route has blocked paths — Gandalf needed")
		}
	}

	return RankedRouteList{
		Routes:      scores,
		Recommended: recommended,
		Warnings:    warnings,
	}
}

// canonicalRoutes returns the 4 canonical Ring Bearer routes from Section 2.3.
func canonicalRoutes() []routeWork {
	return []routeWork{
		{
			routeName: "Route 1 — Fellowship",
			regions:   []string{"bree", "weathertop", "rivendell", "moria", "lothlorien", "emyn-muil", "ithilien", "cirith-ungol", "mount-doom"},
			pathIDs:   []string{"shire-to-bree", "bree-to-weathertop", "weathertop-to-rivendell", "rivendell-to-moria", "moria-to-lothlorien", "lothlorien-to-emyn-muil", "emyn-muil-to-ithilien", "ithilien-to-cirith-ungol", "cirith-ungol-to-mount-doom"},
		},
		{
			routeName: "Route 2 — Northern Bypass",
			regions:   []string{"bree", "rivendell", "lothlorien", "emyn-muil", "dead-marshes", "ithilien", "cirith-ungol", "mount-doom"},
			pathIDs:   []string{"shire-to-bree", "bree-to-rivendell", "rivendell-to-lothlorien", "lothlorien-to-emyn-muil", "emyn-muil-to-dead-marshes", "dead-marshes-to-ithilien", "ithilien-to-cirith-ungol", "cirith-ungol-to-mount-doom"},
		},
		{
			routeName: "Route 3 — Dark Route",
			regions:   []string{"bree", "rivendell", "lothlorien", "emyn-muil", "dead-marshes", "mordor", "mount-doom"},
			pathIDs:   []string{"shire-to-bree", "bree-to-rivendell", "rivendell-to-lothlorien", "lothlorien-to-emyn-muil", "emyn-muil-to-dead-marshes", "dead-marshes-to-mordor", "mordor-to-mount-doom"},
		},
		{
			routeName: "Route 4 — Southern Corridor",
			regions:   []string{"tharbad", "fords-of-isen", "edoras", "minas-tirith", "osgiliath", "minas-morgul", "cirith-ungol", "mount-doom"},
			pathIDs:   []string{"shire-to-tharbad", "tharbad-to-fords-of-isen", "fords-of-isen-to-edoras", "edoras-to-minas-tirith", "minas-tirith-to-osgiliath", "osgiliath-to-minas-morgul", "minas-morgul-to-cirith-ungol", "cirith-ungol-to-mount-doom"},
		},
	}
}

func resolveRouteRegions(startRegion string, pathIDs []string, mapCfg *config.MapConfig) []string {
	current := startRegion
	var regions []string
	for _, pathID := range pathIDs {
		pc, ok := mapCfg.Paths[pathID]
		if !ok {
			continue
		}
		switch current {
		case pc.From:
			current = pc.To
			regions = append(regions, current)
		case pc.To:
			current = pc.From
			regions = append(regions, current)
		default:
			// If the chain is malformed, keep the route analyzable by appending the
			// configured destination and continuing from there.
			current = pc.To
			regions = append(regions, current)
		}
	}
	return regions
}

// orDone wraps a channel with context cancellation (or-done pattern).
func orDone[T any](ctx context.Context, ch <-chan T) <-chan T {
	out := make(chan T)
	go func() {
		defer close(out)
		for {
			select {
			case <-ctx.Done():
				return
			case v, ok := <-ch:
				if !ok {
					return
				}
				select {
				case <-ctx.Done():
					return
				case out <- v:
				}
			}
		}
	}()
	return out
}
