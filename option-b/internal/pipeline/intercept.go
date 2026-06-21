// Package pipeline — intercept.go implements Pipeline 2 (Interception — Dark Side).
// Section 33 of the spec.
//
// Architecture:
//
//	Dispatcher → buffered ch (cap=30) → 4 workers → unbuffered ch → Aggregator → Deliverer
//
// Each worker processes one (Nazgul, route-candidate) pair.
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
	interceptBufferCap = 30
	interceptWorkers   = 4
	interceptTimeout   = 2 * time.Second
)

// InterceptDispatcher manages the Pipeline 2 goroutines.
type InterceptDispatcher struct {
	cache     *game.WorldStateCache
	graph     *game.Graph
	gameCfg   *config.GameConfig
	triggerCh chan shared.AnalysisRequest
	resultCh  chan InterceptPlan
}

// InterceptPlan is the output of Pipeline 2.
type InterceptPlan struct {
	ByUnit []UnitIntercept `json:"byUnit"`
}

// UnitIntercept is the best interception target for one Nazgul unit.
type UnitIntercept struct {
	UnitID         string  `json:"unitId"`
	TargetRegion   string  `json:"targetRegion"`
	Score          float64 `json:"score"`
	RouteCandidate string  `json:"routeCandidate"`
}

// interceptWork is the unit of work per worker.
// One worker handles one (Nazgul unitID, route-candidate) pair.
type interceptWork struct {
	nazgulID     string
	nazgulRegion string
	route        routeWork
}

// interceptResult is the scored result for one (Nazgul, route) pair.
type interceptResult struct {
	nazgulID     string
	targetRegion string
	score        float64
	routeName    string
}

// NewInterceptDispatcher creates the dispatcher.
func NewInterceptDispatcher(cache *game.WorldStateCache, graph *game.Graph, gameCfg *config.GameConfig) *InterceptDispatcher {
	return &InterceptDispatcher{
		cache:     cache,
		graph:     graph,
		gameCfg:   gameCfg,
		triggerCh: make(chan shared.AnalysisRequest, 5),
		resultCh:  make(chan InterceptPlan, 1),
	}
}

// Run starts the pipeline and waits for triggers.
func (d *InterceptDispatcher) Run(doneCh <-chan struct{}) {
	for {
		select {
		case <-doneCh:
			return
		case req := <-d.triggerCh:
			result := d.compute(req)
			select {
			case d.resultCh <- result:
			default:
			}
		}
	}
}

// Trigger sends an analysis request to the pipeline.
func (d *InterceptDispatcher) Trigger(req shared.AnalysisRequest) {
	select {
	case d.triggerCh <- req:
	default:
		log.Println("[pipeline2] trigger channel full, dropping")
	}
}

// Result returns the last computed result (non-blocking).
func (d *InterceptDispatcher) Result() (InterceptPlan, bool) {
	select {
	case r := <-d.resultCh:
		return r, true
	default:
		return InterceptPlan{}, false
	}
}

// compute runs the fan-out/fan-in pipeline.
func (d *InterceptDispatcher) compute(req shared.AnalysisRequest) InterceptPlan {
	snap := d.cache.Snapshot()

	// Build work items: all (Nazgul, route) pairs
	routes := canonicalRoutes()
	var works []interceptWork

	for uid, u := range snap.Units {
		if u.Status != game.StatusActive {
			continue
		}
		cfg := d.gameCfg.Units[uid]
		// Config-driven Nazgul identification: detectionRange > 0
		if cfg.DetectionRange <= 0 {
			continue
		}
		for _, route := range routes {
			works = append(works, interceptWork{
				nazgulID:     uid,
				nazgulRegion: u.CurrentRegion,
				route:        route,
			})
		}
	}

	ctx, cancel := context.WithTimeout(context.Background(), interceptTimeout)
	defer cancel()

	// Stage 1: Dispatcher → buffered work channel (cap=30)
	workCh := make(chan interceptWork, interceptBufferCap)
	go func() {
		defer close(workCh)
		for _, w := range works {
			select {
			case <-ctx.Done():
				return
			case workCh <- w:
			}
		}
	}()

	// Stage 2: 4 Workers → unbuffered result channel
	resCh := make(chan interceptResult)
	var wg sync.WaitGroup
	for i := 0; i < interceptWorkers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for work := range orDone(ctx, workCh) {
				res := computeInterceptScore(work, snap, d.graph)
				select {
				case <-ctx.Done():
					return
				case resCh <- res:
				}
			}
		}()
	}

	go func() {
		wg.Wait()
		close(resCh)
	}()

	// Stage 3: Aggregator — pick best (score, targetRegion) per Nazgul
	bestByUnit := map[string]interceptResult{}
	for res := range resCh {
		if prev, ok := bestByUnit[res.nazgulID]; !ok || res.score > prev.score {
			bestByUnit[res.nazgulID] = res
		}
	}

	// Stage 4: Deliverer
	var plan InterceptPlan
	for _, res := range bestByUnit {
		plan.ByUnit = append(plan.ByUnit, UnitIntercept{
			UnitID:         res.nazgulID,
			TargetRegion:   res.targetRegion,
			Score:          res.score,
			RouteCandidate: res.routeName,
		})
	}
	return plan
}

// computeInterceptScore applies the formula from Section 33.
//
// turnsToIntercept = graph.shortestPath(nazgul.region, routeRegion)
// rbTurnsToReach   = sum of traversal costs to that region from the-shire
// interceptWindow  = rbTurnsToReach - turnsToIntercept
// score = interceptWindow >= 0 ? 1.0 - (turnsToIntercept / routeLength) : 0.0
//
// We compute this for each region in the route and return the best score.
func computeInterceptScore(work interceptWork, snap game.WorldStateCache, graph *game.Graph) interceptResult {
	best := interceptResult{
		nazgulID:  work.nazgulID,
		routeName: work.route.routeName,
		score:     0.0,
	}

	routeLength := float64(len(work.route.regions))
	if routeLength == 0 {
		return best
	}

	// Compute cumulative Ring Bearer travel turns to each region along the route
	cumulativeCost := 0
	for i, regionID := range work.route.regions {
		// Cost to reach this region = configured cost of path[i] (index-matched).
		if i < len(work.route.pathIDs) {
			pathID := work.route.pathIDs[i]
			if ps, ok := snap.Paths[pathID]; ok {
				_ = ps // path state could be used for blocked cost in future
			}
			cost := graph.PathCost(pathID)
			if cost <= 0 {
				cost = 1
			}
			cumulativeCost += cost
		}

		rbTurnsToReach := cumulativeCost
		turnsToIntercept := graph.ShortestTurns(work.nazgulRegion, regionID)

		if turnsToIntercept < 0 {
			continue // unreachable
		}

		interceptWindow := rbTurnsToReach - turnsToIntercept
		var score float64
		if interceptWindow >= 0 {
			score = 1.0 - (float64(turnsToIntercept) / routeLength)
			if score < 0 {
				score = 0
			}
		}

		if score > best.score {
			best.score = score
			best.targetRegion = regionID
		}
	}

	return best
}
