// Package game provides the map graph with BFS and Dijkstra traversal.
// Used by: detection formula, route risk pipeline, interception pipeline.
package game

import "container/heap"

// Graph represents the Middle-earth map as an adjacency list.
// Edges are bidirectional. Edge weight = path cost (turns to traverse).
type Graph struct {
	// adjacency: regionID → []Edge
	adj map[string][]Edge
	// pathsByEndpoints: (from,to) sorted → pathID
	pathsByEndpoints map[[2]string]string
	pathCosts        map[string]int
}

// Edge represents a directed connection between two regions via a path.
type Edge struct {
	To     string
	PathID string
	Cost   int
}

// NewGraph creates an empty graph.
func NewGraph() *Graph {
	return &Graph{
		adj:              make(map[string][]Edge),
		pathsByEndpoints: make(map[[2]string]string),
		pathCosts:        make(map[string]int),
	}
}

// AddPath adds a bidirectional edge to the graph.
func (g *Graph) AddPath(pathID, from, to string, cost int) {
	g.adj[from] = append(g.adj[from], Edge{To: to, PathID: pathID, Cost: cost})
	g.adj[to] = append(g.adj[to], Edge{To: from, PathID: pathID, Cost: cost})

	key1 := endpoint(from, to)
	key2 := endpoint(to, from)
	g.pathsByEndpoints[key1] = pathID
	g.pathsByEndpoints[key2] = pathID
	g.pathCosts[pathID] = cost
}

// endpoint creates a canonical key for a (from,to) pair.
func endpoint(a, b string) [2]string { return [2]string{a, b} }

// Neighbours returns all edges from a given region.
func (g *Graph) Neighbours(regionID string) []Edge {
	return g.adj[regionID]
}

// PathID returns the path ID connecting two adjacent regions, or "" if not adjacent.
func (g *Graph) PathID(from, to string) string {
	return g.pathsByEndpoints[endpoint(from, to)]
}

// PathCost returns the configured traversal cost for a path ID.
func (g *Graph) PathCost(pathID string) int {
	if cost, ok := g.pathCosts[pathID]; ok {
		return cost
	}
	return 0
}

// ─── BFS (hop count, unweighted) ─────────────────────────────────────────────

// BFSDistance returns the minimum number of hops between two regions.
// Used for Nazgul detection range checks (range is in hops, not turns).
func (g *Graph) BFSDistance(from, to string) int {
	if from == to {
		return 0
	}
	visited := map[string]bool{from: true}
	queue := []struct {
		region string
		dist   int
	}{{from, 0}}

	for len(queue) > 0 {
		cur := queue[0]
		queue = queue[1:]
		for _, e := range g.adj[cur.region] {
			if e.To == to {
				return cur.dist + 1
			}
			if !visited[e.To] {
				visited[e.To] = true
				queue = append(queue, struct {
					region string
					dist   int
				}{e.To, cur.dist + 1})
			}
		}
	}
	return -1 // not reachable
}

// RegionsWithinHops returns all region IDs reachable within maxHops hops from start.
// Used for nazgulProximityCount in pipeline risk calculations.
func (g *Graph) RegionsWithinHops(start string, maxHops int) []string {
	visited := map[string]int{start: 0}
	queue := []struct {
		region string
		hops   int
	}{{start, 0}}
	var result []string

	for len(queue) > 0 {
		cur := queue[0]
		queue = queue[1:]
		if cur.hops > 0 {
			result = append(result, cur.region)
		}
		if cur.hops >= maxHops {
			continue
		}
		for _, e := range g.adj[cur.region] {
			if _, seen := visited[e.To]; !seen {
				visited[e.To] = cur.hops + 1
				queue = append(queue, struct {
					region string
					hops   int
				}{e.To, cur.hops + 1})
			}
		}
	}
	return result
}

// ─── Dijkstra (weighted, turn cost) ──────────────────────────────────────────

// ShortestTurns returns the minimum number of turns to travel from src to dst,
// using path costs as edge weights. Returns -1 if unreachable.
// Used by Pipeline 2 (interception score).
func (g *Graph) ShortestTurns(src, dst string) int {
	if src == dst {
		return 0
	}

	dist := map[string]int{src: 0}
	pq := &minHeap{{region: src, cost: 0}}
	heap.Init(pq)

	for pq.Len() > 0 {
		cur := heap.Pop(pq).(heapItem)
		if cur.region == dst {
			return cur.cost
		}
		if cur.cost > dist[cur.region] {
			continue // stale entry
		}
		for _, e := range g.adj[cur.region] {
			newCost := cur.cost + e.Cost
			if prev, ok := dist[e.To]; !ok || newCost < prev {
				dist[e.To] = newCost
				heap.Push(pq, heapItem{region: e.To, cost: newCost})
			}
		}
	}

	if d, ok := dist[dst]; ok {
		return d
	}
	return -1
}

// ShortestPath returns the ordered list of region IDs on the shortest (by turns) path.
// Used for BFS route verification.
func (g *Graph) ShortestPath(src, dst string) []string {
	if src == dst {
		return []string{src}
	}

	prev := map[string]string{}
	dist := map[string]int{src: 0}
	visited := map[string]bool{}
	pq := &minHeap{{region: src, cost: 0}}
	heap.Init(pq)

	for pq.Len() > 0 {
		cur := heap.Pop(pq).(heapItem)
		if visited[cur.region] {
			continue
		}
		visited[cur.region] = true
		if cur.region == dst {
			break
		}
		for _, e := range g.adj[cur.region] {
			newCost := cur.cost + e.Cost
			if d, ok := dist[e.To]; !ok || newCost < d {
				dist[e.To] = newCost
				prev[e.To] = cur.region
				heap.Push(pq, heapItem{region: e.To, cost: newCost})
			}
		}
	}

	// reconstruct
	if _, ok := prev[dst]; !ok && dst != src {
		return nil
	}
	var path []string
	for r := dst; r != ""; r = prev[r] {
		path = append([]string{r}, path...)
		if r == src {
			break
		}
	}
	return path
}

// ─── min-heap for Dijkstra ────────────────────────────────────────────────────

type heapItem struct {
	region string
	cost   int
}

type minHeap []heapItem

func (h minHeap) Len() int            { return len(h) }
func (h minHeap) Less(i, j int) bool  { return h[i].cost < h[j].cost }
func (h minHeap) Swap(i, j int)       { h[i], h[j] = h[j], h[i] }
func (h *minHeap) Push(x interface{}) { *h = append(*h, x.(heapItem)) }
func (h *minHeap) Pop() interface{} {
	old := *h
	n := len(old)
	x := old[n-1]
	*h = old[:n-1]
	return x
}
