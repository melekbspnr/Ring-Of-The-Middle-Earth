// Package streams — topology2.go implements Kafka Streams Topology 2: Route Risk Enrichment.
// Section 12 of the spec.
//
// Source: game.orders.validated — filter ASSIGN_ROUTE and REDIRECT_UNIT
// KTables: PathKTable, RegionKTable
//
// Enrichment formula:
//   routeRiskScore =
//       sum(region.threatLevel       for each destination region in route)
//     + sum(path.surveillanceLevel   for each path in route) * 3
//     + count(THREATENED paths) * 2
//     + count(BLOCKED paths)    * 5
//     + nazgulProximityCount    * 2
//
// nazgulProximityCount = number of Nazgul within 2 graph hops of any region in the route
//                        sourced from UnitKTable
//
// Output: enriched OrderValidated record re-emitted to game.orders.validated
//         with routeRiskScore, threatenedPaths[], blockedPaths[] attached.
package streams

// RouteRiskFormula documents the risk enrichment formula for reference.
// The same formula is implemented in pipeline/route_risk.go (Go pipeline).
var RouteRiskFormula = struct {
	ThreatLevelWeight      int
	SurveillanceWeight     int
	ThreatenedPathWeight   int
	BlockedPathWeight      int
	NazgulProximityWeight  int
	NazgulProximityHops    int
}{
	ThreatLevelWeight:     1,
	SurveillanceWeight:    3,
	ThreatenedPathWeight:  2,
	BlockedPathWeight:     5,
	NazgulProximityWeight: 2,
	NazgulProximityHops:   2,
}
