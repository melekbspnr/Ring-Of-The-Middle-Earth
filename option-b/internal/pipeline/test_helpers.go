// Package pipeline — test_helpers.go provides exported test helpers.
package pipeline

import (
	"ring-of-the-middle-earth/internal/shared"
)

// ComputeForTest runs Pipeline 1 synchronously with the current cache state.
func (d *RouteRiskDispatcher) ComputeForTest() RankedRouteList {
	return d.compute(shared.AnalysisRequest{Type: "routes"})
}

// ComputeForTest runs Pipeline 2 synchronously with the current cache state.
func (d *InterceptDispatcher) ComputeForTest() InterceptPlan {
	return d.compute(shared.AnalysisRequest{Type: "intercept"})
}
