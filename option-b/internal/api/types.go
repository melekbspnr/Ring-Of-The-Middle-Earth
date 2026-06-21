// Package api — types.go re-exports shared types for backward compatibility.
// The canonical definitions live in internal/shared to break import cycles.
package api

import "ring-of-the-middle-earth/internal/shared"

// Type aliases — api.Event, api.PlayerConnection, api.AnalysisRequest
// continue to work everywhere that already imports the api package.
type Event = shared.Event
type PlayerConnection = shared.PlayerConnection
type AnalysisRequest = shared.AnalysisRequest
