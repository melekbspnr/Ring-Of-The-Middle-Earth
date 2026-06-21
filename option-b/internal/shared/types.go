// Package shared defines types shared across multiple packages to prevent import cycles.
// Any type needed by both api and pipeline (or kafka and api) should be placed here.
package shared

// Event represents a Kafka event consumed from a game topic.
type Event struct {
	Topic   string
	Payload []byte
	Key     string
}

// PlayerConnection is sent to main's newConnectionCh when an SSE client connects.
type PlayerConnection struct {
	PlayerID string
	Side     string // "light" or "dark"
	EventCh  chan Event
}

// AnalysisRequest is sent to analysisRequestCh when a player hits /analysis/* endpoints.
type AnalysisRequest struct {
	Type     string // "routes" or "intercept"
	PlayerID string
	Side     string
	ReplyCh  chan interface{} // typed reply sent back to HTTP handler
}
