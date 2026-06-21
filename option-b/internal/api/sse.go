// Package api — sse.go implements Server-Sent Events for real-time game updates.
// One SSE goroutine per connected player. Light Side and Dark Side get different streams.
//
// B7 criterion: EventRouter enforces DarkView.RingBearerRegion always "".
// router_test.go -race must pass.
package api

import (
	"fmt"
	"log"
	"net/http"
)

// GET /events?playerId=X&side=light|dark
func (s *Server) handleSSE(w http.ResponseWriter, r *http.Request) {
	playerID := r.URL.Query().Get("playerId")
	side := r.URL.Query().Get("side")
	if playerID == "" || (side != "light" && side != "dark") {
		http.Error(w, `{"error":"playerId and side (light|dark) required"}`, http.StatusBadRequest)
		return
	}

	// SSE headers
	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("Connection", "keep-alive")
	w.Header().Set("X-Accel-Buffering", "no") // disable NGINX buffering

	flusher, ok := w.(http.Flusher)
	if !ok {
		http.Error(w, "streaming not supported", http.StatusInternalServerError)
		return
	}

	// Create per-player event channel
	playerEventCh := make(chan Event, 50)

	conn := PlayerConnection{
		PlayerID: playerID,
		Side:     side,
		EventCh:  playerEventCh,
	}

	// Register connection
	select {
	case s.newConnectionCh <- conn:
	default:
		log.Printf("[sse] newConnectionCh full, dropped registration for %s", playerID)
	}

	defer func() {
		select {
		case s.disconnectCh <- playerID:
		default:
		}
		log.Printf("[sse] player disconnected: %s", playerID)
	}()

	// Determine which SSE channel to read from based on side
	// The EventRouter already filters events per side — we forward them here.
	// Each player gets their own buffered channel created above.
	// The HTTP layer routes events from lightSideSSECh / darkSideSSECh to per-player channels.
	// (In this simplified implementation, we read from the shared side channel.)
	// In production: SSEManager would maintain a list of per-player channels.

	// Send initial connection event
	fmt.Fprintf(w, "event: connected\ndata: {\"playerId\":\"%s\",\"side\":\"%s\"}\n\n", playerID, side)
	flusher.Flush()

	// Stream events
	ctx := r.Context()
	for {
		select {
		case <-ctx.Done():
			return
		case ev, ok := <-playerEventCh:
			if !ok {
				return
			}
			fmt.Fprintf(w, "event: %s\ndata: %s\n\n", ev.Topic, ev.Payload)
			flusher.Flush()
		}
	}
}
