// Package api implements the EventRouter goroutine.
//
// CRITICAL: This is the single enforcement point for information asymmetry.
// Q&A Question 4: "Show where Ring Bearer position is removed before reaching Dark Side."
// Answer: This file, in the EventRouter select loop.
//
// go test -race must pass on router_test.go - no data races on channel sends.
package api

import (
	"log"

	"ring-of-the-middle-earth/internal/game"
)

// Event is defined as a type alias in types.go (from shared package).
// Do not redefine it here.

// EventRouter reads from the central event channel and routes to:
//   - lightSideSSECh: events Light Side players receive
//   - darkSideSSECh:  events Dark Side players receive (ring-bearer position stripped)
//   - cacheUpdateCh:  events that update WorldStateCache
//   - engineCh:       validated orders for TurnProcessor
//
// Information asymmetry rules (Section 30):
//
//	game.ring.position   -> lightSideSSECh ONLY
//	game.ring.detection  -> darkSideSSECh ONLY
//	game.broadcast       -> both, but Dark Side copy has ring-bearer.currentRegion=""
//	game.events.*        -> both (no ring bearer position in these topics)
//	game.orders.validated -> engineCh only (leader; nil = drop)
func EventRouter(
	eventCh <-chan Event,
	lightSideSSECh chan<- Event,
	darkSideSSECh chan<- Event,
	cacheUpdateCh chan<- Event,
	engineCh chan<- Event,
	doneCh <-chan struct{},
	ringBearerID string,
) {
	for {
		select {
		case <-doneCh:
			return
		case ev, ok := <-eventCh:
			if !ok {
				return
			}
			routeEvent(ev, lightSideSSECh, darkSideSSECh, cacheUpdateCh, engineCh, ringBearerID)
		}
	}
}

// routeEvent applies the routing rules defined in Section 30.
func routeEvent(
	ev Event,
	lightSideSSECh chan<- Event,
	darkSideSSECh chan<- Event,
	cacheUpdateCh chan<- Event,
	engineCh chan<- Event,
	ringBearerID string,
) {
	switch ev.Topic {
	case "game.ring.position":
		// Ring Bearer moved - Light Side ONLY.
		lightSideSSECh <- ev
		cacheUpdateCh <- ev

	case "game.ring.detection":
		// Detection event - Dark Side ONLY.
		darkSideSSECh <- ev
		cacheUpdateCh <- ev

	case "game.broadcast":
		// World state snapshot - both sides, but Dark Side copy is stripped.
		lightSideSSECh <- ev
		darkSideSSECh <- stripRingBearerPosition(ev, ringBearerID)
		cacheUpdateCh <- ev

	case "game.events.unit", "game.events.region", "game.events.path":
		// Unit/region/path events - both sides (no ring bearer position here).
		lightSideSSECh <- ev
		darkSideSSECh <- ev
		cacheUpdateCh <- ev

	case "game.orders.validated":
		if engineCh != nil {
			select {
			case engineCh <- ev:
			default:
				log.Printf("[EventRouter] engineCh full, dropping validated order")
			}
		}

	case "game.session":
		// Session state (including game-over) goes to cache AND both SSE streams
		// so the UI can react to game-over immediately.
		cacheUpdateCh <- ev
		lightSideSSECh <- ev
		darkSideSSECh <- ev

		// game.orders.raw and game.dlq are not consumed by this layer.
	}
}

// stripRingBearerPosition creates a copy of the event with the ring-bearer's
// currentRegion set to "" in the JSON payload.
//
// This is the ONLY place where ring-bearer region is actively removed.
// DarkSideView.RingBearerRegion is always "" - enforced here.
func stripRingBearerPosition(ev Event, ringBearerID string) Event {
	return Event{
		Topic:   ev.Topic,
		Key:     ev.Key,
		Payload: stripRingBearerFromJSON(ev.Payload, ringBearerID),
	}
}

// stripRingBearerFromJSON sets the currentRegion of the ring-bearer unit to ""
// in a WorldStateSnapshot JSON payload.
func stripRingBearerFromJSON(payload []byte, rbID string) []byte {
	// Full deserialization approach for correctness (performance optimization later).
	return zeroRingBearerRegion(payload, rbID)
}

// zeroRingBearerRegion zeroes the ring-bearer currentRegion field.
func zeroRingBearerRegion(payload []byte, rbID string) []byte {
	return game.StripRingBearerFromSnapshot(payload, rbID)
}
