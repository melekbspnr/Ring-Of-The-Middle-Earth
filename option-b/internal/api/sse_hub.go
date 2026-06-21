// Package api — sse_hub.go fans out Light/Dark SSE streams to all connected players.
package api

import (
	"sync"
)

type sseSub struct {
	playerID string
	side     string
	ch       chan<- Event
}

// RunSSEHub registers SSE clients and broadcasts Kafka-routed events to them.
func RunSSEHub(
	lightIn <-chan Event,
	darkIn <-chan Event,
	register <-chan PlayerConnection,
	unregister <-chan string,
	done <-chan struct{},
) {
	var mu sync.Mutex
	var subs []sseSub

	broadcast := func(side string, ev Event) {
		mu.Lock()
		defer mu.Unlock()
		for _, s := range subs {
			if s.side != side {
				continue
			}
			select {
			case s.ch <- ev:
			default:
				// slow client: drop
			}
		}
	}

	for {
		select {
		case <-done:
			return
		case c := <-register:
			mu.Lock()
			subs = append(subs, sseSub{playerID: c.PlayerID, side: c.Side, ch: c.EventCh})
			mu.Unlock()
		case pid := <-unregister:
			mu.Lock()
			out := subs[:0]
			for _, s := range subs {
				if s.playerID != pid {
					out = append(out, s)
				}
			}
			subs = out
			mu.Unlock()
		case ev := <-lightIn:
			broadcast("light", ev)
		case ev := <-darkIn:
			broadcast("dark", ev)
		}
	}
}
