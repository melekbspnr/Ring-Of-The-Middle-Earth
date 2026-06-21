// Package tests — router_test.go
// Section 35: Required Unit Tests — 3 cases for EventRouter information hiding.
// Run with: go test ./tests/... -v -race
// CRITICAL: All 3 tests run with -race flag to detect data races.
// B7 criterion: DarkView.RingBearerRegion is always "".
package tests

import (
	"encoding/json"
	"testing"
	"time"

	"ring-of-the-middle-earth/internal/api"
	"ring-of-the-middle-earth/internal/game"
)

// buildWorldStateSnapshotPayload creates a WorldStateSnapshot JSON where
// the ring-bearer unit has a real currentRegion (simulating internal state).
func buildWorldStateSnapshotPayload(rbRegion string) []byte {
	snap := game.WorldStateSnapshot{
		Turn: 5,
		Units: []game.UnitPublic{
			{ID: "ring-bearer", CurrentRegion: rbRegion, Strength: 1, Status: "ACTIVE", Side: "FREE_PEOPLES"},
			{ID: "aragorn", CurrentRegion: "rivendell", Strength: 5, Status: "ACTIVE", Side: "FREE_PEOPLES"},
			{ID: "witch-king", CurrentRegion: "minas-morgul", Strength: 5, Status: "ACTIVE", Side: "SHADOW"},
		},
	}
	b, _ := json.Marshal(snap)
	return b
}

// runRouter spins up the EventRouter for the duration of the test.
func runRouter(
	eventCh chan api.Event,
	lightSSECh, darkSSECh, cacheCh, engineCh chan api.Event,
) (done func()) {
	doneCh := make(chan struct{})
	go api.EventRouter(eventCh, lightSSECh, darkSSECh, cacheCh, engineCh, doneCh, "ring-bearer")
	return func() { close(doneCh) }
}

// ─── Test Cases ───────────────────────────────────────────────────────────────

// Case 1: WorldStateSnapshot with ring-bearer region set →
//
//	Dark Side receives currentRegion="", Light Side receives real value "weathertop"
func TestRouter_WorldStateBroadcast_StripsDarkSide(t *testing.T) {
	eventCh := make(chan api.Event, 10)
	lightSSE := make(chan api.Event, 10)
	darkSSE := make(chan api.Event, 10)
	cacheCh := make(chan api.Event, 10)
	engineCh := make(chan api.Event, 10)

	stop := runRouter(eventCh, lightSSE, darkSSE, cacheCh, engineCh)
	defer stop()

	payload := buildWorldStateSnapshotPayload("weathertop")
	eventCh <- api.Event{Topic: "game.broadcast", Payload: payload}

	timeout := time.After(500 * time.Millisecond)

	var lightEv, darkEv api.Event
	received := 0
	for received < 2 {
		select {
		case ev := <-lightSSE:
			lightEv = ev
			received++
		case ev := <-darkSSE:
			darkEv = ev
			received++
		case <-timeout:
			t.Fatal("Case 1: timed out waiting for events")
		}
	}

	// Light Side: ring-bearer should have real region
	var lightSnap game.WorldStateSnapshot
	if err := json.Unmarshal(lightEv.Payload, &lightSnap); err != nil {
		t.Fatalf("Case 1: unmarshal light payload: %v", err)
	}
	for _, u := range lightSnap.Units {
		if u.ID == "ring-bearer" && u.CurrentRegion != "weathertop" {
			t.Errorf("Case 1: Light Side should see ring-bearer at 'weathertop', got '%s'", u.CurrentRegion)
		}
	}

	// Dark Side: ring-bearer.currentRegion must be ""
	var darkSnap game.WorldStateSnapshot
	if err := json.Unmarshal(darkEv.Payload, &darkSnap); err != nil {
		t.Fatalf("Case 1: unmarshal dark payload: %v", err)
	}
	for _, u := range darkSnap.Units {
		if u.ID == "ring-bearer" && u.CurrentRegion != "" {
			t.Errorf("Case 1: Dark Side MUST NOT see ring-bearer region — got '%s'", u.CurrentRegion)
		}
	}
}

// Case 2: RingBearerMoved event (game.ring.position) → never reaches Dark Side SSE channel
func TestRouter_RingBearerMoved_NeverReachesDarkSide(t *testing.T) {
	eventCh := make(chan api.Event, 10)
	lightSSE := make(chan api.Event, 10)
	darkSSE := make(chan api.Event, 10)
	cacheCh := make(chan api.Event, 10)
	engineCh := make(chan api.Event, 10)

	stop := runRouter(eventCh, lightSSE, darkSSE, cacheCh, engineCh)
	defer stop()

	eventCh <- api.Event{
		Topic:   "game.ring.position",
		Payload: []byte(`{"trueRegion":"weathertop","turn":5}`),
	}

	// Light Side SHOULD receive it
	select {
	case ev := <-lightSSE:
		if ev.Topic != "game.ring.position" {
			t.Errorf("Case 2: expected game.ring.position on light SSE, got %s", ev.Topic)
		}
	case <-time.After(300 * time.Millisecond):
		t.Error("Case 2: Light Side did not receive RingBearerMoved")
	}

	// Dark Side MUST NOT receive it — wait to confirm silence
	select {
	case ev := <-darkSSE:
		t.Errorf("Case 2: Dark Side received %s — MUST NOT receive game.ring.position", ev.Topic)
	case <-time.After(200 * time.Millisecond):
		// ✅ correct — dark side got nothing
	}
}

// Case 3: cache.DarkView.RingBearerRegion is always "" after any cache update
func TestRouter_DarkViewRingBearerRegionAlwaysEmpty(t *testing.T) {
	// Directly test the cache enforcement in UpdateDarkView and UpdateUnit
	cache := &game.WorldStateCache{RingBearerUnitID: "ring-bearer"}
	// Use the unexported zero-value test: simulate cache init
	snap := game.WorldStateCache{
		Units: map[string]game.UnitSnapshot{
			"ring-bearer": {ID: "ring-bearer", CurrentRegion: "", Strength: 1, Status: game.StatusActive},
		},
		DarkView: game.DarkSideView{RingBearerRegion: ""},
	}

	// Attempt to inject a real region into DarkView — this should be impossible
	// via the public API (UpdateDarkView enforces "")
	cache.UpdateDarkView("weathertop", 5)
	snap2 := cache.Snapshot()

	if snap2.DarkView.RingBearerRegion != "" {
		t.Errorf("Case 3: DarkView.RingBearerRegion MUST always be '' — got '%s'",
			snap2.DarkView.RingBearerRegion)
	}

	// Also verify via UpdateUnit that ring-bearer's public region is never set
	cache.UpdateUnit("ring-bearer", "weathertop", 1, game.StatusActive, nil)
	snap3 := cache.Snapshot()
	if rb, ok := snap3.Units["ring-bearer"]; ok {
		if rb.CurrentRegion != "" {
			t.Errorf("Case 3: ring-bearer.CurrentRegion MUST always be '' in public state — got '%s'",
				rb.CurrentRegion)
		}
	}
	_ = snap
}
