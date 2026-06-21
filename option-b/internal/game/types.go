// Package game — types.go defines all shared game state types.
// These are the in-memory representations derived from Kafka KTable state stores.
package game

import "sync"

// ─── Status enums ─────────────────────────────────────────────────────────────

// UnitStatus represents a unit's lifecycle state.
type UnitStatus string

const (
	StatusActive     UnitStatus = "ACTIVE"
	StatusDestroyed  UnitStatus = "DESTROYED"
	StatusRespawning UnitStatus = "RESPAWNING"
)

// PathStatus represents the current traversal state of a path.
type PathStatus string

const (
	PathOpen            PathStatus = "OPEN"
	PathThreatened      PathStatus = "THREATENED"
	PathBlocked         PathStatus = "BLOCKED"
	PathTemporarilyOpen PathStatus = "TEMPORARILY_OPEN"
)

// Controller represents which side controls a region.
type Controller string

const (
	ControlFreePeoples Controller = "FREE_PEOPLES"
	ControlShadow      Controller = "SHADOW"
	ControlNeutral     Controller = "NEUTRAL"
)

// ─── Unit state ───────────────────────────────────────────────────────────────

// UnitSnapshot is the live state of a unit, maintained via KTable.
// For the Ring Bearer, CurrentRegion is always "" in public state.
type UnitSnapshot struct {
	ID                   string
	CurrentRegion        string // always "" for ring-bearer in public state
	Strength             int
	Status               UnitStatus
	RespawnTurns         int
	Route                []string // ordered list of path IDs
	RouteIdx             int      // index into Route
	TravelPathID         string   // path currently being traversed when cost > 1
	TravelTurnsRemaining int
	Cooldown             int
}

// ─── Path state ───────────────────────────────────────────────────────────────

// PathState is the live state of a map path.
type PathState struct {
	ID                string
	Status            PathStatus
	SurveillanceLevel int    // 0–3
	TempOpenTurns     int    // countdown for TEMPORARILY_OPEN
	BlockedByUnitID   string // unitID of blocking unit, or ""
}

// ─── Region state ─────────────────────────────────────────────────────────────

// RegionState is the live state of a map region.
type RegionState struct {
	ID           string
	ControlledBy Controller
	ThreatLevel  int
	Fortified    bool
	FortifyTurns int
	UnitsPresent []string // unit IDs currently in this region
}

// ─── Ring Bearer state ────────────────────────────────────────────────────────

// RingBearerState is stored only in RingBearerKTable.
// TrueRegion must NEVER be sent to any shared topic or Dark Side client.
type RingBearerState struct {
	TrueRegion           string
	Exposed              bool
	Route                []string // path IDs
	RouteIdx             int
	TravelPathID         string
	TravelTurnsRemaining int
	LastDetectedTurn     int
	LastDetectedRegion   string
}

// ─── World State Cache ────────────────────────────────────────────────────────

// WorldStateCache is the in-memory aggregation of all KTable views.
// Owned by the CacheManager goroutine. Workers receive value copies — never pointers.
type WorldStateCache struct {
	mu sync.RWMutex

	Turn             int
	Session          SessionState
	RingBearerUnitID string
	Units            map[string]UnitSnapshot
	Regions          map[string]RegionState
	Paths            map[string]PathState
	LightView        LightSideView
	DarkView         DarkSideView
}

// LightSideView holds Ring Bearer information visible only to the Light Side.
type LightSideView struct {
	RingBearerRegion string
	AssignedRoute    []string
	RouteIdx         int
}

// DarkSideView holds Ring Bearer information for the Dark Side.
// RingBearerRegion is ALWAYS "" — no code path may ever set this to a real region.
type DarkSideView struct {
	RingBearerRegion   string // ALWAYS ""
	LastDetectedRegion string
	LastDetectedTurn   int
}

// SessionState mirrors the compacted game.session topic.
type SessionState struct {
	LeaderID          string
	LeaderHeartbeatTs int64
	Epoch             int64
	GameOver          bool
	GameOverWinner    string
	GameOverCause     string
	GameOverTurn      int
}

// Snapshot returns a deep copy of the cache for safe concurrent use.
// Workers operate on copies — never on the shared cache directly.
func (c *WorldStateCache) Snapshot() WorldStateCache {
	c.mu.RLock()
	defer c.mu.RUnlock()

	snap := WorldStateCache{
		Turn:             c.Turn,
		Session:          c.Session,
		RingBearerUnitID: c.RingBearerUnitID,
		LightView:        c.LightView,
		DarkView: DarkSideView{
			RingBearerRegion:   "", // enforced: always empty
			LastDetectedRegion: c.DarkView.LastDetectedRegion,
			LastDetectedTurn:   c.DarkView.LastDetectedTurn,
		},
		Units:   make(map[string]UnitSnapshot, len(c.Units)),
		Regions: make(map[string]RegionState, len(c.Regions)),
		Paths:   make(map[string]PathState, len(c.Paths)),
	}

	for k, v := range c.Units {
		u := v
		if r := copyRoute(u.Route); r != nil {
			u.Route = r
		}
		snap.Units[k] = u
	}
	for k, v := range c.Regions {
		r := v
		up := make([]string, len(v.UnitsPresent))
		copy(up, v.UnitsPresent)
		r.UnitsPresent = up
		snap.Regions[k] = r
	}
	for k, v := range c.Paths {
		snap.Paths[k] = v
	}
	return snap
}

func copyRoute(r []string) []string {
	if r == nil {
		return nil
	}
	out := make([]string, len(r))
	copy(out, r)
	return out
}
