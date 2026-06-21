// Package api — http_handlers.go implements the REST API from Section 34.
//
// Endpoints:
//
//	POST /game/start           — start a new game session
//	POST /order                — submit an order (produces to game.orders.raw)
//	GET  /game/state           — world state (ring-bearer stripped for Dark Side)
//	GET  /orders/available     — available orders for a unit
//	GET  /analysis/routes      — Pipeline 1 result (Light Side only)
//	GET  /analysis/intercept   — Pipeline 2 result (Dark Side only)
//	GET  /events               — SSE stream
//	GET  /health               — 200 or 503
package api

import (
	"encoding/json"
	"net/http"
	"strings"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/go-chi/chi/v5/middleware"
	"ring-of-the-middle-earth/internal/config"
	"ring-of-the-middle-earth/internal/game"
	"ring-of-the-middle-earth/internal/pipeline"
)

// Producer captures the Kafka operations the HTTP layer needs.
type Producer interface {
	Produce(topic, key string, value []byte) error
	ProduceToRaw(playerID string, payload []byte) error
}

// Server holds handler dependencies.
type Server struct {
	cache             *game.WorldStateCache
	producer          Producer
	gameCfg           *config.GameConfig
	mapCfg            *config.MapConfig
	graph             *game.Graph
	newConnectionCh   chan<- PlayerConnection
	disconnectCh      chan<- string
	analysisRequestCh chan<- AnalysisRequest
	p1                *pipeline.RouteRiskDispatcher
	p2                *pipeline.InterceptDispatcher
	onGameStart       func()
}

// NewRouter creates the chi router with all routes wired.
func NewRouter(
	cache *game.WorldStateCache,
	producer Producer,
	gameCfg *config.GameConfig,
	mapCfg *config.MapConfig,
	graph *game.Graph,
	newConnectionCh chan<- PlayerConnection,
	disconnectCh chan<- string,
	analysisRequestCh chan<- AnalysisRequest,
	p1 *pipeline.RouteRiskDispatcher,
	p2 *pipeline.InterceptDispatcher,
	onGameStart func(),
) http.Handler {
	s := &Server{
		cache:             cache,
		producer:          producer,
		gameCfg:           gameCfg,
		mapCfg:            mapCfg,
		graph:             graph,
		newConnectionCh:   newConnectionCh,
		disconnectCh:      disconnectCh,
		analysisRequestCh: analysisRequestCh,
		p1:                p1,
		p2:                p2,
		onGameStart:       onGameStart,
	}

	r := chi.NewRouter()
	r.Use(middleware.Logger)
	r.Use(middleware.Recoverer)
	r.Use(corsMiddleware)

	r.Post("/game/start", s.handleGameStart)
	r.Post("/order", s.handleSubmitOrder)
	r.Get("/game/state", s.handleGameState)
	r.Get("/orders/available", s.handleAvailableOrders)
	r.Get("/analysis/routes", s.handleAnalysisRoutes)
	r.Get("/analysis/intercept", s.handleAnalysisIntercept)
	r.Get("/events", s.handleSSE)
	r.Get("/health", s.handleHealth)
	r.Mount("/debug", middleware.Profiler())

	return r
}

// ─── Handlers ─────────────────────────────────────────────────────────────────

// POST /game/start
func (s *Server) handleGameStart(w http.ResponseWriter, r *http.Request) {
	var body struct {
		Mode string `json:"mode"` // "HVH"
	}
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil || body.Mode != "HVH" {
		http.Error(w, `{"error":"invalid mode"}`, http.StatusBadRequest)
		return
	}
	prevSession := s.cache.Snapshot().Session
	s.cache.ResetFromConfig(s.gameCfg, s.mapCfg)
	s.cache.InitRingBearerState(game.RingBearerStartRegion(s.gameCfg))
	now := time.Now().UnixMilli()
	epoch := prevSession.Epoch + 1
	gameOver := false
	gameOverWinner := ""
	gameOverCause := ""
	gameOverTurn := 0
	s.cache.UpdateSession(1, &prevSession.LeaderID, &now, &epoch, &gameOver, &gameOverWinner, &gameOverCause, &gameOverTurn)
	if s.onGameStart != nil {
		s.onGameStart()
	}
	rb := s.cache.RingBearerSnapshot()
	snap := s.cache.Snapshot()
	b, err := game.MarshalWorldBroadcast(snap, rb, snap.Turn, s.mapCfg, s.gameCfg)
	if err == nil {
		_ = s.producer.Produce("game.broadcast", "world", b)
		sess, _ := json.Marshal(map[string]interface{}{
			"turn":              snap.Turn,
			"epoch":             snap.Session.Epoch,
			"leaderId":          snap.Session.LeaderID,
			"leaderHeartbeatTs": now,
			"gameOver":          false,
			"gameOverWinner":    "",
			"gameOverCause":     "",
			"gameOverTurn":      0,
		})
		_ = s.producer.Produce("game.session", "session", sess)
	}
	writeJSON(w, http.StatusOK, map[string]string{"status": "started", "mode": "HVH"})
}

// POST /order — produces to game.orders.raw (202 Accepted, fire-and-forget)
func (s *Server) handleSubmitOrder(w http.ResponseWriter, r *http.Request) {
	playerID := r.URL.Query().Get("playerId")
	if playerID == "" {
		playerID = r.Header.Get("X-Player-Id")
	}

	var body map[string]interface{}
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		http.Error(w, `{"error":"invalid JSON"}`, http.StatusBadRequest)
		return
	}
	body["timestamp"] = time.Now().UnixMilli()

	payload, _ := json.Marshal(body)
	if err := s.producer.ProduceToRaw(playerID, payload); err != nil {
		http.Error(w, `{"error":"kafka unavailable"}`, http.StatusServiceUnavailable)
		return
	}

	writeJSON(w, http.StatusAccepted, map[string]string{"status": "accepted"})
}

// GET /game/state — returns world state; ring-bearer stripped for Dark Side
func (s *Server) handleGameState(w http.ResponseWriter, r *http.Request) {
	side := sideFromRequest(r)
	snap := s.cache.Snapshot()
	rbID := snap.RingBearerUnitID
	if rbID == "" {
		rbID = game.RingBearerID(s.gameCfg)
	}

	// Build public unit list
	var units []game.UnitPublic
	for id, u := range snap.Units {
		pub := game.UnitPublic{
			ID:       id,
			Status:   string(u.Status),
			Strength: u.Strength,
		}
		cfg := s.gameCfg.Units[id]
		pub.Side = string(cfg.Side)

		if side == "light" {
			// Light Side sees ring-bearer's true region from LightView
			if id == rbID {
				pub.CurrentRegion = snap.LightView.RingBearerRegion
			} else {
				pub.CurrentRegion = u.CurrentRegion
			}
		} else {
			// Dark Side: ring-bearer.currentRegion is ALWAYS ""
			pub.CurrentRegion = u.CurrentRegion // already "" for ring-bearer (UpdateUnit enforces)
		}
		units = append(units, pub)
	}

	var regions []game.RegionSnapshot
	for id, reg := range snap.Regions {
		regions = append(regions, game.RegionSnapshot{
			ID:           id,
			ControlledBy: string(reg.ControlledBy),
			ThreatLevel:  reg.ThreatLevel,
			Fortified:    reg.Fortified,
		})
	}

	var paths []game.BroadcastPath
	for id, p := range snap.Paths {
		paths = append(paths, game.BroadcastPath{
			ID:                id,
			NewStatus:         string(p.Status),
			SurveillanceLevel: p.SurveillanceLevel,
			TempOpenTurns:     p.TempOpenTurns,
		})
	}

	out := map[string]interface{}{
		"turn":             snap.Turn,
		"units":            units,
		"regions":          regions,
		"paths":            paths,
		"gameOver":         snap.Session.GameOver,
		"gameOverWinner":   snap.Session.GameOverWinner,
		"gameOverCause":    snap.Session.GameOverCause,
		"gameOverTurn":     snap.Session.GameOverTurn,
	}
	if side == "light" {
		out["ringBearerTrueRegion"] = snap.LightView.RingBearerRegion
	}

	writeJSON(w, http.StatusOK, out)
}

// GET /orders/available?unitId=X&playerId=Y
func (s *Server) handleAvailableOrders(w http.ResponseWriter, r *http.Request) {
	unitID := r.URL.Query().Get("unitId")
	playerID := r.URL.Query().Get("playerId")
	if unitID == "" || playerID == "" {
		http.Error(w, `{"error":"unitId and playerId required"}`, http.StatusBadRequest)
		return
	}

	snap := s.cache.Snapshot()
	orders := availableOrders(unitID, playerID, snap, s.gameCfg, s.graph)
	writeJSON(w, http.StatusOK, map[string]interface{}{"orders": orders})
}

// GET /analysis/routes — Pipeline 1 (Light Side only)
func (s *Server) handleAnalysisRoutes(w http.ResponseWriter, r *http.Request) {
	side := sideFromRequest(r)
	if side != "light" {
		http.Error(w, `{"error":"light side only"}`, http.StatusForbidden)
		return
	}

	playerID := r.URL.Query().Get("playerId")
	req := AnalysisRequest{Type: "routes", PlayerID: playerID, Side: side}
	s.p1.Trigger(req)

	// Wait briefly for result (Pipeline has 2s timeout)
	timer := time.NewTimer(2100 * time.Millisecond)
	defer timer.Stop()

	for {
		select {
		case <-r.Context().Done():
			http.Error(w, `{"error":"request cancelled"}`, http.StatusGatewayTimeout)
			return
		case <-timer.C:
			http.Error(w, `{"error":"pipeline timeout"}`, http.StatusGatewayTimeout)
			return
		default:
			if result, ok := s.p1.Result(); ok {
				writeJSON(w, http.StatusOK, result)
				return
			}
			time.Sleep(50 * time.Millisecond)
		}
	}
}

// GET /analysis/intercept — Pipeline 2 (Dark Side only)
func (s *Server) handleAnalysisIntercept(w http.ResponseWriter, r *http.Request) {
	side := sideFromRequest(r)
	if side != "dark" {
		http.Error(w, `{"error":"dark side only"}`, http.StatusForbidden)
		return
	}

	playerID := r.URL.Query().Get("playerId")
	req := AnalysisRequest{Type: "intercept", PlayerID: playerID, Side: side}
	s.p2.Trigger(req)

	timer := time.NewTimer(2100 * time.Millisecond)
	defer timer.Stop()

	for {
		select {
		case <-r.Context().Done():
			http.Error(w, `{"error":"request cancelled"}`, http.StatusGatewayTimeout)
			return
		case <-timer.C:
			http.Error(w, `{"error":"pipeline timeout"}`, http.StatusGatewayTimeout)
			return
		default:
			if result, ok := s.p2.Result(); ok {
				writeJSON(w, http.StatusOK, result)
				return
			}
			time.Sleep(50 * time.Millisecond)
		}
	}
}

// GET /health
func (s *Server) handleHealth(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
}

// ─── Available orders logic ───────────────────────────────────────────────────

// availableOrders returns valid order types for a unit based on current game state.
// This is what the UI calls when a player clicks a unit.
func availableOrders(unitID, playerID string, snap game.WorldStateCache, gameCfg *config.GameConfig, graph *game.Graph) []string {
	cfg, ok := gameCfg.Units[unitID]
	if !ok {
		return nil
	}
	if cfg.Side != sideForPlayer(playerID) {
		return nil
	}

	u, ok := snap.Units[unitID]
	if !ok || u.Status != game.StatusActive {
		return nil
	}

	var orders []string
	orders = append(orders, "ASSIGN_ROUTE", "REDIRECT_UNIT")

	if cfg.CanFortify {
		orders = append(orders, "FORTIFY_REGION")
	}
	if cfg.Maia && u.Cooldown == 0 {
		orders = append(orders, "MAIA_ABILITY")
	}
	if cfg.Side == config.SideShadow {
		orders = append(orders, "BLOCK_PATH", "SEARCH_PATH")
		if cfg.DetectionRange > 0 { // config-driven: Nazgul can DeployNazgul
			orders = append(orders, "DEPLOY_NAZGUL")
		}
	}

	// Can attack adjacent enemy regions
	for _, edge := range graph.Neighbours(u.CurrentRegion) {
		if r, ok := snap.Regions[edge.To]; ok {
			if string(r.ControlledBy) != string(cfg.Side) && string(r.ControlledBy) != "NEUTRAL" {
				orders = append(orders, "ATTACK_REGION")
				break
			}
		}
	}

	// Ring Bearer: true region only in LightView (public unit region is always "")
	if cfg.Class == "RingBearer" {
		rbReg := snap.LightView.RingBearerRegion
		if rbReg == "mount-doom" {
			orders = append(orders, "DESTROY_RING")
		}
	}

	return orders
}

func sideForPlayer(playerID string) config.Side {
	playerID = strings.ToLower(strings.TrimSpace(playerID))
	if strings.Contains(playerID, "dark") {
		return config.SideShadow
	}
	return config.SideFreePeoples
}

// ─── Helpers ──────────────────────────────────────────────────────────────────

func writeJSON(w http.ResponseWriter, code int, v interface{}) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	json.NewEncoder(w).Encode(v)
}

func sideFromRequest(r *http.Request) string {
	side := r.URL.Query().Get("side")
	if side == "" {
		side = r.Header.Get("X-Side")
	}
	return side
}

func corsMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Access-Control-Allow-Origin", "*")
		w.Header().Set("Access-Control-Allow-Methods", "GET, POST, OPTIONS")
		w.Header().Set("Access-Control-Allow-Headers", "Content-Type, X-Player-Id, X-Side")
		if r.Method == http.MethodOptions {
			w.WriteHeader(http.StatusNoContent)
			return
		}
		next.ServeHTTP(w, r)
	})
}
