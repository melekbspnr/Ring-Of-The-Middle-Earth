// cmd/server/main.go - Entry point for the Ring of the Middle Earth Go game server.
package main

import (
	"context"
	"encoding/json"
	"log"
	"net/http"
	"os"
	"os/signal"
	goruntime "runtime"
	"strings"
	"syscall"
	"time"

	"ring-of-the-middle-earth/internal/api"
	"ring-of-the-middle-earth/internal/config"
	"ring-of-the-middle-earth/internal/engine"
	"ring-of-the-middle-earth/internal/game"
	"ring-of-the-middle-earth/internal/kafkaclient"
	"ring-of-the-middle-earth/internal/pipeline"
	"ring-of-the-middle-earth/internal/shared"
)

const (
	leaderElectionInterval = 3 * time.Second
	leaderHeartbeatTimeout = 15 * time.Second
)

type leaderRuntime struct {
	instanceID string
	brokers    string
	cache      *game.WorldStateCache
	gameCfg    *config.GameConfig
	mapCfg     *config.MapConfig
	graph      *game.Graph
	producer   *kafkaclient.Producer
	kafkaRawCh chan<- kafkaclient.KafkaMessage
	engineCh   chan api.Event

	active   bool
	doneCh   chan struct{}
	consumer *kafkaclient.Consumer
	tp       *engine.TurnProcessor
}

func main() {
	configPath := envOrDefault("CONFIG_PATH", "../../config")

	gameCfg, err := config.LoadGameConfig(configPath + "/units.conf")
	if err != nil {
		log.Fatalf("load units config: %v", err)
	}
	mapCfg, err := config.LoadMapConfig(configPath + "/map.conf")
	if err != nil {
		log.Fatalf("load map config: %v", err)
	}

	graph := game.NewGraph()
	for _, p := range mapCfg.Paths {
		graph.AddPath(p.ID, p.From, p.To, p.Cost)
	}

	cache := game.NewWorldStateCache(gameCfg, mapCfg)
	cache.InitRingBearerState(game.RingBearerStartRegion(gameCfg))

	kafkaRawCh := make(chan kafkaclient.KafkaMessage, 400)
	kafkaEventCh := make(chan api.Event, 200)
	lightSideSSECh := make(chan api.Event, 200)
	darkSideSSECh := make(chan api.Event, 200)
	cacheUpdateCh := make(chan api.Event, 200)
	engineCh := make(chan api.Event, 200)
	newConnectionCh := make(chan api.PlayerConnection, 10)
	disconnectCh := make(chan string, 10)
	analysisRequestCh := make(chan api.AnalysisRequest, 20)
	doneCh := make(chan struct{})

	brokers := envOrDefault("KAFKA_BROKERS", "localhost:9092")
	instanceID := envOrDefault("INSTANCE_ID", "local")
	consumerGroup := envOrDefault("CONSUMER_GROUP", "rotr-game-engine")
	stateConsumerGroup := envOrDefault("STATE_CONSUMER_GROUP", "rotr-state-"+instanceID)
	bootstrapLeaderID := strings.TrimSpace(os.Getenv("LEADER_INSTANCE_ID"))
	// B2 Requirement: Shared Consumer Group to demonstrate rebalancing on node crash
	clusterGroup := envOrDefault("CLUSTER_CONSUMER_GROUP", "rotr-go-cluster")
	clusterConsumer, err := kafkaclient.NewConsumer(brokers, clusterGroup, instanceID+"-cluster", []string{"game.session"})
	if err == nil {
		dummyCh := make(chan kafkaclient.KafkaMessage, 100)
		go clusterConsumer.Run(dummyCh, doneCh)
		go func() {
			for {
				select {
				case <-dummyCh: // drain
				case <-doneCh:
					clusterConsumer.Close()
					return
				}
			}
		}()
	}

	stateConsumer, err := kafkaclient.NewConsumer(brokers, stateConsumerGroup, instanceID+"-state", kafkaStateTopics())
	if err != nil {
		log.Fatalf("kafka state consumer: %v", err)
	}
	go stateConsumer.Run(kafkaRawCh, doneCh)

	go func() {
		for {
			select {
			case <-doneCh:
				return
			case msg, ok := <-kafkaRawCh:
				if !ok {
					return
				}
				kafkaEventCh <- api.Event{
					Topic:   msg.Topic,
					Payload: msg.Payload,
					Key:     msg.Key,
				}
			}
		}
	}()

	kProducer, err := kafkaclient.NewProducer(brokers)
	if err != nil {
		log.Fatalf("kafka producer: %v", err)
	}

	rawOrderKafkaCh := make(chan kafkaclient.KafkaMessage, 200)
	rawOrderCh := make(chan shared.Event, 200)
	orderIngestGroup := envOrDefault("ORDER_INGEST_GROUP", "rotr-order-ingest")
	orderConsumer, err := kafkaclient.NewConsumer(brokers, orderIngestGroup, instanceID+"-order-ingest", []string{"game.orders.raw"})
	if err != nil {
		log.Fatalf("kafka order ingest consumer: %v", err)
	}
	go orderConsumer.Run(rawOrderKafkaCh, doneCh)
	go func() {
		for {
			select {
			case <-doneCh:
				return
			case msg, ok := <-rawOrderKafkaCh:
				if !ok {
					return
				}
				rawOrderCh <- shared.Event{Topic: msg.Topic, Payload: msg.Payload, Key: msg.Key}
			}
		}
	}()
	go kafkaclient.RunOrderIngest(rawOrderCh, cache, gameCfg, mapCfg, graph, kProducer, doneCh)

	go api.EventRouter(kafkaEventCh, lightSideSSECh, darkSideSSECh, cacheUpdateCh, engineCh, doneCh, game.RingBearerID(gameCfg))
	go api.RunCacheManager(cache, cacheUpdateCh, doneCh)
	go api.RunSSEHub(lightSideSSECh, darkSideSSECh, newConnectionCh, disconnectCh, doneCh)

	p1Dispatcher := pipeline.NewRouteRiskDispatcher(cache, graph, gameCfg)
	go p1Dispatcher.Run(doneCh)
	p2Dispatcher := pipeline.NewInterceptDispatcher(cache, graph, gameCfg)
	go p2Dispatcher.Run(doneCh)

	runtime := &leaderRuntime{
		instanceID: instanceID,
		brokers:    brokers,
		cache:      cache,
		gameCfg:    gameCfg,
		mapCfg:     mapCfg,
		graph:      graph,
		producer:   kProducer,
		kafkaRawCh: kafkaRawCh,
		engineCh:   engineCh,
	}

	onGameStart := func() {
		if runtime.active {
			runtime.tp.Reset()
		}
	}

	httpPort := envOrDefault("HTTP_PORT", "8080")
	router := api.NewRouter(cache, kProducer, gameCfg, mapCfg, graph,
		newConnectionCh, disconnectCh, analysisRequestCh,
		p1Dispatcher, p2Dispatcher, onGameStart)

	srv := &http.Server{
		Addr:    ":" + httpPort,
		Handler: router,
	}
	go func() {
		log.Printf("[server] instance=%s listening on :%s", instanceID, httpPort)
		log.Printf("[server] state-group=%s work-group=%s bootstrap-leader=%s", stateConsumerGroup, consumerGroup, bootstrapLeaderID)
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatalf("http server: %v", err)
		}
	}()

	processStart := time.Now()
	turnDuration := time.Duration(gameCfg.TurnDurationSeconds) * time.Second
	turnTicker := time.NewTicker(turnDuration)
	electionTicker := time.NewTicker(leaderElectionInterval)
	// B9: state logging ticker for monitoring goroutine health
	stateLogTicker := time.NewTicker(30 * time.Second)
	// B9: goroutine monitor — detect leaks; evidence via pprof
	goroutineMonitorTicker := time.NewTicker(60 * time.Second)
	defer turnTicker.Stop()
	defer electionTicker.Stop()
	defer stateLogTicker.Stop()
	defer goroutineMonitorTicker.Stop()

	// Bootstrap a preferred leader quickly on a fresh cluster.
	bootstrapLeadership(cache, kProducer, instanceID, bootstrapLeaderID)

	signalCh := make(chan os.Signal, 1)
	signal.Notify(signalCh, syscall.SIGINT, syscall.SIGTERM)

	// B9: Select loop — all 7 cases handled:
	// 1. analysisRequestCh  — Pipeline trigger dispatch
	// 2. turnTicker.C       — Turn advancement (leader only)
	// 3. electionTicker.C   — Leader election heartbeat
	// 4. signalCh           — OS signal graceful shutdown
	// 5. doneCh             — Programmatic shutdown
	// 6. stateLogTicker.C   — State logging / monitoring
	// 7. goroutineMonitorTicker.C — Goroutine leak detection
	for {
		select {
		case req := <-analysisRequestCh: // Case 1: Pipeline triggers
			if req.Type == "routes" {
				p1Dispatcher.Trigger(req)
			} else if req.Type == "intercept" {
				p2Dispatcher.Trigger(req)
			}

		case <-turnTicker.C: // Case 2: Turn advancement
			if runtime.active {
				runtime.tp.AdvanceTurn()
				if err := runtime.CommitProcessedOffsets(); err != nil {
					log.Printf("[leader] commit processed offsets: %v", err)
				}
			}

		case <-electionTicker.C: // Case 3: Leader election
			manageLeadership(cache, kProducer, runtime, consumerGroup, processStart, bootstrapLeaderID)

		case sig := <-signalCh: // Case 4: OS signal
			log.Printf("[main] signal %v - shutting down", sig)
			goto shutdown

		case <-doneCh: // Case 5: Programmatic shutdown
			log.Println("[main] doneCh closed — shutting down")
			goto shutdown

		case <-stateLogTicker.C: // Case 6: State logging
			snap := cache.Snapshot()
			log.Printf("[state] turn=%d leader=%s gameOver=%v units=%d",
				snap.Turn, snap.Session.LeaderID, snap.Session.GameOver, len(snap.Units))

		case <-goroutineMonitorTicker.C: // Case 7: Goroutine monitor
			numGoroutines := goruntime.NumGoroutine()
			log.Printf("[goroutine-monitor] active goroutines: %d", numGoroutines)
			if numGoroutines > 100 {
				log.Printf("[goroutine-monitor] WARNING: high goroutine count (%d) — potential leak", numGoroutines)
			}
		}
	}

shutdown:
	close(doneCh)
	runtime.Stop()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := srv.Shutdown(ctx); err != nil {
		log.Printf("[main] http shutdown error: %v", err)
	}
	stateConsumer.Close()
	orderConsumer.Close()
	kProducer.Close()
	log.Println("[main] graceful shutdown complete")
}

func (lr *leaderRuntime) Start(workConsumerGroup string) error {
	if lr.active {
		return nil
	}

	consumer, err := kafkaclient.NewManualCommitConsumer(lr.brokers, workConsumerGroup, lr.instanceID+"-work", kafkaWorkTopics())
	if err != nil {
		return err
	}
	doneCh := make(chan struct{})
	tp := engine.NewTurnProcessor(lr.gameCfg, lr.mapCfg, lr.graph, lr.cache, lr.producer, kafkaclient.ProduceExactlyOnce, lr.brokers, lr.instanceID)

	lr.doneCh = doneCh
	lr.consumer = consumer
	lr.tp = tp
	lr.active = true

	go consumer.Run(lr.kafkaRawCh, doneCh)
	go tp.Run(lr.engineCh, doneCh)

	log.Printf("[leader] instance=%s activated leader runtime", lr.instanceID)
	return nil
}

func (lr *leaderRuntime) CommitProcessedOffsets() error {
	if !lr.active || lr.consumer == nil {
		return nil
	}
	return lr.consumer.Commit()
}

func (lr *leaderRuntime) Stop() {
	if !lr.active {
		return
	}
	log.Printf("[leader] instance=%s stopping leader runtime", lr.instanceID)
	close(lr.doneCh)
	lr.consumer.Close()
	lr.doneCh = nil
	lr.consumer = nil
	lr.tp = nil
	lr.active = false
}

func bootstrapLeadership(cache *game.WorldStateCache, producer *kafkaclient.Producer, instanceID, bootstrapLeaderID string) {
	if bootstrapLeaderID != "" && bootstrapLeaderID != instanceID {
		return
	}
	snap := cache.Snapshot()
	if snap.Session.LeaderID != "" {
		return
	}
	if ts, err := publishSessionState(producer, snap.Turn, instanceID, snap.Session, snap.Session.Epoch+1); err == nil {
		epoch := snap.Session.Epoch + 1
		cache.UpdateSession(snap.Turn, &instanceID, &ts, &epoch, nil, nil, nil, nil)
	}
}

func manageLeadership(
	cache *game.WorldStateCache,
	producer *kafkaclient.Producer,
	runtime *leaderRuntime,
	workConsumerGroup string,
	processStart time.Time,
	bootstrapLeaderID string,
) {
	snap := cache.Snapshot()
	now := time.Now()
	nowMs := now.UnixMilli()
	stale := sessionStale(snap.Session, nowMs)

	if runtime.active {
		if snap.Session.LeaderID != runtime.instanceID && !stale {
			runtime.Stop()
			return
		}
		if ts, err := publishSessionState(producer, snap.Turn, runtime.instanceID, snap.Session, snap.Session.Epoch); err == nil {
			leaderID := runtime.instanceID
			epoch := snap.Session.Epoch
			cache.UpdateSession(snap.Turn, &leaderID, &ts, &epoch, nil, nil, nil, nil)
		}
		return
	}

	if snap.Session.LeaderID == runtime.instanceID {
		if err := runtime.Start(workConsumerGroup); err != nil {
			log.Printf("[leader] start runtime: %v", err)
		}
		return
	}

	if !stale {
		return
	}

	if !claimWindowReached(runtime.instanceID, bootstrapLeaderID, processStart, now) {
		return
	}
	if ts, err := publishSessionState(producer, snap.Turn, runtime.instanceID, snap.Session, snap.Session.Epoch+1); err == nil {
		leaderID := runtime.instanceID
		epoch := snap.Session.Epoch + 1
		cache.UpdateSession(snap.Turn, &leaderID, &ts, &epoch, nil, nil, nil, nil)
	}
}

func sessionStale(session game.SessionState, nowMs int64) bool {
	if session.LeaderID == "" || session.LeaderHeartbeatTs == 0 {
		return true
	}
	return nowMs-session.LeaderHeartbeatTs > leaderHeartbeatTimeout.Milliseconds()
}

func claimWindowReached(instanceID, bootstrapLeaderID string, processStart, now time.Time) bool {
	if bootstrapLeaderID != "" && bootstrapLeaderID == instanceID {
		return true
	}
	if bootstrapLeaderID != "" && bootstrapLeaderID != instanceID {
		// Give the preferred bootstrap instance one full heartbeat window to
		// publish and stabilize before any standby node claims leadership.
		return now.Sub(processStart) >= leaderHeartbeatTimeout
	}
	delayMs := int64(1500 + leaderClaimJitter(instanceID))
	return now.Sub(processStart).Milliseconds() >= delayMs
}

func leaderClaimJitter(instanceID string) int {
	sum := 0
	for _, ch := range instanceID {
		sum += int(ch)
	}
	return sum % 1500
}

func publishSessionState(producer *kafkaclient.Producer, turn int, leaderID string, session game.SessionState, epoch int64) (int64, error) {
	now := time.Now().UnixMilli()
	b, err := json.Marshal(map[string]interface{}{
		"turn":              turn,
		"leaderId":          leaderID,
		"leaderHeartbeatTs": now,
		"epoch":             epoch,
		"gameOver":          session.GameOver,
		"gameOverWinner":    session.GameOverWinner,
		"gameOverCause":     session.GameOverCause,
		"gameOverTurn":      session.GameOverTurn,
	})
	if err != nil {
		return 0, err
	}
	if err := producer.Produce("game.session", "session", b); err != nil {
		return 0, err
	}
	return now, nil
}

func kafkaStateTopics() []string {
	return []string{
		"game.events.unit",
		"game.events.region",
		"game.events.path",
		"game.broadcast",
		"game.ring.position",
		"game.ring.detection",
		"game.session",
	}
}

func kafkaWorkTopics() []string {
	return []string{"game.orders.validated"}
}

func envOrDefault(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return strings.TrimSpace(v)
	}
	return def
}
