package tests

import (
	"sync"
	"testing"

	"ring-of-the-middle-earth/internal/config"
	"ring-of-the-middle-earth/internal/engine"
	"ring-of-the-middle-earth/internal/game"
)

type recordingProducer struct {
	mu     sync.Mutex
	topics []string
}

func (p *recordingProducer) Produce(topic, key string, value []byte) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.topics = append(p.topics, topic)
	return nil
}

func minimalGameConfig(maxTurns int) *config.GameConfig {
	return &config.GameConfig{
		HiddenUntilTurn:     3,
		MaxTurns:            maxTurns,
		TurnDurationSeconds: 60,
		Units: map[string]config.UnitConfig{
			"ring-bearer": {
				ID:          "ring-bearer",
				Class:       "RingBearer",
				Side:        config.SideFreePeoples,
				StartRegion: "the-shire",
				Strength:    1,
			},
		},
	}
}

func minimalMapConfig() *config.MapConfig {
	return &config.MapConfig{
		Regions: map[string]config.RegionConfig{
			"the-shire": {
				ID:           "the-shire",
				Name:         "The Shire",
				Terrain:      "PLAINS",
				StartControl: "FREE_PEOPLES",
				StartThreat:  0,
			},
		},
		Paths: map[string]config.PathConfig{},
	}
}

func TestGameOverProducedOnlyOnceAfterMaxTurns(t *testing.T) {
	gameCfg := minimalGameConfig(1)
	mapCfg := minimalMapConfig()
	cache := game.NewWorldStateCache(gameCfg, mapCfg)
	cache.InitRingBearerState("the-shire")

	producer := &recordingProducer{}
	exactOnceCalls := 0
	tp := engine.NewTurnProcessor(
		gameCfg,
		mapCfg,
		game.NewGraph(),
		cache,
		producer,
		func(brokers, topic, key string, value []byte) error {
			exactOnceCalls++
			return nil
		},
		"unused",
		"go-test",
	)

	tp.AdvanceTurn()
	tp.AdvanceTurn()

	if exactOnceCalls != 1 {
		t.Fatalf("expected exactly one GameOver produce, got %d", exactOnceCalls)
	}
	if len(producer.topics) != 1 || producer.topics[0] != "game.session" {
		t.Fatalf("expected exactly one game.session update, got %#v", producer.topics)
	}
	session := cache.Snapshot().Session
	if !session.GameOver || session.GameOverWinner != "DRAW" || session.GameOverCause != "MAX_TURNS_REACHED" || session.GameOverTurn != 1 {
		t.Fatalf("expected sticky game-over session state, got %#v", session)
	}
}

func TestGameOverStatePreventsReEmitAfterRestart(t *testing.T) {
	gameCfg := minimalGameConfig(1)
	mapCfg := minimalMapConfig()
	cache := game.NewWorldStateCache(gameCfg, mapCfg)
	cache.InitRingBearerState("the-shire")

	gameOver := true
	winner := "DRAW"
	cause := "MAX_TURNS_REACHED"
	turn := 1
	cache.UpdateSession(1, nil, nil, nil, &gameOver, &winner, &cause, &turn)

	producer := &recordingProducer{}
	exactOnceCalls := 0
	tp := engine.NewTurnProcessor(
		gameCfg,
		mapCfg,
		game.NewGraph(),
		cache,
		producer,
		func(brokers, topic, key string, value []byte) error {
			exactOnceCalls++
			return nil
		},
		"unused",
		"go-test",
	)

	tp.AdvanceTurn()

	if exactOnceCalls != 0 {
		t.Fatalf("expected no GameOver re-emit after restart, got %d", exactOnceCalls)
	}
	if len(producer.topics) != 0 {
		t.Fatalf("expected no producer calls after restart, got %#v", producer.topics)
	}
}
