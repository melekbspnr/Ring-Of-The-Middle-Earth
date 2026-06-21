.PHONY: up down test test-race build logs shell-go1 topics pprof-go1

# ─── Start the full system ────────────────────────────────────────────────────
up:
	docker compose up --build -d
	@echo "System is up. Light Side: http://localhost?side=light | Dark Side: http://localhost?side=dark"

# ─── Stop all services ───────────────────────────────────────────────────────
down:
	docker compose down -v

# ─── Run all unit tests (no Docker required) ─────────────────────────────────
test:
	cd option-b && go test ./... -count=1

test-race:
	cd option-b && go test ./tests/... -v -race -count=1

# ─── Build only ──────────────────────────────────────────────────────────────
build:
	cd option-b && go build ./...

# ─── Show logs ───────────────────────────────────────────────────────────────
logs:
	docker compose logs -f go-1 go-2 go-3

# ─── Tail Kafka consumer group status ────────────────────────────────────────
topics:
	docker exec kafka-1 kafka-topics --bootstrap-server kafka-1:9092 --describe

# ─── Consumer group info ─────────────────────────────────────────────────────
group:
	docker exec kafka-1 kafka-consumer-groups --bootstrap-server kafka-1:9092 \
		--group rotr-game-engine --describe

# ─── Demo Scenario 3 helper: stop go-2 to test rebalance ─────────────────────
kill-go2:
	docker stop go-2
	@echo "go-2 stopped. Watch rebalance:"
	$(MAKE) group

restart-go2:
	docker start go-2
	@echo "go-2 restarted. Watch rejoin:"
	$(MAKE) group

# ─── Inspect game.broadcast for GameOver exactly-once ────────────────────────
check-gameover:
	docker exec kafka-1 kafka-console-consumer \
		--bootstrap-server kafka-1:9092 \
		--topic game.broadcast \
		--from-beginning --max-messages 100 | grep -i gameover

# ─── Shell into go-1 for debugging ───────────────────────────────────────────
shell-go1:
	docker exec -it go-1 /bin/sh

pprof-go1:
	powershell -ExecutionPolicy Bypass -File tools/capture_pprof.ps1 -Port 8080 -Profile goroutine -OutFile artifacts/pprof-go1-goroutine.txt
