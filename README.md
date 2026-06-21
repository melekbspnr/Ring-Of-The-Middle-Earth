# Ring of the Middle Earth

A browser-based, turn-based strategy game backed by an event-driven distributed system. Two human players control opposing sides: the Light Side attempts to move the hidden Ring Bearer from the Shire to Mount Doom, while the Dark Side tries to detect and intercept the Ring Bearer before the Ring is destroyed.

The project was developed for a Distributed Application Development course using the Go implementation option.

## Game Concept

The game is built around asymmetric information:

- The Light Side always knows the Ring Bearer's location.
- The Dark Side sees the Ring Bearer only when a detection event is produced.
- Both players can inspect units, regions, paths, and world-state changes relevant to their side.
- Orders are processed through Kafka rather than being applied directly by the web interface.

The system includes configurable units, route planning, path state changes, combat, detection, Maia abilities, turn processing, game-over conditions, and side-specific event routing.

## System Architecture

```text
Two browser clients
        |
        v
      Nginx
        |
        v
Three Go game-engine instances
        |
        v
Three-node Kafka cluster
        |
        |---- Kafka Streams validation and route analysis
        |---- Schema Registry and Avro contracts
        `---- Compacted world/session state
```

### Main Components

- **Go game engine:** processes validated orders, advances turns, updates world state, and publishes events.
- **Kafka cluster:** provides durable order, event, state, and broadcast topics.
- **Kafka Streams:** validates submitted orders and performs route-risk analysis.
- **Schema Registry:** manages Avro message contracts.
- **Nginx UI:** serves the two-player browser interface and proxies backend requests.
- **Configuration files:** define the map and units without hardcoding unit-specific rules into the engine.

## Distributed-System Features

- Three Go engine instances in a shared consumer group
- Kafka-backed state recovery
- Consumer-group rebalancing when an engine instance stops
- Avro serialization
- Dead-letter queue handling
- Side-specific information filtering
- Concurrent turn processing with goroutines and channels
- Route-risk and interception analysis
- Exactly-once-oriented game-over processing

## Kafka Topics

The initialization script creates ten topics:

| Topic | Purpose |
|---|---|
| `game.orders.raw` | Orders submitted by clients |
| `game.orders.validated` | Orders accepted by validation |
| `game.events.unit` | Unit movement and combat events |
| `game.events.region` | Region-control events |
| `game.events.path` | Path-state events |
| `game.session` | Compacted session and world state |
| `game.broadcast` | Public game events |
| `game.ring.position` | Restricted Ring Bearer position state |
| `game.ring.detection` | Detection and spotting events |
| `game.dlq` | Invalid or unprocessable messages |

## Requirements

- Docker Desktop or Docker Engine
- Docker Compose v2
- At least 8 GB of available memory is recommended for the full stack
- Go 1.22 or later for local tests

## Running the Full System

```bash
git clone https://github.com/melekbspnr/Ring-Of-The-Middle-Earth.git
cd Ring-Of-The-Middle-Earth

docker compose up --build -d
```

The first startup may take several minutes while Docker downloads images and builds the Go and Kafka Streams services.

## Player Interfaces

- Light Side: http://localhost/?side=light
- Dark Side: http://localhost/?side=dark
- Alternative exposed UI port: http://localhost:8888

Open the Light and Dark URLs in separate browser windows to play or demonstrate information hiding.

## Operations

Follow the main service logs:

```bash
docker compose logs -f go-1 go-2 go-3 kafka-streams
```

Inspect Kafka topics:

```bash
make topics
```

Inspect the game-engine consumer group:

```bash
make group
```

Stop and remove containers and volumes:

```bash
docker compose down -v
```

## Testing

Run the Go test suite without Docker:

```bash
cd option-b
go test ./...
```

Run the race detector:

```bash
cd option-b
go test -race ./...
```

Or use the included Makefile:

```bash
make test
make test-race
make build
```

The test suite covers areas including:

- Combat rules
- Ring Bearer detection
- Maia abilities
- Route-risk and interception pipelines
- Side-specific event routing
- Order validation
- Game-over behavior
- Avro wire encoding

## Fault-Tolerance Demo

Stop one engine instance:

```bash
make kill-go2
```

The remaining instances continue processing while Kafka rebalances the consumer group. Restart the instance with:

```bash
make restart-go2
```

## Project Structure

```text
.
|-- config/             # Map and unit configuration
|-- kafka/
|   |-- schemas/        # Avro schemas
|   `-- streams/        # Kafka Streams application
|-- option-b/
|   |-- cmd/server/     # Go service entry point
|   |-- internal/       # Engine, API, Kafka, and pipeline packages
|   `-- tests/          # Integration-oriented Go tests
|-- ui/                 # Browser client
|-- tools/              # Demo and profiling scripts
|-- docs/               # Architecture report and specification
|-- docker-compose.yml
|-- Makefile
`-- README.md
```

## Documentation

- [Architecture report](docs/architecture.pdf)
- [Project specification](docs/project-specification.md)
- [Middle-earth map](docs/middle-earth-map.svg)

## Verification Status

The complete Go test suite passes with:

```bash
go test ./...
```

Running the complete distributed application still requires Docker because Kafka, ZooKeeper, Schema Registry, Kafka Streams, Nginx, and the three engine instances are containerized.
