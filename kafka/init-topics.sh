#!/bin/bash
set -euo pipefail

echo "Waiting for Kafka..."
cub kafka-ready -b kafka-1:9092 3 120

kafka-topics --create --if-not-exists --bootstrap-server kafka-1:9092 \
  --topic game.orders.raw --partitions 3 --replication-factor 3 \
  --config cleanup.policy=delete --config retention.ms=3600000

kafka-topics --create --if-not-exists --bootstrap-server kafka-1:9092 \
  --topic game.orders.validated --partitions 6 --replication-factor 3 \
  --config cleanup.policy=delete --config retention.ms=3600000

kafka-topics --create --if-not-exists --bootstrap-server kafka-1:9092 \
  --topic game.events.unit --partitions 6 --replication-factor 3 \
  --config cleanup.policy=delete --config retention.ms=604800000

kafka-topics --create --if-not-exists --bootstrap-server kafka-1:9092 \
  --topic game.events.region --partitions 6 --replication-factor 3 \
  --config cleanup.policy=delete --config retention.ms=604800000

kafka-topics --create --if-not-exists --bootstrap-server kafka-1:9092 \
  --topic game.events.path --partitions 6 --replication-factor 3 \
  --config cleanup.policy=delete --config retention.ms=604800000

kafka-topics --create --if-not-exists --bootstrap-server kafka-1:9092 \
  --topic game.session --partitions 1 --replication-factor 3 \
  --config cleanup.policy=compact

kafka-topics --create --if-not-exists --bootstrap-server kafka-1:9092 \
  --topic game.broadcast --partitions 1 --replication-factor 3 \
  --config cleanup.policy=delete --config retention.ms=3600000

kafka-topics --create --if-not-exists --bootstrap-server kafka-1:9092 \
  --topic game.ring.position --partitions 1 --replication-factor 3 \
  --config cleanup.policy=delete --config retention.ms=3600000

kafka-topics --create --if-not-exists --bootstrap-server kafka-1:9092 \
  --topic game.ring.detection --partitions 2 --replication-factor 3 \
  --config cleanup.policy=delete --config retention.ms=3600000

kafka-topics --create --if-not-exists --bootstrap-server kafka-1:9092 \
  --topic game.dlq --partitions 3 --replication-factor 3 \
  --config cleanup.policy=delete --config retention.ms=604800000

echo "All 10 topics created."
