package rotr.streams;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericRecordBuilder;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.kstream.ValueTransformerWithKey;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.ReadOnlyKeyValueStore;
import org.apache.kafka.streams.state.Stores;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.Objects;

final class ValidationTopology {
    private static final Logger LOG = LoggerFactory.getLogger(ValidationTopology.class);

    private ValidationTopology() {
    }

    static Topology build(AppConfig appConfig, GameConfig gameConfig, Schema validatedSchema, Schema dlqSchema) {
        StreamsBuilder builder = new StreamsBuilder();
        Serde<GenericRecord> rawSerde = App.avroSerde(appConfig.schemaRegistryUrl);
        Serde<GenericRecord> unitSerde = App.avroSerde(appConfig.schemaRegistryUrl);
        Serde<GenericRecord> pathSerde = App.avroSerde(appConfig.schemaRegistryUrl);
        Serde<GenericRecord> regionSerde = App.avroSerde(appConfig.schemaRegistryUrl);
        Serde<GenericRecord> ringSerde = App.avroSerde(appConfig.schemaRegistryUrl);
        Serde<GenericRecord> validatedSerde = App.avroSerde(appConfig.schemaRegistryUrl);
        Serde<GenericRecord> dlqSerde = App.avroSerde(appConfig.schemaRegistryUrl);

        builder.globalTable("game.session",
            Consumed.with(Serdes.String(), Serdes.ByteArray()),
            Materialized.<String, byte[], KeyValueStore<Bytes, byte[]>>as("session-store")
                .withKeySerde(Serdes.String())
                .withValueSerde(Serdes.ByteArray()));
        builder.globalTable("game.events.unit",
            Consumed.with(Serdes.String(), unitSerde),
            Materialized.<String, GenericRecord, KeyValueStore<Bytes, byte[]>>as("unit-store")
                .withKeySerde(Serdes.String())
                .withValueSerde(unitSerde));
        builder.globalTable("game.events.path",
            Consumed.with(Serdes.String(), pathSerde),
            Materialized.<String, GenericRecord, KeyValueStore<Bytes, byte[]>>as("path-store")
                .withKeySerde(Serdes.String())
                .withValueSerde(pathSerde));
        builder.globalTable("game.events.region",
            Consumed.with(Serdes.String(), regionSerde),
            Materialized.<String, GenericRecord, KeyValueStore<Bytes, byte[]>>as("region-store")
                .withKeySerde(Serdes.String())
                .withValueSerde(regionSerde));
        builder.globalTable("game.ring.position",
            Consumed.with(Serdes.String(), ringSerde),
            Materialized.<String, GenericRecord, KeyValueStore<Bytes, byte[]>>as("ring-store")
                .withKeySerde(Serdes.String())
                .withValueSerde(ringSerde));
        builder.addStateStore(Stores.keyValueStoreBuilder(
            Stores.persistentKeyValueStore("seen-orders-store"),
            Serdes.String(),
            Serdes.Integer()));

        KStream<String, ValidationResult> results = builder
            .stream("game.orders.raw", Consumed.with(Serdes.String(), rawSerde))
            .transformValues(
                () -> new OrderValidationTransformer(gameConfig, validatedSchema, dlqSchema),
                "seen-orders-store");

        results.filter((key, value) -> value.validRecord != null)
            .map((key, value) -> KeyValue.pair(value.outputKey, value.validRecord))
            .to("game.orders.validated", Produced.with(Serdes.String(), validatedSerde));

        results.filter((key, value) -> value.dlqRecord != null)
            .map((key, value) -> KeyValue.pair(value.outputKey, value.dlqRecord))
            .to("game.dlq", Produced.with(Serdes.String(), dlqSerde));

        return builder.build();
    }

    private static final class OrderValidationTransformer implements ValueTransformerWithKey<String, GenericRecord, ValidationResult> {
        private final GameConfig gameConfig;
        private final Schema validatedSchema;
        private final Schema dlqSchema;
        private ProcessorContext context;
        private ReadOnlyKeyValueStore<String, byte[]> sessionStore;
        private ReadOnlyKeyValueStore<String, GenericRecord> unitStore;
        private ReadOnlyKeyValueStore<String, GenericRecord> pathStore;
        private ReadOnlyKeyValueStore<String, GenericRecord> regionStore;
        private ReadOnlyKeyValueStore<String, GenericRecord> ringStore;
        private KeyValueStore<String, Integer> seenOrdersStore;

        private OrderValidationTransformer(GameConfig gameConfig, Schema validatedSchema, Schema dlqSchema) {
            this.gameConfig = gameConfig;
            this.validatedSchema = validatedSchema;
            this.dlqSchema = dlqSchema;
        }

        @Override
        @SuppressWarnings("unchecked")
        public void init(ProcessorContext context) {
            this.context = context;
            this.sessionStore = (ReadOnlyKeyValueStore<String, byte[]>) context.getStateStore("session-store");
            this.unitStore = (ReadOnlyKeyValueStore<String, GenericRecord>) context.getStateStore("unit-store");
            this.pathStore = (ReadOnlyKeyValueStore<String, GenericRecord>) context.getStateStore("path-store");
            this.regionStore = (ReadOnlyKeyValueStore<String, GenericRecord>) context.getStateStore("region-store");
            this.ringStore = (ReadOnlyKeyValueStore<String, GenericRecord>) context.getStateStore("ring-store");
            this.seenOrdersStore = (KeyValueStore<String, Integer>) context.getStateStore("seen-orders-store");
        }

        @Override
        public ValidationResult transform(String key, GenericRecord value) {
            try {
                OrderData order = OrderData.fromRawRecord(value);
                String errorCode = validate(order);
                if (errorCode != null) {
                    System.out.printf("[topology1] invalid unit=%s order=%s turn=%d code=%s%n", order.unitId, order.orderType, order.turn, errorCode);
                    return ValidationResult.invalid(errorCode, buildDlqRecord(order, errorCode, errorMessage(errorCode)));
                }
                seenOrdersStore.put(order.unitId, order.turn);
                System.out.printf("[topology1] valid unit=%s order=%s turn=%d%n", order.unitId, order.orderType, order.turn);
                return ValidationResult.valid(order.unitId, buildValidatedRecord(order));
            } catch (Exception ex) {
                LOG.warn("Topology 1 validation failed", ex);
                return ValidationResult.invalid("INVALID_JSON", buildDlqRecord(null, "INVALID_JSON", ex.getMessage()));
            }
        }

        @Override
        public void close() {
        }

        private String validate(OrderData order) {
            if (order.turn != currentTurn()) {
                return "WRONG_TURN";
            }
            UnitMeta unitMeta = gameConfig.units.get(order.unitId);
            if (unitMeta == null || !Objects.equals(unitMeta.side, playerSide(order.playerId))) {
                return "NOT_YOUR_UNIT";
            }
            Integer seenTurn = seenOrdersStore.get(order.unitId);
            if (seenTurn != null && seenTurn == order.turn) {
                return "DUPLICATE_UNIT_ORDER";
            }
            UnitState unitState = currentUnitState(unitMeta);
            if (!StreamModels.ACTIVE.equals(unitState.status)) {
                return "INVALID_TARGET";
            }
            return switch (order.orderType) {
                case "ASSIGN_ROUTE", "REDIRECT_UNIT" -> validateRoute(order, unitMeta, unitState);
                case "BLOCK_PATH", "SEARCH_PATH" -> validatePathAction(order, unitMeta, unitState);
                case "ATTACK_REGION" -> validateAttack(order, unitMeta, unitState);
                case "MAIA_ABILITY" -> validateMaia(order, unitMeta, unitState);
                case "DEPLOY_NAZGUL" -> (!"SHADOW".equals(unitMeta.side) || unitMeta.detectionRange <= 0) ? "NOT_YOUR_UNIT" : null;
                case "FORTIFY_REGION" -> unitMeta.canFortify ? null : "NOT_YOUR_UNIT";
                case "DESTROY_RING" -> validateDestroy(unitMeta);
                default -> null;
            };
        }

        private String validateRoute(OrderData order, UnitMeta unitMeta, UnitState unitState) {
            List<String> route = order.routePathIds();
            if (route.isEmpty()) {
                return "INVALID_PATH";
            }
            String currentRegion = "RingBearer".equals(unitMeta.className) ? ringBearerRegion() : unitState.region;
            for (int i = 0; i < route.size(); i++) {
                String pathId = route.get(i);
                PathMeta path = gameConfig.paths.get(pathId);
                if (path == null) {
                    return "INVALID_PATH";
                }
                if (i == 0 && "RingBearer".equals(unitMeta.className)) {
                    PathState firstState = currentPathState(pathId);
                    if (StreamModels.BLOCKED.equals(firstState.status)) {
                        return "PATH_BLOCKED";
                    }
                }
                if (Objects.equals(currentRegion, path.from)) {
                    currentRegion = path.to;
                } else if (Objects.equals(currentRegion, path.to)) {
                    currentRegion = path.from;
                } else {
                    return "INVALID_PATH";
                }
            }
            return null;
        }

        private String validatePathAction(OrderData order, UnitMeta unitMeta, UnitState unitState) {
            PathMeta path = gameConfig.paths.get(order.targetPathId);
            if (path == null) {
                return "INVALID_TARGET";
            }
            if (!Objects.equals(unitState.region, path.from) && !Objects.equals(unitState.region, path.to)) {
                return "UNIT_NOT_ADJACENT";
            }
            if ("SEARCH_PATH".equals(order.orderType) && !"SHADOW".equals(unitMeta.side)) {
                return "NOT_YOUR_UNIT";
            }
            if ("BLOCK_PATH".equals(order.orderType) && guardedByEnemyFellowship(path, unitMeta.side)) {
                return "INVALID_TARGET";
            }
            return null;
        }

        private String validateAttack(OrderData order, UnitMeta unitMeta, UnitState unitState) {
            if (order.targetRegion == null || order.targetRegion.isEmpty()) {
                return "INVALID_TARGET";
            }
            String controller = currentRegionController(order.targetRegion);
            if (!gameConfig.areAdjacent(unitState.region, order.targetRegion) || "NEUTRAL".equals(controller) || Objects.equals(controller, unitMeta.side)) {
                return "INVALID_TARGET";
            }
            return null;
        }

        private String validateMaia(OrderData order, UnitMeta unitMeta, UnitState unitState) {
            if (!unitMeta.maia) {
                return "NOT_YOUR_UNIT";
            }
            if (unitState.cooldown > 0) {
                return "ABILITY_ON_COOLDOWN";
            }
            if (!gameConfig.paths.containsKey(order.targetPathId)) {
                return "INVALID_TARGET";
            }
            if (unitMeta.isCorruptPathMaia() && "FREE_PEOPLES".equals(currentRegionController("isengard"))) {
                return "MAIA_DISABLED";
            }
            return null;
        }

        private String validateDestroy(UnitMeta unitMeta) {
            if (!"RingBearer".equals(unitMeta.className)) {
                return "NOT_YOUR_UNIT";
            }
            if (!Objects.equals(ringBearerRegion(), "mount-doom")) {
                return "DESTROY_CONDITION_NOT_MET";
            }
            for (UnitMeta meta : gameConfig.units.values()) {
                if (!"SHADOW".equals(meta.side)) {
                    continue;
                }
                UnitState state = currentUnitState(meta);
                if (StreamModels.ACTIVE.equals(state.status) && Objects.equals(state.region, "mount-doom")) {
                    return "DESTROY_CONDITION_NOT_MET";
                }
            }
            return null;
        }

        private GenericRecord buildValidatedRecord(OrderData order) {
            GenericRecordBuilder builder = new GenericRecordBuilder(validatedSchema);
            builder.set("playerId", order.playerId);
            builder.set("unitId", order.unitId);
            builder.set("orderType", order.orderType);
            builder.set("payload", ByteBuffer.wrap(order.payloadBytes));
            builder.set("turn", order.turn);
            builder.set("timestamp", order.timestamp);
            builder.set("routeRiskScore", null);
            builder.set("threatenedPaths", StreamModels.EMPTY_LIST);
            builder.set("blockedPaths", StreamModels.EMPTY_LIST);
            return builder.build();
        }

        private GenericRecord buildDlqRecord(OrderData order, String errorCode, String errorMessage) {
            GenericRecordBuilder builder = new GenericRecordBuilder(dlqSchema);
            builder.set("originalTopic", "game.orders.raw");
            builder.set("partition", context.partition());
            builder.set("offset", context.offset());
            builder.set("errorCode", errorCode);
            builder.set("errorMessage", errorMessage == null ? errorCode : errorMessage);
            builder.set("rawPayload", ByteBuffer.wrap(order == null ? new byte[0] : order.rawJson()));
            builder.set("timestamp", System.currentTimeMillis());
            return builder.build();
        }

        private int currentTurn() {
            try {
                byte[] payload = sessionStore.get("session");
                return payload == null ? 1 : StreamModels.MAPPER.readTree(payload).path("turn").asInt(1);
            } catch (Exception ignored) {
                return 1;
            }
        }

        private UnitState currentUnitState(UnitMeta unitMeta) {
            if ("RingBearer".equals(unitMeta.className)) {
                return new UnitState(ringBearerRegion(), StreamModels.ACTIVE, unitMeta.cooldown);
            }
            GenericRecord record = unitStore.get(unitMeta.id);
            if (record == null) {
                return new UnitState(unitMeta.startRegion, StreamModels.ACTIVE, unitMeta.cooldown);
            }
            String status = StreamModels.stringValue(record.get("status"));
            if (status.isEmpty()) {
                status = StreamModels.ACTIVE;
            }
            return new UnitState(
                StreamModels.stringValue(record.get("to")),
                status,
                StreamModels.nullableInt(record.get("cooldown"), unitMeta.cooldown)
            );
        }

        private PathState currentPathState(String pathId) {
            GenericRecord record = pathStore.get(pathId);
            return record == null
                ? new PathState("OPEN", 0)
                : new PathState(StreamModels.stringValue(record.get("newStatus")), StreamModels.nullableInt(record.get("surveillanceLevel"), 0));
        }

        private String ringBearerRegion() {
            GenericRecord record = ringStore.get(gameConfig.ringBearerId);
            if (record == null) {
                UnitMeta ringBearer = gameConfig.units.get(gameConfig.ringBearerId);
                return ringBearer == null ? "" : ringBearer.startRegion;
            }
            return StreamModels.stringValue(record.get("trueRegion"));
        }

        private boolean guardedByEnemyFellowship(PathMeta path, String blockerSide) {
            for (UnitMeta unitMeta : gameConfig.units.values()) {
                if (!"FellowshipGuard".equals(unitMeta.className) || Objects.equals(blockerSide, unitMeta.side)) {
                    continue;
                }
                UnitState state = currentUnitState(unitMeta);
                if (StreamModels.ACTIVE.equals(state.status) && (Objects.equals(state.region, path.from) || Objects.equals(state.region, path.to))) {
                    return true;
                }
            }
            return false;
        }

        private String currentRegionController(String regionId) {
            GenericRecord record = regionStore.get(regionId);
            if (record != null) {
                return StreamModels.stringValue(record.get("newController"));
            }
            RegionMeta region = gameConfig.regions.get(regionId);
            return region == null ? "NEUTRAL" : region.startControl;
        }

        private String playerSide(String playerId) {
            return playerId != null && playerId.startsWith("player-dark") ? "SHADOW" : "FREE_PEOPLES";
        }

        private String errorMessage(String errorCode) {
            return switch (errorCode) {
                case "WRONG_TURN" -> "order turn does not match game turn";
                case "NOT_YOUR_UNIT" -> "unit not on player's side";
                case "PATH_BLOCKED" -> "next path blocked";
                case "INVALID_PATH" -> "invalid path";
                case "UNIT_NOT_ADJACENT" -> "unit not adjacent";
                case "INVALID_TARGET" -> "invalid target";
                case "ABILITY_ON_COOLDOWN" -> "ability on cooldown";
                case "DUPLICATE_UNIT_ORDER" -> "duplicate unit order";
                case "MAIA_DISABLED" -> "maia is disabled";
                case "DESTROY_CONDITION_NOT_MET" -> "destroy condition not met";
                default -> errorCode;
            };
        }
    }
}
