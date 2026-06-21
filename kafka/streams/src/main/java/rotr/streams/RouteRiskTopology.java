package rotr.streams;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericRecordBuilder;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.kstream.ValueTransformerWithKey;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.ReadOnlyKeyValueStore;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Set;

final class RouteRiskTopology {
    private static final Set<String> ROUTE_ORDERS = Set.of("ASSIGN_ROUTE", "REDIRECT_UNIT");

    private RouteRiskTopology() {
    }

    static Topology build(AppConfig appConfig, GameConfig gameConfig, Schema validatedSchema) {
        StreamsBuilder builder = new StreamsBuilder();
        Serde<GenericRecord> validatedSerde = App.avroSerde(appConfig.schemaRegistryUrl);
        Serde<GenericRecord> unitSerde = App.avroSerde(appConfig.schemaRegistryUrl);
        Serde<GenericRecord> pathSerde = App.avroSerde(appConfig.schemaRegistryUrl);
        Serde<GenericRecord> ringSerde = App.avroSerde(appConfig.schemaRegistryUrl);

        builder.globalTable("game.events.unit",
            Consumed.with(Serdes.String(), unitSerde),
            Materialized.<String, GenericRecord, KeyValueStore<Bytes, byte[]>>as("risk-unit-store")
                .withKeySerde(Serdes.String())
                .withValueSerde(unitSerde));
        builder.globalTable("game.events.path",
            Consumed.with(Serdes.String(), pathSerde),
            Materialized.<String, GenericRecord, KeyValueStore<Bytes, byte[]>>as("risk-path-store")
                .withKeySerde(Serdes.String())
                .withValueSerde(pathSerde));
        builder.globalTable("game.ring.position",
            Consumed.with(Serdes.String(), ringSerde),
            Materialized.<String, GenericRecord, KeyValueStore<Bytes, byte[]>>as("risk-ring-store")
                .withKeySerde(Serdes.String())
                .withValueSerde(ringSerde));

        builder.stream("game.orders.validated", Consumed.with(Serdes.String(), validatedSerde))
            .filter((key, value) -> value != null && ROUTE_ORDERS.contains(StreamModels.stringValue(value.get("orderType"))) && value.get("routeRiskScore") == null)
            .transformValues(() -> new RouteRiskTransformer(gameConfig, validatedSchema))
            .to("game.orders.validated", Produced.with(Serdes.String(), validatedSerde));

        return builder.build();
    }

    private static final class RouteRiskTransformer implements ValueTransformerWithKey<String, GenericRecord, GenericRecord> {
        private final GameConfig gameConfig;
        private final Schema validatedSchema;
        private ReadOnlyKeyValueStore<String, GenericRecord> unitStore;
        private ReadOnlyKeyValueStore<String, GenericRecord> pathStore;
        private ReadOnlyKeyValueStore<String, GenericRecord> ringStore;

        private RouteRiskTransformer(GameConfig gameConfig, Schema validatedSchema) {
            this.gameConfig = gameConfig;
            this.validatedSchema = validatedSchema;
        }

        @Override
        @SuppressWarnings("unchecked")
        public void init(ProcessorContext context) {
            this.unitStore = (ReadOnlyKeyValueStore<String, GenericRecord>) context.getStateStore("risk-unit-store");
            this.pathStore = (ReadOnlyKeyValueStore<String, GenericRecord>) context.getStateStore("risk-path-store");
            this.ringStore = (ReadOnlyKeyValueStore<String, GenericRecord>) context.getStateStore("risk-ring-store");
        }

        @Override
        public GenericRecord transform(String key, GenericRecord value) {
            try {
                OrderData order = OrderData.fromValidatedRecord(value);
                RouteRisk risk = computeRouteRisk(startRegion(order.unitId), order.routePathIds());
                System.out.printf("[topology2] enriched unit=%s order=%s turn=%d risk=%d%n", order.unitId, order.orderType, order.turn, risk.score);
                GenericRecordBuilder builder = new GenericRecordBuilder(validatedSchema);
                builder.set("playerId", order.playerId);
                builder.set("unitId", order.unitId);
                builder.set("orderType", order.orderType);
                builder.set("payload", ByteBuffer.wrap(order.payloadBytes));
                builder.set("turn", order.turn);
                builder.set("timestamp", order.timestamp);
                builder.set("routeRiskScore", risk.score);
                builder.set("threatenedPaths", risk.threatenedPaths);
                builder.set("blockedPaths", risk.blockedPaths);
                return builder.build();
            } catch (Exception ignored) {
                return value;
            }
        }

        @Override
        public void close() {
        }

        private String startRegion(String unitId) {
            UnitMeta unitMeta = gameConfig.units.get(unitId);
            if (unitMeta == null) {
                return "";
            }
            if ("RingBearer".equals(unitMeta.className)) {
                GenericRecord record = ringStore.get(gameConfig.ringBearerId);
                return record == null ? unitMeta.startRegion : StreamModels.stringValue(record.get("trueRegion"));
            }
            GenericRecord record = unitStore.get(unitId);
            return record == null ? unitMeta.startRegion : StreamModels.stringValue(record.get("to"));
        }

        private RouteRisk computeRouteRisk(String startRegion, List<String> pathIds) {
            int score = 0;
            String currentRegion = startRegion;
            List<String> threatenedPaths = new ArrayList<>();
            List<String> blockedPaths = new ArrayList<>();
            List<String> routeRegions = new ArrayList<>();

            for (String pathId : pathIds) {
                PathMeta path = gameConfig.paths.get(pathId);
                if (path == null) {
                    continue;
                }
                currentRegion = resolveDestination(currentRegion, path);
                routeRegions.add(currentRegion);
                RegionMeta region = gameConfig.regions.get(currentRegion);
                if (region != null) {
                    score += region.startThreat;
                }
                PathState pathState = currentPathState(pathId);
                score += pathState.surveillanceLevel * 3;
                if (StreamModels.THREATENED.equals(pathState.status)) {
                    score += 2;
                    threatenedPaths.add(pathId);
                } else if (StreamModels.BLOCKED.equals(pathState.status)) {
                    score += 5;
                    blockedPaths.add(pathId);
                }
            }

            for (UnitMeta unitMeta : gameConfig.units.values()) {
                if (unitMeta.detectionRange <= 0) {
                    continue;
                }
                GenericRecord record = unitStore.get(unitMeta.id);
                String region = record == null ? unitMeta.startRegion : StreamModels.stringValue(record.get("to"));
                String status = record == null ? StreamModels.ACTIVE : StreamModels.stringValue(record.get("status"));
                if (status == null || status.isEmpty()) {
                    status = StreamModels.ACTIVE;
                }
                if (!StreamModels.ACTIVE.equals(status)) {
                    continue;
                }
                for (String routeRegion : routeRegions) {
                    if (gameConfig.distance(region, routeRegion) <= 2) {
                        score += 2;
                        break;
                    }
                }
            }

            return new RouteRisk(score, threatenedPaths, blockedPaths);
        }

        private PathState currentPathState(String pathId) {
            GenericRecord record = pathStore.get(pathId);
            return record == null
                ? new PathState("OPEN", 0)
                : new PathState(StreamModels.stringValue(record.get("newStatus")), StreamModels.nullableInt(record.get("surveillanceLevel"), 0));
        }

        private String resolveDestination(String currentRegion, PathMeta path) {
            if (Objects.equals(currentRegion, path.from)) {
                return path.to;
            }
            if (Objects.equals(currentRegion, path.to)) {
                return path.from;
            }
            return path.to;
        }
    }
}
