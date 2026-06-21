package rotr.streams;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.avro.generic.GenericRecord;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

final class StreamModels {
    static final ObjectMapper MAPPER = new ObjectMapper();
    static final String ACTIVE = "ACTIVE";
    static final String BLOCKED = "BLOCKED";
    static final String THREATENED = "THREATENED";
    static final List<String> EMPTY_LIST = List.of();

    private StreamModels() {
    }

    static String stringValue(Object value) {
        if (value == null) {
            return "";
        }
        if (value instanceof CharSequence chars) {
            return chars.toString();
        }
        return value.toString();
    }

    static int nullableInt(Object value, int fallback) {
        if (value instanceof Number number) {
            return number.intValue();
        }
        return fallback;
    }

    static long nullableLong(Object value, long fallback) {
        if (value instanceof Number number) {
            return number.longValue();
        }
        return fallback;
    }

    static byte[] bytesValue(Object value) {
        if (value == null) {
            return new byte[0];
        }
        if (value instanceof byte[] bytes) {
            return bytes;
        }
        if (value instanceof ByteBuffer buffer) {
            ByteBuffer copy = buffer.duplicate();
            byte[] bytes = new byte[copy.remaining()];
            copy.get(bytes);
            return bytes;
        }
        return new byte[0];
    }

    static List<String> toStringList(JsonNode node) {
        if (node == null || !node.isArray()) {
            return List.of();
        }
        List<String> values = new ArrayList<>();
        node.forEach(item -> values.add(item.asText("")));
        return values;
    }
}

final class RuntimeState {
    private final AtomicInteger currentTurn = new AtomicInteger(1);
    private final Map<String, UnitState> units = new ConcurrentHashMap<>();
    private final Map<String, PathState> paths = new ConcurrentHashMap<>();
    private final Map<String, String> regionControllers = new ConcurrentHashMap<>();
    private volatile String ringBearerRegion = "";

    int currentTurn() {
        return currentTurn.get();
    }

    void updateSession(byte[] payload) {
        try {
            if (payload != null && payload.length > 0) {
                int turn = StreamModels.MAPPER.readTree(payload).path("turn").asInt(1);
                if (turn > 0) {
                    currentTurn.set(turn);
                }
            }
        } catch (Exception ignored) {
            currentTurn.compareAndSet(0, 1);
        }
    }

    void updateUnit(GenericRecord record) {
        if (record == null) {
            return;
        }
        String unitId = StreamModels.stringValue(record.get("unitId"));
        if (unitId.isEmpty()) {
            return;
        }
        String region = StreamModels.stringValue(record.get("to"));
        String status = StreamModels.stringValue(record.get("status"));
        if (status.isEmpty()) {
            status = StreamModels.ACTIVE;
        }
        int cooldown = StreamModels.nullableInt(record.get("cooldown"), 0);
        units.put(unitId, new UnitState(region, status, cooldown));
    }

    void updatePath(GenericRecord record) {
        if (record == null) {
            return;
        }
        String pathId = StreamModels.stringValue(record.get("pathId"));
        if (pathId.isEmpty()) {
            return;
        }
        String status = StreamModels.stringValue(record.get("newStatus"));
        if (status.isEmpty()) {
            status = "OPEN";
        }
        paths.put(pathId, new PathState(status, StreamModels.nullableInt(record.get("surveillanceLevel"), 0)));
    }

    void updateRegion(GenericRecord record) {
        if (record == null) {
            return;
        }
        String regionId = StreamModels.stringValue(record.get("regionId"));
        String controller = StreamModels.stringValue(record.get("newController"));
        if (!regionId.isEmpty() && !controller.isEmpty()) {
            regionControllers.put(regionId, controller);
        }
    }

    void updateRing(GenericRecord record) {
        if (record == null) {
            return;
        }
        String region = StreamModels.stringValue(record.get("trueRegion"));
        if (!region.isEmpty()) {
            ringBearerRegion = region;
        }
    }

    UnitState unit(UnitMeta meta) {
        UnitState state = units.get(meta.id);
        if (state != null) {
            return state;
        }
        return new UnitState(meta.startRegion, StreamModels.ACTIVE, meta.cooldown);
    }

    PathState path(String pathId) {
        PathState state = paths.get(pathId);
        return state == null ? new PathState("OPEN", 0) : state;
    }

    String regionController(String regionId, GameConfig gameConfig) {
        String controller = regionControllers.get(regionId);
        if (controller != null && !controller.isEmpty()) {
            return controller;
        }
        RegionMeta region = gameConfig.regions.get(regionId);
        return region == null ? "NEUTRAL" : region.startControl;
    }

    String ringBearerRegion(GameConfig gameConfig) {
        if (ringBearerRegion != null && !ringBearerRegion.isEmpty()) {
            return ringBearerRegion;
        }
        UnitMeta ringBearer = gameConfig.units.get(gameConfig.ringBearerId);
        return ringBearer == null ? "" : ringBearer.startRegion;
    }
}

final class OrderData {
    final String playerId;
    final String unitId;
    final String orderType;
    final int turn;
    final long timestamp;
    final String targetPathId;
    final String targetRegion;
    final List<String> pathIds;
    final List<String> newPathIds;
    final byte[] payloadBytes;

    OrderData(String playerId, String unitId, String orderType, int turn, long timestamp, String targetPathId, String targetRegion, List<String> pathIds, List<String> newPathIds, byte[] payloadBytes) {
        this.playerId = playerId;
        this.unitId = unitId;
        this.orderType = orderType;
        this.turn = turn;
        this.timestamp = timestamp;
        this.targetPathId = targetPathId;
        this.targetRegion = targetRegion;
        this.pathIds = pathIds;
        this.newPathIds = newPathIds;
        this.payloadBytes = payloadBytes;
    }

    static OrderData fromRawRecord(GenericRecord record) throws IOException {
        return fromRecord(
            StreamModels.stringValue(record.get("playerId")),
            StreamModels.stringValue(record.get("unitId")),
            StreamModels.stringValue(record.get("orderType")),
            StreamModels.nullableInt(record.get("turn"), 0),
            StreamModels.nullableLong(record.get("timestamp"), System.currentTimeMillis()),
            StreamModels.bytesValue(record.get("payload"))
        );
    }

    static OrderData fromValidatedRecord(GenericRecord record) throws IOException {
        return fromRecord(
            StreamModels.stringValue(record.get("playerId")),
            StreamModels.stringValue(record.get("unitId")),
            StreamModels.stringValue(record.get("orderType")),
            StreamModels.nullableInt(record.get("turn"), 0),
            StreamModels.nullableLong(record.get("timestamp"), System.currentTimeMillis()),
            StreamModels.bytesValue(record.get("payload"))
        );
    }

    static OrderData fromRecord(String playerId, String unitId, String orderType, int turn, long timestamp, byte[] payloadBytes) throws IOException {
        JsonNode payload = payloadBytes.length == 0 ? StreamModels.MAPPER.createObjectNode() : StreamModels.MAPPER.readTree(payloadBytes);
        return new OrderData(
            playerId,
            unitId,
            orderType,
            turn,
            timestamp,
            payload.path("targetPathId").asText(""),
            payload.path("targetRegion").asText(""),
            StreamModels.toStringList(payload.path("pathIds")),
            StreamModels.toStringList(payload.path("newPathIds")),
            payloadBytes
        );
    }

    List<String> routePathIds() {
        return "REDIRECT_UNIT".equals(orderType) ? newPathIds : pathIds;
    }

    byte[] rawJson() {
        Map<String, Object> out = new LinkedHashMap<>();
        out.put("orderType", orderType);
        out.put("playerId", playerId);
        out.put("unitId", unitId);
        out.put("turn", turn);
        out.put("timestamp", timestamp);
        if (!pathIds.isEmpty()) {
            out.put("pathIds", pathIds);
        }
        if (!newPathIds.isEmpty()) {
            out.put("newPathIds", newPathIds);
        }
        if (targetPathId != null && !targetPathId.isEmpty()) {
            out.put("targetPathId", targetPathId);
        }
        if (targetRegion != null && !targetRegion.isEmpty()) {
            out.put("targetRegion", targetRegion);
        }
        try {
            return StreamModels.MAPPER.writeValueAsBytes(out);
        } catch (IOException ignored) {
            return new byte[0];
        }
    }
}

final class UnitState {
    final String region;
    final String status;
    final int cooldown;

    UnitState(String region, String status, int cooldown) {
        this.region = region;
        this.status = status;
        this.cooldown = cooldown;
    }
}

final class PathState {
    final String status;
    final int surveillanceLevel;

    PathState(String status, int surveillanceLevel) {
        this.status = status;
        this.surveillanceLevel = surveillanceLevel;
    }
}

final class ValidationResult {
    final String outputKey;
    final GenericRecord validRecord;
    final GenericRecord dlqRecord;

    ValidationResult(String outputKey, GenericRecord validRecord, GenericRecord dlqRecord) {
        this.outputKey = outputKey;
        this.validRecord = validRecord;
        this.dlqRecord = dlqRecord;
    }

    static ValidationResult valid(String outputKey, GenericRecord record) {
        return new ValidationResult(outputKey, record, null);
    }

    static ValidationResult invalid(String outputKey, GenericRecord record) {
        return new ValidationResult(outputKey, null, record);
    }
}

final class RouteRisk {
    final int score;
    final List<String> threatenedPaths;
    final List<String> blockedPaths;

    RouteRisk(int score, List<String> threatenedPaths, List<String> blockedPaths) {
        this.score = score;
        this.threatenedPaths = threatenedPaths;
        this.blockedPaths = blockedPaths;
    }
}
