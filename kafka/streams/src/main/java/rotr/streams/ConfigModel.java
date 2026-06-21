package rotr.streams;

import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayDeque;
import java.util.Deque;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

final class AppConfig {
    final String brokers;
    final String schemaRegistryUrl;
    final Path configDir;
    final Path schemaDir;

    AppConfig(String brokers, String schemaRegistryUrl, Path configDir, Path schemaDir) {
        this.brokers = brokers;
        this.schemaRegistryUrl = schemaRegistryUrl;
        this.configDir = configDir;
        this.schemaDir = schemaDir;
    }

    static AppConfig fromEnv() {
        return new AppConfig(
            env("KAFKA_BROKERS", "localhost:9092"),
            env("SCHEMA_REGISTRY_URL", "http://localhost:8081"),
            Path.of(env("CONFIG_DIR", "/app/config")),
            Path.of(env("SCHEMA_DIR", "/app/schemas"))
        );
    }

    private static String env(String key, String fallback) {
        String value = System.getenv(key);
        return value == null || value.isBlank() ? fallback : value.trim();
    }
}

final class GameConfig {
    private static final Pattern BLOCK_PATTERN = Pattern.compile("\\{([^}]*)\\}", Pattern.DOTALL);
    private static final Pattern STRING_PATTERN = Pattern.compile("([a-zA-Z][a-zA-Z0-9]*)\\s*=\\s*\"([^\"]*)\"");
    private static final Pattern INT_PATTERN = Pattern.compile("([a-zA-Z][a-zA-Z0-9]*)\\s*=\\s*(-?\\d+)");
    private static final Pattern BOOL_PATTERN = Pattern.compile("([a-zA-Z][a-zA-Z0-9]*)\\s*=\\s*(true|false)");
    private static final Pattern LIST_PATTERN = Pattern.compile("([a-zA-Z][a-zA-Z0-9]*)\\s*=\\s*\\[([^]]*)\\]", Pattern.DOTALL);
    private static final Pattern LIST_STRING_PATTERN = Pattern.compile("\"([^\"]*)\"");

    final Map<String, UnitMeta> units;
    final Map<String, PathMeta> paths;
    final Map<String, RegionMeta> regions;
    final Map<String, Set<String>> adjacency;
    final String ringBearerId;

    GameConfig(Map<String, UnitMeta> units, Map<String, PathMeta> paths, Map<String, RegionMeta> regions, Map<String, Set<String>> adjacency, String ringBearerId) {
        this.units = units;
        this.paths = paths;
        this.regions = regions;
        this.adjacency = adjacency;
        this.ringBearerId = ringBearerId;
    }

    static GameConfig load(Path configDir) throws IOException {
        String unitsConf = java.nio.file.Files.readString(configDir.resolve("units.conf"));
        String mapConf = java.nio.file.Files.readString(configDir.resolve("map.conf"));

        Map<String, UnitMeta> units = new LinkedHashMap<>();
        String ringBearerId = "";
        Matcher unitMatcher = BLOCK_PATTERN.matcher(unitsConf);
        while (unitMatcher.find()) {
            String block = unitMatcher.group(1);
            Map<String, String> strings = parseStrings(block);
            Map<String, Integer> ints = parseInts(block);
            Map<String, Boolean> bools = parseBools(block);
            Map<String, List<String>> lists = parseStringLists(block);
            String id = strings.get("id");
            if (id == null || id.isEmpty()) {
                continue;
            }
            UnitMeta unit = new UnitMeta(
                id,
                strings.getOrDefault("side", ""),
                strings.getOrDefault("class", ""),
                strings.getOrDefault("start", ""),
                ints.getOrDefault("detectionRange", 0),
                ints.getOrDefault("cooldown", 0),
                bools.getOrDefault("canFortify", false),
                bools.getOrDefault("maia", false),
                lists.getOrDefault("maiaAbilityPaths", List.of())
            );
            units.put(id, unit);
            if ("RingBearer".equals(unit.className)) {
                ringBearerId = id;
            }
        }

        Map<String, RegionMeta> regions = new LinkedHashMap<>();
        int pathsStart = mapConf.indexOf("paths = [");
        Matcher regionMatcher = BLOCK_PATTERN.matcher(pathsStart >= 0 ? mapConf.substring(0, pathsStart) : mapConf);
        while (regionMatcher.find()) {
            String block = regionMatcher.group(1);
            Map<String, String> strings = parseStrings(block);
            Map<String, Integer> ints = parseInts(block);
            String id = strings.get("id");
            if (id == null || id.isEmpty()) {
                continue;
            }
            regions.put(id, new RegionMeta(id, strings.getOrDefault("startControl", "NEUTRAL"), ints.getOrDefault("startThreat", 0)));
        }

        Map<String, PathMeta> paths = new LinkedHashMap<>();
        Map<String, Set<String>> adjacency = new HashMap<>();
        Matcher pathMatcher = BLOCK_PATTERN.matcher(pathsStart >= 0 ? mapConf.substring(pathsStart) : "");
        while (pathMatcher.find()) {
            String block = pathMatcher.group(1);
            Map<String, String> strings = parseStrings(block);
            Map<String, Integer> ints = parseInts(block);
            String id = strings.get("id");
            if (id == null || id.isEmpty()) {
                continue;
            }
            PathMeta path = new PathMeta(id, strings.getOrDefault("from", ""), strings.getOrDefault("to", ""), ints.getOrDefault("cost", 1));
            paths.put(id, path);
            adjacency.computeIfAbsent(path.from, ignored -> new LinkedHashSet<>()).add(path.to);
            adjacency.computeIfAbsent(path.to, ignored -> new LinkedHashSet<>()).add(path.from);
        }

        return new GameConfig(units, paths, regions, adjacency, ringBearerId);
    }

    boolean areAdjacent(String from, String to) {
        return adjacency.getOrDefault(from, Set.of()).contains(to);
    }

    int distance(String start, String target) {
        if (Objects.equals(start, target)) {
            return 0;
        }
        Deque<String> queue = new ArrayDeque<>();
        Map<String, Integer> distance = new HashMap<>();
        queue.add(start);
        distance.put(start, 0);
        while (!queue.isEmpty()) {
            String current = queue.removeFirst();
            int currentDistance = distance.get(current);
            for (String next : adjacency.getOrDefault(current, Set.of())) {
                if (distance.containsKey(next)) {
                    continue;
                }
                int nextDistance = currentDistance + 1;
                if (Objects.equals(next, target)) {
                    return nextDistance;
                }
                distance.put(next, nextDistance);
                queue.addLast(next);
            }
        }
        return Integer.MAX_VALUE / 2;
    }

    private static Map<String, String> parseStrings(String block) {
        Map<String, String> values = new HashMap<>();
        Matcher matcher = STRING_PATTERN.matcher(block);
        while (matcher.find()) {
            values.put(matcher.group(1), matcher.group(2));
        }
        return values;
    }

    private static Map<String, Integer> parseInts(String block) {
        Map<String, Integer> values = new HashMap<>();
        Matcher matcher = INT_PATTERN.matcher(block);
        while (matcher.find()) {
            values.put(matcher.group(1), Integer.parseInt(matcher.group(2)));
        }
        return values;
    }

    private static Map<String, Boolean> parseBools(String block) {
        Map<String, Boolean> values = new HashMap<>();
        Matcher matcher = BOOL_PATTERN.matcher(block);
        while (matcher.find()) {
            values.put(matcher.group(1), Boolean.parseBoolean(matcher.group(2)));
        }
        return values;
    }

    private static Map<String, List<String>> parseStringLists(String block) {
        Map<String, List<String>> values = new HashMap<>();
        Matcher matcher = LIST_PATTERN.matcher(block);
        while (matcher.find()) {
            java.util.ArrayList<String> items = new java.util.ArrayList<>();
            Matcher itemMatcher = LIST_STRING_PATTERN.matcher(matcher.group(2));
            while (itemMatcher.find()) {
                items.add(itemMatcher.group(1));
            }
            values.put(matcher.group(1), List.copyOf(items));
        }
        return values;
    }
}

final class UnitMeta {
    final String id;
    final String side;
    final String className;
    final String startRegion;
    final int detectionRange;
    final int cooldown;
    final boolean canFortify;
    final boolean maia;
    final List<String> maiaAbilityPaths;

    UnitMeta(String id, String side, String className, String startRegion, int detectionRange, int cooldown, boolean canFortify, boolean maia, List<String> maiaAbilityPaths) {
        this.id = id;
        this.side = side;
        this.className = className;
        this.startRegion = startRegion;
        this.detectionRange = detectionRange;
        this.cooldown = cooldown;
        this.canFortify = canFortify;
        this.maia = maia;
        this.maiaAbilityPaths = List.copyOf(maiaAbilityPaths);
    }

    boolean isCorruptPathMaia() {
        return maia && !maiaAbilityPaths.isEmpty();
    }
}

final class PathMeta {
    final String id;
    final String from;
    final String to;
    final int cost;

    PathMeta(String id, String from, String to, int cost) {
        this.id = id;
        this.from = from;
        this.to = to;
        this.cost = cost;
    }
}

final class RegionMeta {
    final String id;
    final String startControl;
    final int startThreat;

    RegionMeta(String id, String startControl, int startThreat) {
        this.id = id;
        this.startControl = startControl;
        this.startThreat = startThreat;
    }
}
