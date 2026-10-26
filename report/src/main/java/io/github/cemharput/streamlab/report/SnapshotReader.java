package io.github.cemharput.streamlab.report;

import io.github.cemharput.streamlab.bench.json.Json;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/** Reads {@code snapshot-pre.json} / {@code snapshot-post.json}: gateway metric values by path and simulator stats. */
public final class SnapshotReader {

    public static final long MAX_BYTES = 16L * 1024 * 1024;

    /**
     * {@code gateway} maps the actuator path to its numeric value (entries without a number are absent);
     * {@code simulator} is null when the simulator stats were unavailable.
     */
    public record Data(Map<String, Double> gateway, Map<String, Long> simulator, boolean gatewayListed) {
    }

    private SnapshotReader() {
    }

    /** Returns null when the file is too large, unreadable or not a snapshot. */
    public static Data read(Path file) {
        try {
            if (Files.size(file) > MAX_BYTES) {
                return null;
            }
            return parse(Files.readString(file, StandardCharsets.UTF_8));
        } catch (IOException | RuntimeException ex) {
            return null;
        }
    }

    static Data parse(String text) {
        Map<String, Object> root;
        try {
            root = Json.parseObject(text);
        } catch (RuntimeException ex) {
            return null;
        }
        List<Object> entries = Jv.list(root.get("gateway"));
        Map<String, Double> gateway = new LinkedHashMap<>();
        if (entries != null) {
            for (Object o : entries) {
                String path = Jv.str(Jv.get(o, "path"));
                Object value = Jv.get(o, "value");
                if (path != null && value instanceof Number n) {
                    gateway.put(path, n.doubleValue());
                }
            }
        }
        Map<String, Long> simulator = null;
        Map<String, Object> sim = Jv.map(root.get("simulator"));
        if (sim != null && !sim.containsKey("unavailable")) {
            simulator = new LinkedHashMap<>();
            for (Map.Entry<String, Object> e : sim.entrySet()) {
                Long v = Jv.lng(e.getValue());
                if (v != null) {
                    simulator.put(e.getKey(), v);
                }
            }
        }
        return new Data(gateway, simulator, entries != null);
    }
}
