package io.github.cemharput.streamlab.bench;

import java.time.Instant;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/** One metric snapshot: gateway actuator entries plus the simulator's GetStats (or why it was unavailable). */
public record Snapshot(Instant capturedAt, List<Entry> gateway, Map<String, Long> simulator, String simulatorError) {

    /** One actuator request. {@code value} is a Double for metrics and the status string for health. */
    public record Entry(String path, int status, Object value, String raw) {

        public Map<String, Object> toMap() {
            Map<String, Object> m = new LinkedHashMap<>();
            m.put("path", path);
            m.put("status", status);
            m.put("value", value);
            m.put("raw", raw);
            return m;
        }
    }

    /** The numeric value of the entry for {@code path}, or null (missing, 404, not numeric). */
    public Double metric(String path) {
        for (Entry e : gateway) {
            if (e.path().equals(path) && e.value() instanceof Double d) {
                return d;
            }
        }
        return null;
    }

    public Long simulatorStat(String name) {
        return simulator == null ? null : simulator.get(name);
    }

    public Map<String, Object> toMap() {
        Map<String, Object> m = new LinkedHashMap<>();
        m.put("capturedAt", capturedAt.toString());
        List<Object> entries = new ArrayList<>();
        gateway.forEach(e -> entries.add(e.toMap()));
        m.put("gateway", entries);
        if (simulator != null) {
            m.put("simulator", new LinkedHashMap<>(simulator));
        } else {
            Map<String, Object> unavailable = new LinkedHashMap<>();
            unavailable.put("unavailable", simulatorError == null ? "unknown" : simulatorError);
            m.put("simulator", unavailable);
        }
        return m;
    }
}
