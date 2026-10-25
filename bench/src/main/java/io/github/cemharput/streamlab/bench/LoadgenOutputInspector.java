package io.github.cemharput.streamlab.bench;

import io.github.cemharput.streamlab.bench.json.Json;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/** Reads the loadgen summary and the first and last JSONL line. Never modifies or deletes anything. */
public final class LoadgenOutputInspector {

    private static final int TAIL_BYTES = 1024 * 1024;

    /** {@code outcomes} maps the outcome name (without prefix) to its count; {@code footer} is null if missing. */
    public record LoadgenFacts(boolean summaryPresent, Map<String, Long> outcomes, Map<String, Object> footer,
            List<String> flags) {

        public long outcome(String name) {
            return outcomes.getOrDefault(name, 0L);
        }

        public Map<String, Object> toMap() {
            Map<String, Object> m = new LinkedHashMap<>();
            m.put("outcomes", new LinkedHashMap<>(outcomes));
            m.put("footer", footer);
            return m;
        }
    }

    private LoadgenOutputInspector() {
    }

    public static LoadgenFacts inspect(Path summary, Path jsonl) {
        List<String> flags = new ArrayList<>();

        Map<String, Long> outcomes = new LinkedHashMap<>();
        boolean summaryPresent = false;
        try {
            Map<String, Object> s = Json.parseObject(Files.readString(summary, StandardCharsets.UTF_8));
            summaryPresent = "summary".equals(s.get("type"));
            for (Map.Entry<String, Object> e : s.entrySet()) {
                if (e.getKey().startsWith("outcome_") && e.getValue() instanceof Number n) {
                    outcomes.put(e.getKey().substring("outcome_".length()), n.longValue());
                }
            }
        } catch (IOException | RuntimeException ex) {
            summaryPresent = false;
        }
        if (!summaryPresent) {
            flags.add(Flags.SUMMARY_MISSING);
        }

        String first = firstLine(jsonl);
        if (!"header".equals(type(first))) {
            flags.add(Flags.JSONL_HEADER_UNEXPECTED);
        }
        Map<String, Object> footer = null;
        Map<String, Object> last = parse(lastLine(jsonl));
        if (last != null && "footer".equals(last.get("type"))) {
            footer = new LinkedHashMap<>();
            for (String key : List.of("stopReason", "requestsStarted", "recordsWritten", "interrupted")) {
                footer.put(key, last.get(key));
            }
        } else {
            flags.add(Flags.JSONL_FOOTER_MISSING);
        }
        if (footer != null) {
            if (!"duration".equals(footer.get("stopReason"))) {
                flags.add(Flags.STOP_REASON_NOT_DURATION);
            }
            if (footer.get("interrupted") instanceof Number n && n.longValue() > 0) {
                flags.add(Flags.FOOTER_INTERRUPTED_NONZERO);
            }
        }
        for (Map.Entry<String, Long> e : outcomes.entrySet()) {
            if (!e.getKey().equals("COMPLETED") && !e.getKey().equals("REJECTED") && e.getValue() > 0) {
                flags.add(Flags.outcomeNonzero(e.getKey()));
            }
        }
        return new LoadgenFacts(summaryPresent, outcomes, footer, List.copyOf(flags));
    }

    private static String type(String line) {
        Map<String, Object> m = parse(line);
        return m == null ? null : String.valueOf(m.get("type"));
    }

    private static Map<String, Object> parse(String line) {
        if (line == null || line.isBlank()) {
            return null;
        }
        try {
            return Json.parseObject(line);
        } catch (RuntimeException ex) {
            return null;
        }
    }

    private static String firstLine(Path file) {
        try (BufferedReader reader = Files.newBufferedReader(file, StandardCharsets.UTF_8)) {
            return reader.readLine();
        } catch (IOException | RuntimeException ex) {
            return null;
        }
    }

    private static String lastLine(Path file) {
        try (RandomAccessFile raf = new RandomAccessFile(file.toFile(), "r")) {
            long size = raf.length();
            int window = (int) Math.min(size, TAIL_BYTES);
            byte[] tail = new byte[window];
            raf.seek(size - window);
            raf.readFully(tail);
            String text = new String(tail, StandardCharsets.UTF_8).stripTrailing();
            int nl = text.lastIndexOf('\n');
            if (nl < 0 && size > window) {
                return null; // the last line is longer than the window
            }
            return text.substring(nl + 1);
        } catch (IOException | RuntimeException ex) {
            return null;
        }
    }
}
