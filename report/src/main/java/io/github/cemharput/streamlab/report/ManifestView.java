package io.github.cemharput.streamlab.report;

import io.github.cemharput.streamlab.bench.Manifest;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Typed, null-tolerant view of {@code manifest.json}. The required keys and their types are validated once
 * ({@link InputException} otherwise); everything else is optional and read defensively. Fields that are never
 * written to any output (operator note text, failure message, harness failure text, status lines, command lines,
 * java.home) are not exposed here except as presence or count.
 */
public final class ManifestView {

    /** Everything the report reads about how one child process ended. */
    public record ProcessView(String stopMethod, Long stopExitCode, Long bytesDropped, Long bytesLostAfterWriteError,
            boolean writeError) {
    }

    /** What the report reads about one loadgen phase of a run. */
    public record PhaseView(boolean present, List<String> flags, Long exitCode, Map<String, Object> files,
            Map<String, Long> outcomes, Map<String, Object> footer) {
    }

    /** One run entry of the manifest. */
    public static final class RunView {
        private final Map<String, Object> raw;
        private final long index;
        private final String id;
        private final String dir;
        private final String mode;
        private final int concurrency;
        private final long repetition;
        private final String orderLabel;
        private final String status;

        private RunView(Map<String, Object> raw, long index, String id, String dir, String mode, int concurrency,
                long repetition, String orderLabel, String status) {
            this.raw = raw;
            this.index = index;
            this.id = id;
            this.dir = dir;
            this.mode = mode;
            this.concurrency = concurrency;
            this.repetition = repetition;
            this.orderLabel = orderLabel;
            this.status = status;
        }

        public long index() {
            return index;
        }

        public String id() {
            return id;
        }

        public String dir() {
            return dir;
        }

        public String mode() {
            return mode;
        }

        public int concurrency() {
            return concurrency;
        }

        public long repetition() {
            return repetition;
        }

        public String orderLabel() {
            return orderLabel;
        }

        public String status() {
            return status;
        }

        public List<String> flags() {
            return Jv.strings(raw.get("flags"));
        }

        /** The phase in which the run failed, or null. The failure message itself is never read into output. */
        public String failurePhase() {
            return Jv.str(Jv.get(raw, "failure", "phase"));
        }

        public ProcessView process(String name) {
            Object p = Jv.get(raw, "processes", name);
            if (Jv.map(p) == null) {
                return null;
            }
            Object stop = Jv.get(p, "stop");
            Object log = Jv.get(p, "log");
            return new ProcessView(Jv.str(Jv.get(stop, "method")), Jv.lng(Jv.get(stop, "exitCode")),
                    Jv.lng(Jv.get(log, "bytesDropped")), Jv.lng(Jv.get(log, "bytesLostAfterWriteError")),
                    Jv.get(log, "writeError") != null);
        }

        public Map<String, Object> timeWaitBefore() {
            return Jv.map(raw.get("timeWaitBefore"));
        }

        /** CPU load samples before the measured phase; unavailable samples are null; null when not recorded. */
        public List<Double> cpuLoadBefore() {
            List<Object> l = Jv.list(raw.get("cpuLoadBefore"));
            if (l == null) {
                return null;
            }
            List<Double> out = new ArrayList<>();
            for (Object o : l) {
                out.add(Jv.dbl(o));
            }
            return out;
        }

        public Boolean portsFreed() {
            return Jv.bool(raw.get("portsFreed"));
        }

        public PhaseView phase(String name) {
            Map<String, Object> p = Jv.map(raw.get(name));
            if (p == null) {
                return new PhaseView(false, List.of(), null, Map.of(), Map.of(), null);
            }
            Map<String, Long> outcomes = new LinkedHashMap<>();
            Map<String, Object> om = Jv.map(p.get("outcomes"));
            if (om != null) {
                for (Map.Entry<String, Object> e : om.entrySet()) {
                    Long v = Jv.lng(e.getValue());
                    if (v != null) {
                        outcomes.put(e.getKey(), v);
                    }
                }
            }
            Map<String, Object> files = Jv.map(Jv.get(p, "loadgen", "files"));
            return new PhaseView(true, Jv.strings(p.get("flags")), Jv.lng(Jv.get(p, "loadgen", "exitCode")),
                    files == null ? Map.of() : files, outcomes, Jv.map(p.get("footer")));
        }

        /** Names of optional fields of a completed run that are absent; for the data quality section. */
        public List<String> missingFields() {
            List<String> missing = new ArrayList<>();
            if (!Manifest.STATUS_COMPLETED.equals(status)) {
                return missing;
            }
            for (String proc : List.of("gateway", "simulator")) {
                if (Jv.str(Jv.get(raw, "processes", proc, "stop", "method")) == null) {
                    missing.add("processes." + proc + ".stop.method");
                }
                if (Jv.lng(Jv.get(raw, "processes", proc, "stop", "exitCode")) == null) {
                    missing.add("processes." + proc + ".stop.exitCode");
                }
            }
            check(missing, "timeWaitBefore");
            check(missing, "cpuLoadBefore");
            check(missing, "snapshots", "pre");
            check(missing, "snapshots", "post");
            check(missing, "portsFreed");
            check(missing, "measured");
            return missing;
        }

        private void check(List<String> missing, String... path) {
            if (Jv.get(raw, path) == null) {
                missing.add(String.join(".", path));
            }
        }
    }

    private final Map<String, Object> root;
    private final List<RunView> runs;

    private ManifestView(Map<String, Object> root, List<RunView> runs) {
        this.root = root;
        this.runs = runs;
    }

    /** Validates the required keys and types; throws {@link InputException} (exit code 2) when they are wrong. */
    public static ManifestView of(Map<String, Object> root) throws InputException {
        Long schema = Jv.lng(root.get("schemaVersion"));
        if (schema == null) {
            throw new InputException("manifest: required key schemaVersion is missing or not a number");
        }
        if (schema != Manifest.SCHEMA_VERSION) {
            throw new InputException("manifest: unsupported schemaVersion " + schema + " (this tool reads "
                    + Manifest.SCHEMA_VERSION + ")");
        }
        Long protocol = Jv.lng(root.get("protocolVersion"));
        if (protocol == null) {
            throw new InputException("manifest: required key protocolVersion is missing or not a number");
        }
        if (protocol != Manifest.PROTOCOL_VERSION) {
            throw new InputException("manifest: unsupported protocolVersion " + protocol + " (this tool reads "
                    + Manifest.PROTOCOL_VERSION + ")");
        }
        String tool = Jv.str(root.get("tool"));
        if (!Manifest.TOOL.equals(tool)) {
            throw new InputException("manifest: tool is not " + Manifest.TOOL);
        }
        requireString(root, "runId", "manifest");
        requireString(root, "status", "manifest");
        Object runsValue = root.get("runs");
        List<Object> rawRuns = runsValue == null ? List.of() : Jv.list(runsValue);
        if (rawRuns == null) {
            throw new InputException("manifest: runs is not a list");
        }
        List<RunView> views = new ArrayList<>();
        Set<String> ids = new HashSet<>();
        Set<Long> indices = new HashSet<>();
        int position = 0;
        for (Object o : rawRuns) {
            position++;
            Map<String, Object> m = Jv.map(o);
            String where = "manifest: runs[" + position + "]";
            if (m == null) {
                throw new InputException(where + " is not an object");
            }
            Long index = requireLong(m, "index", where);
            String id = requireString(m, "id", where);
            String dir = requireString(m, "dir", where);
            String mode = requireString(m, "mode", where);
            Long concurrency = requireLong(m, "concurrency", where);
            Long repetition = requireLong(m, "repetition", where);
            String orderLabel = requireString(m, "orderLabel", where);
            String status = requireString(m, "status", where);
            if (concurrency < 1 || concurrency > 1_000_000) {
                throw new InputException(where + ": concurrency is out of range");
            }
            if (!ids.add(id) || !indices.add(index)) {
                throw new InputException(where + ": duplicate run id or index");
            }
            views.add(new RunView(m, index, id, dir, mode, concurrency.intValue(), repetition, orderLabel, status));
        }
        views.sort(java.util.Comparator.comparingLong(RunView::index));
        return new ManifestView(root, List.copyOf(views));
    }

    private static String requireString(Map<String, Object> m, String key, String where) throws InputException {
        String s = Jv.str(m.get(key));
        if (s == null) {
            throw new InputException(where + ": required key " + key + " is missing or not a string");
        }
        return s;
    }

    private static Long requireLong(Map<String, Object> m, String key, String where) throws InputException {
        Long v = Jv.lng(m.get(key));
        if (v == null) {
            throw new InputException(where + ": required key " + key + " is missing or not a whole number");
        }
        return v;
    }

    public List<RunView> runs() {
        return runs;
    }

    public String runId() {
        return Jv.str(root.get("runId"));
    }

    public String status() {
        return Jv.str(root.get("status"));
    }

    public String preset() {
        return Jv.str(root.get("preset"));
    }

    public String createdAt() {
        return Jv.str(root.get("createdAt"));
    }

    public String finishedAt() {
        return Jv.str(root.get("finishedAt"));
    }

    public String toolVersion() {
        return Jv.str(root.get("toolVersion"));
    }

    public List<String> flags() {
        return Jv.strings(root.get("flags"));
    }

    /** True when an operator note was recorded; the text is never read into output. */
    public boolean hasOperatorNote() {
        Object note = root.get("operatorNote");
        return note instanceof String s && !s.isBlank();
    }

    public boolean hasHarnessFailure() {
        return root.get("harnessFailure") != null;
    }

    public String checklistSource() {
        return Jv.str(Jv.get(root, "operatorChecklist", "source"));
    }

    public Map<String, Object> checklistAnswers() {
        return Jv.map(Jv.get(root, "operatorChecklist", "answers"));
    }

    /** A value below {@code environment}; path elements are map keys. */
    public Object environment(String... path) {
        Object env = root.get("environment");
        return path.length == 0 ? env : Jv.get(env, path);
    }

    public String environmentString(String... path) {
        return Jv.str(environment(path));
    }

    public Long environmentLong(String... path) {
        return Jv.lng(environment(path));
    }

    public Boolean environmentBool(String... path) {
        return Jv.bool(environment(path));
    }

    /** Number of git status lines (the lines themselves are never read into output); null when not recorded. */
    public Integer gitStatusLineCount() {
        List<Object> l = Jv.list(environment("git", "statusLines"));
        return l == null ? null : l.size();
    }

    public List<String> removedEnvironmentVariables() {
        return Jv.strings(environment("removedEnvironmentVariables"));
    }

    /** A value below {@code parameters}. */
    public Object parameter(String... path) {
        return Jv.get(root.get("parameters"), path);
    }

    public Object artifacts() {
        return root.get("artifacts");
    }

    /** The plan parameters used to rebuild the expected run list; null entries when not recorded. */
    public Long planLong(String key) {
        return Jv.lng(parameter("plan", key));
    }

    public List<String> modeOrder() {
        return Jv.strings(parameter("plan", "modeOrder"));
    }

    public List<Integer> planConcurrency() {
        List<Object> l = Jv.list(parameter("plan", "concurrency"));
        if (l == null) {
            return null;
        }
        List<Integer> out = new ArrayList<>();
        for (Object o : l) {
            Long v = Jv.lng(o);
            if (v == null) {
                return null;
            }
            out.add(v.intValue());
        }
        return out;
    }

    /** Names of optional set-level fields that are absent; for the data quality section. */
    public List<String> missingFields() {
        List<String> missing = new ArrayList<>();
        for (String key : List.of("preset", "createdAt", "finishedAt", "toolVersion")) {
            if (root.get(key) == null) {
                missing.add(key);
            }
        }
        List<List<String>> paths = List.of(List.of("operatorChecklist", "source"),
                List.of("operatorChecklist", "answers"), List.of("environment", "os"),
                List.of("environment", "cpu", "model"), List.of("environment", "cpu", "logicalProcessors"),
                List.of("environment", "cpu", "physicalCores"), List.of("environment", "memory", "totalBytes"),
                List.of("environment", "java", "version"), List.of("environment", "java", "vendor"),
                List.of("environment", "java", "vmName"), List.of("environment", "git", "commit"),
                List.of("environment", "git", "commitTime"), List.of("environment", "powerPlan"),
                List.of("environment", "timeWaitAtStart"), List.of("artifacts"), List.of("parameters", "plan"));
        for (List<String> path : paths) {
            if (Jv.get(root, path.toArray(new String[0])) == null) {
                missing.add(String.join(".", path));
            }
        }
        return missing;
    }
}
