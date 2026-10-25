package io.github.cemharput.streamlab.bench;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/** Shape of {@code manifest.json}: fixed key sets, insertion ordered maps. The schema is documented in the protocol. */
public final class Manifest {

    public static final int SCHEMA_VERSION = 1;
    public static final int PROTOCOL_VERSION = 1;
    public static final String TOOL = "streamlab-bench";
    public static final String NOTE = "Raw data only. No analysis, no performance claim.";

    public static final String STATUS_RUNNING = "running";
    public static final String STATUS_COMPLETED = "completed";
    public static final String STATUS_FAILED = "failed";
    public static final String STATUS_INTERRUPTED = "interrupted";
    public static final String RUN_PLANNED = "planned";
    public static final String RUN_NOT_RUN = "not_run";

    public static final List<String> TOP_LEVEL_KEYS = List.of("schemaVersion", "protocolVersion", "runId", "tool",
            "toolVersion", "status", "createdAt", "finishedAt", "note", "preset", "commandLine", "operatorNote", "operatorChecklist",
            "environment", "artifacts", "parameters", "runs", "flags");

    public static final List<String> RUN_KEYS = List.of("index", "id", "dir", "mode", "concurrency", "repetition",
            "orderLabel", "status", "startedAt", "endedAt", "failure", "processes", "timeWaitBefore",
            "cpuLoadBefore", "warmup", "measured", "snapshots", "portsFreed", "flags");

    private Manifest() {
    }

    /** A new top-level object with every key present (values null or empty until filled). */
    public static Map<String, Object> newRoot(String runId, String toolVersion, String createdAt) {
        Map<String, Object> root = new LinkedHashMap<>();
        root.put("schemaVersion", SCHEMA_VERSION);
        root.put("protocolVersion", PROTOCOL_VERSION);
        root.put("runId", runId);
        root.put("tool", TOOL);
        root.put("toolVersion", toolVersion);
        root.put("status", STATUS_RUNNING);
        root.put("createdAt", createdAt);
        root.put("finishedAt", null);
        root.put("note", NOTE);
        root.put("preset", null);
        root.put("commandLine", null);
        root.put("operatorNote", null);
        root.put("operatorChecklist", null);
        root.put("environment", null);
        root.put("artifacts", null);
        root.put("parameters", null);
        root.put("runs", new ArrayList<Object>());
        root.put("flags", new ArrayList<String>());
        return root;
    }

    /** A new run entry with every key present. */
    public static Map<String, Object> newRun(PlanBuilder.PlannedRun run) {
        Map<String, Object> m = new LinkedHashMap<>();
        m.put("index", run.index());
        m.put("id", run.id());
        m.put("dir", ResultLayout.relativeRunDir(run));
        m.put("mode", run.mode().property());
        m.put("concurrency", run.concurrency());
        m.put("repetition", run.repetition());
        m.put("orderLabel", run.orderLabel());
        m.put("status", RUN_PLANNED);
        m.put("startedAt", null);
        m.put("endedAt", null);
        m.put("failure", null);
        m.put("processes", new LinkedHashMap<String, Object>());
        m.put("timeWaitBefore", null);
        m.put("cpuLoadBefore", null);
        m.put("warmup", null);
        m.put("measured", null);
        m.put("snapshots", new LinkedHashMap<String, Object>());
        m.put("portsFreed", null);
        m.put("flags", new ArrayList<String>());
        return m;
    }
}
