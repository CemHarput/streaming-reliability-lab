package io.github.cemharput.streamlab.bench;

import java.nio.file.Path;

/** All file names and paths of one result directory. */
public record ResultLayout(Path root) {

    public static final String MANIFEST = "manifest.json";
    public static final String RUNS_DIR = "runs";
    public static final String SIMULATOR_LOG = "simulator.log";
    public static final String SIMULATOR_GC_LOG = "simulator-gc.log";
    public static final String GATEWAY_LOG = "gateway.log";
    public static final String GATEWAY_GC_LOG = "gateway-gc.log";
    public static final String SNAPSHOT_READY = "snapshot-ready.json";
    public static final String SNAPSHOT_PRE = "snapshot-pre.json";
    public static final String SNAPSHOT_POST = "snapshot-post.json";

    public Path manifest() {
        return root.resolve(MANIFEST);
    }

    public Path manifestTmp() {
        return root.resolve(MANIFEST + ".tmp");
    }

    public Path runDir(PlanBuilder.PlannedRun run) {
        return root.resolve(RUNS_DIR).resolve(run.id());
    }

    /** Relative directory used in the manifest. */
    public static String relativeRunDir(PlanBuilder.PlannedRun run) {
        return RUNS_DIR + "/" + run.id();
    }

    /** Phase is {@code warmup} or {@code measured}. */
    public static String loadgenJsonl(String phase) {
        return phase + ".jsonl";
    }

    public static String loadgenSummary(String phase) {
        return phase + "-summary.json";
    }

    public static String loadgenLog(String phase) {
        return phase + "-loadgen.log";
    }

    public static String loadgenGcLog(String phase) {
        return phase + "-loadgen-gc.log";
    }
}
