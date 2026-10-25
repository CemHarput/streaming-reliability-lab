package io.github.cemharput.streamlab.bench;

import io.github.cemharput.streamlab.bench.LoadgenOutputInspector.LoadgenFacts;
import java.util.ArrayList;
import java.util.List;

/** Derives flags (records, not verdicts) from loadgen facts, snapshot differences and process liveness. */
public final class RunFlags {

    private RunFlags() {
    }

    /** Flags of one loadgen phase. {@code exitCode} is null when the process did not exit in time. */
    public static List<String> loadgen(LoadgenFacts facts, Integer exitCode, boolean waitExceeded, int concurrency) {
        List<String> flags = new ArrayList<>(facts.flags());
        if (exitCode != null && exitCode != 0) {
            flags.add(Flags.LOADGEN_EXIT_NONZERO);
        }
        if (waitExceeded) {
            flags.add(Flags.LOADGEN_WAIT_EXCEEDED);
        }
        if (facts.outcome("REJECTED") > 0 && concurrency <= ProcessSpecs.MAX_ACTIVE_STREAMS) {
            flags.add(Flags.REJECTED_AT_OR_BELOW_LIMIT);
        }
        return flags;
    }

    /** Flags from the {@code pre} and {@code post} snapshots of the measured phase. */
    public static List<String> state(Snapshot pre, Snapshot post) {
        List<String> flags = new ArrayList<>();
        if (nonZero(pre.metric(GatewayProbe.ACTIVE_STREAMS))) {
            flags.add(Flags.ACTIVE_STREAMS_NOT_ZERO_BEFORE);
        }
        if (nonZero(post.metric(GatewayProbe.ACTIVE_STREAMS))) {
            flags.add(Flags.ACTIVE_STREAMS_NOT_ZERO_AFTER);
        }
        if (nonZero(post.metric(GatewayProbe.QUEUE_CHUNKS)) || nonZero(post.metric(GatewayProbe.QUEUE_ADMISSION))) {
            flags.add(Flags.QUEUE_DEPTH_NOT_ZERO_AFTER);
        }
        for (String reason : GatewayProbe.REJECTED_REASONS) {
            if (!reason.equals("limit") && nonZero(delta(pre, post, GatewayProbe.rejectedPath(reason)))) {
                flags.add(Flags.rejectedReasonDelta(reason));
            }
        }
        if (GatewayProbe.TIMEOUT_KINDS.stream().anyMatch(k -> nonZero(delta(pre, post, GatewayProbe.timeoutsPath(k))))) {
            flags.add(Flags.TIMEOUTS_DELTA_NONZERO);
        }
        if (GatewayProbe.RETRY_REASONS.stream().anyMatch(r -> nonZero(delta(pre, post, GatewayProbe.retriesPath(r))))) {
            flags.add(Flags.RETRIES_DELTA_NONZERO);
        }
        if (nonZero(delta(pre, post, GatewayProbe.RETRY_EXHAUSTED))) {
            flags.add(Flags.RETRY_EXHAUSTED_DELTA_NONZERO);
        }
        if (post.simulator() == null || pre.simulator() == null) {
            flags.add(Flags.SIMULATOR_STATS_UNAVAILABLE);
        } else {
            Long active = post.simulatorStat("active_generations");
            if (active != null && active != 0) {
                flags.add(Flags.SIMULATOR_ACTIVE_GENERATIONS_NOT_ZERO_AFTER);
            }
            if (simDelta(pre, post, "faulted_total") != 0) {
                flags.add(Flags.SIMULATOR_FAULTED_DELTA_NONZERO);
            }
            if (simDelta(pre, post, "rejected_total") != 0) {
                flags.add(Flags.SIMULATOR_REJECTED_DELTA_NONZERO);
            }
        }
        return flags;
    }

    private static long simDelta(Snapshot pre, Snapshot post, String name) {
        Long a = pre.simulatorStat(name);
        Long b = post.simulatorStat(name);
        return a == null || b == null ? 0 : b - a;
    }

    private static Double delta(Snapshot pre, Snapshot post, String path) {
        Double a = pre.metric(path);
        Double b = post.metric(path);
        return a == null || b == null ? null : b - a;
    }

    private static boolean nonZero(Double d) {
        return d != null && d != 0.0;
    }
}
