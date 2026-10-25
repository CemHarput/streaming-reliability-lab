package io.github.cemharput.streamlab.bench;

import static org.assertj.core.api.Assertions.assertThat;

import io.github.cemharput.streamlab.bench.LoadgenOutputInspector.LoadgenFacts;
import java.time.Instant;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.Timeout.ThreadMode;

@Timeout(value = 30, unit = TimeUnit.SECONDS, threadMode = ThreadMode.SEPARATE_THREAD)
class RunFlagsTest {

    private static Snapshot snapshot(Map<String, Double> metrics, Map<String, Long> sim) {
        List<Snapshot.Entry> entries = new ArrayList<>();
        metrics.forEach((path, v) -> entries.add(new Snapshot.Entry(path, 200, v, "{}")));
        return new Snapshot(Instant.EPOCH, entries, sim, sim == null ? "down" : null);
    }

    private static Map<String, Long> sim(long active, long faulted, long rejected) {
        Map<String, Long> m = new LinkedHashMap<>();
        m.put("active_generations", active);
        m.put("faulted_total", faulted);
        m.put("rejected_total", rejected);
        return m;
    }

    private static LoadgenFacts facts(Map<String, Long> outcomes) {
        return new LoadgenFacts(true, outcomes, Map.of(), List.of("FROM_INSPECTOR"));
    }

    @Test
    void loadgenFlagsCombineInspectorExitCodeWaitAndRejectionAtOrBelowTheLimit() {
        List<String> flags = RunFlags.loadgen(facts(Map.of("REJECTED", 3L)), 1, true, 100);
        assertThat(flags).containsExactlyInAnyOrder("FROM_INSPECTOR", Flags.LOADGEN_EXIT_NONZERO,
                Flags.LOADGEN_WAIT_EXCEEDED, Flags.REJECTED_AT_OR_BELOW_LIMIT);
        assertThat(RunFlags.loadgen(facts(Map.of("REJECTED", 3L)), 0, false, 150))
                .containsExactly("FROM_INSPECTOR");
        assertThat(RunFlags.loadgen(facts(Map.of()), null, false, 10)).containsExactly("FROM_INSPECTOR");
    }

    @Test
    void cleanSnapshotsProduceNoFlags() {
        Map<String, Double> m = Map.of(GatewayProbe.ACTIVE_STREAMS, 0.0, GatewayProbe.QUEUE_CHUNKS, 0.0,
                GatewayProbe.QUEUE_ADMISSION, 0.0, GatewayProbe.rejectedPath("limit"), 0.0);
        assertThat(RunFlags.state(snapshot(m, sim(0, 0, 0)), snapshot(m, sim(0, 0, 0)))).isEmpty();
    }

    @Test
    void stateFlagsFromDeltasAndAbsoluteValues() {
        Map<String, Double> pre = Map.of(GatewayProbe.ACTIVE_STREAMS, 2.0, GatewayProbe.timeoutsPath("idle"), 1.0,
                GatewayProbe.rejectedPath("limit"), 4.0);
        Map<String, Double> post = Map.of(GatewayProbe.ACTIVE_STREAMS, 1.0, GatewayProbe.QUEUE_ADMISSION, 3.0,
                GatewayProbe.timeoutsPath("idle"), 2.0, GatewayProbe.rejectedPath("limit"), 9.0,
                GatewayProbe.rejectedPath("queue_timeout"), 0.0, GatewayProbe.retriesPath("restart"), 0.0,
                GatewayProbe.RETRY_EXHAUSTED, 1.0);
        Map<String, Double> preFull = new LinkedHashMap<>(pre);
        preFull.put(GatewayProbe.rejectedPath("queue_timeout"), 0.0);
        preFull.put(GatewayProbe.retriesPath("restart"), 0.0);
        preFull.put(GatewayProbe.RETRY_EXHAUSTED, 0.0);
        Map<String, Double> postFull = new LinkedHashMap<>(post);
        postFull.put(GatewayProbe.rejectedPath("shutdown"), 2.0);

        List<String> flags = RunFlags.state(snapshot(preFull, sim(0, 0, 0)), snapshot(postFull, sim(1, 2, 3)));

        assertThat(flags).containsExactlyInAnyOrder(Flags.ACTIVE_STREAMS_NOT_ZERO_BEFORE,
                Flags.ACTIVE_STREAMS_NOT_ZERO_AFTER, Flags.QUEUE_DEPTH_NOT_ZERO_AFTER, Flags.TIMEOUTS_DELTA_NONZERO,
                Flags.RETRY_EXHAUSTED_DELTA_NONZERO, Flags.SIMULATOR_ACTIVE_GENERATIONS_NOT_ZERO_AFTER,
                Flags.SIMULATOR_FAULTED_DELTA_NONZERO, Flags.SIMULATOR_REJECTED_DELTA_NONZERO);
        // limit rejections are expected above the limit and are not flagged as a reason; shutdown has no pre value
        assertThat(flags).doesNotContain(Flags.rejectedReasonDelta("limit"), Flags.rejectedReasonDelta("shutdown"));
    }

    @Test
    void nonLimitRejectReasonsAreFlagged() {
        Map<String, Double> pre = Map.of(GatewayProbe.rejectedPath("upstream_unavailable"), 0.0);
        Map<String, Double> post = Map.of(GatewayProbe.rejectedPath("upstream_unavailable"), 5.0);
        assertThat(RunFlags.state(snapshot(pre, sim(0, 0, 0)), snapshot(post, sim(0, 0, 0))))
                .containsExactly("REJECTED_REASON_UPSTREAM_UNAVAILABLE_DELTA_NONZERO");
    }

    @Test
    void unavailableSimulatorStatsAreFlagged() {
        assertThat(RunFlags.state(snapshot(Map.of(), sim(0, 0, 0)), snapshot(Map.of(), null)))
                .containsExactly(Flags.SIMULATOR_STATS_UNAVAILABLE);
    }
}
