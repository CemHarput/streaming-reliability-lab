package io.github.cemharput.streamlab.bench;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;

/**
 * Builds the ordered run list. Concurrency is the outer loop. For every concurrency repetition {@code r}
 * (0 based) runs {@code [A,B]} when {@code r} is even and {@code [B,A]} when it is odd, where A and B are the
 * first and second mode of {@code --mode-order}. Four repetitions therefore give ABBA ABBA.
 */
public final class PlanBuilder {

    public static final String ALTERNATION = "ABBA";

    /** One cell: one fresh simulator and gateway, one warm-up and one measured loadgen run. */
    public record PlannedRun(int index, String id, int concurrency, ConnectionMode mode, int repetition,
            String orderLabel) {
    }

    public record RunPlan(List<PlannedRun> runs) {

        public RunPlan {
            runs = List.copyOf(runs);
        }

        /** Nominal total: per run warm-up + measured + cooldown; start-up, drain and stop times are extra. */
        public Duration nominalDuration(BenchOptions o) {
            Duration perRun = o.warmup().plus(o.duration()).plus(o.cooldown());
            return perRun.multipliedBy(runs.size());
        }
    }

    private PlanBuilder() {
    }

    public static RunPlan build(List<Integer> concurrency, int repetitions, List<ConnectionMode> modeOrder) {
        ConnectionMode a = modeOrder.get(0);
        ConnectionMode b = modeOrder.get(1);
        List<PlannedRun> runs = new ArrayList<>();
        int index = 0;
        for (int c : concurrency) {
            for (int r = 0; r < repetitions; r++) {
                boolean aFirst = r % 2 == 0;
                String label = aFirst ? "AB" : "BA";
                for (ConnectionMode mode : aFirst ? List.of(a, b) : List.of(b, a)) {
                    index++;
                    String id = String.format("%03d-c%d-%s-r%d", index, c, mode.property(), r + 1);
                    runs.add(new PlannedRun(index, id, c, mode, r + 1, label));
                }
            }
        }
        return new RunPlan(runs);
    }

    public static RunPlan build(BenchOptions o) {
        return build(o.concurrency(), o.repetitions(), o.modeOrder());
    }
}
