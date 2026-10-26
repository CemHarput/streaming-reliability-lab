package io.github.cemharput.streamlab.report.testsupport;

import io.github.cemharput.streamlab.bench.Flags;
import io.github.cemharput.streamlab.bench.GatewayProbe;
import io.github.cemharput.streamlab.report.testsupport.ResultSetBuilder.Req;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

/** Ready-made synthetic result sets for the scenario tests. Every value is artificial. */
public final class Scenarios {

    private Scenarios() {
    }

    /** Two concurrencies x four repetitions x two modes, all clean; run directory {@code <dir>/set}. */
    public static ResultSetBuilder clean(Path dir) {
        return ResultSetBuilder.at(dir.resolve("set")).concurrency(2, 4).repetitions(4).plan();
    }

    /** A set with one of everything: clean, flagged (caution), flagged (exclude), failed, rejected-heavy. */
    public static ResultSetBuilder mixed(Path dir) {
        ResultSetBuilder b = ResultSetBuilder.at(dir.resolve("set")).concurrency(4).repetitions(4).plan();
        // run 1 shared r1, 2 per-request r1, 3 per-request r2, 4 shared r2, 5 shared r3, 6 per-request r3,
        // 7 per-request r4, 8 shared r4
        b.run(3).measuredFlag("OUTCOME_TIMEOUT_NONZERO"); // caution
        b.run(5).runFlag(Flags.CPU_BUSY_BEFORE_RUN); // exclude
        b.run(6).status("failed");
        List<Req> heavy = new ArrayList<>();
        for (int i = 0; i < 40; i++) {
            heavy.add(Req.rejected(1000 + i * 100));
        }
        for (int i = 0; i < 10; i++) {
            heavy.add(Req.completed(1500 + i * 500, 1, 30 + i, 130 + i));
        }
        b.run(7).requests(heavy).measuredFlag(Flags.REJECTED_AT_OR_BELOW_LIMIT);
        b.run(8).postGateway.put(GatewayProbe.timeoutsPath("idle"), 2.0);
        b.run(8).runFlag(Flags.TIMEOUTS_DELTA_NONZERO);
        return b;
    }
}
