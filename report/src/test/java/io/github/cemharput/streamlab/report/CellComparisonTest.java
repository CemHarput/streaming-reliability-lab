package io.github.cemharput.streamlab.report;

import static org.assertj.core.api.Assertions.assertThat;

import io.github.cemharput.streamlab.bench.ConnectionMode;
import io.github.cemharput.streamlab.bench.Flags;
import io.github.cemharput.streamlab.report.CellComparison.Cell;
import io.github.cemharput.streamlab.report.RunAssessment.Analysis;
import io.github.cemharput.streamlab.report.testsupport.Analyses;
import io.github.cemharput.streamlab.report.testsupport.ResultSetBuilder;
import java.math.BigDecimal;
import java.nio.file.Path;
import java.util.List;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.Timeout.ThreadMode;
import org.junit.jupiter.api.io.TempDir;

/**
 * Concurrency 4, four repetitions: shared runs are #1, #4, #5, #8 (repetitions 1..4) and per-request runs are #2, #3,
 * #6, #7. Every run has a single completed request, so the per-run ttfc p50 is exactly the value that is set.
 */
@Timeout(value = 30, unit = TimeUnit.SECONDS, threadMode = ThreadMode.SEPARATE_THREAD)
class CellComparisonTest {

    private static final int[] SHARED = {1, 4, 5, 8};
    private static final int[] PER_REQUEST = {2, 3, 6, 7};

    @TempDir
    Path tmp;

    private ResultSetBuilder builder() {
        return ResultSetBuilder.at(tmp.resolve("set")).concurrency(4).repetitions(4).plan();
    }

    private static void set(ResultSetBuilder b, int[] runs, long... ttfcMs) {
        for (int i = 0; i < runs.length; i++) {
            b.run(runs[i]).completedMs(new long[] {ttfcMs[i]}, new long[] {ttfcMs[i] + 100});
        }
    }

    private static Cell ttfc50(Analysis a) {
        return CellComparison.compare(a, CellComparison.modeOrder(a), 4, ComparedMetric.TTFC_P50);
    }

    @Test
    void separatedRangesWithALargeDifferenceStateHigher() throws Exception {
        ResultSetBuilder b = builder();
        set(b, SHARED, 10, 11, 12, 13);
        set(b, PER_REQUEST, 20, 21, 22, 23);
        Cell c = ttfc50(Analyses.of(b));
        assertThat(c.decision().statement()).isEqualTo(Statement.HIGHER);
        assertThat(c.stats().get("shared").median().toPlainString()).isEqualTo("11500000");
        assertThat(c.stats().get("per-request").median().toPlainString()).isEqualTo("21500000");
        assertThat(c.decision().difference().stripTrailingZeros().toPlainString()).isEqualTo("10000000");
        assertThat(c.decision().spread().stripTrailingZeros().toPlainString()).isEqualTo("3000000");
        assertThat(c.sentence()).isEqualTo("In this result set, at concurrency 4, the median time to first chunk p50"
                + " of per-request (21.500 ms) was higher than that of shared (11.500 ms); the ranges did not overlap"
                + " and the difference between the medians (10.000 ms) was larger than the largest within-mode range"
                + " (3.000 ms). With n=4 and n=4 repetitions this describes this set only.");
        assertThat(c.withoutCaution()).isNull();
    }

    @Test
    void overlappingRangesCannotBeSeparated() throws Exception {
        ResultSetBuilder b = builder();
        set(b, SHARED, 10, 12, 14, 16);
        set(b, PER_REQUEST, 15, 17, 19, 21);
        Cell c = ttfc50(Analyses.of(b));
        assertThat(c.decision().statement()).isEqualTo(Statement.CANNOT_SEPARATE_OVERLAP);
        assertThat(c.sentence()).contains("the ranges overlapped").contains("cannot separate the two modes");
    }

    @Test
    void touchingRangesCountAsOverlapping() throws Exception {
        ResultSetBuilder b = builder();
        set(b, SHARED, 10, 11, 12, 13);
        set(b, PER_REQUEST, 13, 14, 15, 16);
        assertThat(ttfc50(Analyses.of(b)).decision().statement()).isEqualTo(Statement.CANNOT_SEPARATE_OVERLAP);
    }

    @Test
    void separatedRangesWithADifferenceNotLargerThanTheSpreadCannotBeSeparated() throws Exception {
        ResultSetBuilder b = builder();
        set(b, SHARED, 10, 20, 10, 20); // median 15, range 10
        set(b, PER_REQUEST, 21, 40, 22, 41); // median 31, range 20; d = 16 <= 20
        Cell c = ttfc50(Analyses.of(b));
        assertThat(c.decision().statement()).isEqualTo(Statement.CANNOT_SEPARATE_SPREAD);
        assertThat(c.sentence()).contains("was not larger than the largest within-mode range")
                .contains("cannot separate the two modes");
    }

    @Test
    void aDifferenceExactlyEqualToTheSpreadIsNotLargerThanIt() throws Exception {
        ResultSetBuilder b = builder();
        set(b, SHARED, 10, 10, 20, 20); // median 15, range 10
        set(b, PER_REQUEST, 21, 24, 26, 29); // median 25, range 8, disjoint from [10, 20]; d = 10 = S
        Cell c = ttfc50(Analyses.of(b));
        assertThat(c.decision().difference()).isEqualByComparingTo("10000000");
        assertThat(c.decision().spread()).isEqualByComparingTo("10000000");
        assertThat(c.decision().statement()).isEqualTo(Statement.CANNOT_SEPARATE_SPREAD);
    }

    @Test
    void aDifferenceJustAboveTheSpreadIsHigher() throws Exception {
        ResultSetBuilder b = builder();
        set(b, SHARED, 10, 10, 20, 20); // median 15, range 10
        set(b, PER_REQUEST, 21, 24, 27, 30); // median 25.5, range 9; d = 10.5 > 10
        assertThat(ttfc50(Analyses.of(b)).decision().statement()).isEqualTo(Statement.HIGHER);
    }

    @Test
    void equalMediansAreReportedAsEqual() throws Exception {
        ResultSetBuilder b = builder();
        set(b, SHARED, 10, 12, 14, 16); // median 13
        set(b, PER_REQUEST, 11, 12, 14, 15); // median 13
        Cell c = ttfc50(Analyses.of(b));
        assertThat(c.decision().statement()).isEqualTo(Statement.EQUAL);
        assertThat(c.sentence()).contains("were equal").contains("cannot separate");
    }

    @Test
    void fewerThanTwoRunsInAModeAreInsufficient() throws Exception {
        ResultSetBuilder b = builder();
        set(b, SHARED, 10, 11, 12, 13);
        set(b, PER_REQUEST, 20, 21, 22, 23);
        for (int idx : new int[] {3, 6, 7}) {
            b.run(idx).status("failed");
        }
        Cell c = ttfc50(Analyses.of(b));
        assertThat(c.stats().get("per-request").n()).isEqualTo(1);
        assertThat(c.decision().statement()).isEqualTo(Statement.INSUFFICIENT);
        assertThat(c.decision().difference()).isNull();
        assertThat(c.sentence()).isEqualTo("In this result set, at concurrency 4, there were fewer than two usable"
                + " repetitions for per-request (n=1) for the time to first chunk p50; no comparison is stated.");
    }

    @Test
    void excludedRunsNeverEnterAndCautionRunsAreMarked() throws Exception {
        ResultSetBuilder b = builder();
        set(b, SHARED, 10, 11, 12, 13);
        set(b, PER_REQUEST, 20, 21, 22, 23);
        b.run(5).runFlag(Flags.CPU_BUSY_BEFORE_RUN); // excluded shared r3
        b.run(2).measuredFlag("OUTCOME_TIMEOUT_NONZERO"); // caution per-request r1
        Cell c = ttfc50(Analyses.of(b));
        assertThat(c.stats().get("shared").n()).isEqualTo(3);
        assertThat(c.entries().get("shared")).extracting(CellComparison.Entry::index).containsExactly(1L, 4L, 8L);
        assertThat(c.entries().get("per-request")).extracting(CellComparison.Entry::caution)
                .containsExactly(true, false, false, false);
    }

    @Test
    void positionsFollowAbbaForEveryRepetition() throws Exception {
        ResultSetBuilder b = builder();
        Analysis a = Analyses.of(b);
        Cell c = ttfc50(a);
        // shared r1 AB -> 1st, per-request r1 AB -> 2nd; r2 BA reverses; r3 like r1; r4 like r2
        assertThat(c.entries().get("shared")).extracting(CellComparison.Entry::position)
                .containsExactly("1st", "2nd", "1st", "2nd");
        assertThat(c.entries().get("per-request")).extracting(CellComparison.Entry::position)
                .containsExactly("2nd", "1st", "2nd", "1st");
    }

    @Test
    void aReversedModeOrderReversesThePositionsAndTheColumns() throws Exception {
        ResultSetBuilder b = ResultSetBuilder.at(tmp.resolve("rev")).concurrency(4).repetitions(4)
                .modeOrder(ConnectionMode.PER_REQUEST, ConnectionMode.SHARED).plan();
        Analysis a = Analyses.of(b);
        assertThat(CellComparison.modeOrder(a)).containsExactly("per-request", "shared");
        Cell c = ttfc50(a);
        assertThat(c.entries().get("per-request")).extracting(CellComparison.Entry::position)
                .containsExactly("1st", "2nd", "1st", "2nd");
        assertThat(c.entries().get("shared")).extracting(CellComparison.Entry::position)
                .containsExactly("2nd", "1st", "2nd", "1st");
        assertThat(c.modes()).containsExactly("per-request", "shared");
    }

    @Test
    void aStatementThatDependsOnCautionRunsGetsAnExtraSentence() throws Exception {
        ResultSetBuilder b = builder();
        set(b, SHARED, 10, 11, 12, 13);
        set(b, PER_REQUEST, 20, 21, 22, 12); // run #7 (repetition 4) is the low value
        b.run(7).measuredFlag("OUTCOME_TIMEOUT_NONZERO");
        Cell c = ttfc50(Analyses.of(b));
        assertThat(c.decision().statement()).isEqualTo(Statement.CANNOT_SEPARATE_OVERLAP);
        assertThat(c.withoutCaution()).isEqualTo(Statement.HIGHER);
        assertThat(c.cautionSentence()).isEqualTo("This statement depends on the runs marked * (caution). Without them"
                + " the outcome for this metric would be: the ranges do not overlap and the difference between the"
                + " medians is larger than the largest within-mode range.");
    }

    @Test
    void aCautionRunThatDoesNotChangeTheStatementAddsNoSentence() throws Exception {
        ResultSetBuilder b = builder();
        set(b, SHARED, 10, 11, 12, 13);
        set(b, PER_REQUEST, 20, 21, 22, 23);
        b.run(7).measuredFlag("OUTCOME_TIMEOUT_NONZERO");
        Cell c = ttfc50(Analyses.of(b));
        assertThat(c.decision().statement()).isEqualTo(Statement.HIGHER);
        assertThat(c.cautionSentence()).isNull();
    }

    @Test
    void medianOfAnEvenCountIsTheExactMeanOfTheTwoMiddleValues() {
        List<BigDecimal> values = List.of(new BigDecimal("1"), new BigDecimal("2"), new BigDecimal("4"),
                new BigDecimal("10"));
        assertThat(CellComparison.stats("m", values).median()).isEqualByComparingTo("3");
        assertThat(CellComparison.stats("m", List.of(new BigDecimal("5"), new BigDecimal("1"), new BigDecimal("3")))
                .median()).isEqualByComparingTo("3");
        assertThat(CellComparison.stats("m", List.of()).n()).isZero();
    }

    @Test
    void everyConcurrencyGetsEveryMetricInAFixedOrder() throws Exception {
        ResultSetBuilder b = ResultSetBuilder.at(tmp.resolve("two")).concurrency(2, 4).repetitions(2).plan();
        List<Cell> cells = CellComparison.compareAll(Analyses.of(b));
        assertThat(cells).hasSize(2 * ComparedMetric.values().length);
        assertThat(cells.get(0).concurrency()).isEqualTo(2);
        assertThat(cells.get(0).metric()).isEqualTo(ComparedMetric.values()[0]);
        assertThat(cells.get(cells.size() - 1).concurrency()).isEqualTo(4);
        assertThat(cells.get(cells.size() - 1).metric()).isEqualTo(ComparedMetric.REJECTED_SHARE);
    }

    @Test
    void theComparedMetricsAreTheClosedSetOfThePlan() {
        assertThat(ComparedMetric.values()).extracting(Enum::name).containsExactly("COMPLETED_PER_SECOND", "TTFC_P50",
                "TTFC_P99", "TOTAL_P50", "TOTAL_P99", "INTER_CHUNK_P99", "REJECTED_SHARE");
    }
}
