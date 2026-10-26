package io.github.cemharput.streamlab.report;

import static org.assertj.core.api.Assertions.assertThat;

import io.github.cemharput.streamlab.bench.GatewayProbe;
import io.github.cemharput.streamlab.loadgen.client.Outcome;
import io.github.cemharput.streamlab.report.RunAssessment.Analysed;
import io.github.cemharput.streamlab.report.RunAssessment.Analysis;
import io.github.cemharput.streamlab.report.testsupport.Analyses;
import io.github.cemharput.streamlab.report.testsupport.ResultSetBuilder;
import io.github.cemharput.streamlab.report.testsupport.ResultSetBuilder.Req;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.Timeout.ThreadMode;
import org.junit.jupiter.api.io.TempDir;

@Timeout(value = 30, unit = TimeUnit.SECONDS, threadMode = ThreadMode.SEPARATE_THREAD)
class RunMetricsTest {

    private static final long MS = ResultSetBuilder.MS;

    @TempDir
    Path tmp;

    private ResultSetBuilder builder() {
        return ResultSetBuilder.at(tmp.resolve("set")).concurrency(4).repetitions(1).plan();
    }

    private static RunMetrics metrics(Analysis a, int index) {
        return a.runs().get(index - 1).metrics();
    }

    @Test
    void knownOutcomeOrderEqualsLoadgensEnum() {
        assertThat(RunMetrics.KNOWN_OUTCOMES)
                .isEqualTo(Arrays.stream(Outcome.values()).map(Enum::name).toList());
    }

    @Test
    void outcomeOrderIsKnownFirstThenAlphabetical() {
        assertThat(RunMetrics.orderOutcomes(List.of("ZED", "TIMEOUT", "ALPHA", "COMPLETED", "REJECTED")))
                .containsExactly("COMPLETED", "REJECTED", "TIMEOUT", "ALPHA", "ZED");
    }

    @Test
    void outcomeCountsSharesAndTheDistributionsOfCompletedOnly() throws Exception {
        ResultSetBuilder b = builder();
        List<Req> reqs = new ArrayList<>();
        // 4 completed with ttfc 1..4 ms and total 10..40 ms, 4 rejected, 2 timeouts (not in the distributions)
        for (int i = 1; i <= 4; i++) {
            reqs.add(Req.completed(1000 + i * 100, 1, i, i * 10));
        }
        for (int i = 0; i < 4; i++) {
            reqs.add(Req.rejected(2000 + i));
        }
        reqs.add(Req.of("TIMEOUT", 3000, 900));
        reqs.add(Req.of("TIMEOUT", 3100, 900));
        b.run(1).requests(reqs);
        RunMetrics m = metrics(Analyses.of(b), 1);
        assertThat(m.requestLines()).isEqualTo(10);
        assertThat(m.outcomes()).containsExactly(java.util.Map.entry("COMPLETED", 4L), java.util.Map.entry("REJECTED", 4L),
                java.util.Map.entry("TIMEOUT", 2L));
        assertThat(m.rejected()).isEqualTo(4);
        assertThat(m.rejectedShare().stripTrailingZeros().toPlainString()).isEqualTo("40");
        // sorted 1,2,3,4 ms: p50 rank 2 -> 2, p90 rank 4 -> 4, p99 rank 4 -> 4
        assertThat(m.ttfc().count()).isEqualTo(4);
        assertThat(m.ttfc().p50()).isEqualTo(2 * MS);
        assertThat(m.ttfc().p90()).isEqualTo(4 * MS);
        assertThat(m.ttfc().max()).isEqualTo(4 * MS);
        assertThat(m.total().p50()).isEqualTo(20 * MS);
        assertThat(m.total().p99()).isEqualTo(40 * MS);
    }

    @Test
    void completedPerSecondUsesTheWindowAndExactDivision() throws Exception {
        // window [1000 ms, 10000 ms) = 9 s. Two completions inside, one before, one at the end bound (out).
        ResultSetBuilder b = builder();
        b.run(1).requests(List.of(
                new Req("COMPLETED", 0, 0, 999 * MS, 1 * MS, 2 * MS),
                new Req("COMPLETED", 0, 0, 1000 * MS, 1 * MS, 2 * MS),
                new Req("COMPLETED", 0, 0, 9999 * MS, 1 * MS, 2 * MS),
                new Req("COMPLETED", 0, 0, 10_000 * MS, 1 * MS, 2 * MS)));
        RunMetrics m = metrics(Analyses.of(b), 1);
        assertThat(m.completedInWindow()).isEqualTo(2);
        assertThat(m.windowMs()).isEqualTo(9000);
        // 2 * 1000 / 9000 = 0.222222222...
        assertThat(m.completedPerSecond().toPlainString()).isEqualTo("0.222222222222222222222222222222");
        assertThat(Fmt.dec(m.completedPerSecond(), 2)).isEqualTo("0.22");
        assertThat(m.footerElapsedNanos()).isEqualTo(10_005 * MS);
    }

    @Test
    void interChunkValuesComeFromTheFooterHistogramAndIncludeEveryStream() throws Exception {
        ResultSetBuilder b = builder();
        b.run(1).interChunkNanos = new long[] {1, 2, 3, 4, 5, 6, 7, 8, 9, 10};
        RunMetrics m = metrics(Analyses.of(b), 1);
        assertThat(m.interChunk().count()).isEqualTo(10);
        assertThat(m.interChunk().p50()).isEqualTo(5);
        assertThat(m.interChunk().p90()).isEqualTo(9);
        assertThat(m.interChunk().p99()).isEqualTo(10);
        assertThat(m.interChunk().max()).isEqualTo(10);
    }

    @Test
    void aMismatchingInterChunkCountMakesItUnavailableWithACaution() throws Exception {
        ResultSetBuilder b = builder();
        b.run(1).footerOverrides.put("interChunkCount", 99L);
        Analysis a = Analyses.of(b);
        assertThat(metrics(a, 1).interChunk()).isNull();
        assertThat(a.runs().get(0).assessment().reasons()).extracting(RunAssessment.Reason::code)
                .contains(Checks.INTER_CHUNK_UNAVAILABLE);
        assertThat(a.runs().get(0).assessment().classification()).isEqualTo("caution");
    }

    @Test
    void aBrokenBase64HistogramIsACautionNotACrash() throws Exception {
        ResultSetBuilder b = builder();
        b.run(1).footerOverrides.put("interChunkHistogram", "%%%");
        Analysis a = Analyses.of(b);
        assertThat(metrics(a, 1).interChunk()).isNull();
        assertThat(a.runs().get(0).assessment().classification()).isEqualTo("caution");
    }

    @Test
    void snapshotDeltasArePostMinusPreAndGaugesAreValues() throws Exception {
        ResultSetBuilder b = builder();
        b.run(1).preGateway.put(GatewayProbe.REJECTED_TOTAL, 3.0);
        b.run(1).preGateway.put(GatewayProbe.ACTIVE_STREAMS, 2.0);
        b.run(1).requests(List.of(Req.rejected(1000), Req.rejected(1100), Req.rejected(1200), Req.rejected(1300),
                Req.rejected(1400), Req.rejected(1500), Req.completed(2000, 1, 2, 3)));
        b.run(1).postGateway.put(GatewayProbe.rejectedPath("limit"), 6.0);
        b.run(1).postGateway.put(GatewayProbe.REJECTED_TOTAL, 9.0);
        b.run(1).postGateway.put(GatewayProbe.ACTIVE_STREAMS, 0.0);
        b.run(1).postGateway.put(GatewayProbe.retriesPath("resume"), 1.0);
        RunMetrics m = metrics(Analyses.of(b), 1);
        RunMetrics.Snapshots s = m.snapshots();
        assertThat(s.available()).isTrue();
        assertThat(Fmt.num(s.gatewayDeltas().get("rejected_total"))).isEqualTo("6");
        assertThat(Fmt.num(s.gatewayDeltas().get("rejected_total{reason=limit}"))).isEqualTo("6");
        assertThat(Fmt.num(s.gatewayDeltas().get("rejected_total{reason=shutdown}"))).isEqualTo("0");
        assertThat(Fmt.num(s.gatewayDeltas().get("retries_total{reason=resume}"))).isEqualTo("1");
        assertThat(s.gatewayGauges().get("active_streams").pre()).isEqualTo(2.0);
        assertThat(s.gatewayGauges().get("active_streams").post()).isEqualTo(0.0);
        assertThat(Fmt.num(s.simulatorDeltas().get("started_total"))).isEqualTo("7");
        assertThat(Fmt.num(s.simulatorDeltas().get("completed_total"))).isEqualTo("1");
        // 6 rejected lines and a delta of 6: consistent, so no REJECTED_METRIC_MISMATCH
        assertThat(m.findings()).isEmpty();
    }

    @Test
    void aDecreasedCounterIsKeptNegativeAndFlagged() throws Exception {
        ResultSetBuilder b = builder();
        b.run(1).preGateway.put(GatewayProbe.timeoutsPath("idle"), 5.0);
        b.run(1).postGateway.put(GatewayProbe.timeoutsPath("idle"), 2.0);
        Analysis a = Analyses.of(b);
        RunMetrics m = metrics(a, 1);
        assertThat(Fmt.num(m.snapshots().gatewayDeltas().get("timeouts_total{kind=idle}"))).isEqualTo("-3");
        assertThat(m.findings()).extracting(Checks.Finding::code).containsExactly(Checks.COUNTER_DECREASED);
        assertThat(a.runs().get(0).assessment().classification()).isEqualTo("caution");
    }

    @Test
    void aRejectedMetricThatDisagreesWithTheLinesIsACaution() throws Exception {
        ResultSetBuilder b = builder();
        b.run(1).postGateway.put(GatewayProbe.REJECTED_TOTAL, 5.0); // the lines have no REJECTED request
        RunMetrics m = metrics(Analyses.of(b), 1);
        assertThat(m.findings()).extracting(Checks.Finding::code).containsExactly(Checks.REJECTED_METRIC_MISMATCH);
    }

    @Test
    void aMissingOrUnavailableSimulatorGivesNotAvailableDeltasNotZeros() throws Exception {
        ResultSetBuilder b = builder();
        b.run(1).simulatorUnavailable = true;
        RunMetrics m = metrics(Analyses.of(b), 1);
        assertThat(m.snapshots().available()).isTrue();
        assertThat(m.snapshots().simulatorDeltas().values()).containsOnlyNulls();
        assertThat(m.snapshots().simulatorActive()).isNull();
    }

    @Test
    void missingSnapshotsMeanNoDeltas() throws Exception {
        ResultSetBuilder b = builder();
        b.run(1).writeSnapshots = false;
        Analysis a = Analyses.of(b);
        assertThat(metrics(a, 1).snapshots().available()).isFalse();
        assertThat(a.runs().get(0).assessment().reasons()).extracting(RunAssessment.Reason::code)
                .contains(Checks.SNAPSHOT_UNAVAILABLE);
    }

    @Test
    void countMismatchesBetweenFooterSummaryManifestAndLinesExcludeTheRun() throws Exception {
        ResultSetBuilder b = builder();
        b.run(1).footerOverrides.put("recordsWritten", 11L);
        b.run(2).summaryOverrides.put("outcome_COMPLETED", 9L);
        Analysis a = Analyses.of(b);
        for (int i = 1; i <= 2; i++) {
            Analysed r = a.runs().get(i - 1);
            assertThat(r.assessment().classification()).isEqualTo("excluded");
            assertThat(r.assessment().reasons()).extracting(RunAssessment.Reason::code).contains(Checks.COUNT_MISMATCH);
        }
        assertThat(a.runs().get(0).metrics().findings().get(0).detail()).contains("footer recordsWritten 11 vs 10");
    }

    @Test
    void headerParametersThatDifferFromTheManifestExcludeTheRun() throws Exception {
        ResultSetBuilder b = builder();
        b.run(1).headerOverrides.put("concurrency", 7L);
        b.run(2).headerOverrides.put("durationMs", 99L);
        Analysis a = Analyses.of(b);
        assertThat(a.runs().get(0).assessment().reasons()).extracting(RunAssessment.Reason::code)
                .contains(Checks.HEADER_PARAMETERS_MISMATCH);
        assertThat(a.runs().get(1).assessment().reasons()).extracting(RunAssessment.Reason::code)
                .contains(Checks.HEADER_PARAMETERS_MISMATCH);
    }

    @Test
    void generatorDelayIsReportedAsADataQualityIndicator() throws Exception {
        ResultSetBuilder b = builder();
        b.run(1).requests(List.of(new Req("COMPLETED", 100 * MS, 104 * MS, 5 * MS, 1 * MS, 2 * MS)));
        RunMetrics m = metrics(Analyses.of(b), 1);
        assertThat(m.generatorDelayCount()).isEqualTo(1);
        assertThat(m.generatorDelayMax()).isBetween(4 * MS, 4 * MS + 4_000);
    }

    @Test
    void anUnreadableMeasuredFileGivesNoMetricsAtAll() throws Exception {
        ResultSetBuilder b = builder();
        b.run(1).headerOverrides.put("schemaVersion", 2L);
        b.run(2).writeJsonl = false;
        Analysis a = Analyses.of(b);
        assertThat(a.runs().get(0).metrics()).isNull();
        assertThat(a.runs().get(1).metrics()).isNull();
    }
}
