package io.github.cemharput.streamlab.report;

import static org.assertj.core.api.Assertions.assertThat;

import io.github.cemharput.streamlab.bench.Flags;
import io.github.cemharput.streamlab.report.RunAssessment.Analysis;
import io.github.cemharput.streamlab.report.RunAssessment.Reason;
import io.github.cemharput.streamlab.report.testsupport.Analyses;
import io.github.cemharput.streamlab.report.testsupport.ResultSetBuilder;
import java.nio.file.Path;
import java.util.List;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.Timeout.ThreadMode;
import org.junit.jupiter.api.io.TempDir;

@Timeout(value = 30, unit = TimeUnit.SECONDS, threadMode = ThreadMode.SEPARATE_THREAD)
class RunAssessmentTest {

    @TempDir
    Path tmp;

    private ResultSetBuilder builder() {
        return ResultSetBuilder.at(tmp.resolve("set")).concurrency(4).repetitions(2).plan();
    }

    private static List<String> labels(Analysis a, int index) {
        return a.runs().get(index - 1).assessment().reasons().stream().map(Reason::label).toList();
    }

    private static String cls(Analysis a, int index) {
        return a.runs().get(index - 1).assessment().classification();
    }

    @Test
    void aCleanRunIsUsable() throws Exception {
        Analysis a = Analyses.of(builder());
        for (int i = 1; i <= 4; i++) {
            assertThat(cls(a, i)).isEqualTo("usable");
            assertThat(labels(a, i)).isEmpty();
        }
        assertThat(a.count("usable")).isEqualTo(4);
    }

    @Test
    void theWorstReasonDecidesAndReasonsAreSortedByClassThenCode() {
        RunAssessment r = RunAssessment.assess(List.of(
                new Reason("RUN", "ZETA", Severity.CAUTION, ""),
                new Reason("CHECK", "CHECK_FIELD_MISSING", Severity.INFO, ""),
                new Reason("RUN", "ALPHA", Severity.CAUTION, ""),
                new Reason("MEASURED", "OMEGA", Severity.EXCLUDE, ""),
                new Reason("RUN", "ALPHA", Severity.CAUTION, "duplicate")));
        assertThat(r.classification()).isEqualTo("excluded");
        assertThat(r.reasons()).extracting(Reason::label).containsExactly("MEASURED:OMEGA", "RUN:ALPHA", "RUN:ZETA",
                "CHECK:CHECK_FIELD_MISSING");
        assertThat(RunAssessment.assess(List.of(new Reason("RUN", "X", Severity.CAUTION, ""))).classification())
                .isEqualTo("caution");
        assertThat(RunAssessment.assess(List.of(new Reason("CHECK", "Y", Severity.INFO, ""))).classification())
                .isEqualTo("usable");
        assertThat(RunAssessment.assess(List.of()).classification()).isEqualTo("usable");
    }

    @Test
    void everyStatusThatIsNotCompletedExcludesTheRun() throws Exception {
        ResultSetBuilder b = builder();
        b.run(1).status("failed");
        b.run(2).status("interrupted");
        b.run(3).status("not_run");
        b.run(4).status("planned");
        Analysis a = Analyses.of(b);
        assertThat(labels(a, 1)).contains("STATUS:STATUS_FAILED");
        assertThat(labels(a, 2)).contains("STATUS:STATUS_INTERRUPTED");
        assertThat(labels(a, 3)).contains("STATUS:STATUS_NOT_RUN");
        assertThat(labels(a, 4)).contains("STATUS:STATUS_PLANNED");
        for (int i = 1; i <= 4; i++) {
            assertThat(cls(a, i)).isEqualTo("excluded");
        }
    }

    @Test
    void aRunningStatusIsExcludedToo() throws Exception {
        ResultSetBuilder b = builder();
        b.run(1).status("running");
        assertThat(labels(Analyses.of(b), 1)).contains("STATUS:STATUS_RUNNING");
    }

    @Test
    void flagsAreClassifiedPerSourceAndWarmUpNeverExcludes() throws Exception {
        ResultSetBuilder b = builder();
        b.run(1).measuredFlag("OUTCOME_TIMEOUT_NONZERO"); // caution
        b.run(2).runFlag(Flags.CPU_BUSY_BEFORE_RUN); // exclude
        b.run(3).warmupFlag(Flags.GATEWAY_EXITED); // warm-up: caution only
        b.run(4).measuredFlag(Flags.LOADGEN_EXIT_NONZERO); // exclude
        Analysis a = Analyses.of(b);
        assertThat(cls(a, 1)).isEqualTo("caution");
        assertThat(labels(a, 1)).containsExactly("MEASURED:OUTCOME_TIMEOUT_NONZERO");
        assertThat(cls(a, 2)).isEqualTo("excluded");
        assertThat(labels(a, 2)).containsExactly("RUN:CPU_BUSY_BEFORE_RUN");
        assertThat(cls(a, 3)).isEqualTo("caution");
        assertThat(labels(a, 3)).containsExactly("WARMUP:GATEWAY_EXITED");
        assertThat(cls(a, 4)).isEqualTo("excluded");
    }

    @Test
    void unknownFlagsExcludeAndAreListed() throws Exception {
        ResultSetBuilder b = builder();
        b.run(2).measuredFlag("SOMETHING_NEW");
        b.run(3).warmupFlag("ANOTHER_NEW_ONE");
        Analysis a = Analyses.of(b);
        assertThat(cls(a, 2)).isEqualTo("excluded");
        assertThat(cls(a, 3)).isEqualTo("caution");
        assertThat(a.unknownFlags()).containsExactly("ANOTHER_NEW_ONE", "SOMETHING_NEW");
    }

    @Test
    void anExcludingSetFlagExcludesEveryRunAndACautionSetFlagOnlyAppearsAsABanner() throws Exception {
        ResultSetBuilder b = builder().setFlag(Flags.SMOKE_PRESET).setFlag(Flags.GIT_DIRTY);
        Analysis a = Analyses.of(b);
        for (int i = 1; i <= 4; i++) {
            assertThat(cls(a, i)).isEqualTo("excluded");
            assertThat(labels(a, i)).containsExactly("SET:SMOKE_PRESET");
        }
        assertThat(a.smoke()).isTrue();
        assertThat(a.setReasons()).extracting(Reason::label).containsExactly("SET:SMOKE_PRESET", "SET:GIT_DIRTY");

        Analysis only = Analyses.of(ResultSetBuilder.at(tmp.resolve("o")).concurrency(4).repetitions(2)
                .setFlag(Flags.GIT_DIRTY).plan());
        assertThat(cls(only, 1)).isEqualTo("usable");
        assertThat(only.smoke()).isFalse();
    }

    @Test
    void aSmokePresetWithoutTheFlagIsStillSmoke() throws Exception {
        Analysis a = Analyses.of(ResultSetBuilder.at(tmp.resolve("s")).preset("smoke").plan());
        assertThat(a.smoke()).isTrue();
        assertThat(cls(a, 1)).isEqualTo("excluded");
    }

    @Test
    void anUnknownConnectionModeIsExcluded() throws Exception {
        ResultSetBuilder b = builder();
        b.run(1).manifestRunOverrides.put("mode", "carrier-pigeon");
        Analysis a = Analyses.of(b);
        assertThat(labels(a, 1)).contains("CHECK:CHECK_MODE_UNKNOWN");
        assertThat(cls(a, 1)).isEqualTo("excluded");
    }

    @Test
    void aRunThatDiffersFromThePlanRebuiltFromTheRecordedParametersIsACaution() throws Exception {
        ResultSetBuilder b = builder();
        b.run(2).manifestRunOverrides.put("orderLabel", "BA");
        Analysis a = Analyses.of(b);
        assertThat(labels(a, 2)).containsExactly("CHECK:CHECK_PLAN_MISMATCH");
        assertThat(cls(a, 2)).isEqualTo("caution");
        assertThat(a.expectedRunCount()).isEqualTo(4);
        assertThat(cls(a, 1)).isEqualTo("usable");
    }

    @Test
    void withoutRecordedPlanParametersThePlanCheckIsSkipped() throws Exception {
        Analysis a = Analyses.of(ResultSetBuilder.at(tmp.resolve("np")).omitPlan().plan());
        assertThat(a.expectedRunCount()).isNull();
        assertThat(cls(a, 1)).isEqualTo("usable");
    }

    @Test
    void anUnknownStopMethodIsInformationOnly() throws Exception {
        ResultSetBuilder b = builder();
        b.run(1).stopMethod = "graceful";
        Analysis a = Analyses.of(b);
        assertThat(labels(a, 1)).containsExactly("CHECK:CHECK_STOP_METHOD_UNKNOWN");
        assertThat(cls(a, 1)).isEqualTo("usable");
    }

    @Test
    void everyKnownStopMethodIsAccepted() throws Exception {
        for (String method : List.of("already_exited", "destroy", "forced", "survived")) {
            ResultSetBuilder b = ResultSetBuilder.at(tmp.resolve("m-" + method)).concurrency(4).repetitions(1).plan();
            b.run(1).stopMethod = method;
            assertThat(labels(Analyses.of(b), 1)).as(method).isEmpty();
        }
    }

    @Test
    void missingOptionalRunFieldsAreInformationOnly() throws Exception {
        ResultSetBuilder b = builder();
        b.run(1).manifestRunOverrides.put("portsFreed", null);
        Analysis a = Analyses.of(b);
        assertThat(labels(a, 1)).containsExactly("CHECK:CHECK_FIELD_MISSING");
        assertThat(cls(a, 1)).isEqualTo("usable");
    }

    @Test
    void anExcludedRunKeepsItsMetrics() throws Exception {
        ResultSetBuilder b = builder();
        b.run(1).status("interrupted");
        Analysis a = Analyses.of(b);
        assertThat(cls(a, 1)).isEqualTo("excluded");
        assertThat(a.runs().get(0).metrics()).isNotNull();
        assertThat(a.runs().get(0).included()).isFalse();
    }
}
