package io.github.cemharput.streamlab.report;

import static org.assertj.core.api.Assertions.assertThat;

import io.github.cemharput.streamlab.bench.Flags;
import io.github.cemharput.streamlab.bench.GatewayProbe;
import io.github.cemharput.streamlab.report.FlagPolicy.Source;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.Timeout.ThreadMode;

/** The classification table of the plan, written out a second time here so that a change to either one is noticed. */
@Timeout(value = 30, unit = TimeUnit.SECONDS, threadMode = ThreadMode.SEPARATE_THREAD)
class FlagPolicyTest {

    private static final Map<String, Severity> EXPECTED = new LinkedHashMap<>();

    static {
        Severity x = Severity.EXCLUDE;
        Severity c = Severity.CAUTION;
        Severity i = Severity.INFO;
        EXPECTED.put("OUTCOME_INTERRUPTED_NONZERO", x);
        EXPECTED.put("OUTCOME_<NAME>_NONZERO", c);
        EXPECTED.put("REJECTED_AT_OR_BELOW_LIMIT", c);
        EXPECTED.put("LOADGEN_EXIT_NONZERO", x);
        EXPECTED.put("LOADGEN_WAIT_EXCEEDED", x);
        EXPECTED.put("SUMMARY_MISSING", c);
        EXPECTED.put("JSONL_HEADER_UNEXPECTED", x);
        EXPECTED.put("JSONL_FOOTER_MISSING", x);
        EXPECTED.put("STOP_REASON_NOT_DURATION", x);
        EXPECTED.put("FOOTER_INTERRUPTED_NONZERO", x);
        EXPECTED.put("ACTIVE_STREAMS_NOT_ZERO_BEFORE", x);
        EXPECTED.put("ACTIVE_STREAMS_NOT_ZERO_AFTER", c);
        EXPECTED.put("QUEUE_DEPTH_NOT_ZERO_AFTER", c);
        EXPECTED.put("REJECTED_REASON_SHUTDOWN_DELTA_NONZERO", x);
        EXPECTED.put("REJECTED_REASON_<R>_DELTA_NONZERO", c);
        EXPECTED.put("TIMEOUTS_DELTA_NONZERO", c);
        EXPECTED.put("RETRIES_DELTA_NONZERO", c);
        EXPECTED.put("RETRY_EXHAUSTED_DELTA_NONZERO", c);
        EXPECTED.put("SIMULATOR_ACTIVE_GENERATIONS_NOT_ZERO_AFTER", c);
        EXPECTED.put("SIMULATOR_REJECTED_DELTA_NONZERO", c);
        EXPECTED.put("SIMULATOR_STATS_UNAVAILABLE", c);
        EXPECTED.put("SIMULATOR_FAULTED_DELTA_NONZERO", x);
        EXPECTED.put("GATEWAY_EXITED", x);
        EXPECTED.put("SIMULATOR_EXITED", x);
        EXPECTED.put("CONNECTION_MODE_MISMATCH", x);
        EXPECTED.put("PORT_NOT_FREED", c);
        EXPECTED.put("CPU_BUSY_BEFORE_RUN", x);
        EXPECTED.put("CPU_LOAD_UNAVAILABLE", c);
        EXPECTED.put("LOG_TRUNCATED", c);
        EXPECTED.put("LOG_WRITE_ERROR", c);
        EXPECTED.put("TIME_WAIT_UNAVAILABLE", i);
        EXPECTED.put("OUTPUT_NOT_GITIGNORED", i);
        EXPECTED.put("GIT_DIRTY", c);
        EXPECTED.put("GIT_UNAVAILABLE", c);
        EXPECTED.put("ARTIFACT_OLDER_THAN_HEAD", c);
        EXPECTED.put("CHECKLIST_NOT_CONFIRMED", c);
        EXPECTED.put("CHECKLIST_ANSWER_NO", c);
        EXPECTED.put("INTERRUPTED", c);
        EXPECTED.put("SMOKE_PRESET", x);
    }

    @Test
    void theTableEqualsThePlanRowForRow() {
        Map<String, Severity> actual = new LinkedHashMap<>();
        FlagPolicy.entries().forEach(e -> actual.put(e.name(), e.severity()));
        assertThat(actual).isEqualTo(EXPECTED);
    }

    @Test
    void everyDocumentedFlagOfTheRunnerHasAClassAndNoneIsUnknown() {
        for (String documented : Flags.DOCUMENTED) {
            String concrete = switch (documented) {
                case "OUTCOME_<NAME>_NONZERO" -> Flags.outcomeNonzero("TIMEOUT");
                case "REJECTED_REASON_<R>_DELTA_NONZERO" -> Flags.rejectedReasonDelta("queue_timeout");
                default -> documented;
            };
            FlagPolicy.Classification cls = FlagPolicy.classify(concrete, Source.RUN);
            assertThat(cls.known()).as(documented).isTrue();
            assertThat(cls.severity()).as(documented).isEqualTo(EXPECTED.get(documented));
        }
        // the table has no entry that the runner does not document
        // the table has the two concrete rows (interrupted outcome, shutdown reason) on top of the documented names
        Set<String> names = new HashSet<>(EXPECTED.keySet());
        names.remove("OUTCOME_INTERRUPTED_NONZERO");
        names.remove("REJECTED_REASON_SHUTDOWN_DELTA_NONZERO");
        assertThat(names).containsExactlyInAnyOrderElementsOf(Flags.DOCUMENTED);
    }

    @Test
    void outcomePatternInstancesAreCautionExceptInterrupted() {
        for (String o : List.of("ERROR_EVENT", "HTTP_ERROR", "PROTOCOL_ERROR", "NO_TERMINAL", "RESET", "TIMEOUT",
                "CONNECT_FAILURE", "CLIENT_ERROR")) {
            assertThat(FlagPolicy.classify(Flags.outcomeNonzero(o), Source.MEASURED).severity()).as(o)
                    .isEqualTo(Severity.CAUTION);
        }
        assertThat(FlagPolicy.classify(Flags.outcomeNonzero("INTERRUPTED"), Source.MEASURED).severity())
                .isEqualTo(Severity.EXCLUDE);
    }

    @Test
    void everyRejectedReasonOfTheGatewayIsClassifiedAndOnlyShutdownExcludes() {
        for (String reason : GatewayProbe.REJECTED_REASONS) {
            if (reason.equals("limit")) {
                continue; // the runner never records a delta flag for the normal rejection reason
            }
            Severity expected = reason.equals("shutdown") ? Severity.EXCLUDE : Severity.CAUTION;
            assertThat(FlagPolicy.classify(Flags.rejectedReasonDelta(reason), Source.RUN)).as(reason)
                    .extracting(FlagPolicy.Classification::severity, FlagPolicy.Classification::known)
                    .containsExactly(expected, true);
        }
    }

    @Test
    void anUnknownFlagIsExcludedAndMarkedUnknown() {
        FlagPolicy.Classification cls = FlagPolicy.classify("SOMETHING_NEW", Source.RUN);
        assertThat(cls.severity()).isEqualTo(Severity.EXCLUDE);
        assertThat(cls.known()).isFalse();
        assertThat(FlagPolicy.isKnown("SOMETHING_NEW")).isFalse();
        assertThat(FlagPolicy.classify("OUTCOME_x_NONZERO", Source.RUN).known()).isFalse(); // lower case: not a runner flag
    }

    @Test
    void aWarmUpFlagIsNeverExcludingEvenWhenUnknown() {
        for (String flag : List.of(Flags.GATEWAY_EXITED, Flags.outcomeNonzero("INTERRUPTED"), "SOMETHING_NEW",
                Flags.LOADGEN_EXIT_NONZERO)) {
            FlagPolicy.Classification cls = FlagPolicy.classify(flag, Source.WARMUP);
            assertThat(cls.severity()).as(flag).isEqualTo(Severity.CAUTION);
            assertThat(cls.rationale()).isEqualTo("warm-up anomaly");
        }
        assertThat(FlagPolicy.classify("SOMETHING_NEW", Source.WARMUP).known()).isFalse();
    }

    @Test
    void setScopedRowsAreTheSetLevelFlagsOfTheRunner() {
        Set<String> setScoped = new HashSet<>();
        FlagPolicy.entries().stream().filter(e -> e.scope().equals("set")).forEach(e -> setScoped.add(e.name()));
        assertThat(setScoped).containsExactlyInAnyOrder(Flags.OUTPUT_NOT_GITIGNORED,
                Flags.GIT_DIRTY, Flags.GIT_UNAVAILABLE, Flags.ARTIFACT_OLDER_THAN_HEAD, Flags.CHECKLIST_NOT_CONFIRMED,
                Flags.CHECKLIST_ANSWER_NO, Flags.INTERRUPTED, Flags.SMOKE_PRESET);
    }

    @Test
    void checkCodesNeverCollideWithTheRunnerDictionary() {
        for (String code : Checks.TABLE.keySet()) {
            assertThat(code).startsWith("CHECK_");
            assertThat(Flags.DOCUMENTED).doesNotContain(code);
            assertThat(FlagPolicy.isKnown(code)).as(code).isFalse();
        }
        assertThat(Checks.TABLE).hasSize(18);
        assertThat(Checks.TABLE.values().stream().filter(s -> s == Severity.EXCLUDE).count()).isEqualTo(9);
        assertThat(Checks.TABLE.values().stream().filter(s -> s == Severity.CAUTION).count()).isEqualTo(7);
        assertThat(Checks.TABLE.values().stream().filter(s -> s == Severity.INFO).count()).isEqualTo(2);
    }

    @Test
    void everyCheckCodeIsCoveredByACrossCheckOrIsInformation() {
        Set<String> covered = new HashSet<>();
        Checks.CROSS_CHECKS.forEach(c -> covered.addAll(c.codes()));
        for (String code : Checks.TABLE.keySet()) {
            if (Checks.TABLE.get(code) != Severity.INFO) {
                assertThat(covered).as(code).contains(code);
            }
        }
        assertThat(Checks.CROSS_CHECKS).hasSize(12);
    }
}
