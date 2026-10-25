package io.github.cemharput.streamlab.bench;

import java.util.List;

/**
 * The flag dictionary: fixed strings that only record an observation. A flag never changes the exit code,
 * hides a run or deletes a file. The same list is documented in docs/benchmark-protocol.md (a test compares them).
 */
public final class Flags {

    // loadgen output
    public static final String OUTCOME_NONZERO_PATTERN = "OUTCOME_<NAME>_NONZERO";
    public static final String REJECTED_AT_OR_BELOW_LIMIT = "REJECTED_AT_OR_BELOW_LIMIT";
    public static final String LOADGEN_EXIT_NONZERO = "LOADGEN_EXIT_NONZERO";
    public static final String LOADGEN_WAIT_EXCEEDED = "LOADGEN_WAIT_EXCEEDED";
    public static final String SUMMARY_MISSING = "SUMMARY_MISSING";
    public static final String JSONL_HEADER_UNEXPECTED = "JSONL_HEADER_UNEXPECTED";
    public static final String JSONL_FOOTER_MISSING = "JSONL_FOOTER_MISSING";
    public static final String STOP_REASON_NOT_DURATION = "STOP_REASON_NOT_DURATION";
    public static final String FOOTER_INTERRUPTED_NONZERO = "FOOTER_INTERRUPTED_NONZERO";

    // gateway and simulator state
    public static final String ACTIVE_STREAMS_NOT_ZERO_BEFORE = "ACTIVE_STREAMS_NOT_ZERO_BEFORE";
    public static final String ACTIVE_STREAMS_NOT_ZERO_AFTER = "ACTIVE_STREAMS_NOT_ZERO_AFTER";
    public static final String QUEUE_DEPTH_NOT_ZERO_AFTER = "QUEUE_DEPTH_NOT_ZERO_AFTER";
    public static final String REJECTED_REASON_DELTA_PATTERN = "REJECTED_REASON_<R>_DELTA_NONZERO";
    public static final String TIMEOUTS_DELTA_NONZERO = "TIMEOUTS_DELTA_NONZERO";
    public static final String RETRIES_DELTA_NONZERO = "RETRIES_DELTA_NONZERO";
    public static final String RETRY_EXHAUSTED_DELTA_NONZERO = "RETRY_EXHAUSTED_DELTA_NONZERO";
    public static final String SIMULATOR_ACTIVE_GENERATIONS_NOT_ZERO_AFTER =
            "SIMULATOR_ACTIVE_GENERATIONS_NOT_ZERO_AFTER";
    public static final String SIMULATOR_FAULTED_DELTA_NONZERO = "SIMULATOR_FAULTED_DELTA_NONZERO";
    public static final String SIMULATOR_REJECTED_DELTA_NONZERO = "SIMULATOR_REJECTED_DELTA_NONZERO";
    public static final String SIMULATOR_STATS_UNAVAILABLE = "SIMULATOR_STATS_UNAVAILABLE";
    public static final String GATEWAY_EXITED = "GATEWAY_EXITED";
    public static final String SIMULATOR_EXITED = "SIMULATOR_EXITED";
    public static final String CONNECTION_MODE_MISMATCH = "CONNECTION_MODE_MISMATCH";
    public static final String PORT_NOT_FREED = "PORT_NOT_FREED";

    // environment
    public static final String CPU_BUSY_BEFORE_RUN = "CPU_BUSY_BEFORE_RUN";
    public static final String CPU_LOAD_UNAVAILABLE = "CPU_LOAD_UNAVAILABLE";
    public static final String TIME_WAIT_UNAVAILABLE = "TIME_WAIT_UNAVAILABLE";
    public static final String LOG_TRUNCATED = "LOG_TRUNCATED";
    public static final String LOG_WRITE_ERROR = "LOG_WRITE_ERROR";

    // run-set level
    public static final String GIT_DIRTY = "GIT_DIRTY";
    public static final String GIT_UNAVAILABLE = "GIT_UNAVAILABLE";
    public static final String ARTIFACT_OLDER_THAN_HEAD = "ARTIFACT_OLDER_THAN_HEAD";
    public static final String OUTPUT_NOT_GITIGNORED = "OUTPUT_NOT_GITIGNORED";
    public static final String CHECKLIST_NOT_CONFIRMED = "CHECKLIST_NOT_CONFIRMED";
    public static final String CHECKLIST_ANSWER_NO = "CHECKLIST_ANSWER_NO";
    public static final String SMOKE_PRESET = "SMOKE_PRESET";
    public static final String INTERRUPTED = "INTERRUPTED";

    /** Every documented flag name or pattern. */
    public static final List<String> DOCUMENTED = List.of(OUTCOME_NONZERO_PATTERN, REJECTED_AT_OR_BELOW_LIMIT,
            LOADGEN_EXIT_NONZERO, LOADGEN_WAIT_EXCEEDED, SUMMARY_MISSING, JSONL_HEADER_UNEXPECTED,
            JSONL_FOOTER_MISSING, STOP_REASON_NOT_DURATION, FOOTER_INTERRUPTED_NONZERO,
            ACTIVE_STREAMS_NOT_ZERO_BEFORE, ACTIVE_STREAMS_NOT_ZERO_AFTER, QUEUE_DEPTH_NOT_ZERO_AFTER,
            REJECTED_REASON_DELTA_PATTERN, TIMEOUTS_DELTA_NONZERO, RETRIES_DELTA_NONZERO,
            RETRY_EXHAUSTED_DELTA_NONZERO, SIMULATOR_ACTIVE_GENERATIONS_NOT_ZERO_AFTER,
            SIMULATOR_FAULTED_DELTA_NONZERO, SIMULATOR_REJECTED_DELTA_NONZERO, SIMULATOR_STATS_UNAVAILABLE,
            GATEWAY_EXITED, SIMULATOR_EXITED, CONNECTION_MODE_MISMATCH, PORT_NOT_FREED, CPU_BUSY_BEFORE_RUN,
            CPU_LOAD_UNAVAILABLE, TIME_WAIT_UNAVAILABLE, LOG_TRUNCATED, LOG_WRITE_ERROR, GIT_DIRTY, GIT_UNAVAILABLE,
            ARTIFACT_OLDER_THAN_HEAD, OUTPUT_NOT_GITIGNORED, CHECKLIST_NOT_CONFIRMED, CHECKLIST_ANSWER_NO,
            SMOKE_PRESET, INTERRUPTED);

    /** Threshold of {@link #CPU_BUSY_BEFORE_RUN}: a marker, not a judgement. */
    public static final double CPU_BUSY_THRESHOLD = 0.25;

    private Flags() {
    }

    public static String outcomeNonzero(String outcome) {
        return "OUTCOME_" + outcome + "_NONZERO";
    }

    public static String rejectedReasonDelta(String reason) {
        return "REJECTED_REASON_" + reason.toUpperCase(java.util.Locale.ROOT) + "_DELTA_NONZERO";
    }
}
