package io.github.cemharput.streamlab.report;

import io.github.cemharput.streamlab.bench.Flags;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;

/**
 * Classifies the runner's flags into exclude, caution and info. A flag is a record of an observation; this table is
 * the analyser's decision about what it means for a comparison. Unknown flags are excluded.
 */
public final class FlagPolicy {

    /** Where a flag was recorded. */
    public enum Source {
        SET, RUN, MEASURED, WARMUP
    }

    /** One row of the table; {@code name} is a flag name or a pattern such as {@code OUTCOME_<NAME>_NONZERO}. */
    public record Entry(String name, Severity severity, String scope, String rationale) {
    }

    /** The class of one concrete flag; {@code known} is false for a flag that is not in the dictionary. */
    public record Classification(Severity severity, boolean known, String rationale) {
    }

    private static final String SET_SCOPE = "set";
    private static final String RUN_SCOPE = "run";

    private static final Pattern OUTCOME = Pattern.compile("OUTCOME_[A-Z][A-Z_]*_NONZERO");
    private static final Pattern REJECTED_REASON = Pattern.compile("REJECTED_REASON_[A-Z][A-Z_]*_DELTA_NONZERO");

    private static final List<Entry> ENTRIES = new ArrayList<>();

    static {
        add(Flags.outcomeNonzero("INTERRUPTED"), Severity.EXCLUDE, RUN_SCOPE,
                "drain timeout interrupted streams, the distributions are censored");
        add(Flags.OUTCOME_NONZERO_PATTERN, Severity.CAUTION, RUN_SCOPE,
                "errors are part of the observation, the shares are reported");
        add(Flags.REJECTED_AT_OR_BELOW_LIMIT, Severity.CAUTION, RUN_SCOPE, "unexpected rejection, the share is reported");
        add(Flags.LOADGEN_EXIT_NONZERO, Severity.EXCLUDE, RUN_SCOPE, "the load generator did not finish properly");
        add(Flags.LOADGEN_WAIT_EXCEEDED, Severity.EXCLUDE, RUN_SCOPE, "the load generator did not finish properly");
        add(Flags.SUMMARY_MISSING, Severity.CAUTION, RUN_SCOPE,
                "the JSONL is the result itself (ADR-0004), only the cross-check is lost");
        add(Flags.JSONL_HEADER_UNEXPECTED, Severity.EXCLUDE, RUN_SCOPE, "the file is incomplete or the run did not end by duration");
        add(Flags.JSONL_FOOTER_MISSING, Severity.EXCLUDE, RUN_SCOPE, "the file is incomplete or the run did not end by duration");
        add(Flags.STOP_REASON_NOT_DURATION, Severity.EXCLUDE, RUN_SCOPE,
                "the file is incomplete or the run did not end by duration");
        add(Flags.FOOTER_INTERRUPTED_NONZERO, Severity.EXCLUDE, RUN_SCOPE,
                "the file is incomplete or the run did not end by duration");
        add(Flags.ACTIVE_STREAMS_NOT_ZERO_BEFORE, Severity.EXCLUDE, RUN_SCOPE,
                "the measurement started with leftover streams");
        add(Flags.ACTIVE_STREAMS_NOT_ZERO_AFTER, Severity.CAUTION, RUN_SCOPE, "state after the measurement, the data is complete");
        add(Flags.QUEUE_DEPTH_NOT_ZERO_AFTER, Severity.CAUTION, RUN_SCOPE, "state after the measurement, the data is complete");
        add(Flags.rejectedReasonDelta("shutdown"), Severity.EXCLUDE, RUN_SCOPE, "shutdown started during the measurement");
        add(Flags.REJECTED_REASON_DELTA_PATTERN, Severity.CAUTION, RUN_SCOPE, "behaviour of the system under test");
        add(Flags.TIMEOUTS_DELTA_NONZERO, Severity.CAUTION, RUN_SCOPE,
                "not expected without a fault, but it is behaviour of the system under test");
        add(Flags.RETRIES_DELTA_NONZERO, Severity.CAUTION, RUN_SCOPE,
                "not expected without a fault, but it is behaviour of the system under test");
        add(Flags.RETRY_EXHAUSTED_DELTA_NONZERO, Severity.CAUTION, RUN_SCOPE,
                "not expected without a fault, but it is behaviour of the system under test");
        add(Flags.SIMULATOR_ACTIVE_GENERATIONS_NOT_ZERO_AFTER, Severity.CAUTION, RUN_SCOPE,
                "additional observation, simulator deltas may be n/a");
        add(Flags.SIMULATOR_REJECTED_DELTA_NONZERO, Severity.CAUTION, RUN_SCOPE,
                "additional observation, simulator deltas may be n/a");
        add(Flags.SIMULATOR_STATS_UNAVAILABLE, Severity.CAUTION, RUN_SCOPE,
                "additional observation, simulator deltas may be n/a");
        add(Flags.SIMULATOR_FAULTED_DELTA_NONZERO, Severity.EXCLUDE, RUN_SCOPE,
                "the pinned workload has no faults, the workload deviated from the protocol");
        add(Flags.GATEWAY_EXITED, Severity.EXCLUDE, RUN_SCOPE, "the run data cannot be used");
        add(Flags.SIMULATOR_EXITED, Severity.EXCLUDE, RUN_SCOPE, "the run data cannot be used");
        add(Flags.CONNECTION_MODE_MISMATCH, Severity.EXCLUDE, RUN_SCOPE, "the run data cannot be used");
        add(Flags.PORT_NOT_FREED, Severity.CAUTION, RUN_SCOPE,
                "recorded after the measurement, the port check of the next run catches it");
        add(Flags.CPU_BUSY_BEFORE_RUN, Severity.EXCLUDE, RUN_SCOPE,
                "the runner's own marker (cpuBusyThreshold) saw background load before the measurement");
        add(Flags.CPU_LOAD_UNAVAILABLE, Severity.CAUTION, RUN_SCOPE, "the data is complete, diagnostic information is missing");
        add(Flags.LOG_TRUNCATED, Severity.CAUTION, RUN_SCOPE, "the data is complete, diagnostic information is missing");
        add(Flags.LOG_WRITE_ERROR, Severity.CAUTION, RUN_SCOPE, "the data is complete, diagnostic information is missing");
        add(Flags.TIME_WAIT_UNAVAILABLE, Severity.INFO, "set and run", "environment record");
        add(Flags.OUTPUT_NOT_GITIGNORED, Severity.INFO, SET_SCOPE, "environment record");
        add(Flags.GIT_DIRTY, Severity.CAUTION, SET_SCOPE, "identification banner");
        add(Flags.GIT_UNAVAILABLE, Severity.CAUTION, SET_SCOPE, "identification banner");
        add(Flags.ARTIFACT_OLDER_THAN_HEAD, Severity.CAUTION, SET_SCOPE, "identification banner");
        add(Flags.CHECKLIST_NOT_CONFIRMED, Severity.CAUTION, SET_SCOPE, "identification banner");
        add(Flags.CHECKLIST_ANSWER_NO, Severity.CAUTION, SET_SCOPE, "identification banner");
        add(Flags.INTERRUPTED, Severity.CAUTION, SET_SCOPE,
                "identification banner; the interrupted run is excluded by its status");
        add(Flags.SMOKE_PRESET, Severity.EXCLUDE, SET_SCOPE, "a harness check, not a measurement");
    }

    private static void add(String name, Severity severity, String scope, String rationale) {
        ENTRIES.add(new Entry(name, severity, scope, rationale));
    }

    private FlagPolicy() {
    }

    /** The table in a fixed order; the last row of the section is the rule for unknown flags. */
    public static List<Entry> entries() {
        return List.copyOf(ENTRIES);
    }

    /** The classification of one concrete flag recorded at {@code source}. */
    public static Classification classify(String flag, Source source) {
        Classification base = base(flag);
        if (source == Source.WARMUP) {
            return new Classification(Severity.CAUTION, base.known(), "warm-up anomaly");
        }
        return base;
    }

    private static Classification base(String flag) {
        Map<String, Entry> exact = exact();
        Entry e = exact.get(flag);
        if (e != null) {
            return new Classification(e.severity(), true, e.rationale());
        }
        if (OUTCOME.matcher(flag).matches()) {
            Entry p = exact.get(Flags.OUTCOME_NONZERO_PATTERN);
            return new Classification(p.severity(), true, p.rationale());
        }
        if (REJECTED_REASON.matcher(flag).matches()) {
            Entry p = exact.get(Flags.REJECTED_REASON_DELTA_PATTERN);
            return new Classification(p.severity(), true, p.rationale());
        }
        return new Classification(Severity.EXCLUDE, false,
                "the analyser does not know this flag, so the run is excluded");
    }

    private static Map<String, Entry> exact() {
        Map<String, Entry> m = new LinkedHashMap<>();
        for (Entry e : ENTRIES) {
            m.put(e.name(), e);
        }
        return m;
    }

    /** True for a flag name that is in the runner's dictionary (a concrete name or an instance of a pattern). */
    public static boolean isKnown(String flag) {
        return base(flag).known();
    }
}
