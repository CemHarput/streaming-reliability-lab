package io.github.cemharput.streamlab.report;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * The analyser's own checks. Their codes start with {@code CHECK_}, so they never collide with the runner's flag
 * dictionary ({@code Flags.DOCUMENTED}).
 */
public final class Checks {

    public static final String RUN_DIR_INVALID = "CHECK_RUN_DIR_INVALID";
    public static final String FILE_NAME_UNEXPECTED = "CHECK_FILE_NAME_UNEXPECTED";
    public static final String MEASURED_JSONL_MISSING = "CHECK_MEASURED_JSONL_MISSING";
    public static final String JSONL_SCHEMA_UNSUPPORTED = "CHECK_JSONL_SCHEMA_UNSUPPORTED";
    public static final String JSONL_MALFORMED_LINE = "CHECK_JSONL_MALFORMED_LINE";
    public static final String JSONL_STRUCTURE = "CHECK_JSONL_STRUCTURE";
    public static final String HEADER_PARAMETERS_MISMATCH = "CHECK_HEADER_PARAMETERS_MISMATCH";
    public static final String COUNT_MISMATCH = "CHECK_COUNT_MISMATCH";
    public static final String MODE_UNKNOWN = "CHECK_MODE_UNKNOWN";
    public static final String SUMMARY_SCHEMA_UNSUPPORTED = "CHECK_SUMMARY_SCHEMA_UNSUPPORTED";
    public static final String SUMMARY_UNREADABLE = "CHECK_SUMMARY_UNREADABLE";
    public static final String INTER_CHUNK_UNAVAILABLE = "CHECK_INTER_CHUNK_UNAVAILABLE";
    public static final String SNAPSHOT_UNAVAILABLE = "CHECK_SNAPSHOT_UNAVAILABLE";
    public static final String COUNTER_DECREASED = "CHECK_COUNTER_DECREASED";
    public static final String REJECTED_METRIC_MISMATCH = "CHECK_REJECTED_METRIC_MISMATCH";
    public static final String PLAN_MISMATCH = "CHECK_PLAN_MISMATCH";
    public static final String STOP_METHOD_UNKNOWN = "CHECK_STOP_METHOD_UNKNOWN";
    public static final String FIELD_MISSING = "CHECK_FIELD_MISSING";

    /** One finding about a run; {@code detail} contains counts, names and line numbers only. */
    public record Finding(String code, String detail) {
    }

    /** One row of the cross-check table in the data quality section. */
    public record CrossCheck(String id, String description, List<String> codes) {
    }

    private static final Map<String, Severity> SEVERITY = new LinkedHashMap<>();

    static {
        for (String code : List.of(RUN_DIR_INVALID, FILE_NAME_UNEXPECTED, MEASURED_JSONL_MISSING,
                JSONL_SCHEMA_UNSUPPORTED, JSONL_MALFORMED_LINE, JSONL_STRUCTURE, HEADER_PARAMETERS_MISMATCH,
                COUNT_MISMATCH, MODE_UNKNOWN)) {
            SEVERITY.put(code, Severity.EXCLUDE);
        }
        for (String code : List.of(SUMMARY_SCHEMA_UNSUPPORTED, SUMMARY_UNREADABLE, INTER_CHUNK_UNAVAILABLE, SNAPSHOT_UNAVAILABLE,
                COUNTER_DECREASED, REJECTED_METRIC_MISMATCH, PLAN_MISMATCH)) {
            SEVERITY.put(code, Severity.CAUTION);
        }
        for (String code : List.of(STOP_METHOD_UNKNOWN, FIELD_MISSING)) {
            SEVERITY.put(code, Severity.INFO);
        }
    }

    /** The classification table of the checks (code to class), in a fixed order. */
    public static final Map<String, Severity> TABLE = java.util.Collections.unmodifiableMap(SEVERITY);

    /** C1 to C12: what the data quality section cross-checks. */
    public static final List<CrossCheck> CROSS_CHECKS = List.of(
            new CrossCheck("C1", "Manifest: schemaVersion, protocolVersion, tool and required keys (otherwise nothing is analysed)",
                    List.of()),
            new CrossCheck("C2", "Run directory is runs/<id> inside the result set; file names are the expected ones",
                    List.of(RUN_DIR_INVALID, FILE_NAME_UNEXPECTED)),
            new CrossCheck("C3", "measured.jsonl exists and is readable", List.of(MEASURED_JSONL_MISSING)),
            new CrossCheck("C4", "Schema versions of the JSONL header and the summary; the summary is present and readable",
                    List.of(JSONL_SCHEMA_UNSUPPORTED, SUMMARY_SCHEMA_UNSUPPORTED, SUMMARY_UNREADABLE)),
            new CrossCheck("C5", "JSONL lines that could not be read (skipped)", List.of(JSONL_MALFORMED_LINE)),
            new CrossCheck("C6", "JSONL structure: one header first, one footer last", List.of(JSONL_STRUCTURE)),
            new CrossCheck("C7", "JSONL header parameters equal the manifest plan",
                    List.of(HEADER_PARAMETERS_MISMATCH)),
            new CrossCheck("C8", "Footer and summary record counts equal the number of request lines (not evaluated for an unreadable summary)",
                    List.of(COUNT_MISMATCH, SUMMARY_UNREADABLE)),
            new CrossCheck("C9", "Summary and manifest outcome counts equal the outcome counts of the request lines (not evaluated for an unreadable summary)",
                    List.of(COUNT_MISMATCH, SUMMARY_UNREADABLE)),
            new CrossCheck("C10", "Inter-chunk histogram of the footer can be decoded and its count equals interChunkCount",
                    List.of(INTER_CHUNK_UNAVAILABLE)),
            new CrossCheck("C11", "Snapshots readable, counters did not decrease, gateway rejected_total delta equals the REJECTED lines",
                    List.of(SNAPSHOT_UNAVAILABLE, COUNTER_DECREASED, REJECTED_METRIC_MISMATCH)),
            new CrossCheck("C12", "Run list equals the plan rebuilt from the recorded plan parameters; mode is a known mode",
                    List.of(PLAN_MISMATCH, MODE_UNKNOWN)));

    private Checks() {
    }

    public static boolean isCheck(String code) {
        return SEVERITY.containsKey(code);
    }

    public static Severity severity(String code) {
        Severity s = SEVERITY.get(code);
        if (s == null) {
            throw new IllegalArgumentException("not a check code: " + code);
        }
        return s;
    }
}
