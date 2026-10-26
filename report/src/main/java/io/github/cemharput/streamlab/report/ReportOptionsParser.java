package io.github.cemharput.streamlab.report;

import java.nio.file.Files;
import java.nio.file.InvalidPathException;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

/** Hand-written parser for {@code --name value} and {@code --name=value}. Fails fast with a {@link UsageException}. */
public final class ReportOptionsParser {

    private static final Set<String> FLAGS = Set.of("check", "overwrite", "placeholder", "allow-smoke", "help");
    private static final Set<String> VALUE_OPTIONS = Set.of("results", "out");

    public static final String USAGE = """
            Usage: java -jar report.jar (--results DIR (--check | --out DIR) | --placeholder --out DIR) [options]

            Reads ONE result set written by the bench runner and writes a descriptive report, derived tables and
            charts. It never modifies the result set. The report describes one result set on one machine and makes
            no performance claim.

              --results DIR    the result set directory (contains manifest.json); not combinable with --placeholder
              --out DIR        an existing directory; writes <DIR>/benchmark-report.md and the data files in
                               <DIR>/benchmark/ (created when missing); must not be inside --results nor contain it
              --check          only read and validate, print the summary, write nothing; not combinable with --out
              --overwrite      allow replacing files of the fixed output list (only with --out)
              --placeholder    write only the report file with the text "No result set analysed yet" (no numbers)
              --allow-smoke    allow writing output for a smoke preset set (harness check, not a measurement)
              -h, --help       print this text and exit 0

            Output files (always all of them): benchmark-report.md, benchmark/runs.csv, benchmark/cells.csv,
            benchmark/analysis.json, benchmark/chart-completed-per-second.svg, benchmark/chart-ttfc-p50.svg,
            benchmark/chart-ttfc-p99.svg, benchmark/chart-total-p99.svg.
            Standard output ends with one line of JSON (type report-summary).
            Exit codes: 0 success (excluded runs are reported, not an error), 1 unexpected error (I/O, internal
            guard), 2 invalid usage or invalid input (nothing is written).
            """;

    private ReportOptionsParser() {
    }

    public static ReportOptions parse(String[] args, Path cwd) throws UsageException {
        for (String arg : args) {
            if (arg.equals("-h") || arg.equals("--help")) {
                return new ReportOptions(true, null, null, false, false, false, false);
            }
        }
        Map<String, String> values = new HashMap<>();
        Set<String> flags = new java.util.HashSet<>();
        for (int i = 0; i < args.length; i++) {
            String arg = args[i];
            if (arg.equals("-h")) {
                flags.add("help");
                continue;
            }
            if (!arg.startsWith("--") || arg.length() <= 2) {
                throw new UsageException("unexpected argument: " + arg);
            }
            String name = arg.substring(2);
            String value = null;
            int eq = name.indexOf('=');
            if (eq >= 0) {
                value = name.substring(eq + 1);
                name = name.substring(0, eq);
            }
            if (FLAGS.contains(name)) {
                if (value != null) {
                    throw new UsageException("--" + name + " does not take a value");
                }
                flags.add(name);
                continue;
            }
            if (!VALUE_OPTIONS.contains(name)) {
                throw new UsageException("unknown option: --" + name);
            }
            if (value == null) {
                if (i + 1 >= args.length) {
                    throw new UsageException("--" + name + " needs a value");
                }
                value = args[++i];
            }
            if (value.isBlank()) {
                throw new UsageException("--" + name + " must not be empty");
            }
            if (values.put(name, value) != null) {
                throw new UsageException("--" + name + " given twice");
            }
        }
        if (flags.contains("help")) {
            return new ReportOptions(true, null, null, false, false, false, false);
        }
        boolean placeholder = flags.contains("placeholder");
        boolean check = flags.contains("check");
        boolean overwrite = flags.contains("overwrite");
        boolean results = values.containsKey("results");
        boolean out = values.containsKey("out");
        if (placeholder && results) {
            throw new UsageException("--placeholder cannot be combined with --results");
        }
        if (placeholder && check) {
            throw new UsageException("--placeholder cannot be combined with --check");
        }
        if (check && out) {
            throw new UsageException("--check writes nothing and cannot be combined with --out");
        }
        if (overwrite && !out) {
            throw new UsageException("--overwrite only makes sense with --out");
        }
        if (!placeholder && !results) {
            throw new UsageException("--results DIR is required (or use --placeholder)");
        }
        if (!check && !out) {
            throw new UsageException(placeholder ? "--placeholder needs --out DIR"
                    : "give --out DIR to write the report, or --check to only validate");
        }
        if (flags.contains("allow-smoke") && !results) {
            throw new UsageException("--allow-smoke only makes sense with --results");
        }
        Path resultsPath = results ? directory("--results", values.get("results"), cwd) : null;
        Path outPath = out ? directory("--out", values.get("out"), cwd) : null;
        return new ReportOptions(false, resultsPath, outPath, check, overwrite, placeholder,
                flags.contains("allow-smoke"));
    }

    private static Path directory(String option, String text, Path cwd) throws UsageException {
        Path p;
        try {
            p = cwd.resolve(text).toAbsolutePath().normalize();
        } catch (InvalidPathException ex) {
            throw new UsageException(option + " is not a valid path");
        }
        if (!Files.isDirectory(p)) {
            throw new UsageException(option + " must be an existing directory");
        }
        return p;
    }
}
