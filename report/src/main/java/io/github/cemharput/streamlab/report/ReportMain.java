package io.github.cemharput.streamlab.report;

import io.github.cemharput.streamlab.bench.json.Json;
import io.github.cemharput.streamlab.report.CellComparison.Cell;
import io.github.cemharput.streamlab.report.RunAssessment.Analysed;
import io.github.cemharput.streamlab.report.RunAssessment.Analysis;
import io.github.cemharput.streamlab.report.RunAssessment.Reason;
import java.io.IOException;
import java.io.PrintStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Entry point of the report tool. It reads one result set (never modifying it) and writes a descriptive report,
 * derived tables and charts. It makes no performance claim. Exit codes: 0 success (excluded runs are reported, not an
 * error), 1 unexpected error (I/O, guard violation), 2 invalid usage or input (nothing is written).
 */
public final class ReportMain {

    /** Version of the analysis rules (classification, metrics, comparison, wording); not the tool version. */
    public static final int ANALYSIS_VERSION = 1;
    private static final char NL = (char) 10;

    private ReportMain() {
    }

    public static void main(String[] args) {
        System.exit(run(args, System.out, System.err, Path.of("").toAbsolutePath()));
    }

    /** The whole tool without {@code System.exit}; {@code cwd} resolves relative option values. */
    public static int run(String[] args, PrintStream out, PrintStream err, Path cwd) {
        ReportOptions options;
        try {
            options = ReportOptionsParser.parse(args, cwd);
        } catch (UsageException ex) {
            err.println("report: " + ex.getMessage() + " (see --help)");
            return 2;
        }
        if (options.help()) {
            out.print(ReportOptionsParser.USAGE);
            return 0;
        }
        try {
            return options.placeholder() ? placeholder(options, out, err) : analyse(options, out, err);
        } catch (InputException ex) {
            err.println("report: invalid input: " + ex.getMessage());
            return 2;
        } catch (IOException | RuntimeException ex) {
            err.println("report: unexpected error: " + ex.getClass().getSimpleName() + ": " + ex.getMessage());
            return 1;
        }
    }

    private static int placeholder(ReportOptions o, PrintStream out, PrintStream err) throws IOException {
        List<String> data = OutputWriter.existing(o.out(), OutputWriter.DATA_FILES);
        if (!data.isEmpty()) {
            err.println("report: --placeholder refuses to run: data files of an analysed set exist in "
                    + OutputWriter.DATA_DIR + "/ (" + String.join(", ", data) + ")");
            return 2;
        }
        List<String> existing = OutputWriter.existing(o.out(), List.of(OutputWriter.REPORT));
        if (!existing.isEmpty() && !o.overwrite()) {
            err.println("report: " + OutputWriter.REPORT + " exists; pass --overwrite to replace it");
            return 2;
        }
        Map<String, String> files = new LinkedHashMap<>();
        files.put(OutputWriter.REPORT, ReportRenderer.placeholder());
        String guard = guards(files, List.of());
        if (guard != null) {
            err.println("report: internal guard violation: " + guard);
            return 1;
        }
        StringBuilder text = new StringBuilder();
        text.append("report: placeholder (no result set analysed)\n");
        text.append("written: ").append(OutputWriter.REPORT).append('\n');
        text.append(summaryJson(null, null, List.of(OutputWriter.REPORT))).append('\n');
        if (!stdoutClean(text.toString(), List.of(), err)) {
            return 1;
        }
        OutputWriter.write(o.out(), files);
        out.print(text);
        return 0;
    }

    private static int analyse(ReportOptions o, PrintStream out, PrintStream err) throws IOException, InputException {
        Path realResults = o.results().toRealPath();
        if (o.out() != null) {
            Path realOut = o.out().toRealPath();
            if (realOut.startsWith(realResults) || realResults.startsWith(realOut)) {
                err.println("report: --out must not be inside --results and must not contain it");
                return 2;
            }
            Path data = realOut.resolve(OutputWriter.DATA_DIR);
            if (Files.exists(data) && (data.toRealPath().startsWith(realResults)
                    || realResults.startsWith(data.toRealPath()))) {
                err.println("report: " + OutputWriter.DATA_DIR + "/ below --out resolves into --results (a link?)");
                return 2;
            }
        }
        ResultSetReader.ResultSet set = ResultSetReader.read(realResults);
        Analysis analysis = RunAssessment.analyse(set);
        boolean writing = !o.check();
        if (writing && analysis.smoke() && !o.allowSmoke()) {
            err.println("report: the result set is a smoke preset set (a harness check, not a measurement);"
                    + " pass --allow-smoke to write output anyway");
            return 2;
        }
        if (writing && !o.overwrite()) {
            List<String> existing = OutputWriter.existing(o.out(), OutputWriter.ALL_FILES);
            if (!existing.isEmpty()) {
                err.println("report: output files exist (" + String.join(", ", existing)
                        + "); pass --overwrite to replace them");
                return 2;
            }
        }
        List<Cell> cells = CellComparison.compareAll(analysis);
        Map<String, String> files = new LinkedHashMap<>();
        files.put(OutputWriter.REPORT, ReportRenderer.render(analysis, cells));
        files.put("benchmark/runs.csv", CsvWriter.runs(analysis));
        files.put("benchmark/cells.csv", CsvWriter.cells(cells));
        files.put("benchmark/analysis.json", AnalysisJson.write(analysis, cells));
        List<String> modes = CellComparison.modeOrder(analysis);
        for (Map.Entry<String, ComparedMetric> chart : ReportRenderer.CHARTS.entrySet()) {
            List<Cell> forMetric = cells.stream().filter(c -> c.metric() == chart.getValue()).toList();
            files.put("benchmark/" + chart.getKey(), SvgDotPlot.render(chart.getValue(), forMetric, modes));
        }
        List<String> secrets = secrets(realResults);
        String guard = guards(files, secrets);
        if (guard != null) {
            err.println("report: internal guard violation: " + guard);
            return 1;
        }
        List<String> written = writing ? new ArrayList<>(files.keySet()) : List.of();
        String text = humanSummary(analysis, written, writing) + summaryJson(analysis, analysis.manifest().runId(), written)
                + "\n";
        if (!stdoutClean(text, secrets, err)) {
            return 1;
        }
        if (writing) {
            OutputWriter.write(o.out(), files);
        }
        out.print(text);
        return 0;
    }

    private static List<String> secrets(Path realResults) {
        List<String> secrets = new ArrayList<>();
        String r = realResults.toString();
        secrets.add(r);
        secrets.add(r.replace('\\', '/'));
        secrets.add(System.getProperty("user.home"));
        return secrets;
    }

    /** Runs both guards over every file; returns a message for the first violation, or null. */
    static String guards(Map<String, String> files, List<String> secrets) {
        for (Map.Entry<String, String> e : files.entrySet()) {
            List<String> wording = WordingGuard.scan(e.getValue(), e.getKey().equals(OutputWriter.REPORT));
            if (!wording.isEmpty()) {
                return e.getKey() + ": " + wording.get(0);
            }
            List<String> privacy = PrivacyGuard.scan(e.getValue(), secrets);
            if (!privacy.isEmpty()) {
                return e.getKey() + ": " + privacy.get(0);
            }
        }
        return null;
    }

    private static boolean stdoutClean(String text, List<String> secrets, PrintStream err) {
        List<String> privacy = PrivacyGuard.scan(text, secrets);
        if (!privacy.isEmpty()) {
            err.println("report: internal guard violation in the summary: " + privacy.get(0));
            return false;
        }
        return true;
    }

    private static String humanSummary(Analysis a, List<String> written, boolean writing) {
        StringBuilder sb = new StringBuilder();
        ManifestView m = a.manifest();
        sb.append("report: result set ").append(Fmt.line(m.runId())).append(" (status ").append(Fmt.line(m.status()))
                .append(", preset ").append(m.preset() == null ? "none" : Fmt.line(m.preset())).append(", schema 1, protocol 1)\n");
        java.util.Map<String, Long> statuses = new java.util.TreeMap<>();
        for (Analysed r : a.runs()) {
            statuses.merge(Fmt.line(r.run().status()), 1L, Long::sum);
        }
        List<String> statusParts = new ArrayList<>();
        statuses.forEach((k, v) -> statusParts.add(k + " " + v));
        sb.append("runs: ").append(a.runs().size()).append(" total, ").append(a.count(RunAssessment.USABLE))
                .append(" usable, ").append(a.count(RunAssessment.CAUTION)).append(" caution, ")
                .append(a.count(RunAssessment.EXCLUDED)).append(" excluded\n");
        for (Analysed r : a.runs()) {
            if (!r.included()) {
                sb.append("excluded ").append(Fmt.line(r.run().id())).append(": ")
                        .append(String.join(", ", excludeLabels(r).stream().map(Fmt::line).toList())).append('\n');
            }
        }
        sb.append("statuses: ").append(statusParts.isEmpty() ? "none" : String.join(", ", statusParts)).append(NL);
        long warm = a.runs().stream().filter(r -> r.run().phase("warmup").present()).count();
        sb.append("warm-up phases listed, not analysed: ").append(warm).append('\n');
        if (!a.unknownFlags().isEmpty()) {
            sb.append("unknown flags: ").append(String.join(", ", a.unknownFlags().stream().map(Fmt::line).toList())).append('\n');
        }
        sb.append(writing ? "written: " + String.join(", ", written) : "check only: nothing written").append('\n');
        return sb.toString();
    }

    private static List<String> excludeLabels(Analysed r) {
        List<String> labels = new ArrayList<>();
        for (Reason x : r.assessment().reasons()) {
            if (x.severity() == Severity.EXCLUDE) {
                labels.add(x.label());
            }
        }
        return labels;
    }

    /** The last stdout line: one line of JSON. */
    static String summaryJson(Analysis a, String runId, List<String> written) {
        Map<String, Object> m = new LinkedHashMap<>();
        m.put("type", "report-summary");
        m.put("analysisVersion", (long) ANALYSIS_VERSION);
        m.put("runId", runId);
        m.put("runs", a == null ? 0L : (long) a.runs().size());
        m.put("usable", a == null ? 0L : a.count(RunAssessment.USABLE));
        m.put("caution", a == null ? 0L : a.count(RunAssessment.CAUTION));
        m.put("excluded", a == null ? 0L : a.count(RunAssessment.EXCLUDED));
        List<Object> excluded = new ArrayList<>();
        if (a != null) {
            for (Analysed r : a.runs()) {
                if (!r.included()) {
                    Map<String, Object> e = new LinkedHashMap<>();
                    e.put("id", r.run().id());
                    e.put("reasons", new ArrayList<Object>(excludeLabels(r)));
                    excluded.add(e);
                }
            }
        }
        m.put("excludedRuns", excluded);
        m.put("unknownFlags", a == null ? new ArrayList<Object>() : new ArrayList<Object>(a.unknownFlags()));
        m.put("written", new ArrayList<Object>(written));
        return Json.writeCompact(m);
    }
}
