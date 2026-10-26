package io.github.cemharput.streamlab.report;

import io.github.cemharput.streamlab.bench.Manifest;
import io.github.cemharput.streamlab.report.CellComparison.Cell;
import io.github.cemharput.streamlab.report.CellComparison.Entry;
import io.github.cemharput.streamlab.report.CellComparison.ModeStats;
import io.github.cemharput.streamlab.report.Percentiles.Dist;
import io.github.cemharput.streamlab.report.RunAssessment.Analysed;
import io.github.cemharput.streamlab.report.RunAssessment.Analysis;
import io.github.cemharput.streamlab.report.RunAssessment.Reason;
import io.github.cemharput.streamlab.report.RunMetrics.Gauge;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Writes {@code benchmark-report.md}: tables and sentences from the analysis, prose from {@link ReportText}. The
 * document is fully generated; the output always uses {@code \n}.
 */
public final class ReportRenderer {

    public static final List<String> SECTION_TITLES = List.of("Identification", "What was measured", "Run inventory",
            "Per-run metrics", "Comparisons per concurrency", "Gateway and simulator deltas", "Data quality",
            "Metric definitions and caveats", "Flag classification", "Limits", "How to regenerate");

    /** The chart files and the metric each one shows (fixed list). */
    public static final Map<String, ComparedMetric> CHARTS = chartMap();

    private static final Pattern SAFE_NAME = Pattern.compile("[A-Za-z0-9._+-]{1,120}");
    private static final Pattern LAST_PAREN = Pattern.compile("\\(([^()]*)\\)\\s*$");

    private ReportRenderer() {
    }

    private static Map<String, ComparedMetric> chartMap() {
        Map<String, ComparedMetric> m = new java.util.LinkedHashMap<>();
        m.put("chart-completed-per-second.svg", ComparedMetric.COMPLETED_PER_SECOND);
        m.put("chart-ttfc-p50.svg", ComparedMetric.TTFC_P50);
        m.put("chart-ttfc-p99.svg", ComparedMetric.TTFC_P99);
        m.put("chart-total-p99.svg", ComparedMetric.TOTAL_P99);
        return java.util.Collections.unmodifiableMap(m);
    }

    public static String placeholder() {
        StringBuilder sb = new StringBuilder();
        sb.append(ReportText.placeholderFirstLine()).append('\n');
        sb.append(ReportText.TITLE).append('\n');
        sb.append(ReportText.PLACEHOLDER_STATUS).append('\n');
        sb.append("## Limits\n\n");
        sb.append(ReportText.LIMITS_PREFACE).append('\n');
        sb.append(ReportText.limitsBlock()).append('\n');
        sb.append("## How to regenerate\n\n");
        sb.append(ReportText.REGENERATE);
        return sb.toString();
    }

    public static String render(Analysis a, List<Cell> cells) {
        StringBuilder sb = new StringBuilder();
        sb.append(ReportText.firstLine(a.manifest().runId())).append('\n');
        sb.append(ReportText.TITLE).append('\n');
        sb.append(ReportText.INTRO).append('\n');
        int n = 0;
        section(sb, ++n, SECTION_TITLES.get(0));
        identification(sb, a);
        section(sb, ++n, SECTION_TITLES.get(1));
        measured(sb, a.manifest());
        section(sb, ++n, SECTION_TITLES.get(2));
        inventory(sb, a);
        section(sb, ++n, SECTION_TITLES.get(3));
        perRun(sb, a);
        section(sb, ++n, SECTION_TITLES.get(4));
        comparisons(sb, a, cells);
        section(sb, ++n, SECTION_TITLES.get(5));
        deltas(sb, a);
        section(sb, ++n, SECTION_TITLES.get(6));
        dataQuality(sb, a);
        section(sb, ++n, SECTION_TITLES.get(7));
        sb.append(ReportText.DEFINITIONS).append('\n');
        section(sb, ++n, SECTION_TITLES.get(8));
        flagClassification(sb);
        section(sb, ++n, SECTION_TITLES.get(9));
        sb.append(ReportText.LIMITS_PREFACE).append('\n');
        sb.append(ReportText.limitsBlock()).append('\n');
        sb.append(ReportText.LIMITS_SUFFIX).append('\n');
        section(sb, ++n, SECTION_TITLES.get(10));
        sb.append(ReportText.REGENERATE).append('\n');
        sb.append(ReportText.DATA_FILES);
        return sb.toString();
    }

    private static void section(StringBuilder sb, int number, String title) {
        sb.append("## ").append(number).append(". ").append(title).append("\n\n");
    }

    // ---------------------------------------------------------------- 1

    private static void identification(StringBuilder sb, Analysis a) {
        ManifestView m = a.manifest();
        if (a.smoke()) {
            sb.append("> **Smoke preset: this set is a harness check, not a measurement.**\n\n");
        }
        if (!Manifest.STATUS_COMPLETED.equals(m.status())) {
            sb.append("> **Set incomplete: the set status is ").append(Fmt.codeInline(m.status()))
                    .append("; runs that did not complete are excluded.**\n\n");
        }
        List<String> banners = new ArrayList<>();
        for (Reason r : a.setReasons()) {
            if (r.severity() == Severity.CAUTION) {
                banners.add(Fmt.codeInline(r.code()));
            }
        }
        if (!banners.isEmpty()) {
            sb.append("> Caution flags recorded for the whole set: ").append(String.join(", ", banners)).append(".\n\n");
        }
        if (!a.unknownFlags().isEmpty()) {
            sb.append("> Flags this tool does not know (runs with them are excluded, except a flag recorded in a warm-up phase, which is a caution): ")
                    .append(String.join(", ", a.unknownFlags().stream().map(Fmt::codeInline).toList()))
                    .append(".\n\n");
        }
        List<String[]> rows = new ArrayList<>();
        rows.add(row("Run set id", m.runId()));
        rows.add(row("Set status", m.status()));
        rows.add(row("Preset", rec(m.preset())));
        rows.add(row("Created (manifest)", rec(m.createdAt())));
        rows.add(row("Finished (manifest)", rec(m.finishedAt())));
        rows.add(row("Runner version", rec(m.toolVersion())));
        rows.add(row("Analysis version", Integer.toString(ReportMain.ANALYSIS_VERSION)));
        rows.add(row("Git commit", rec(m.environmentString("git", "commit"))));
        rows.add(row("Git short commit", rec(m.environmentString("git", "shortCommit"))));
        Boolean dirty = m.environmentBool("git", "dirty");
        rows.add(row("Git working tree dirty", dirty == null ? Fmt.NOT_RECORDED : dirty.toString()));
        rows.add(row("Git commit time", rec(m.environmentString("git", "commitTime"))));
        Integer lines = m.gitStatusLineCount();
        rows.add(row("Git status lines (count only)", lines == null ? Fmt.NOT_RECORDED : Integer.toString(lines)));
        rows.add(row("Operating system", rec(m.environmentString("os"))));
        String model = m.environmentString("cpu", "model");
        String source = m.environmentString("cpu", "modelSource");
        rows.add(row("CPU model", model == null ? Fmt.NOT_RECORDED : model + (source == null ? "" : " (source: " + source.replace("/proc/", "proc ") + ")")));
        rows.add(row("Logical processors", rec(m.environmentLong("cpu", "logicalProcessors"))));
        rows.add(row("Physical cores", rec(m.environmentLong("cpu", "physicalCores"))));
        Long mem = m.environmentLong("memory", "totalBytes");
        rows.add(row("Total memory", mem == null ? Fmt.NOT_RECORDED : Fmt.gib(mem) + " GiB"));
        rows.add(row("Java version", rec(m.environmentString("java", "version"))));
        rows.add(row("Java vendor", rec(m.environmentString("java", "vendor"))));
        rows.add(row("Java VM", rec(m.environmentString("java", "vmName"))));
        rows.add(row("Power plan", powerPlan(m.environmentString("powerPlan"))));
        rows.add(row("Operator checklist source", rec(m.checklistSource())));
        Map<String, Object> answers = m.checklistAnswers();
        if (answers == null) {
            rows.add(row("Operator checklist answers", Fmt.NOT_RECORDED));
        } else {
            for (Map.Entry<String, Object> e : new TreeMap<>(answers).entrySet()) {
                rows.add(row("Checklist: " + e.getKey(), describe(e.getValue())));
            }
        }
        rows.add(row("Operator note", m.hasOperatorNote() ? "present (text not reproduced)" : "absent"));
        rows.add(row("Harness failure text", m.hasHarnessFailure() ? "present (text not reproduced)" : "absent"));
        List<String> flagText = new ArrayList<>();
        for (Reason r : a.setReasons()) {
            flagText.add(Fmt.code(r.code()) + " (" + r.severity().label() + ")");
        }
        rows.add(row("Set flags", flagText.isEmpty() ? "none" : String.join(", ", flagText)));
        List<String> removed = m.removedEnvironmentVariables();
        rows.add(row("Removed environment variables (names)", removed.isEmpty() ? "none recorded"
                : String.join(", ", removed)));
        table(sb, List.of("Field", "Value"), rows);
        sb.append('\n');
        sb.append("Runs: ").append(a.runs().size()).append(" in total, ").append(a.count(RunAssessment.USABLE))
                .append(" usable, ").append(a.count(RunAssessment.CAUTION)).append(" caution, ")
                .append(a.count(RunAssessment.EXCLUDED)).append(" excluded.\n\n");
    }

    private static String powerPlan(String raw) {
        if (raw == null) {
            return "not recorded (not captured on this OS)";
        }
        Matcher m = LAST_PAREN.matcher(raw.trim());
        return m.find() ? (m.group(1)) : "recorded without a plan name";
    }

    // ---------------------------------------------------------------- 2

    private static void measured(StringBuilder sb, ManifestView m) {
        sb.append(ReportText.MEASURED_INTRO).append('\n');
        Object plan = m.parameter("plan");
        if (!(plan instanceof Map<?, ?>)) {
            sb.append("The plan is ").append(Fmt.NOT_RECORDED).append(".\n\n");
        } else {
            List<String[]> rows = new ArrayList<>();
            for (String key : List.of("concurrency", "repetitions", "durationMs", "warmupMs", "rampUpMs",
                    "cooldownMs", "modeOrder", "alternation", "seed", "cpuBusyThreshold", "cpuWindowMs",
                    "logLimitBytes")) {
                rows.add(row(key, describe(Jv.get(plan, key))));
            }
            table(sb, List.of("Plan parameter", "Recorded value"), rows);
            sb.append('\n');
        }
        Object props = m.parameter("gateway", "properties");
        sb.append("Pinned gateway properties:\n\n");
        if (props instanceof Map<?, ?> map) {
            List<String[]> rows = new ArrayList<>();
            for (Map.Entry<String, Object> e : new TreeMap<>(Jv.map(map)).entrySet()) {
                rows.add(row(e.getKey(), describe(e.getValue())));
            }
            table(sb, List.of("Property", "Value"), rows);
        } else {
            sb.append(Fmt.NOT_RECORDED).append('\n');
        }
        sb.append('\n');
        List<String[]> rows = new ArrayList<>();
        rows.add(row("Gateway JVM options", describe(m.parameter("jvm", "gateway"))));
        rows.add(row("Simulator JVM options", describe(m.parameter("jvm", "simulator"))));
        rows.add(row("Load generator JVM options", describe(m.parameter("jvm", "loadgen"))));
        rows.add(row("Load generator target", describe(m.parameter("loadgen", "target"))));
        rows.add(row("Load generator fixed arguments", describe(m.parameter("loadgen", "fixedArguments"))));
        table(sb, List.of("Setting", "Recorded value"), rows);
        sb.append('\n');
    }

    // ---------------------------------------------------------------- 3

    private static void inventory(StringBuilder sb, Analysis a) {
        sb.append(ReportText.INVENTORY_INTRO).append('\n');
        List<String> modes = CellComparison.modeOrder(a);
        List<String[]> rows = new ArrayList<>();
        for (Analysed r : a.runs()) {
            ManifestView.RunView run = r.run();
            List<String> reasons = new ArrayList<>();
            for (Reason x : r.assessment().reasons()) {
                if (x.severity() != Severity.INFO) {
                    reasons.add(Fmt.code(x.label()));
                }
            }
            List<String> infos = new ArrayList<>();
            for (Reason x : r.assessment().reasons()) {
                if (x.severity() == Severity.INFO) {
                    infos.add(Fmt.code(x.label()));
                }
            }
            rows.add(new String[] {Long.toString(run.index()), (run.id()), (run.mode()),
                Integer.toString(run.concurrency()), Long.toString(run.repetition()), (run.orderLabel()),
                modes.contains(run.mode()) ? CellComparison.position(run, modes) : "n/a", (run.status()),
                r.assessment().classification(), reasons.isEmpty() ? "none" : String.join(", ", reasons),
                infos.isEmpty() ? "none" : String.join(", ", infos)});
        }
        table(sb, List.of("#", "Run", "Mode", "Concurrency", "Repetition", "Order", "Position", "Status", "Class",
                "Exclude and caution reasons", "Info"), rows);
        sb.append('\n');
        sb.append("Warm-up phases and other files are listed by name and size and are not analysed:\n\n");
        List<String[]> files = new ArrayList<>();
        for (Analysed r : a.runs()) {
            ManifestView.PhaseView warm = r.run().phase("warmup");
            List<String> names = new ArrayList<>();
            for (ResultSetReader.ListedFile f : r.data().listed()) {
                if (f.name().startsWith("warmup") || f.name().equals("snapshot-ready.json")) {
                    names.add(f.name() + " (" + f.sizeBytes() + " bytes)");
                }
            }
            files.add(new String[] {(r.run().id()), warm.present() ? "recorded" : "not recorded",
                names.isEmpty() ? "none" : String.join(", ", names)});
        }
        table(sb, List.of("Run", "Warm-up phase in the manifest", "Listed, not analysed"), files);
        sb.append('\n');
    }

    // ---------------------------------------------------------------- 4

    private static void perRun(StringBuilder sb, Analysis a) {
        sb.append(ReportText.METRICS_INTRO).append('\n');
        java.util.TreeSet<String> names = new java.util.TreeSet<>();
        for (Analysed r : a.runs()) {
            if (r.metrics() != null) {
                names.addAll(r.metrics().outcomes().keySet());
            }
        }
        List<String> outcomeNames = RunMetrics.orderOutcomes(names);
        sb.append("### Outcomes (count and share of the request lines)\n\n");
        List<String> head = new ArrayList<>(List.of("Run", "Request lines"));
        head.addAll(outcomeNames);
        List<String[]> rows = new ArrayList<>();
        for (Analysed r : a.runs()) {
            List<String> row = new ArrayList<>();
            row.add(label(r));
            RunMetrics m = r.metrics();
            if (m == null) {
                row.add(Fmt.NA);
                outcomeNames.forEach(o -> row.add(Fmt.NA));
            } else {
                row.add(Long.toString(m.requestLines()));
                for (String o : outcomeNames) {
                    long c = m.outcomes().getOrDefault(o, 0L);
                    row.add(c + " (" + Fmt.percent(c, m.requestLines()) + " %)");
                }
            }
            rows.add(row.toArray(new String[0]));
        }
        table(sb, head, rows);
        sb.append('\n');

        sb.append("### Completed streams per second and generator delay\n\n");
        rows = new ArrayList<>();
        for (Analysed r : a.runs()) {
            RunMetrics m = r.metrics();
            if (m == null) {
                rows.add(new String[] {label(r), Fmt.NA, Fmt.NA, Fmt.NA, Fmt.NA, Fmt.NA, Fmt.NA});
            } else {
                rows.add(new String[] {label(r), Fmt.decOrNa(m.completedPerSecond(), 2),
                    m.completedInWindow() == null ? Fmt.NA : Long.toString(m.completedInWindow()),
                    m.windowMs() == null ? Fmt.NA : Long.toString(m.windowMs()),
                    m.footerElapsedNanos() == null ? Fmt.NA : Fmt.ms(m.footerElapsedNanos()),
                    Fmt.msOrNa(m.generatorDelayP99()), Fmt.msOrNa(m.generatorDelayMax())});
            }
        }
        table(sb, List.of("Run", "Completed per second", "COMPLETED in window", "Window (ms)",
                "Footer elapsed (ms)", "Generator delay p99 (ms)", "Generator delay max (ms)"), rows);
        sb.append('\n');

        distributionTable(sb, a, "Time to first byte (ms, COMPLETED requests)", 0);
        distributionTable(sb, a, "Time to first chunk (ms, COMPLETED requests)", 1);
        distributionTable(sb, a, "Total stream duration (ms, COMPLETED requests)", 2);

        sb.append("### Inter-chunk gaps (ms, all streams, from the footer histogram)\n\n");
        rows = new ArrayList<>();
        for (Analysed r : a.runs()) {
            RunMetrics m = r.metrics();
            if (m == null || m.interChunk() == null) {
                rows.add(new String[] {label(r), Fmt.NA, Fmt.NA, Fmt.NA, Fmt.NA, Fmt.NA});
            } else {
                InterChunk.Result ic = m.interChunk();
                rows.add(new String[] {label(r), Long.toString(ic.count()), Fmt.msOrNa(ic.p50()),
                    Fmt.msOrNa(ic.p90()), Fmt.msOrNa(ic.p99()), Fmt.msOrNa(ic.max())});
            }
        }
        table(sb, List.of("Run", "Gaps", "p50", "p90", "p99", "max"), rows);
        sb.append('\n');
    }

    private static void distributionTable(StringBuilder sb, Analysis a, String title, int which) {
        sb.append("### ").append(title).append("\n\n");
        List<String[]> rows = new ArrayList<>();
        for (Analysed r : a.runs()) {
            RunMetrics m = r.metrics();
            if (m == null) {
                rows.add(new String[] {label(r), Fmt.NA, Fmt.NA, Fmt.NA, Fmt.NA, Fmt.NA, Fmt.NA});
                continue;
            }
            Dist d = which == 0 ? m.ttfb() : which == 1 ? m.ttfc() : m.total();
            rows.add(new String[] {label(r), Long.toString(d.count()), Long.toString(d.missing()),
                Fmt.msOrNa(d.p50()), Fmt.msOrNa(d.p90()), Fmt.msOrNa(d.p99()), Fmt.msOrNa(d.max())});
        }
        table(sb, List.of("Run", "n", "Missing", "p50", "p90", "p99", "max"), rows);
        sb.append('\n');
    }

    // ---------------------------------------------------------------- 5

    private static void comparisons(StringBuilder sb, Analysis a, List<Cell> cells) {
        sb.append(ReportText.COMPARISON_INTRO).append('\n');
        sb.append("Charts (one point per run, excluded runs are not drawn):\n\n");
        for (Map.Entry<String, ComparedMetric> e : CHARTS.entrySet()) {
            sb.append("- [").append(e.getValue().title()).append("](benchmark/").append(e.getKey()).append(")\n");
        }
        sb.append('\n');
        List<String> modes = CellComparison.modeOrder(a);
        sb.append("Mode order of the plan: ").append(String.join(", ", modes.stream().map(m -> "`" + m + "`").toList()))
                .append(".\n\n");
        int lastConcurrency = -1;
        for (Cell c : cells) {
            if (c.concurrency() != lastConcurrency) {
                lastConcurrency = c.concurrency();
                sb.append("### Concurrency ").append(lastConcurrency).append("\n\n");
            }
            sb.append("#### ").append(c.metric().title()).append(" (").append(c.metric().unit().label())
                    .append(")\n\n");
            java.util.TreeSet<Long> reps = new java.util.TreeSet<>();
            c.entries().values().forEach(l -> l.forEach(e -> reps.add(e.repetition())));
            List<String> head = new ArrayList<>(List.of("Repetition"));
            head.addAll(c.modes());
            List<String[]> rows = new ArrayList<>();
            for (long rep : reps) {
                List<String> row = new ArrayList<>();
                row.add(Long.toString(rep));
                for (String mode : c.modes()) {
                    Entry found = null;
                    for (Entry e : c.entries().get(mode)) {
                        if (e.repetition() == rep) {
                            found = e;
                        }
                    }
                    row.add(found == null ? Fmt.NA : c.metric().bare(found.value(), c.scale()) + (found.caution() ? "*" : "")
                            + " #" + found.index() + " " + found.position());
                }
                rows.add(row.toArray(new String[0]));
            }
            if (rows.isEmpty()) {
                sb.append("No included run has a value for this metric.\n\n");
            } else {
                table(sb, head, rows);
                sb.append('\n');
            }
            rows = new ArrayList<>();
            for (String mode : c.modes()) {
                ModeStats s = c.stats().get(mode);
                rows.add(new String[] {mode, Integer.toString(s.n()),
                    s.median() == null ? Fmt.NA : c.metric().bare(s.median(), c.scale()),
                    s.min() == null ? Fmt.NA : c.metric().bare(s.min(), c.scale()),
                    s.max() == null ? Fmt.NA : c.metric().bare(s.max(), c.scale())});
            }
            table(sb, List.of("Mode", "n", "Median", "Min", "Max"), rows);
            sb.append('\n').append(c.sentence()).append("\n\n");
            if (c.cautionSentence() != null) {
                sb.append(c.cautionSentence()).append("\n\n");
            }
        }
        if (cells.isEmpty()) {
            sb.append("The manifest lists no runs, so there is nothing to compare.\n\n");
        }
    }

    // ---------------------------------------------------------------- 6

    private static void deltas(StringBuilder sb, Analysis a) {
        sb.append(ReportText.DELTAS_INTRO).append('\n');
        List<String> gwLabels = RunMetrics.gatewayCounterLabels();
        List<String> head = new ArrayList<>(List.of("Run"));
        head.addAll(gwLabels);
        List<String[]> rows = new ArrayList<>();
        for (Analysed r : a.runs()) {
            List<String> row = new ArrayList<>();
            row.add(label(r));
            RunMetrics.Snapshots s = r.metrics() == null ? null : r.metrics().snapshots();
            for (String l : gwLabels) {
                row.add(s == null || !s.available() ? Fmt.NA : Fmt.num(s.gatewayDeltas().get(l)));
            }
            rows.add(row.toArray(new String[0]));
        }
        sb.append("### Gateway counter deltas\n\n");
        table(sb, head, rows);
        sb.append('\n');

        head = new ArrayList<>(List.of("Run"));
        head.addAll(RunMetrics.SIMULATOR_COUNTERS);
        rows = new ArrayList<>();
        for (Analysed r : a.runs()) {
            List<String> row = new ArrayList<>();
            row.add(label(r));
            RunMetrics.Snapshots s = r.metrics() == null ? null : r.metrics().snapshots();
            for (String l : RunMetrics.SIMULATOR_COUNTERS) {
                row.add(s == null || !s.available() ? Fmt.NA : Fmt.num(s.simulatorDeltas().get(l)));
            }
            rows.add(row.toArray(new String[0]));
        }
        sb.append("### Simulator counter deltas\n\n");
        table(sb, head, rows);
        sb.append('\n');

        List<String> gaugeLabels = RunMetrics.gatewayGaugeLabels();
        head = new ArrayList<>(List.of("Run", "simulator active_generations"));
        for (String l : gaugeLabels) {
            head.add(l.startsWith("jvm.memory") ? l + " (GiB)" : l);
        }
        rows = new ArrayList<>();
        for (Analysed r : a.runs()) {
            List<String> row = new ArrayList<>();
            row.add(label(r));
            RunMetrics.Snapshots s = r.metrics() == null ? null : r.metrics().snapshots();
            row.add(s == null || !s.available() || s.simulatorActive() == null ? Fmt.NA : gauge(s.simulatorActive(), false));
            for (String l : gaugeLabels) {
                row.add(s == null || !s.available() ? Fmt.NA : gauge(s.gatewayGauges().get(l), l.startsWith("jvm.memory")));
            }
            rows.add(row.toArray(new String[0]));
        }
        sb.append("### Gauges (before / after)\n\n");
        table(sb, head, rows);
        sb.append('\n');
    }

    private static String gauge(Gauge g, boolean bytes) {
        if (g == null) {
            return Fmt.NA;
        }
        return value(g.pre(), bytes) + " / " + value(g.post(), bytes);
    }

    private static String value(Double v, boolean bytes) {
        if (v == null) {
            return Fmt.NA;
        }
        BigDecimal d = BigDecimal.valueOf(v);
        return bytes ? Fmt.gib(d.setScale(0, RoundingMode.HALF_EVEN).longValue()) : Fmt.num(d);
    }

    // ---------------------------------------------------------------- 7

    private static void dataQuality(StringBuilder sb, Analysis a) {
        ManifestView m = a.manifest();
        sb.append("### Cross-checks\n\n");
        List<String[]> rows = new ArrayList<>();
        for (Checks.CrossCheck c : Checks.CROSS_CHECKS) {
            List<String> affected = new ArrayList<>();
            for (Analysed r : a.runs()) {
                for (Reason x : r.assessment().reasons()) {
                    if (x.source().equals("CHECK") && c.codes().contains(x.code())) {
                        affected.add("#" + r.run().index());
                        break;
                    }
                }
            }
            String result = c.codes().isEmpty() ? "passed (otherwise no report is written)"
                    : affected.isEmpty() ? "no finding" : "findings in " + String.join(", ", affected);
            rows.add(new String[] {c.id(), (c.description()), c.codes().isEmpty() ? "-"
                : String.join(", ", c.codes().stream().map(Fmt::code).toList()), result});
        }
        table(sb, List.of("Check", "What is compared", "Codes", "Result"), rows);
        sb.append('\n');

        sb.append("### Findings of the checks\n\n");
        rows = new ArrayList<>();
        for (Analysed r : a.runs()) {
            for (Reason x : r.assessment().reasons()) {
                if (x.source().equals("CHECK")) {
                    rows.add(new String[] {label(r), Fmt.code(x.code()), x.severity().label(), (x.detail())});
                }
            }
        }
        if (rows.isEmpty()) {
            sb.append("No finding.\n\n");
        } else {
            table(sb, List.of("Run", "Code", "Class", "Detail"), rows);
            sb.append('\n');
        }

        sb.append("### Environment record\n\n");
        List<String> missing = m.missingFields();
        sb.append("Set-level fields not recorded: ")
                .append(missing.isEmpty() ? "none" : String.join(", ", missing.stream().map(f -> "`" + f + "`").toList()))
                .append(".\n\n");
        if (a.expectedRunCount() == null) {
            sb.append("The plan parameters needed to rebuild the run list are not recorded; `CHECK_PLAN_MISMATCH` could not be evaluated.\n\n");
        } else {
            sb.append("The plan rebuilt from the recorded parameters has ").append(a.expectedRunCount())
                    .append(" runs; the manifest lists ").append(a.runs().size()).append(".\n\n");
        }
        rows = new ArrayList<>();
        for (Analysed r : a.runs()) {
            List<String> f = r.run().missingFields();
            if (!f.isEmpty()) {
                rows.add(new String[] {label(r), String.join(", ", f.stream().map(Fmt::code).toList())});
            }
        }
        if (!rows.isEmpty()) {
            sb.append("Run-level fields not recorded:\n\n");
            table(sb, List.of("Run", "Fields"), rows);
            sb.append('\n');
        }

        sb.append("### CPU load samples before each measured phase\n\n");
        rows = new ArrayList<>();
        for (Analysed r : a.runs()) {
            List<Double> samples = r.run().cpuLoadBefore();
            if (samples == null) {
                rows.add(new String[] {label(r), Fmt.NOT_RECORDED, Fmt.NA, Fmt.NA, Fmt.NA, Fmt.NA});
                continue;
            }
            List<Double> usable = samples.stream().filter(java.util.Objects::nonNull).toList();
            if (usable.isEmpty()) {
                rows.add(new String[] {label(r), Integer.toString(samples.size()), Integer.toString(samples.size()),
                    Fmt.NA, Fmt.NA, Fmt.NA});
                continue;
            }
            BigDecimal sum = BigDecimal.ZERO;
            double min = usable.get(0);
            double max = usable.get(0);
            for (double d : usable) {
                sum = sum.add(BigDecimal.valueOf(d));
                min = Math.min(min, d);
                max = Math.max(max, d);
            }
            BigDecimal mean = sum.divide(BigDecimal.valueOf(usable.size()), 6, RoundingMode.HALF_EVEN);
            rows.add(new String[] {label(r), Integer.toString(samples.size()),
                Integer.toString(samples.size() - usable.size()), Fmt.cpu(min), Fmt.dec(mean, 3), Fmt.cpu(max)});
        }
        table(sb, List.of("Run", "Samples", "Unavailable", "Min", "Mean", "Max"), rows);
        sb.append('\n');

        sb.append("### TIME_WAIT sockets\n\n");
        Object tw = m.environment("timeWaitAtStart");
        sb.append("At the start of the set: ").append(timeWait(Jv.map(tw))).append(".\n\n");
        rows = new ArrayList<>();
        for (Analysed r : a.runs()) {
            rows.add(new String[] {label(r), timeWait(r.run().timeWaitBefore())});
        }
        table(sb, List.of("Run", "Before the run (total, gateway port, simulator port)"), rows);
        sb.append('\n');

        sb.append("### Artifacts\n\n");
        artifacts(sb, m.artifacts());

        sb.append("### Stop records and logs\n\n");
        sb.append("`destroy` means that the process exited after `destroy()`; on Windows that is a hard termination,"
                + " on Linux a SIGTERM that lets shutdown hooks run. The exit code is shown as recorded.\n\n");
        rows = new ArrayList<>();
        for (Analysed r : a.runs()) {
            ManifestView.RunView run = r.run();
            rows.add(new String[] {label(r), proc(run.process("gateway")), proc(run.process("simulator")),
                logs(run.process("gateway")), logs(run.process("simulator")),
                run.portsFreed() == null ? Fmt.NOT_RECORDED : run.portsFreed().toString()});
        }
        table(sb, List.of("Run", "Gateway stop", "Simulator stop", "Gateway log bytes dropped / lost",
                "Simulator log bytes dropped / lost", "Ports freed"), rows);
        sb.append('\n');

        sb.append("### GC logs and process logs (listed, not analysed)\n\n");
        rows = new ArrayList<>();
        for (Analysed r : a.runs()) {
            List<String> names = new ArrayList<>();
            for (ResultSetReader.ListedFile f : r.data().listed()) {
                if (!f.name().startsWith("warmup") && !f.name().equals("snapshot-ready.json")) {
                    names.add(f.name() + " (" + f.sizeBytes() + " bytes)");
                }
            }
            rows.add(new String[] {label(r), names.isEmpty() ? "none" : String.join(", ", names)});
        }
        table(sb, List.of("Run", "Files"), rows);
        sb.append('\n');
    }

    private static String timeWait(Map<String, Object> tw) {
        if (tw == null) {
            return Fmt.NOT_RECORDED;
        }
        Long total = Jv.lng(tw.get("total"));
        Long gw = Jv.lng(tw.get("gatewayPort"));
        Long sim = Jv.lng(tw.get("simulatorPort"));
        return Fmt.numOrNa(total) + ", " + Fmt.numOrNa(gw) + ", " + Fmt.numOrNa(sim);
    }

    private static String proc(ManifestView.ProcessView p) {
        if (p == null || p.stopMethod() == null) {
            return Fmt.NOT_RECORDED;
        }
        return (p.stopMethod()) + (p.stopExitCode() == null ? "" : " (exit code " + p.stopExitCode() + ")");
    }

    private static String logs(ManifestView.ProcessView p) {
        if (p == null) {
            return Fmt.NOT_RECORDED;
        }
        return Fmt.numOrNa(p.bytesDropped()) + " / " + Fmt.numOrNa(p.bytesLostAfterWriteError())
                + (p.writeError() ? " (write error recorded)" : "");
    }

    private static void artifacts(StringBuilder sb, Object artifacts) {
        Map<String, Object> root = Jv.map(artifacts);
        if (root == null) {
            sb.append(Fmt.NOT_RECORDED).append("\n\n");
            return;
        }
        List<String[]> rows = new ArrayList<>();
        for (String key : List.of("benchJar", "gatewayJar", "loadgenJar")) {
            artifactRow(rows, key, root.get(key));
        }
        for (String key : List.of("loadgenLib", "simulatorClasspath")) {
            List<Object> l = Jv.list(root.get(key));
            if (l != null) {
                for (Object o : l) {
                    artifactRow(rows, key, o);
                }
            }
        }
        table(sb, List.of("Group", "File name", "Size (bytes)", "sha256"), rows);
        sb.append('\n');
    }

    private static void artifactRow(List<String[]> rows, String group, Object o) {
        Map<String, Object> m = Jv.map(o);
        if (m == null) {
            rows.add(new String[] {group, Fmt.NOT_RECORDED, Fmt.NOT_RECORDED, Fmt.NOT_RECORDED});
            return;
        }
        String path = Jv.str(m.get("path"));
        String name = path == null ? null : path.substring(Math.max(path.lastIndexOf('/'), path.lastIndexOf('\\')) + 1);
        String sha = Jv.str(m.get("sha256"));
        rows.add(new String[] {group, name != null && SAFE_NAME.matcher(name).matches() ? name : "name not shown",
            Fmt.numOrNa(Jv.lng(m.get("sizeBytes"))),
            sha != null && sha.matches("[0-9a-fA-F]{64}") ? sha : Fmt.NOT_RECORDED});
    }

    // ---------------------------------------------------------------- 9

    private static void flagClassification(StringBuilder sb) {
        sb.append("Every flag of the runner's dictionary has a class. `exclude`: the run does not enter any comparison."
                + " `caution`: the run enters and is marked `*`. `info`: recorded only. A flag recorded for the whole"
                + " set (`set` scope) as `exclude` excludes every run; as `caution` it appears only in the"
                + " identification block. Every flag recorded in a warm-up phase counts as `caution` (warm-up"
                + " anomaly) and never excludes. A flag that is not in the dictionary excludes the run (except in a"
                + " warm-up phase, where it is a caution) and is listed as unknown. A run whose status is not `completed` is excluded.\n\n");
        List<String[]> rows = new ArrayList<>();
        for (FlagPolicy.Entry e : FlagPolicy.entries()) {
            rows.add(new String[] {Fmt.code(e.name()), e.severity().label(), e.scope(), e.rationale()});
        }
        for (Map.Entry<String, Severity> e : Checks.TABLE.entrySet()) {
            rows.add(new String[] {Fmt.code(e.getKey()), e.getValue().label(), "check", checkNote(e.getKey())});
        }
        table(sb, List.of("Flag or check", "Class", "Scope", "Rationale"), rows);
        sb.append('\n');
    }

    private static String checkNote(String code) {
        for (Checks.CrossCheck c : Checks.CROSS_CHECKS) {
            if (c.codes().contains(code)) {
                return "cross-check " + c.id();
            }
        }
        return "recorded as information";
    }

    // ---------------------------------------------------------------- helpers

    private static String label(Analysed a) {
        String mark = RunAssessment.CAUTION.equals(a.assessment().classification()) ? "*"
                : RunAssessment.EXCLUDED.equals(a.assessment().classification()) ? " (excluded)" : "";
        return "#" + a.run().index() + " " + (a.run().id()) + mark;
    }

    private static String rec(Object value) {
        return value == null ? Fmt.NOT_RECORDED : String.valueOf(value);
    }

    private static String[] row(String a, String b) {
        return new String[] {(a), (b)};
    }

    /** Scalars as text, lists joined by a space, maps as {@code key=value} pairs; null is "not recorded". */
    static String describe(Object value) {
        if (value == null) {
            return Fmt.NOT_RECORDED;
        }
        if (value instanceof List<?> l) {
            List<String> parts = new ArrayList<>();
            for (Object o : l) {
                parts.add(describe(o));
            }
            return String.join(" ", parts);
        }
        if (value instanceof Map<?, ?> m) {
            List<String> parts = new ArrayList<>();
            for (Map.Entry<String, Object> e : new TreeMap<>(Jv.map(m)).entrySet()) {
                parts.add(e.getKey() + "=" + describe(e.getValue()));
            }
            return String.join(", ", parts);
        }
        if (value instanceof Double d) {
            return Fmt.num(BigDecimal.valueOf(d));
        }
        return String.valueOf(value);
    }

    private static void table(StringBuilder sb, List<String> head, List<String[]> rows) {
        // the one place where untrusted text is escaped for a table: every cell exactly once
        sb.append("| ").append(String.join(" | ", head.stream().map(Fmt::cell).toList())).append(" |\n");
        sb.append("|").append("---|".repeat(head.size())).append('\n');
        for (String[] row : rows) {
            sb.append("| ").append(String.join(" | ", java.util.Arrays.stream(row).map(Fmt::cell).toList()))
                    .append(" |\n");
        }
    }
}
