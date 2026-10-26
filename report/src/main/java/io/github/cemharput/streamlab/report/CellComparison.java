package io.github.cemharput.streamlab.report;

import io.github.cemharput.streamlab.bench.ConnectionMode;
import io.github.cemharput.streamlab.report.RunAssessment.Analysed;
import io.github.cemharput.streamlab.report.RunAssessment.Analysis;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeSet;

/**
 * Compares the two modes within one concurrency and one metric, as observations only: values side by side with run
 * index and ABBA position, median and range per mode, and one sentence from a closed template set. There is no
 * pooled percentile, no test and no ranking. The rule is applied to the exact values, before any rounding.
 */
public final class CellComparison {

    /** One run in a cell. {@code position} is {@code 1st} or {@code 2nd} within its ABBA pair. */
    public record Entry(long index, long repetition, String position, BigDecimal value, boolean caution) {
    }

    /** Statistics of one mode; the numbers are null when {@code n} is zero. */
    public record ModeStats(String mode, int n, BigDecimal median, BigDecimal min, BigDecimal max) {

        public BigDecimal range() {
            return max.subtract(min);
        }
    }

    /** The outcome of the rule with the two descriptive numbers it used (null when not applicable). */
    public record Decision(Statement statement, BigDecimal difference, BigDecimal spread) {
    }

    /** One concurrency x metric cell. */
    public record Cell(int concurrency, ComparedMetric metric, List<String> modes, Map<String, List<Entry>> entries,
            Map<String, ModeStats> stats, Decision decision, String sentence, Statement withoutCaution,
            String cautionSentence, int scale) {
    }

    private CellComparison() {
    }

    public static List<String> modeOrder(Analysis analysis) {
        List<String> declared = analysis.manifest().modeOrder();
        if (validModeOrder(declared)) {
            return declared;
        }
        List<String> modes = new ArrayList<>();
        for (Analysed r : analysis.runs()) {
            if (!modes.contains(r.run().mode()) && ConnectionMode.fromProperty(r.run().mode()).isPresent()) {
                modes.add(r.run().mode());
            }
        }
        for (ConnectionMode m : ConnectionMode.values()) {
            if (modes.size() < 2 && !modes.contains(m.property())) {
                modes.add(m.property());
            }
        }
        return List.copyOf(modes.subList(0, 2));
    }

    /** Two different known connection modes. */
    public static boolean validModeOrder(List<String> declared) {
        return declared.size() == 2 && !declared.get(0).equals(declared.get(1))
                && declared.stream().allMatch(m -> ConnectionMode.fromProperty(m).isPresent());
    }

    public static List<Integer> concurrencies(Analysis analysis) {
        TreeSet<Integer> set = new TreeSet<>();
        analysis.runs().forEach(r -> set.add(r.run().concurrency()));
        return List.copyOf(set);
    }

    /** All cells: concurrency ascending, then the fixed metric order. */
    public static List<Cell> compareAll(Analysis analysis) {
        List<String> modes = modeOrder(analysis);
        List<Cell> cells = new ArrayList<>();
        for (int c : concurrencies(analysis)) {
            for (ComparedMetric metric : ComparedMetric.values()) {
                cells.add(compare(analysis, modes, c, metric));
            }
        }
        return List.copyOf(cells);
    }

    public static Cell compare(Analysis analysis, List<String> modes, int concurrency, ComparedMetric metric) {
        Map<String, List<Entry>> entries = new LinkedHashMap<>();
        for (String mode : modes) {
            entries.put(mode, new ArrayList<>());
        }
        for (Analysed r : analysis.runs()) {
            ManifestView.RunView run = r.run();
            if (!r.included() || run.concurrency() != concurrency || !entries.containsKey(run.mode())) {
                continue;
            }
            BigDecimal value = metric.value(r.metrics());
            if (value == null) {
                continue;
            }
            entries.get(run.mode()).add(new Entry(run.index(), run.repetition(), position(run, modes),
                    value, RunAssessment.CAUTION.equals(r.assessment().classification())));
        }
        entries.values().forEach(l -> l.sort(Comparator.comparingLong(Entry::repetition).thenComparingLong(Entry::index)));

        Map<String, ModeStats> stats = statsOf(entries, false);
        Decision decision = decide(stats, modes);
        int scale = displayScale(decision, stats, modes, metric);
        String sentence = sentence(decision, stats, modes, concurrency, metric, scale);

        Statement without = null;
        String cautionSentence = null;
        boolean anyCaution = entries.values().stream().anyMatch(l -> l.stream().anyMatch(Entry::caution));
        if (anyCaution) {
            Decision alt = decide(statsOf(entries, true), modes);
            if (alt.statement() != decision.statement()) {
                without = alt.statement();
                cautionSentence = Statement.DEPENDS_ON_CAUTION.render(Map.of("alt", alt.statement().shortLabel()));
            }
        }
        return new Cell(concurrency, metric, List.copyOf(modes), entries, stats, decision, sentence, without,
                cautionSentence, scale);
    }

    /** {@code 1st} when {@code (orderLabel == "AB") == (mode == modeOrder[0])}, otherwise {@code 2nd}. */
    static String position(ManifestView.RunView run, List<String> modes) {
        return ("AB".equals(run.orderLabel())) == run.mode().equals(modes.get(0)) ? "1st" : "2nd";
    }

    private static Map<String, ModeStats> statsOf(Map<String, List<Entry>> entries, boolean withoutCaution) {
        Map<String, ModeStats> out = new LinkedHashMap<>();
        for (Map.Entry<String, List<Entry>> e : entries.entrySet()) {
            List<BigDecimal> values = new ArrayList<>();
            for (Entry x : e.getValue()) {
                if (!withoutCaution || !x.caution()) {
                    values.add(x.value());
                }
            }
            out.put(e.getKey(), stats(e.getKey(), values));
        }
        return out;
    }

    static ModeStats stats(String mode, List<BigDecimal> values) {
        if (values.isEmpty()) {
            return new ModeStats(mode, 0, null, null, null);
        }
        List<BigDecimal> sorted = new ArrayList<>(values);
        sorted.sort(Comparator.naturalOrder());
        int n = sorted.size();
        BigDecimal median = n % 2 == 1 ? sorted.get(n / 2)
                : sorted.get(n / 2 - 1).add(sorted.get(n / 2)).divide(BigDecimal.valueOf(2));
        return new ModeStats(mode, n, median, sorted.get(0), sorted.get(n - 1));
    }

    /** The rule: n below 2, equal medians, overlapping ranges (touching counts), spread, otherwise higher. */
    static Decision decide(Map<String, ModeStats> stats, List<String> modes) {
        ModeStats x = stats.get(modes.get(0));
        ModeStats y = stats.get(modes.get(1));
        if (x.n() < 2 || y.n() < 2) {
            return new Decision(Statement.INSUFFICIENT, null, null);
        }
        if (x.median().compareTo(y.median()) == 0) {
            return new Decision(Statement.EQUAL, BigDecimal.ZERO, x.range().max(y.range()));
        }
        BigDecimal d = x.median().subtract(y.median()).abs();
        BigDecimal s = x.range().max(y.range());
        if (x.min().max(y.min()).compareTo(x.max().min(y.max())) <= 0) {
            return new Decision(Statement.CANNOT_SEPARATE_OVERLAP, d, s);
        }
        if (d.compareTo(s) <= 0) {
            return new Decision(Statement.CANNOT_SEPARATE_SPREAD, d, s);
        }
        return new Decision(Statement.HIGHER, d, s);
    }

    /** The decimals used for every value of the cell (table and sentence), see {@link ComparedMetric#scaleFor}. */
    static int displayScale(Decision decision, Map<String, ModeStats> stats, List<String> modes,
            ComparedMetric metric) {
        ModeStats x = stats.get(modes.get(0));
        ModeStats y = stats.get(modes.get(1));
        if (decision.statement() == Statement.INSUFFICIENT || x.median() == null || y.median() == null) {
            return metric.baseScale();
        }
        return metric.scaleFor(x, y, decision.statement());
    }

    static String sentence(Decision decision, Map<String, ModeStats> stats, List<String> modes,
            int concurrency, ComparedMetric metric, int scale) {
        Map<String, String> v = new LinkedHashMap<>();
        v.put("c", Integer.toString(concurrency));
        v.put("metric", metric.title());
        ModeStats x = stats.get(modes.get(0));
        ModeStats y = stats.get(modes.get(1));
        if (decision.statement() == Statement.INSUFFICIENT) {
            String lacking = x.n() < 2 && y.n() < 2
                    ? x.mode() + " (n=" + x.n() + ") and " + y.mode() + " (n=" + y.n() + ")"
                    : x.n() < 2 ? x.mode() + " (n=" + x.n() + ")" : y.mode() + " (n=" + y.n() + ")";
            v.put("modes", lacking);
            return decision.statement().render(v);
        }
        ModeStats first = x;
        ModeStats second = y;
        if (decision.statement() == Statement.HIGHER && y.median().compareTo(x.median()) > 0) {
            first = y;
            second = x;
        }
        v.put("m1", first.mode());
        v.put("v1", metric.withUnit(first.median(), scale));
        v.put("m2", second.mode());
        v.put("v2", metric.withUnit(second.median(), scale));
        v.put("n1", Integer.toString(first.n()));
        v.put("n2", Integer.toString(second.n()));
        if (decision.statement() != Statement.EQUAL) {
            v.put("d", metric.format(metric.displayDifference(x, y, scale)));
            v.put("s", metric.format(metric.displaySpread(x, y, scale)));
        }
        return decision.statement().render(v);
    }
}
