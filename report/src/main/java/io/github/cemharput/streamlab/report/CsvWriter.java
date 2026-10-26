package io.github.cemharput.streamlab.report;

import io.github.cemharput.streamlab.report.CellComparison.Cell;
import io.github.cemharput.streamlab.report.CellComparison.ModeStats;
import io.github.cemharput.streamlab.report.Percentiles.Dist;
import io.github.cemharput.streamlab.report.RunAssessment.Analysed;
import io.github.cemharput.streamlab.report.RunAssessment.Analysis;
import java.util.ArrayList;
import java.util.List;

/** RFC 4180 CSV with {@code \n} line ends and a fixed column order. Values are raw nanoseconds or exact decimals. */
public final class CsvWriter {

    private static final String FORMULA_START = "=+-@" + (char) 9 + (char) 13 + (char) 10;

    private CsvWriter() {
    }

    public static String runs(Analysis analysis) {
        List<String> modes = CellComparison.modeOrder(analysis);
        List<String> header = new ArrayList<>(List.of("index", "id", "mode", "concurrency", "repetition",
                "order_label", "position", "status", "class", "reasons", "request_lines", "completed", "rejected",
                "rejected_share_percent", "completed_per_second", "completed_in_window", "window_ms",
                "footer_elapsed_ns"));
        for (String d : List.of("ttfb", "ttfc", "total")) {
            for (String f : List.of("n", "missing", "p50_ns", "p90_ns", "p99_ns", "max_ns")) {
                header.add(d + "_" + f);
            }
        }
        for (String f : List.of("n", "p50_ns", "p90_ns", "p99_ns", "max_ns")) {
            header.add("inter_chunk_" + f);
        }
        header.add("generator_delay_p99_ns");
        header.add("generator_delay_max_ns");
        for (String o : RunMetrics.KNOWN_OUTCOMES) {
            header.add("outcome_" + o);
        }
        header.add("outcome_other");

        StringBuilder sb = new StringBuilder();
        line(sb, header);
        for (Analysed a : analysis.runs()) {
            ManifestView.RunView run = a.run();
            RunMetrics m = a.metrics();
            List<String> row = new ArrayList<>();
            row.add(Long.toString(run.index()));
            row.add(text(run.id()));
            row.add(text(run.mode()));
            row.add(Integer.toString(run.concurrency()));
            row.add(Long.toString(run.repetition()));
            row.add(text(run.orderLabel()));
            row.add(modes.contains(run.mode()) ? CellComparison.position(run, modes) : "");
            row.add(text(run.status()));
            row.add(a.assessment().classification());
            row.add(text(String.join(";", a.assessment().reasons().stream().map(RunAssessment.Reason::label).toList())));
            if (m == null) {
                for (int i = header.size() - row.size(); i > 0; i--) {
                    row.add("");
                }
            } else {
                row.add(Long.toString(m.requestLines()));
                row.add(Long.toString(m.completed()));
                row.add(Long.toString(m.rejected()));
                row.add(m.rejectedShare() == null ? "" : m.rejectedShare().stripTrailingZeros().toPlainString());
                row.add(m.completedPerSecond() == null ? ""
                        : m.completedPerSecond().stripTrailingZeros().toPlainString());
                row.add(opt(m.completedInWindow()));
                row.add(opt(m.windowMs()));
                row.add(opt(m.footerElapsedNanos()));
                dist(row, m.ttfb());
                dist(row, m.ttfc());
                dist(row, m.total());
                if (m.interChunk() == null) {
                    for (int i = 0; i < 5; i++) {
                        row.add("");
                    }
                } else {
                    row.add(Long.toString(m.interChunk().count()));
                    row.add(opt(m.interChunk().p50()));
                    row.add(opt(m.interChunk().p90()));
                    row.add(opt(m.interChunk().p99()));
                    row.add(opt(m.interChunk().max()));
                }
                row.add(opt(m.generatorDelayP99()));
                row.add(opt(m.generatorDelayMax()));
                long other = m.requestLines();
                for (String o : RunMetrics.KNOWN_OUTCOMES) {
                    long c = m.outcomes().getOrDefault(o, 0L);
                    other -= c;
                    row.add(Long.toString(c));
                }
                row.add(Long.toString(other));
            }
            line(sb, row);
        }
        return sb.toString();
    }

    private static void dist(List<String> row, Dist d) {
        row.add(Long.toString(d.count()));
        row.add(Long.toString(d.missing()));
        row.add(opt(d.p50()));
        row.add(opt(d.p90()));
        row.add(opt(d.p99()));
        row.add(opt(d.max()));
    }

    public static String cells(List<Cell> cells) {
        StringBuilder sb = new StringBuilder();
        line(sb, List.of("concurrency", "metric", "unit", "mode", "n", "median", "min", "max", "statement"));
        for (Cell c : cells) {
            for (String mode : c.modes()) {
                ModeStats s = c.stats().get(mode);
                List<String> row = new ArrayList<>();
                row.add(Integer.toString(c.concurrency()));
                row.add(c.metric().code());
                row.add(unit(c.metric()));
                row.add(text(mode));
                row.add(Integer.toString(s.n()));
                row.add(c.metric().exact(s.median()));
                row.add(c.metric().exact(s.min()));
                row.add(c.metric().exact(s.max()));
                row.add(c.decision().statement().name());
                line(sb, row);
            }
        }
        return sb.toString();
    }

    static String unit(ComparedMetric metric) {
        return switch (metric.unit()) {
            case MILLIS -> "ns";
            case RATE -> "per_second";
            case PERCENT -> "percent";
        };
    }

    /** Untrusted text: a leading equals, plus, minus, at sign, tab or line break would be a formula in a spreadsheet, so it gets a quote. */
    static String text(String value) {
        return !value.isEmpty() && FORMULA_START.indexOf(value.charAt(0)) >= 0 ? "'" + value : value;
    }

    private static String opt(Long v) {
        return v == null ? "" : Long.toString(v);
    }

    private static void line(StringBuilder sb, List<String> fields) {
        for (int i = 0; i < fields.size(); i++) {
            if (i > 0) {
                sb.append(',');
            }
            sb.append(escape(fields.get(i)));
        }
        sb.append('\n');
    }

    static String escape(String value) {
        if (value.indexOf(',') >= 0 || value.indexOf('"') >= 0 || value.indexOf('\n') >= 0
                || value.indexOf('\r') >= 0) {
            return '"' + value.replace("\"", "\"\"") + '"';
        }
        return value;
    }
}
