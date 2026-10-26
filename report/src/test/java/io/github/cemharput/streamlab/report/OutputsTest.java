package io.github.cemharput.streamlab.report;

import static org.assertj.core.api.Assertions.assertThat;

import io.github.cemharput.streamlab.bench.json.Json;
import io.github.cemharput.streamlab.report.CellComparison.Cell;
import io.github.cemharput.streamlab.report.RunAssessment.Analysis;
import io.github.cemharput.streamlab.report.testsupport.Analyses;
import io.github.cemharput.streamlab.report.testsupport.ResultSetBuilder;
import java.math.BigDecimal;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.Timeout.ThreadMode;
import org.junit.jupiter.api.io.TempDir;

/** The small writers: CSV escaping, chart scaling, JSON number handling and the file writer. */
@Timeout(value = 30, unit = TimeUnit.SECONDS, threadMode = ThreadMode.SEPARATE_THREAD)
class OutputsTest {

    @TempDir
    Path tmp;

    @Test
    void csvFieldsAreQuotedOnlyWhenNeeded() {
        assertThat(CsvWriter.escape("plain")).isEqualTo("plain");
        assertThat(CsvWriter.escape("a,b")).isEqualTo("\"a,b\"");
        assertThat(CsvWriter.escape("say \"hi\"")).isEqualTo("\"say \"\"hi\"\"\"");
        assertThat(CsvWriter.escape("two\nlines")).isEqualTo("\"two\nlines\"");
        assertThat(CsvWriter.escape("")).isEmpty();
    }

    @Test
    void theChartScaleIsOneTwoFiveTenTimesAPowerOfTen() {
        assertThat(SvgDotPlot.niceMax(BigDecimal.ZERO)).isEqualByComparingTo("1");
        assertThat(SvgDotPlot.niceMax(new BigDecimal("7"))).isEqualByComparingTo("10");
        assertThat(SvgDotPlot.niceMax(new BigDecimal("12"))).isEqualByComparingTo("20");
        assertThat(SvgDotPlot.niceMax(new BigDecimal("20"))).isEqualByComparingTo("20");
        assertThat(SvgDotPlot.niceMax(new BigDecimal("21"))).isEqualByComparingTo("50");
        assertThat(SvgDotPlot.niceMax(new BigDecimal("51"))).isEqualByComparingTo("100");
        assertThat(SvgDotPlot.niceMax(new BigDecimal("0.3"))).isEqualByComparingTo("0.5");
        assertThat(SvgDotPlot.niceMax(new BigDecimal("4200000"))).isEqualByComparingTo("5000000");
        assertThat(SvgDotPlot.niceMax(new BigDecimal("1.11"))).isEqualByComparingTo("2");
    }

    @Test
    void aChartWithoutDataSaysSoAndStillIsAValidSvgDocument() {
        String svg = SvgDotPlot.render(ComparedMetric.TTFC_P50, List.of(), List.of("shared", "per-request"));
        assertThat(svg).startsWith("<svg ").endsWith("</svg>\n").contains("no included run has a value");
        assertThat(WordingGuard.scan(svg, false)).isEmpty();
    }

    @Test
    void aChartHasIntegerCoordinatesOnlyAndOnePointPerRun() throws Exception {
        ResultSetBuilder b = ResultSetBuilder.at(tmp.resolve("set")).concurrency(4).repetitions(4).plan();
        Analysis a = Analyses.of(b);
        List<Cell> cells = CellComparison.compareAll(a).stream().filter(c -> c.metric() == ComparedMetric.TTFC_P99)
                .toList();
        String svg = SvgDotPlot.render(ComparedMetric.TTFC_P99, cells, CellComparison.modeOrder(a));
        assertThat(svg).doesNotContainPattern("(cx|cy|x|y|x1|y1|x2|y2|width|height|r)=\"-?\\d+\\.\\d");
        assertThat(svg.lines().filter(l -> l.startsWith("<circle") && l.contains("r=\"4\"") && !l.contains("#555555"))
                .count() + svg.lines().filter(l -> l.startsWith("<rect x") && l.contains("width=\"8\"")
                && !l.contains("#555555")).count()).isEqualTo(8);
        assertThat(svg).contains("font-family=\"sans-serif\"").doesNotContain("\r");
    }

    @Test
    void modeNamesInAChartAreEscaped() {
        String svg = SvgDotPlot.render(ComparedMetric.TTFC_P50, List.of(), List.of("a<b", "c&d"));
        assertThat(svg).doesNotContain("a<b");
    }

    @Test
    void decimalsInAnalysisJsonAreStringsAndCountersAreNumbers() throws Exception {
        ResultSetBuilder b = ResultSetBuilder.at(tmp.resolve("j")).concurrency(4).repetitions(2).plan();
        Analysis a = Analyses.of(b);
        String json = AnalysisJson.write(a, CellComparison.compareAll(a));
        Map<String, Object> root = Json.parseObject(json);
        assertThat(root).containsEntry("analysisVersion", 1L).containsEntry("runId", a.manifest().runId());
        Map<?, ?> run = (Map<?, ?>) ((List<?>) root.get("runs")).get(0);
        Map<?, ?> metrics = (Map<?, ?>) run.get("metrics");
        assertThat(metrics.get("completedPerSecond")).isEqualTo("1.111111111111111111111111111111");
        assertThat(metrics.get("requestLines")).isEqualTo(10L);
        assertThat(((Map<?, ?>) metrics.get("ttfcNanos")).get("p50")).isInstanceOf(Long.class);
        assertThat(((Map<?, ?>) ((Map<?, ?>) metrics.get("snapshots")).get("gatewayCounterDeltas")).get("rejected_total"))
                .isEqualTo("0");
        Map<?, ?> cell = (Map<?, ?>) ((List<?>) root.get("cells")).get(1);
        assertThat(cell.get("metric")).isEqualTo("ttfc-p50");
        assertThat(((Map<?, ?>) ((Map<?, ?>) cell.get("modes")).get("shared")).get("median")).isInstanceOf(String.class);
        // the writer's number trap is real: a BigDecimal would be cut to a whole number
        Map<String, Object> trap = new LinkedHashMap<>();
        trap.put("x", new BigDecimal("1.75"));
        assertThat(Json.writeCompact(trap)).isEqualTo("{\"x\":1}");
    }

    @Test
    void theFileWriterCreatesTheDirectoryReplacesFilesAndLeavesNoTmpFile() throws Exception {
        Path out = Files.createDirectory(tmp.resolve("o"));
        Files.createDirectories(out.resolve("benchmark"));
        Files.writeString(out.resolve("benchmark/a.csv"), "old");
        Map<String, String> files = new LinkedHashMap<>();
        files.put("report.md", "text\n");
        files.put("benchmark/a.csv", "new\n");
        files.put("benchmark/b.json", "{}\n");
        OutputWriter.write(out, files);
        assertThat(Files.readString(out.resolve("benchmark/a.csv"))).isEqualTo("new\n");
        assertThat(Files.readString(out.resolve("report.md"))).isEqualTo("text\n");
        try (var s = Files.walk(out)) {
            assertThat(s.filter(p -> p.toString().endsWith(".tmp"))).isEmpty();
        }
        assertThat(OutputWriter.existing(out, List.of("report.md", "benchmark/b.json", "benchmark/none.csv")))
                .containsExactly("report.md", "benchmark/b.json");
    }

    @Test
    void theFixedFileListIsTheOneOfThePlan() {
        assertThat(OutputWriter.ALL_FILES).containsExactly("benchmark-report.md", "benchmark/runs.csv",
                "benchmark/cells.csv", "benchmark/analysis.json", "benchmark/chart-completed-per-second.svg",
                "benchmark/chart-ttfc-p50.svg", "benchmark/chart-ttfc-p99.svg", "benchmark/chart-total-p99.svg");
        assertThat(ReportRenderer.CHARTS.keySet()).containsExactly("chart-completed-per-second.svg",
                "chart-ttfc-p50.svg", "chart-ttfc-p99.svg", "chart-total-p99.svg");
    }

    @Test
    void metricValuesAreShownWithTheirUnitsAndRoundedHalfEven() {
        assertThat(ComparedMetric.TTFC_P50.withUnit(new BigDecimal("1234567"))).isEqualTo("1.235 ms");
        assertThat(ComparedMetric.COMPLETED_PER_SECOND.withUnit(new BigDecimal("1.125000000"))).isEqualTo("1.12 per s");
        assertThat(ComparedMetric.REJECTED_SHARE.withUnit(new BigDecimal("12.345"))).isEqualTo("12.34 %");
        assertThat(ComparedMetric.TTFC_P50.exact(new BigDecimal("1500000.0"))).isEqualTo("1500000");
        assertThat(ComparedMetric.TTFC_P50.exact(null)).isEmpty();
    }
}
