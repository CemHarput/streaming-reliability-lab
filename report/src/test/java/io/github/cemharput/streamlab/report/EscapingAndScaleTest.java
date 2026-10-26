package io.github.cemharput.streamlab.report;

import static org.assertj.core.api.Assertions.assertThat;

import io.github.cemharput.streamlab.bench.json.Json;
import io.github.cemharput.streamlab.report.CellComparison.Decision;
import io.github.cemharput.streamlab.report.CellComparison.ModeStats;
import io.github.cemharput.streamlab.report.testsupport.Cli;
import io.github.cemharput.streamlab.report.testsupport.Cli.Result;
import io.github.cemharput.streamlab.report.testsupport.ResultSetBuilder;
import java.math.BigDecimal;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.Timeout.ThreadMode;
import org.junit.jupiter.api.io.TempDir;

/** Code spans in cells, UNC-like text in every escaped form, and the displayed ranges of a comparison. */
@Timeout(value = 60, unit = TimeUnit.SECONDS, threadMode = ThreadMode.SEPARATE_THREAD)
class EscapingAndScaleTest {

    private static final List<String> MODES = List.of("shared", "per-request");
    private static final String BS = "\\";

    @TempDir
    Path tmp;

    // ------------------------------------------------------------------ P3-1

    private static List<BigDecimal> values(String... v) {
        return java.util.Arrays.stream(v).map(BigDecimal::new).toList();
    }

    @Test
    void theReviewersExampleKeepsTheRangesApartAndTheSpreadConsistent() {
        Map<String, ModeStats> s = new LinkedHashMap<>();
        s.put("shared", CellComparison.stats("shared", values("1.002", "1.0069")));
        s.put("per-request", CellComparison.stats("per-request", values("1.0071", "1.0120")));
        Decision d = CellComparison.decide(s, MODES);
        assertThat(d.statement()).isEqualTo(Statement.HIGHER); // exact: d = 0.0051 > s = 0.0049, no overlap
        ComparedMetric m = ComparedMetric.COMPLETED_PER_SECOND;
        int scale = CellComparison.displayScale(d, s, MODES, m);
        assertThat(scale).isEqualTo(4); // at 3 decimals 1.0069 and 1.0071 would both read 1.007 (touching)
        BigDecimal sharedMax = m.round(s.get("shared").max(), scale);
        BigDecimal perRequestMin = m.round(s.get("per-request").min(), scale);
        assertThat(sharedMax).isLessThan(perRequestMin);
        String text = CellComparison.sentence(d, s, MODES, 50, m, scale);
        assertThat(text).contains("1.0096 per s").contains("1.0044 per s")
                .contains("medians (0.0052 per s) was larger than the largest within-mode range (0.0049 per s)")
                .doesNotContain("(0.00 per s)");
        // the sentence's difference is the difference of the displayed medians
        assertThat(m.displayDifference(s.get("shared"), s.get("per-request"), scale)).isEqualByComparingTo("0.0052");
        assertThat(m.displaySpread(s.get("shared"), s.get("per-request"), scale)).isEqualByComparingTo("0.0049");
    }

    @Test
    void theSpreadCaseKeepsDisplayedRangesApartAndNotLargerThanTheSpread() {
        // exact: ranges 0.0049 and 0.0049, medians 1.00445 and 1.00955 (d = 0.0051): make d <= s with wider ranges
        Map<String, ModeStats> s = new LinkedHashMap<>();
        s.put("shared", CellComparison.stats("shared", values("1.0020", "1.0100")));
        s.put("per-request", CellComparison.stats("per-request", values("1.0101", "1.0190")));
        Decision d = CellComparison.decide(s, MODES);
        assertThat(d.statement()).isEqualTo(Statement.CANNOT_SEPARATE_SPREAD);
        ComparedMetric m = ComparedMetric.COMPLETED_PER_SECOND;
        int scale = CellComparison.displayScale(d, s, MODES, m);
        assertThat(m.round(s.get("shared").max(), scale)).isLessThan(m.round(s.get("per-request").min(), scale));
        assertThat(m.displayDifference(s.get("shared"), s.get("per-request"), scale))
                .isLessThanOrEqualTo(m.displaySpread(s.get("shared"), s.get("per-request"), scale));
    }

    // ------------------------------------------------------------------ P3-2

    @Test
    void backtickRunsInUntrustedTextCannotOpenCodeSpans() {
        String[] hostile = {"a`b", "a``<img src=x>```", "`unbalanced <img src=x>", "``<b>", "x`<i>`y", "```<u>```",
            "<img src=x>"};
        for (String h : hostile) {
            String cell = Fmt.cell(h);
            assertThat(cell).as(h).doesNotContain("`").doesNotContain("<");
        }
        // generated spans survive; text around them is escaped
        assertThat(Fmt.cell(Fmt.code("A`B|<C>") + " <i> `x`")).isEqualTo("`A'B\\|<C>` &lt;i&gt; 'x'");
        // a stray marker (hostile private-use characters) never makes a span with a backtick or a raw bracket outside
        String fake = Fmt.cell("<img src=x>");
        assertThat(fake).doesNotContain("<").doesNotContain("`");
        assertThat(Fmt.cell("a`b <u>")).isEqualTo("`a'b` &lt;u&gt;");
    }

    @Test
    void hostileBackticksAndTagsFromTheManifestKeepEveryTableIntact() throws Exception {
        ResultSetBuilder b = ResultSetBuilder.at(tmp.resolve("set")).concurrency(4).repetitions(2)
                .runId("a``<img src=x>``` |").preset("`unbalanced <img src=x>").setFlag("F`L``AG<b>|x").plan();
        b.run(1).manifestRunOverrides.put("mode", "x`<img src=x>`|y");
        b.run(2).measuredFlag("M``<i>|`");
        Path set = b.build();
        Path manifest = set.resolve("manifest.json");
        Map<String, Object> m = Json.parseObject(Files.readString(manifest));
        m.put("toolVersion", "v``<script>`|");
        Files.writeString(manifest, Json.write(m));
        Path out = Files.createDirectory(tmp.resolve("o"));
        Result r = Cli.run(tmp, "--results", set.toString(), "--out", out.toString());
        assertThat(r.exit()).as(r.err()).isZero();
        List<String> lines = Files.readAllLines(out.resolve("benchmark-report.md"));
        int expected = -1;
        int tables = 0;
        for (String line : lines) {
            if (!line.startsWith("| ") && !line.startsWith("|---")) {
                expected = -1;
                continue;
            }
            int pipes = unescapedPipes(line);
            if (expected < 0) {
                expected = pipes;
                tables++;
            }
            assertThat(pipes).as(line).isEqualTo(expected);
            // outside generated code spans nothing of an HTML tag survives
            assertThat(line.replaceAll("`[^`]*`", "")).as(line).doesNotContain("<");
        }
        assertThat(tables).isGreaterThan(5);
        assertThat(String.join("\n", lines)).doesNotContain("<img").doesNotContain("<script");
    }

    private static int unescapedPipes(String line) {
        int count = 0;
        for (int i = 0; i < line.length(); i++) {
            if (line.charAt(i) == '|') {
                int slashes = 0;
                for (int j = i - 1; j >= 0 && line.charAt(j) == '\\'; j--) {
                    slashes++;
                }
                if (slashes % 2 == 0) {
                    count++;
                }
            }
        }
        return count;
    }

    // ------------------------------------------------------------------ P3-3

    @Test
    void uncPathsAreCaughtInEveryEscapedOrTransformedForm() {
        String raw = BS + BS + "srv" + BS + "share";
        String markdownEscaped = BS + BS + BS + BS + "srv" + BS + BS + "share";
        String jsonEscaped = Json.writeCompact(Map.of("p", raw));
        String viaCell = Fmt.cell("see " + raw);
        String viaCode = Fmt.cell(Fmt.code(raw));
        String ntForm = BS + BS + "?" + BS + "UNC" + BS + "srv" + BS + "share";
        for (String text : List.of(raw, markdownEscaped, jsonEscaped, viaCell, viaCode, ntForm, "//srv/share",
                "x //srv/share y")) {
            assertThat(PrivacyGuard.scan(text, List.of())).as(text).isNotEmpty();
        }
        assertThat(viaCode).contains("//srv/share");
    }

    @Test
    void legitimateTextIsNotMistakenForAnUncPath() {
        for (String text : List.of("http://127.0.0.1:8080/stream", "https://example.org/a/b",
                "--lab.upstream.target=127.0.0.1:50051", "-Xlog:gc:file=gateway-gc.log:uptime,time,level,tags",
                "docs//x/y", "benchmark/runs.csv", "proc cpuinfo model name", "/stream", "a // b / c",
                "<svg xmlns=\"http://www.w3.org/2000/svg\">", "-Xms1g -Xmx1g -XX:+UseG1GC")) {
            assertThat(PrivacyGuard.scan(text, List.of())).as(text).isEmpty();
        }
    }

    @Test
    void anUncPathInTheManifestStopsTheToolWithExitOne() throws Exception {
        Path set = ResultSetBuilder.at(tmp.resolve("unc")).preset(BS + BS + "srv" + BS + "share").plan().build();
        Path out = Files.createDirectory(tmp.resolve("o2"));
        Result r = Cli.run(tmp, "--results", set.toString(), "--out", out.toString());
        assertThat(r.exit()).isEqualTo(1);
        assertThat(r.err()).contains("internal guard violation");
        try (var s = Files.walk(out)) {
            assertThat(s.filter(Files::isRegularFile).count()).isZero();
        }
    }
}
