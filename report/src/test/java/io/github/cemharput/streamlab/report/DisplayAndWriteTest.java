package io.github.cemharput.streamlab.report;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assumptions.assumeThat;

import io.github.cemharput.streamlab.report.CellComparison.Decision;
import io.github.cemharput.streamlab.report.CellComparison.ModeStats;
import io.github.cemharput.streamlab.report.testsupport.Cli;
import io.github.cemharput.streamlab.report.testsupport.Cli.Result;
import io.github.cemharput.streamlab.report.testsupport.ResultSetBuilder;
import io.github.cemharput.streamlab.report.testsupport.Scenarios;
import java.math.BigDecimal;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.PosixFilePermission;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.Timeout.ThreadMode;
import org.junit.jupiter.api.io.TempDir;

/** Display precision of compared values, file permissions, escaping details and warm-up wording. */
@Timeout(value = 60, unit = TimeUnit.SECONDS, threadMode = ThreadMode.SEPARATE_THREAD)
class DisplayAndWriteTest {

    private static final List<String> MODES = List.of("shared", "per-request");

    @TempDir
    Path tmp;

    private static List<BigDecimal> values(String... v) {
        return java.util.Arrays.stream(v).map(BigDecimal::new).toList();
    }

    private static Map<String, ModeStats> stats(List<BigDecimal> shared, List<BigDecimal> perRequest) {
        Map<String, ModeStats> m = new LinkedHashMap<>();
        m.put("shared", CellComparison.stats("shared", shared));
        m.put("per-request", CellComparison.stats("per-request", perRequest));
        return m;
    }

    private static String sentence(ComparedMetric metric, Map<String, ModeStats> stats) {
        Decision d = CellComparison.decide(stats, MODES);
        int scale = CellComparison.displayScale(d, stats, MODES, metric);
        return d.statement() + "|" + scale + "|" + CellComparison.sentence(d, stats, MODES, 50, metric, scale);
    }

    // ------------------------------------------------------------------ P3-A

    @Test
    void sharesThatDifferBelowTwoDecimalsAreShownWithEnoughDecimals() {
        // percent values: medians 0.00105 and 0.0031, difference 0.00205 > largest range 0.0002; all 0.00 at two decimals
        Map<String, ModeStats> s = stats(values("0.0010", "0.0011"), values("0.0030", "0.0032"));
        assertThat(CellComparison.decide(s, MODES).statement()).isEqualTo(Statement.HIGHER);
        String text = sentence(ComparedMetric.REJECTED_SHARE, s);
        assertThat(text).startsWith("HIGHER|3|");
        assertThat(text).contains("of per-request (0.003 %) was higher than that of shared (0.001 %)")
                .contains("medians (0.002 %) was larger than the largest within-mode range (0.000 %)");
        assertThat(text).doesNotContain("(0.00 %)");
    }

    @Test
    void ratesThatDifferBelowTwoDecimalsAreShownWithEnoughDecimals() {
        Map<String, ModeStats> s = stats(values("1.00001", "1.00002"), values("1.00041", "1.00042"));
        String text = sentence(ComparedMetric.COMPLETED_PER_SECOND, s);
        assertThat(text).startsWith("HIGHER|4|");
        assertThat(text).contains("of per-request (1.0004 per s) was higher than that of shared (1.0000 per s)");
        assertThat(text).contains("medians (0.0004 per s) was larger than the largest within-mode range (0.0000 per s)");
    }

    @Test
    void millisecondValuesGetMoreDecimalsWhenTheyDifferByLessThanAMicrosecond() {
        // nanoseconds: 1000000.2 vs 1000000.7 ns look equal at the usual 3 decimals of a millisecond
        Map<String, ModeStats> s = stats(values("1000000.1", "1000000.3"), values("1000000.6", "1000000.8"));
        Decision d = CellComparison.decide(s, MODES);
        int scale = CellComparison.displayScale(d, s, MODES, ComparedMetric.TTFC_P50);
        assertThat(scale).isGreaterThan(3);
        assertThat(ComparedMetric.TTFC_P50.round(s.get("shared").median(), scale))
                .isNotEqualByComparingTo(ComparedMetric.TTFC_P50.round(s.get("per-request").median(), scale));
    }

    @Test
    void theUsualScaleIsKeptWhenItAlreadyShowsEverything() {
        Map<String, ModeStats> s = stats(values("10000000", "12000000"), values("20000000", "22000000"));
        Decision d = CellComparison.decide(s, MODES);
        assertThat(CellComparison.displayScale(d, s, MODES, ComparedMetric.TTFC_P50)).isEqualTo(3);
        assertThat(CellComparison.displayScale(d, s, MODES, ComparedMetric.REJECTED_SHARE)).isEqualTo(2);
        Map<String, ModeStats> equal = stats(values("5", "7"), values("5", "7"));
        assertThat(CellComparison.displayScale(CellComparison.decide(equal, MODES), equal, MODES,
                ComparedMetric.REJECTED_SHARE)).isEqualTo(2);
    }

    // ------------------------------------------------------------------ P3-B, H3

    @Test
    void outputFilesGetNormalPermissionsOnPosix() throws Exception {
        Path out = Files.createDirectory(tmp.resolve("out"));
        assumeThat(out.getFileSystem().supportedFileAttributeViews()).contains("posix");
        Path ref = Files.createFile(out.resolve("reference"));
        var refPerms = Files.getPosixFilePermissions(ref);
        OutputWriter.write(out, Map.of("benchmark-report.md", "x\n"));
        var perms = Files.getPosixFilePermissions(out.resolve("benchmark-report.md"));
        assertThat(perms).contains(PosixFilePermission.OWNER_READ, PosixFilePermission.OWNER_WRITE);
        if (refPerms.contains(PosixFilePermission.GROUP_READ)) {
            assertThat(perms).contains(PosixFilePermission.GROUP_READ);
        }
        if (refPerms.contains(PosixFilePermission.OTHERS_READ)) {
            assertThat(perms).contains(PosixFilePermission.OTHERS_READ);
        }
        assertThat(perms).doesNotContain(PosixFilePermission.OWNER_EXECUTE);
    }

    @Test
    void aDirectoryCreatedByAFailedRunIsRemovedWhenItIsEmpty() throws Exception {
        Path out = Files.createDirectory(tmp.resolve("out"));
        Files.createDirectories(out.resolve("benchmark-report.md")); // a directory: the first move fails
        Files.writeString(out.resolve("benchmark-report.md").resolve("keep.txt"), "mine");
        Map<String, String> files = new LinkedHashMap<>();
        files.put("benchmark-report.md", "b\n");
        files.put("benchmark/runs.csv", "a\n");
        assertThat(org.assertj.core.api.Assertions.catchThrowable(() -> OutputWriter.write(out, files)))
                .isInstanceOf(OutputWriter.WriteException.class);
        assertThat(Files.exists(out.resolve("benchmark"))).isFalse();
        assertThat(Files.readString(out.resolve("benchmark-report.md/keep.txt"))).isEqualTo("mine");
    }

    @Test
    void aPreExistingDirectoryIsNeverRemoved() throws Exception {
        Path out = Files.createDirectory(tmp.resolve("out"));
        Files.createDirectories(out.resolve("benchmark"));
        Files.createDirectories(out.resolve("benchmark-report.md"));
        Files.writeString(out.resolve("benchmark-report.md").resolve("keep.txt"), "mine");
        Map<String, String> files = new LinkedHashMap<>();
        files.put("benchmark-report.md", "b\n");
        files.put("benchmark/runs.csv", "a\n");
        org.assertj.core.api.Assertions.catchThrowable(() -> OutputWriter.write(out, files));
        assertThat(Files.isDirectory(out.resolve("benchmark"))).isTrue();
    }

    // ------------------------------------------------------------------ P3-D

    @Test
    void anUnknownFlagFromAWarmUpPhaseIsOnlyACautionAndTheWordingSaysSo() throws Exception {
        ResultSetBuilder b = Scenarios.clean(tmp);
        b.run(1).warmupFlag("BRAND_NEW_WARMUP_FLAG");
        b.run(2).measuredFlag("BRAND_NEW_MEASURED_FLAG");
        Path set = b.build();
        Path out = Files.createDirectory(tmp.resolve("o"));
        Result r = Cli.run(tmp, "--results", set.toString(), "--out", out.toString());
        assertThat(r.exit()).isZero();
        assertThat(r.summary()).containsEntry("excluded", 1L).containsEntry("caution", 1L);
        String report = Files.readString(out.resolve("benchmark-report.md"));
        assertThat(report).contains("runs with them are excluded, except a flag recorded in a warm-up phase, which is a caution")
                .contains("`BRAND_NEW_WARMUP_FLAG`").contains("`BRAND_NEW_MEASURED_FLAG`")
                .contains("excludes the run (except in a warm-up phase, where it is a caution)");
    }

    // ------------------------------------------------------------------ P3-F, H1, H2

    @Test
    void aBackslashBeforeAPipeCannotSplitACell() {
        // the backslash is escaped first, so the pipe keeps an odd number of backslashes in front of it
        String cell = Fmt.cell("a\\|b");
        assertThat(cell).isEqualTo("a\\\\\\|b");
        assertThat(unescapedPipes(cell)).isZero();
        assertThat(unescapedPipes(Fmt.cell("a\\\\|b"))).isZero();
        assertThat(unescapedPipes(Fmt.cell("plain | pipe"))).isZero();
    }

    @Test
    void aHostileBackslashPipeInTheManifestKeepsTheTableRowsIntact() throws Exception {
        Path set = ResultSetBuilder.at(tmp.resolve("set")).preset("p\\|q").plan().build();
        Path out = Files.createDirectory(tmp.resolve("o"));
        assertThat(Cli.run(tmp, "--results", set.toString(), "--out", out.toString()).exit()).isZero();
        String line = Files.readString(out.resolve("benchmark-report.md")).lines()
                .filter(l -> l.startsWith("| Preset |")).findFirst().orElseThrow();
        assertThat(unescapedPipes(line)).isEqualTo(3); // | name | value |
        assertThat(line).isEqualTo("| Preset | p\\\\\\|q |");
    }

    /** Pipes that a parser sees as cell separators: not preceded by an odd number of backslashes. */
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

    @Test
    void csvAlsoNeutralisesTabAndLineBreakStarts() {
        assertThat(CsvWriter.text("\tcmd")).isEqualTo("'\tcmd");
        assertThat(CsvWriter.text("\rcmd")).isEqualTo("'\rcmd");
        assertThat(CsvWriter.text("\ncmd")).isEqualTo("'\ncmd");
        assertThat(CsvWriter.text("mid\ttab")).isEqualTo("mid\ttab");
    }
}
