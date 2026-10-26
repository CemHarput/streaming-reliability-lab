package io.github.cemharput.streamlab.report;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assumptions.assumeThat;

import io.github.cemharput.streamlab.bench.json.Json;
import io.github.cemharput.streamlab.report.RunAssessment.Analysis;
import io.github.cemharput.streamlab.report.testsupport.Analyses;
import io.github.cemharput.streamlab.report.testsupport.Cli;
import io.github.cemharput.streamlab.report.testsupport.Cli.Result;
import io.github.cemharput.streamlab.report.testsupport.ResultSetBuilder;
import io.github.cemharput.streamlab.report.testsupport.Scenarios;
import java.io.IOException;
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

/** Writing is all-or-nothing as far as practical; untrusted manifest text cannot break the output. */
@Timeout(value = 60, unit = TimeUnit.SECONDS, threadMode = ThreadMode.SEPARATE_THREAD)
class HardeningTest {

    @TempDir
    Path tmp;

    private static Map<String, String> contents() {
        Map<String, String> m = new LinkedHashMap<>();
        m.put("benchmark-report.md", "report\n");
        m.put("benchmark/runs.csv", "runs\n");
        m.put("benchmark/analysis.json", "{}\n");
        return m;
    }

    // ------------------------------------------------------------------ P3-1

    @Test
    void aFailureBeforeAnyMoveLeavesTheOutputDirectoryUnchanged() throws Exception {
        Path out = Files.createDirectory(tmp.resolve("out"));
        Files.writeString(out.resolve("benchmark"), "a file where the directory should be");
        Map<String, String> before = Cli.hashes(out);
        assertThatThrownBy(() -> OutputWriter.write(out, contents())).isInstanceOf(OutputWriter.WriteException.class)
                .hasMessageContaining("no final file was replaced");
        assertThat(Cli.hashes(out)).isEqualTo(before);
    }

    @Test
    void aFailureDuringTheMovesDeletesOnlyOwnTempFilesAndNamesTheReplacedFiles() throws Exception {
        Path out = Files.createDirectory(tmp.resolve("out"));
        Files.createDirectories(out.resolve("benchmark").resolve("analysis.json"));
        Files.writeString(out.resolve("benchmark").resolve("analysis.json").resolve("keep.txt"), "mine");
        Files.writeString(out.resolve("benchmark-report.md.tmp"), "user file");
        assertThatThrownBy(() -> OutputWriter.write(out, contents())).isInstanceOf(OutputWriter.WriteException.class)
                .hasMessageContaining("already replaced: benchmark-report.md, benchmark/runs.csv");
        assertThat(Files.readString(out.resolve("benchmark-report.md"))).isEqualTo("report\n");
        assertThat(Files.readString(out.resolve("benchmark-report.md.tmp"))).isEqualTo("user file");
        assertThat(Files.readString(out.resolve("benchmark/analysis.json/keep.txt"))).isEqualTo("mine");
        try (var s = Files.walk(out)) {
            assertThat(s.filter(p -> p.getFileName().toString().startsWith(".report-"))).isEmpty();
        }
    }

    @Test
    void aUserOwnedTmpFileIsNeverTouchedByASuccessfulRun() throws Exception {
        Path set = Scenarios.clean(tmp).build();
        Path out = Files.createDirectory(tmp.resolve("out"));
        Files.createDirectories(out.resolve("benchmark"));
        Files.writeString(out.resolve("benchmark-report.md.tmp"), "user file");
        Files.writeString(out.resolve("benchmark").resolve("runs.csv.tmp"), "user file 2");
        assertThat(Cli.run(tmp, "--results", set.toString(), "--out", out.toString()).exit()).isZero();
        assertThat(Files.readString(out.resolve("benchmark-report.md.tmp"))).isEqualTo("user file");
        assertThat(Files.readString(out.resolve("benchmark").resolve("runs.csv.tmp"))).isEqualTo("user file 2");
    }

    @Test
    void anIoErrorInTheToolLeavesTheFileListUnchangedAndExitsOne() throws Exception {
        Path set = Scenarios.clean(tmp).build();
        Path out = Files.createDirectory(tmp.resolve("out"));
        Files.writeString(out.resolve("benchmark"), "blocker");
        Map<String, String> before = Cli.hashes(out);
        Result r = Cli.run(tmp, "--results", set.toString(), "--out", out.toString());
        assertThat(r.exit()).isEqualTo(1);
        assertThat(r.err()).contains("no final file was replaced");
        assertThat(Cli.hashes(out)).isEqualTo(before);
    }

    @Test
    void aBenchmarkDirectoryLinkedIntoTheResultSetIsRefused() throws Exception {
        Path set = Scenarios.clean(tmp).build();
        Path out = Files.createDirectory(tmp.resolve("out"));
        try {
            Files.createSymbolicLink(out.resolve("benchmark"), set.resolve("runs"));
        } catch (UnsupportedOperationException | IOException | SecurityException ex) {
            assumeThat(false).as("symbolic links are not available: " + ex).isTrue();
        }
        Map<String, String> before = Cli.hashes(set);
        Result r = Cli.run(tmp, "--results", set.toString(), "--out", out.toString(), "--overwrite");
        assertThat(r.exit()).isEqualTo(2);
        assertThat(r.err()).contains("resolves into --results");
        assertThat(Cli.hashes(set)).isEqualTo(before);
    }

    // ------------------------------------------------------------------ P3-4

    @Test
    void anUnparsableOrWrongTypeSummaryIsACautionNotSilentlyDropped() throws Exception {
        ResultSetBuilder b = ResultSetBuilder.at(tmp.resolve("set")).concurrency(4).repetitions(1).plan();
        Path set = b.build();
        Files.writeString(set.resolve("runs/001-c4-shared-r1/measured-summary.json"), "{broken");
        Files.writeString(set.resolve("runs/002-c4-per-request-r1/measured-summary.json"), "{\"type\":\"other\"}");
        Analysis a = RunAssessment.analyse(ResultSetReader.read(set));
        for (int i = 0; i < 2; i++) {
            assertThat(a.runs().get(i).assessment().reasons()).extracting(RunAssessment.Reason::code)
                    .contains(Checks.SUMMARY_UNREADABLE);
            assertThat(a.runs().get(i).assessment().classification()).isEqualTo("caution");
        }
        assertThat(Checks.TABLE.get(Checks.SUMMARY_UNREADABLE)).isEqualTo(Severity.CAUTION);
        assertThat(Checks.CROSS_CHECKS.stream().filter(c -> c.id().equals("C8") || c.id().equals("C9")))
                .allSatisfy(c -> assertThat(c.codes()).contains(Checks.SUMMARY_UNREADABLE));
    }

    // ------------------------------------------------------------------ P3-5

    @Test
    void hostileTextInTheManifestCannotBreakTheOutputOrTurnIntoExitOne() throws Exception {
        ResultSetBuilder b = ResultSetBuilder.at(tmp.resolve("set")).concurrency(4).repetitions(2)
                .runId("a|b-->c\n=x{y}$1").preset("=cmd|x").setFlag("FLAG|`-->{x}").plan();
        b.run(1).manifestRunOverrides.put("mode", "=cmd{x}$");
        Path set = b.build();
        Path manifest = set.resolve("manifest.json");
        Map<String, Object> m = Json.parseObject(Files.readString(manifest));
        ((Map<String, Object>) ((Map<?, ?>) ((Map<?, ?>) m.get("parameters")).get("plan")))
                .put("modeOrder", new java.util.ArrayList<Object>(List.of("=cmd", "shared")));
        Files.writeString(manifest, Json.write(m));
        Path out = Files.createDirectory(tmp.resolve("out"));
        Result r = Cli.run(tmp, "--results", set.toString(), "--out", out.toString());
        assertThat(r.exit()).as(r.err()).isZero();

        String report = Files.readString(out.resolve("benchmark-report.md"));
        String first = report.lines().findFirst().orElseThrow();
        assertThat(first).startsWith("<!-- Generated by the report module").endsWith("-->");
        assertThat(first.indexOf("-->")).isEqualTo(first.length() - 3);
        assertThat(first).doesNotContain("\n");
        assertThat(report).contains("a\\|b").contains("=cmd\\|x").contains("FLAG\\|'-->{x}");
        assertThat(report).doesNotContain("a|b");
        // no double escaping: a pipe is escaped exactly once
        assertThat(report).doesNotContain("\\\\|");
        assertThat(report).contains("> Caution flags recorded for the whole set: `CHECK_PLAN_MISMATCH`"); // invalid modeOrder
        String csv = Files.readString(out.resolve("benchmark/runs.csv"));
        assertThat(csv).contains(",'=cmd{x}$,");
        assertThat(csv).doesNotContain(",=cmd");
        assertThat(r.out().lines().filter(l -> l.startsWith("{")).count()).isEqualTo(1);
        assertThat(r.out()).doesNotContain("\r");
        assertThat(r.summary()).containsKey("excludedRuns");
    }

    @Test
    void theCsvNeutralisesFormulaStartsInTextCellsOnly() {
        assertThat(CsvWriter.text("=1+1")).isEqualTo("'=1+1");
        assertThat(CsvWriter.text("+x")).isEqualTo("'+x");
        assertThat(CsvWriter.text("-x")).isEqualTo("'-x");
        assertThat(CsvWriter.text("@x")).isEqualTo("'@x");
        assertThat(CsvWriter.text("001-c4")).isEqualTo("001-c4");
        assertThat(CsvWriter.text("")).isEmpty();
    }

    @Test
    void theRunIdInTheCommentUsesASafeAlphabetOnly() {
        assertThat(ReportText.safeComment("a--b-->c\n<d>")).matches("[A-Za-z0-9._:+-]+")
                .doesNotContain("--").doesNotContain(">").doesNotContain("\n");
        assertThat(ReportText.safeComment("20261025T143002Z-1fa78dc-dirty")).isEqualTo("20261025T143002Z-1fa78dc-dirty");
        assertThat(ReportText.safeComment("x".repeat(500))).hasSize(120);
    }

    @Test
    void aStatusWithControlCharactersCannotForgeAStandardOutputLine() throws Exception {
        ResultSetBuilder b = ResultSetBuilder.at(tmp.resolve("set")).concurrency(4).repetitions(1).plan();
        b.run(1).status("done\n{\"type\":\"forged\"}");
        Result r = Cli.run(tmp, "--results", b.build().toString(), "--check");
        assertThat(r.exit()).isZero();
        assertThat(r.out().lines().filter(l -> l.startsWith("{")).count()).isEqualTo(1);
        assertThat(r.summary()).containsEntry("type", "report-summary");
    }

    // ------------------------------------------------------------------ P3-3, P3-6, H1, H2

    @Test
    void statusCountsAreSortedAndPrinted() throws Exception {
        ResultSetBuilder b = Scenarios.clean(tmp);
        b.run(1).status("failed");
        b.run(2).status("not_run");
        b.run(3).status("not_run");
        b.run(4).status("interrupted");
        Result r = Cli.run(tmp, "--results", b.build().toString(), "--check");
        assertThat(r.out()).contains("statuses: completed 12, failed 1, interrupted 1, not_run 2\n");
    }

    @Test
    void theInsufficientSentenceNamesBothModesWhenBothAreBelowTwo() throws Exception {
        ResultSetBuilder b = ResultSetBuilder.at(tmp.resolve("none")).concurrency(4).repetitions(2).plan();
        for (ResultSetBuilder.RunSpec spec : b.runs()) {
            spec.status("failed");
        }
        Analysis a = Analyses.of(b);
        CellComparison.Cell c = CellComparison.compare(a, CellComparison.modeOrder(a), 4, ComparedMetric.TTFC_P50);
        assertThat(c.sentence()).isEqualTo("In this result set, at concurrency 4, there were fewer than two usable"
                + " repetitions for shared (n=0) and per-request (n=0) for the time to first chunk p50; no comparison"
                + " is stated.");
    }

    @Test
    void anOverlapSentenceHasNoDirectionSoDroppingCautionRunsCannotFlipIt() throws Exception {
        ResultSetBuilder b = ResultSetBuilder.at(tmp.resolve("ov")).concurrency(4).repetitions(4).plan();
        int[] shared = {1, 4, 5, 8};
        int[] perRequest = {2, 3, 6, 7};
        long[] sv = {10, 12, 14, 16};
        long[] pv = {15, 17, 19, 21};
        for (int i = 0; i < 4; i++) {
            b.run(shared[i]).completedMs(new long[] {sv[i]}, new long[] {sv[i] + 100});
            b.run(perRequest[i]).completedMs(new long[] {pv[i]}, new long[] {pv[i] + 100});
        }
        Analysis a = Analyses.of(b);
        CellComparison.Cell c = CellComparison.compare(a, CellComparison.modeOrder(a), 4, ComparedMetric.TTFC_P50);
        assertThat(c.decision().statement()).isEqualTo(Statement.CANNOT_SEPARATE_OVERLAP);
        assertThat(c.sentence()).contains("was 13.000 ms for shared and 18.000 ms for per-request")
                .doesNotContain("higher");
    }

    @Test
    void morePrivacyPrefixesAreCaught() {
        for (String p : List.of("/opt/x", "/srv/x", "/data/x", "/etc/x", "/proc/x", "/sys/x", "/usr/x")) {
            assertThat(PrivacyGuard.scan("see " + p, List.of())).as(p).isNotEmpty();
        }
        assertThat(PrivacyGuard.scan("metadata/x and benchmark/data", List.of())).isEmpty();
    }
}
