package io.github.cemharput.streamlab.report;

import static org.assertj.core.api.Assertions.assertThat;

import io.github.cemharput.streamlab.bench.Flags;
import io.github.cemharput.streamlab.report.testsupport.Cli;
import io.github.cemharput.streamlab.report.testsupport.Cli.Result;
import io.github.cemharput.streamlab.report.testsupport.ResultSetBuilder;
import io.github.cemharput.streamlab.report.testsupport.Scenarios;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.Timeout.ThreadMode;
import org.junit.jupiter.api.io.TempDir;

@Timeout(value = 60, unit = TimeUnit.SECONDS, threadMode = ThreadMode.SEPARATE_THREAD)
class ReportMainTest {

    @TempDir
    Path tmp;

    Path out;

    @BeforeEach
    void outDir() throws IOException {
        out = Files.createDirectory(tmp.resolve("out"));
    }

    private Path cleanSet() throws IOException {
        return Scenarios.clean(tmp).build();
    }

    private static long fileCount(Path dir) throws IOException {
        try (Stream<Path> s = Files.walk(dir)) {
            return s.filter(Files::isRegularFile).count();
        }
    }

    // ------------------------------------------------------------------ exit codes and help

    @Test
    void helpExitsZeroAndPrintsTheUsage() {
        Result r = Cli.run(tmp, "--help");
        assertThat(r.exit()).isZero();
        assertThat(r.out()).isEqualTo(ReportOptionsParser.USAGE);
        assertThat(Cli.run(tmp, "-h").exit()).isZero();
    }

    @Test
    void usageErrorsExitTwoAndWriteNothing() throws IOException {
        for (String[] args : new String[][] {{}, {"--bogus"}, {"--check"}, {"--results", "nope", "--check"},
            {"--placeholder"}, {"--results", cleanSet().toString(), "--check", "--out", out.toString()}}) {
            Result r = Cli.run(tmp, args);
            assertThat(r.exit()).as(String.join(" ", args)).isEqualTo(2);
            assertThat(r.err()).startsWith("report: ");
            assertThat(r.out()).isEmpty();
        }
        assertThat(fileCount(out)).isZero();
    }

    @Test
    void aDirectoryWithoutManifestExitsTwoAndAsksForTheRunSetDirectory() throws IOException {
        Path empty = Files.createDirectory(tmp.resolve("empty"));
        Result r = Cli.run(tmp, "--results", empty.toString(), "--out", out.toString());
        assertThat(r.exit()).isEqualTo(2);
        assertThat(r.err()).contains("pass the run-set directory");
        assertThat(fileCount(out)).isZero();
    }

    @Test
    void unsupportedVersionsAndForeignToolsExitTwoAndWriteNothing() throws Exception {
        List<ResultSetBuilder> sets = List.of(
                ResultSetBuilder.at(tmp.resolve("s2")).schemaVersion(2).plan(),
                ResultSetBuilder.at(tmp.resolve("p2")).protocolVersion(2).plan(),
                ResultSetBuilder.at(tmp.resolve("tool")).tool("something-else").plan(),
                ResultSetBuilder.at(tmp.resolve("nokey")).removeRootKey("runId").plan());
        for (ResultSetBuilder b : sets) {
            Result r = Cli.run(tmp, "--results", b.build().toString(), "--out", out.toString());
            assertThat(r.exit()).as(b.root().toString()).isEqualTo(2);
            assertThat(r.err()).contains("invalid input");
            assertThat(r.out()).isEmpty();
        }
        assertThat(fileCount(out)).isZero();
        assertThat(Cli.run(tmp, "--results", sets.get(0).root().toString(), "--check").exit()).isEqualTo(2);
    }

    // ------------------------------------------------------------------ output

    @Test
    void aSuccessfulRunWritesTheFixedFileListAndPrintsTheSummary() throws Exception {
        Path set = Scenarios.mixed(tmp).build();
        Result r = Cli.run(tmp, "--results", set.toString(), "--out", out.toString());
        assertThat(r.exit()).isZero();
        assertThat(r.err()).isEmpty();
        for (String name : OutputWriter.ALL_FILES) {
            assertThat(out.resolve(name)).isRegularFile();
        }
        assertThat(fileCount(out)).isEqualTo(OutputWriter.ALL_FILES.size());
        List<String> lines = Cli.lines(r.out().stripTrailing());
        assertThat(lines.get(0)).isEqualTo("report: result set 20260101T000000Z-abc1234-fixture (status completed,"
                + " preset default, schema 1, protocol 1)");
        assertThat(lines.get(1)).isEqualTo("runs: 8 total, 3 usable, 3 caution, 2 excluded");
        assertThat(lines).contains("statuses: completed 7, failed 1");
        assertThat(lines).contains("excluded 005-c4-shared-r3: RUN:CPU_BUSY_BEFORE_RUN",
                "excluded 006-c4-per-request-r3: STATUS:STATUS_FAILED", "warm-up phases listed, not analysed: 8");
        assertThat(lines.get(lines.size() - 2)).startsWith("written: benchmark-report.md, benchmark/runs.csv");
        Map<String, Object> json = r.summary();
        assertThat(json).containsEntry("type", "report-summary").containsEntry("analysisVersion", 1L)
                .containsEntry("runId", "20260101T000000Z-abc1234-fixture").containsEntry("runs", 8L)
                .containsEntry("usable", 3L).containsEntry("caution", 3L).containsEntry("excluded", 2L);
        assertThat((List<?>) json.get("excludedRuns")).hasSize(2);
        assertThat((List<?>) json.get("written")).hasSize(8);
        assertThat((List<?>) json.get("unknownFlags")).isEmpty();
        assertThat(lines.get(lines.size() - 1)).startsWith("{\"type\":\"report-summary\"");
    }

    @Test
    void excludedRunsDoNotChangeTheExitCode() throws Exception {
        ResultSetBuilder b = Scenarios.clean(tmp);
        for (ResultSetBuilder.RunSpec spec : b.runs()) {
            spec.status("failed");
        }
        Result r = Cli.run(tmp, "--results", b.build().toString(), "--out", out.toString());
        assertThat(r.exit()).isZero();
        assertThat(r.summary()).containsEntry("excluded", 16L).containsEntry("usable", 0L);
    }

    @Test
    void unknownFlagsAreListedInTheSummary() throws Exception {
        ResultSetBuilder b = Scenarios.clean(tmp);
        b.run(1).measuredFlag("BRAND_NEW_FLAG");
        Result r = Cli.run(tmp, "--results", b.build().toString(), "--check");
        assertThat(r.exit()).isZero();
        assertThat(r.summary().get("unknownFlags").toString()).isEqualTo("[BRAND_NEW_FLAG]");
        assertThat(r.out()).contains("unknown flags: BRAND_NEW_FLAG");
    }

    @Test
    void checkWritesNothingAnywhere() throws Exception {
        Path set = cleanSet();
        Path work = Files.createDirectory(tmp.resolve("work"));
        Result r = Cli.run(work, "--results", set.toString(), "--check");
        assertThat(r.exit()).isZero();
        assertThat(r.out()).contains("check only: nothing written");
        assertThat((List<?>) r.summary().get("written")).isEmpty();
        assertThat(fileCount(work)).isZero();
        assertThat(fileCount(out)).isZero();
    }

    @Test
    void existingFilesAreNotReplacedWithoutOverwrite() throws Exception {
        Path set = cleanSet();
        assertThat(Cli.run(tmp, "--results", set.toString(), "--out", out.toString()).exit()).isZero();
        Map<String, String> before = Cli.hashes(out);
        Result again = Cli.run(tmp, "--results", set.toString(), "--out", out.toString());
        assertThat(again.exit()).isEqualTo(2);
        assertThat(again.err()).contains("--overwrite");
        assertThat(Cli.hashes(out)).isEqualTo(before);
        Result over = Cli.run(tmp, "--results", set.toString(), "--out", out.toString(), "--overwrite");
        assertThat(over.exit()).isZero();
        assertThat(Cli.hashes(out)).isEqualTo(before); // same input, byte-identical output
    }

    @Test
    void aSingleExistingFileOfTheListIsEnoughToRefuse() throws Exception {
        Path set = cleanSet();
        Files.createDirectories(out.resolve("benchmark"));
        Files.writeString(out.resolve("benchmark").resolve("cells.csv"), "keep me");
        Result r = Cli.run(tmp, "--results", set.toString(), "--out", out.toString());
        assertThat(r.exit()).isEqualTo(2);
        assertThat(Files.readString(out.resolve("benchmark").resolve("cells.csv"))).isEqualTo("keep me");
        assertThat(fileCount(out)).isEqualTo(1);
    }

    @Test
    void anOverwriteLeavesUnrelatedFilesAndDeletesNothing() throws Exception {
        Path set = cleanSet();
        Files.createDirectories(out.resolve("benchmark"));
        Files.writeString(out.resolve("notes.txt"), "mine");
        Files.writeString(out.resolve("benchmark").resolve("extra.csv"), "mine too");
        assertThat(Cli.run(tmp, "--results", set.toString(), "--out", out.toString()).exit()).isZero();
        assertThat(Files.readString(out.resolve("notes.txt"))).isEqualTo("mine");
        assertThat(Files.readString(out.resolve("benchmark").resolve("extra.csv"))).isEqualTo("mine too");
        try (Stream<Path> s = Files.walk(out)) {
            assertThat(s.filter(p -> p.getFileName().toString().endsWith(".tmp"))).isEmpty();
        }
    }

    @Test
    void outMustNotBeInsideResultsOrContainThem() throws Exception {
        Path set = cleanSet();
        Path inside = Files.createDirectory(set.resolve("report-out"));
        Result r = Cli.run(tmp, "--results", set.toString(), "--out", inside.toString());
        assertThat(r.exit()).isEqualTo(2);
        assertThat(r.err()).contains("--out must not be inside --results");
        assertThat(fileCount(inside)).isZero();

        Result same = Cli.run(tmp, "--results", set.toString(), "--out", set.toString());
        assertThat(same.exit()).isEqualTo(2);

        Result contains = Cli.run(tmp, "--results", set.toString(), "--out", tmp.toString());
        assertThat(contains.exit()).isEqualTo(2);
        assertThat(contains.err()).contains("must not contain it");
        assertThat(Files.exists(tmp.resolve("benchmark-report.md"))).isFalse();
    }

    @Test
    void aSmokeSetNeedsAllowSmokeToWriteButCanAlwaysBeChecked() throws Exception {
        Path set = ResultSetBuilder.at(tmp.resolve("smoke")).preset("smoke").setFlag(Flags.SMOKE_PRESET).plan().build();
        Result refused = Cli.run(tmp, "--results", set.toString(), "--out", out.toString());
        assertThat(refused.exit()).isEqualTo(2);
        assertThat(refused.err()).contains("--allow-smoke");
        assertThat(fileCount(out)).isZero();
        assertThat(Cli.run(tmp, "--results", set.toString(), "--check").exit()).isZero();
        Result allowed = Cli.run(tmp, "--results", set.toString(), "--out", out.toString(), "--allow-smoke");
        assertThat(allowed.exit()).isZero();
        assertThat(Files.readString(out.resolve("benchmark-report.md"))).contains("Smoke preset");
        assertThat(allowed.summary()).containsEntry("excluded", 4L);
    }

    @Test
    void anIncompleteSetIsSaidSoInTheIdentificationBlock() throws Exception {
        ResultSetBuilder b = Scenarios.clean(tmp).setStatus("interrupted").runId("interrupted-fixture");
        b.run(5).status("interrupted");
        for (int i = 6; i <= 16; i++) {
            b.run(i).status("not_run");
        }
        Result r = Cli.run(tmp, "--results", b.build().toString(), "--out", out.toString());
        assertThat(r.exit()).isZero();
        assertThat(Files.readString(out.resolve("benchmark-report.md"))).contains("**Set incomplete:")
                .contains("`interrupted`");
        assertThat(r.summary()).containsEntry("excluded", 12L);
    }

    // ------------------------------------------------------------------ placeholder

    @Test
    void placeholderWritesOnlyTheReportFileWithoutNumbers() throws Exception {
        Result r = Cli.run(tmp, "--placeholder", "--out", out.toString());
        assertThat(r.exit()).isZero();
        assertThat(fileCount(out)).isEqualTo(1);
        String text = Files.readString(out.resolve("benchmark-report.md"));
        assertThat(text).isEqualTo(ReportRenderer.placeholder()).contains("No result set analysed yet");
        assertThat(r.summary()).containsEntry("runId", null).containsEntry("runs", 0L);
        assertThat(r.summary().get("written").toString()).isEqualTo("[benchmark-report.md]");
        assertThat(ReportText.PLACEHOLDER_STATUS).doesNotContainPattern("\\d");
        assertThat(text).doesNotContain("|");
    }

    @Test
    void placeholderRefusesToOverwriteAndRefusesWhenDataFilesExist() throws Exception {
        assertThat(Cli.run(tmp, "--placeholder", "--out", out.toString()).exit()).isZero();
        assertThat(Cli.run(tmp, "--placeholder", "--out", out.toString()).exit()).isEqualTo(2);
        assertThat(Cli.run(tmp, "--placeholder", "--out", out.toString(), "--overwrite").exit()).isZero();
        Files.createDirectories(out.resolve("benchmark"));
        Files.writeString(out.resolve("benchmark").resolve("runs.csv"), "data");
        Result r = Cli.run(tmp, "--placeholder", "--out", out.toString(), "--overwrite");
        assertThat(r.exit()).isEqualTo(2);
        assertThat(r.err()).contains("benchmark/runs.csv");
    }

    // ------------------------------------------------------------------ guards and failures

    @Test
    void aForbiddenPhraseInTheDataStopsTheToolWithExitOneAndWritesNothing() throws Exception {
        Path set = ResultSetBuilder.at(tmp.resolve("loud")).preset("best-effort").plan().build();
        Result r = Cli.run(tmp, "--results", set.toString(), "--out", out.toString());
        assertThat(r.exit()).isEqualTo(1);
        assertThat(r.err()).contains("internal guard violation").contains("forbidden phrase 'best'");
        assertThat(fileCount(out)).isZero();
        assertThat(r.out()).isEmpty();
    }

    @Test
    void anAbsolutePathInTheDataStopsTheToolWithExitOne() throws Exception {
        Path set = ResultSetBuilder.at(tmp.resolve("leaky")).runId("run-in-/home/someone/x").plan().build();
        Result r = Cli.run(tmp, "--results", set.toString(), "--out", out.toString());
        assertThat(r.exit()).isEqualTo(1);
        assertThat(r.err()).contains("internal guard violation");
        assertThat(r.err()).doesNotContain("someone");
        assertThat(fileCount(out)).isZero();
    }

    @Test
    void anIoErrorWhileWritingExitsOne() throws Exception {
        Path set = cleanSet();
        Files.writeString(out.resolve("benchmark"), "a file where the directory should be");
        Result r = Cli.run(tmp, "--results", set.toString(), "--out", out.toString());
        assertThat(r.exit()).isEqualTo(1);
        assertThat(r.err()).contains("unexpected error");
    }

    @Test
    void outputIsUtf8WithLfLineEndsOnly() throws Exception {
        Path set = cleanSet();
        Cli.run(tmp, "--results", set.toString(), "--out", out.toString());
        for (String name : OutputWriter.ALL_FILES) {
            byte[] bytes = Files.readAllBytes(out.resolve(name));
            String text = new String(bytes, StandardCharsets.UTF_8);
            assertThat(text).as(name).doesNotContain("\r").endsWith("\n");
            assertThat(java.util.Arrays.equals(text.getBytes(StandardCharsets.UTF_8), bytes)).as(name).isTrue();
        }
    }
}
