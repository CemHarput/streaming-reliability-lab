package io.github.cemharput.streamlab.report;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.github.cemharput.streamlab.bench.json.Json;
import io.github.cemharput.streamlab.report.Checks.Finding;
import io.github.cemharput.streamlab.report.ResultSetReader.ResultSet;
import io.github.cemharput.streamlab.report.ResultSetReader.RunData;
import io.github.cemharput.streamlab.report.testsupport.ResultSetBuilder;
import io.github.cemharput.streamlab.report.testsupport.ResultSetBuilder.Req;
import java.io.IOException;
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

@Timeout(value = 30, unit = TimeUnit.SECONDS, threadMode = ThreadMode.SEPARATE_THREAD)
class ResultSetReaderTest {

    @TempDir
    Path tmp;

    private ResultSetBuilder builder() {
        return ResultSetBuilder.at(tmp.resolve("set")).concurrency(4).repetitions(2).plan();
    }

    private static List<String> codes(RunData d) {
        return d.findings().stream().map(Finding::code).toList();
    }

    @Test
    void aCleanSetIsReadCompletely() throws Exception {
        ResultSet set = ResultSetReader.read(builder().build());
        assertThat(set.manifest().runId()).isEqualTo("20260101T000000Z-abc1234-fixture");
        assertThat(set.runs()).hasSize(4);
        RunData first = set.runs().get(0);
        assertThat(first.findings()).isEmpty();
        assertThat(first.measured().requestLines()).isEqualTo(10);
        assertThat(first.summary().recordsWritten()).isEqualTo(10);
        assertThat(first.pre()).isNotNull();
        assertThat(first.post()).isNotNull();
        assertThat(first.listed()).extracting(ResultSetReader.ListedFile::name).contains("warmup.jsonl",
                "warmup-summary.json", "snapshot-ready.json", "gateway.log", "gateway-gc.log", "simulator-gc.log",
                "measured-loadgen.log");
        assertThat(first.listed()).extracting(ResultSetReader.ListedFile::name)
                .doesNotContain("measured.jsonl", "snapshot-pre.json");
    }

    @Test
    void aDirectoryWithoutManifestAsksForTheRunSetDirectory() throws IOException {
        Path empty = Files.createDirectory(tmp.resolve("empty"));
        assertThatThrownBy(() -> ResultSetReader.read(empty)).isInstanceOf(InputException.class)
                .hasMessageContaining("pass the run-set directory");
    }

    @Test
    void aManifestThatIsADirectoryIsNotAManifest() throws IOException {
        Path dir = Files.createDirectories(tmp.resolve("d").resolve("manifest.json"));
        assertThatThrownBy(() -> ResultSetReader.read(dir.getParent())).isInstanceOf(InputException.class);
    }

    @Test
    void invalidJsonAndNonObjectsAreInputErrors() throws IOException {
        for (String text : new String[] {"{not json", "[1,2]", "\"text\"", "", "{\"a\":1} trailing"}) {
            Path dir = Files.createDirectories(tmp.resolve("bad" + Math.abs(text.hashCode())));
            Files.writeString(dir.resolve("manifest.json"), text);
            assertThatThrownBy(() -> ResultSetReader.read(dir)).as(text).isInstanceOf(InputException.class);
        }
    }

    @Test
    void invalidUtf8ManifestIsAnInputError() throws IOException {
        Path dir = Files.createDirectories(tmp.resolve("utf"));
        Files.write(dir.resolve("manifest.json"), new byte[] {'{', '"', (byte) 0xC3, (byte) 0x28, '"', '}'});
        assertThatThrownBy(() -> ResultSetReader.read(dir)).isInstanceOf(InputException.class);
    }

    @Test
    void unsupportedVersionsToolAndMissingKeysAreInputErrors() throws Exception {
        assertThatThrownBy(() -> ResultSetReader.read(builder().schemaVersion(2).build()))
                .isInstanceOf(InputException.class).hasMessageContaining("schemaVersion 2");
        assertThatThrownBy(() -> ResultSetReader.read(
                ResultSetBuilder.at(tmp.resolve("p")).protocolVersion(2).plan().build()))
                .isInstanceOf(InputException.class).hasMessageContaining("protocolVersion 2");
        assertThatThrownBy(() -> ResultSetReader.read(
                ResultSetBuilder.at(tmp.resolve("t")).tool("other-tool").plan().build()))
                .isInstanceOf(InputException.class).hasMessageContaining("tool");
        for (String key : new String[] {"schemaVersion", "protocolVersion", "tool", "runId", "status"}) {
            Path dir = tmp.resolve("missing-" + key);
            ResultSetBuilder b = ResultSetBuilder.at(dir).removeRootKey(key).plan();
            Path root = b.build();
            assertThatThrownBy(() -> ResultSetReader.read(root)).as(key).isInstanceOf(InputException.class);
        }
    }

    @Test
    void wrongTypesOfRequiredKeysAreInputErrors() throws Exception {
        assertThatThrownBy(() -> ResultSetReader.read(
                ResultSetBuilder.at(tmp.resolve("a")).rootKey("runId", 5L).plan().build()))
                .isInstanceOf(InputException.class).hasMessageContaining("runId");
        assertThatThrownBy(() -> ResultSetReader.read(
                ResultSetBuilder.at(tmp.resolve("b")).rootKey("schemaVersion", "1").plan().build()))
                .isInstanceOf(InputException.class);
        assertThatThrownBy(() -> ResultSetReader.read(
                ResultSetBuilder.at(tmp.resolve("c")).rootKey("runs", "nope").plan().build()))
                .isInstanceOf(InputException.class).hasMessageContaining("runs");
        assertThatThrownBy(() -> ResultSetReader.read(
                ResultSetBuilder.at(tmp.resolve("d")).rootKey("runs", new ArrayList<Object>(List.of("x"))).plan().build()))
                .isInstanceOf(InputException.class).hasMessageContaining("runs[1]");
    }

    @Test
    void aRunWithAMissingRequiredKeyIsAnInputErrorForTheWholeSet() throws Exception {
        for (String key : new String[] {"index", "id", "dir", "mode", "concurrency", "repetition", "orderLabel",
            "status"}) {
            Path dir = tmp.resolve("run-" + key);
            Path root = ResultSetBuilder.at(dir).plan().build();
            Map<String, Object> m = Json.parseObject(Files.readString(root.resolve("manifest.json")));
            ((Map<?, ?>) ((List<?>) m.get("runs")).get(0)).remove(key);
            Files.writeString(root.resolve("manifest.json"), Json.write(m));
            assertThatThrownBy(() -> ResultSetReader.read(root)).as(key).isInstanceOf(InputException.class)
                    .hasMessageContaining(key);
        }
    }

    @Test
    void aMissingRunsKeyIsAnEmptyListAsInAMinimalFailedManifest() throws Exception {
        Path dir = Files.createDirectories(tmp.resolve("minimal"));
        Map<String, Object> m = new LinkedHashMap<>();
        m.put("schemaVersion", 1L);
        m.put("protocolVersion", 1L);
        m.put("runId", "r");
        m.put("tool", "streamlab-bench");
        m.put("status", "failed");
        m.put("harnessFailure", "boom");
        Files.writeString(dir.resolve("manifest.json"), Json.write(m));
        ResultSet set = ResultSetReader.read(dir);
        assertThat(set.runs()).isEmpty();
        assertThat(set.manifest().status()).isEqualTo("failed");
        assertThat(set.manifest().hasHarnessFailure()).isTrue();
    }

    @Test
    void duplicateRunIdsAreRefused() throws Exception {
        Path root = ResultSetBuilder.at(tmp.resolve("dup")).plan().build();
        Map<String, Object> m = Json.parseObject(Files.readString(root.resolve("manifest.json")));
        List<?> runs = (List<?>) m.get("runs");
        ((Map<String, Object>) runs.get(1)).put("id", ((Map<?, ?>) runs.get(0)).get("id"));
        Files.writeString(root.resolve("manifest.json"), Json.write(m));
        assertThatThrownBy(() -> ResultSetReader.read(root)).isInstanceOf(InputException.class)
                .hasMessageContaining("duplicate");
    }

    @Test
    void invalidRunDirectoriesAreExcludedWithoutReadingAnything() throws Exception {
        for (String dir : new String[] {"../x", "C:\\x", "/etc", "runs/../..", "runs", "other/001-c4-shared-r1"}) {
            Path root = ResultSetBuilder.at(tmp.resolve("inv" + Math.abs(dir.hashCode()))).concurrency(4)
                    .repetitions(2).plan().build();
            Map<String, Object> m = Json.parseObject(Files.readString(root.resolve("manifest.json")));
            ((Map<String, Object>) ((List<?>) m.get("runs")).get(0)).put("dir", dir);
            Files.writeString(root.resolve("manifest.json"), Json.write(m));
            ResultSet set = ResultSetReader.read(root);
            RunData first = set.runs().get(0);
            assertThat(codes(first)).as(dir).containsExactly(Checks.RUN_DIR_INVALID);
            assertThat(first.measured()).isNull();
            assertThat(first.listed()).isEmpty();
            assertThat(set.runs().get(1).findings()).isEmpty();
        }
    }

    @Test
    void aBackslashRunDirFromAWindowsSetIsAccepted() throws Exception {
        Path root = builder().build();
        Map<String, Object> m = Json.parseObject(Files.readString(root.resolve("manifest.json")));
        ((Map<String, Object>) ((List<?>) m.get("runs")).get(0)).put("dir", "runs\\001-c4-shared-r1");
        Files.writeString(root.resolve("manifest.json"), Json.write(m));
        assertThat(ResultSetReader.read(root).runs().get(0).findings()).isEmpty();
    }

    @Test
    void aMissingRunDirectoryIsFlaggedForACompletedRun() throws Exception {
        Path root = builder().build();
        deleteTree(root.resolve("runs").resolve("001-c4-shared-r1"));
        RunData first = ResultSetReader.read(root).runs().get(0);
        assertThat(codes(first)).containsExactly(Checks.RUN_DIR_INVALID);
    }

    @Test
    void missingFilesOfACompletedRunAreFindingsButNotForAFailedRun() throws Exception {
        ResultSetBuilder b = builder();
        b.run(1).writeJsonl = false;
        b.run(1).writeSnapshots = false;
        b.run(2).status("failed").writeJsonl = false;
        b.run(2).writeSnapshots = false;
        ResultSet set = ResultSetReader.read(b.build());
        assertThat(codes(set.runs().get(0))).containsExactly(Checks.MEASURED_JSONL_MISSING,
                Checks.SNAPSHOT_UNAVAILABLE, Checks.SNAPSHOT_UNAVAILABLE);
        assertThat(set.runs().get(1).findings()).isEmpty();
    }

    @Test
    void plannedAndNotRunRunsAreNeverLookedUp() throws Exception {
        ResultSetBuilder b = builder();
        b.run(3).status("planned");
        b.run(4).status("not_run");
        ResultSet set = ResultSetReader.read(b.build());
        assertThat(set.runs().get(2).findings()).isEmpty();
        assertThat(set.runs().get(2).measured()).isNull();
        assertThat(set.runs().get(3).listed()).isEmpty();
    }

    @Test
    void anUnsupportedJsonlSchemaAndAnUnsupportedSummarySchemaAreFindingsOfThatRunOnly() throws Exception {
        ResultSetBuilder b = builder();
        b.run(1).headerOverrides.put("schemaVersion", 2L);
        b.run(2).summaryOverrides.put("schemaVersion", 2L);
        ResultSet set = ResultSetReader.read(b.build());
        assertThat(codes(set.runs().get(0))).containsExactly(Checks.JSONL_SCHEMA_UNSUPPORTED);
        assertThat(codes(set.runs().get(1))).containsExactly(Checks.SUMMARY_SCHEMA_UNSUPPORTED);
        assertThat(set.runs().get(1).summary()).isNull();
        assertThat(set.runs().get(2).findings()).isEmpty();
    }

    @Test
    void malformedLinesAndStructureProblemsBecomeFindingsWithLineNumbersOnly() throws Exception {
        ResultSetBuilder b = builder();
        b.run(1).extraJsonlLines.add("this is not json and contains fixtureuser");
        b.run(2).writeFooter = false;
        ResultSet set = ResultSetReader.read(b.build());
        Finding malformed = set.runs().get(0).findings().get(0);
        assertThat(malformed.code()).isEqualTo(Checks.JSONL_MALFORMED_LINE);
        assertThat(malformed.detail()).contains("1 line(s) skipped").contains("12");
        assertThat(malformed.detail()).doesNotContain("fixtureuser").doesNotContain("not json");
        // the footer sits after the extra line in run 1; run 2 has none
        assertThat(set.runs().get(1).findings()).extracting(Finding::code).containsExactly(Checks.JSONL_STRUCTURE);
    }

    @Test
    void aDifferentFileNameInTheManifestIsFlaggedAndTheStandardNameIsRead() throws Exception {
        Path root = builder().build();
        Map<String, Object> m = Json.parseObject(Files.readString(root.resolve("manifest.json")));
        Map<String, Object> run = (Map<String, Object>) ((List<?>) m.get("runs")).get(0);
        ((Map<String, Object>) ((Map<?, ?>) ((Map<?, ?>) run.get("measured")).get("loadgen")).get("files"))
                .put("jsonl", "../../elsewhere.jsonl");
        Files.writeString(root.resolve("manifest.json"), Json.write(m));
        RunData first = ResultSetReader.read(root).runs().get(0);
        assertThat(codes(first)).containsExactly(Checks.FILE_NAME_UNEXPECTED);
        assertThat(first.measured().requestLines()).isEqualTo(10);
        assertThat(first.findings().get(0).detail()).doesNotContain("elsewhere");
    }

    @Test
    void anOversizedSummaryIsIgnoredAndAnOversizedSnapshotIsUnavailable() throws Exception {
        Path root = builder().build();
        Path run = root.resolve("runs").resolve("001-c4-shared-r1");
        Files.writeString(run.resolve("measured-summary.json"),
                "{\"type\":\"summary\",\"schemaVersion\":1,\"pad\":\"" + "x".repeat(1_100_000) + "\"}");
        Files.writeString(run.resolve("snapshot-pre.json"),
                "{\"gateway\":[],\"pad\":\"" + "x".repeat((int) SnapshotReader.MAX_BYTES + 10) + "\"}");
        RunData first = ResultSetReader.read(root).runs().get(0);
        assertThat(first.summary()).isNull();
        assertThat(codes(first)).containsExactly(Checks.SUMMARY_UNREADABLE, Checks.SNAPSHOT_UNAVAILABLE);
        assertThat(first.pre()).isNull();
    }

    @Test
    void theSetIsNotModifiedByReading() throws Exception {
        Path root = builder().build();
        Map<String, String> before = io.github.cemharput.streamlab.report.testsupport.Cli.hashes(root);
        ResultSetReader.read(root);
        assertThat(io.github.cemharput.streamlab.report.testsupport.Cli.hashes(root)).isEqualTo(before);
    }

    @Test
    void crlfFilesGiveTheSameFacts() throws Exception {
        ResultSet lf = ResultSetReader.read(
                ResultSetBuilder.at(tmp.resolve("lf")).concurrency(4).repetitions(2).plan().build());
        ResultSet crlf = ResultSetReader.read(
                ResultSetBuilder.at(tmp.resolve("crlf")).concurrency(4).repetitions(2).crlf().plan().build());
        for (int i = 0; i < 4; i++) {
            assertThat(crlf.runs().get(i).measured()).isEqualTo(lf.runs().get(i).measured());
            assertThat(crlf.runs().get(i).findings()).isEmpty();
        }
    }

    @Test
    void optionalFieldsMayBeMissingButAreListed() throws Exception {
        ResultSetBuilder b = builder().omitPlan();
        Path root = b.build();
        Map<String, Object> m = Json.parseObject(Files.readString(root.resolve("manifest.json")));
        m.put("environment", null);
        m.put("operatorChecklist", null);
        Files.writeString(root.resolve("manifest.json"), Json.write(m));
        ResultSet set = ResultSetReader.read(root);
        assertThat(set.manifest().missingFields()).contains("environment.os", "operatorChecklist.source",
                "parameters.plan", "environment.powerPlan");
        assertThat(set.manifest().runs().get(0).missingFields()).isEmpty();
    }

    @Test
    void reqRecordsAreUsedByTheFixtures() {
        assertThat(Req.completed(1, 1, 2, 3).ttfc()).isEqualTo(2 * ResultSetBuilder.MS);
    }

    private static void deleteTree(Path p) throws IOException {
        try (var s = Files.walk(p)) {
            for (Path x : (Iterable<Path>) s.sorted(java.util.Comparator.reverseOrder())::iterator) {
                Files.delete(x);
            }
        }
    }
}
