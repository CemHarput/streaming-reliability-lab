package io.github.cemharput.streamlab.bench;

import static org.assertj.core.api.Assertions.assertThat;

import io.github.cemharput.streamlab.bench.PlanBuilder.PlannedRun;
import io.github.cemharput.streamlab.bench.json.Json;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.Timeout.ThreadMode;
import org.junit.jupiter.api.io.TempDir;

@Timeout(value = 30, unit = TimeUnit.SECONDS, threadMode = ThreadMode.SEPARATE_THREAD)
@SuppressWarnings("unchecked")
class ManifestTest {

    private static final PlannedRun RUN = new PlannedRun(1, "001-c4-shared-r1", 4, ConnectionMode.SHARED, 1, "AB");

    @TempDir
    Path dir;

    private ManifestStore store() {
        Map<String, Object> root = Manifest.newRoot("id", "dev", "2026-01-01T00:00:00Z");
        @SuppressWarnings("unchecked")
        List<Object> runs = (List<Object>) root.get("runs");
        runs.add(Manifest.newRun(RUN));
        Map<String, Object> second = Manifest.newRun(new PlannedRun(2, "002-c4-per-request-r1", 4,
                ConnectionMode.PER_REQUEST, 1, "AB"));
        runs.add(second);
        return new ManifestStore(dir.resolve("manifest.json"), root, () -> Instant.parse("2026-01-01T01:00:00Z"));
    }

    @Test
    void topLevelAndRunKeySetsArePinned() {
        Map<String, Object> root = Manifest.newRoot("id", "dev", "t");
        assertThat(root.keySet()).containsExactly("schemaVersion", "protocolVersion", "runId", "tool", "toolVersion",
                "status", "createdAt", "finishedAt", "note", "preset", "commandLine", "operatorNote", "operatorChecklist",
                "environment", "artifacts", "parameters", "runs", "flags");
        assertThat(root.keySet()).containsExactlyElementsOf(Manifest.TOP_LEVEL_KEYS);
        assertThat(root).containsEntry("schemaVersion", 1).containsEntry("protocolVersion", 1)
                .containsEntry("tool", "streamlab-bench")
                .containsEntry("note", "Raw data only. No analysis, no performance claim.")
                .containsEntry("status", "running");
        Map<String, Object> run = Manifest.newRun(RUN);
        assertThat(run.keySet()).containsExactly("index", "id", "dir", "mode", "concurrency", "repetition",
                "orderLabel", "status", "startedAt", "endedAt", "failure", "processes", "timeWaitBefore",
                "cpuLoadBefore", "warmup", "measured", "snapshots", "portsFreed", "flags");
        assertThat(run.keySet()).containsExactlyElementsOf(Manifest.RUN_KEYS);
        assertThat(run).containsEntry("status", "planned").containsEntry("dir", "runs/001-c4-shared-r1")
                .containsEntry("orderLabel", "AB");
    }

    @Test
    void writesAtomicallyAndLeavesNoTemporaryFile() throws IOException {
        ManifestStore store = store();
        store.write();
        store.update(root -> root.put("preset", "smoke"));
        assertThat(Json.parseObject(Files.readString(dir.resolve("manifest.json")))).containsEntry("preset", "smoke");
        try (Stream<Path> files = Files.list(dir)) {
            assertThat(files.map(p -> p.getFileName().toString()).toList()).containsExactly("manifest.json");
        }
    }

    @Test
    void interruptIsMonotoneAndOnlyAffectsAUnfinishedManifest() throws IOException {
        ManifestStore store = store();
        store.update(root -> {
            @SuppressWarnings("unchecked")
            Map<String, Object> first = (Map<String, Object>) ((List<Object>) root.get("runs")).get(0);
            first.put("status", "running");
        });
        store.markInterrupted();
        Map<String, Object> m = Json.parseObject(Files.readString(dir.resolve("manifest.json")));
        assertThat(m).containsEntry("status", "interrupted");
        assertThat((List<Object>) m.get("flags")).containsExactly(Flags.INTERRUPTED);
        List<?> runs = (List<?>) m.get("runs");
        assertThat(((Map<?, ?>) runs.get(0)).get("status")).isEqualTo("interrupted");
        assertThat(((Map<?, ?>) runs.get(1)).get("status")).isEqualTo("not_run");

        store.markInterrupted(); // second call changes nothing
        assertThat(Json.parseObject(store.json()).get("flags")).isEqualTo(new ArrayList<>(List.of(Flags.INTERRUPTED)));
    }

    @Test
    void aTransientMoveFailureIsRetriedWithoutRealSleeping() throws IOException {
        java.util.concurrent.atomic.AtomicInteger calls = new java.util.concurrent.atomic.AtomicInteger();
        io.github.cemharput.streamlab.bench.testsupport.FakeTicker ticker =
                new io.github.cemharput.streamlab.bench.testsupport.FakeTicker();
        Map<String, Object> root = Manifest.newRoot("id", "dev", "t");
        ManifestStore store = new ManifestStore(dir.resolve("m.json"), root, Instant::now, ticker, (from, to) -> {
            if (calls.incrementAndGet() < 4) {
                throw new java.nio.file.AccessDeniedException(to.toString());
            }
            Files.move(from, to, java.nio.file.StandardCopyOption.REPLACE_EXISTING);
        });
        store.write();
        assertThat(calls.get()).isEqualTo(4);
        assertThat(ticker.sleepCount()).isEqualTo(3);
        assertThat(Files.readString(dir.resolve("m.json"))).contains("\"runId\": \"id\"");
    }

    @Test
    void aPersistentMoveFailureGivesUpAfterTheBoundedNumberOfAttempts() {
        java.util.concurrent.atomic.AtomicInteger calls = new java.util.concurrent.atomic.AtomicInteger();
        ManifestStore store = new ManifestStore(dir.resolve("m.json"), Manifest.newRoot("id", "dev", "t"), Instant::now,
                new io.github.cemharput.streamlab.bench.testsupport.FakeTicker(), (from, to) -> {
                    calls.incrementAndGet();
                    throw new java.nio.file.AccessDeniedException(to.toString());
                });
        org.assertj.core.api.Assertions.assertThatThrownBy(store::write)
                .isInstanceOf(java.io.UncheckedIOException.class);
        assertThat(calls.get()).isEqualTo(ManifestStore.MOVE_ATTEMPTS);
    }

    @Test
    void theStoreIsDirtyAfterAFailedWriteAndCleanAfterASuccessfulOne() throws IOException {
        java.util.concurrent.atomic.AtomicBoolean fail = new java.util.concurrent.atomic.AtomicBoolean(true);
        ManifestStore store = new ManifestStore(dir.resolve("d.json"), Manifest.newRoot("id", "dev", "t"), Instant::now,
                new io.github.cemharput.streamlab.bench.testsupport.FakeTicker(), (from, to) -> {
                    if (fail.get()) {
                        throw new java.nio.file.AccessDeniedException(to.toString());
                    }
                    Files.move(from, to, java.nio.file.StandardCopyOption.REPLACE_EXISTING);
                });
        org.assertj.core.api.Assertions.assertThatThrownBy(store::write).isInstanceOf(java.io.UncheckedIOException.class);
        assertThat(store.isDirty()).isTrue();
        fail.set(false);
        store.write();
        assertThat(store.isDirty()).isFalse();
    }

    @Test
    void theDirectWriteReplacesAnExistingFileAtomicallyAndLeavesNoTemporaryFile() throws IOException {
        Path file = dir.resolve("e.json");
        Files.writeString(file, "old content");
        ManifestStore store = new ManifestStore(file, Manifest.newRoot("id", "dev", "t"), Instant::now);
        assertThat(store.writeDirect(root -> root.put("preset", "x"))).isEqualTo(ManifestStore.DirectResult.WRITTEN);
        assertThat(Json.parseObject(Files.readString(file))).containsEntry("preset", "x");
        assertThat(store.directTmp()).doesNotExist();
    }

    @Test
    void aFailedDirectWriteKeepsTheOldFileAndTheCompleteDirectTmp() throws IOException {
        Path file = dir.resolve("f.json");
        Files.writeString(file, "old content");
        ManifestStore store = new ManifestStore(file, Manifest.newRoot("id", "dev", "t"), Instant::now)
                .withDirectMover((from, to) -> {
                    throw new java.nio.file.AccessDeniedException(to.toString());
                });
        assertThat(store.writeDirect(root -> { })).isEqualTo(ManifestStore.DirectResult.TMP_KEPT);
        assertThat(Files.readString(file)).isEqualTo("old content");
        assertThat(Json.parseObject(Files.readString(store.directTmp()))).containsEntry("runId", "id");
    }

    @Test
    void ifEvenTheDirectTmpCannotBeWrittenNoPartialCopyRemains() throws IOException {
        Path file = dir.resolve("g.json");
        Files.writeString(file, "old content");
        ManifestStore store = new ManifestStore(file, Manifest.newRoot("id", "dev", "t"), Instant::now)
                .withDirectWriter((target, content) -> {
                    Files.writeString(target, content.substring(0, 10)); // a partial write ...
                    throw new IOException("No space left on device"); // ... that then fails
                });
        assertThat(store.writeDirect(root -> { })).isEqualTo(ManifestStore.DirectResult.NOTHING);
        assertThat(Files.readString(file)).isEqualTo("old content");
        assertThat(store.directTmp()).doesNotExist();
    }

    @Test
    void aFinishedManifestIsNeverTurnedIntoInterrupted() {
        ManifestStore store = store();
        store.update(root -> root.put("status", "completed"));
        store.markInterrupted();
        assertThat(store.status()).isEqualTo("completed");
    }

    @Test
    void updateIfRunningIsANoOpAfterTheInterrupt() {
        ManifestStore store = store();
        store.markInterrupted();
        store.updateIfRunning(root -> root.put("preset", "late"));
        assertThat(Json.parseObject(store.json())).containsEntry("preset", null);
    }
}
