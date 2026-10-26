package io.github.cemharput.streamlab.report;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assumptions.assumeThat;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.Timeout.ThreadMode;
import org.junit.jupiter.api.io.TempDir;

@Timeout(value = 30, unit = TimeUnit.SECONDS, threadMode = ThreadMode.SEPARATE_THREAD)
class SafePathsTest {

    @TempDir
    Path tmp;

    @Test
    void validRunDirsAcceptBothSeparators() {
        String id = "001-c4-shared-r1";
        assertThat(SafePaths.validRunDir("runs/" + id, id)).isTrue();
        assertThat(SafePaths.validRunDir("runs\\" + id, id)).isTrue();
        assertThat(SafePaths.validRunDir("runs/001-c50-per-request-r12", "001-c50-per-request-r12")).isTrue();
    }

    @Test
    void everythingElseIsRefused() {
        String id = "001-c4-shared-r1";
        assertThat(SafePaths.validRunDir("../x", id)).isFalse();
        assertThat(SafePaths.validRunDir("runs/../x", id)).isFalse();
        assertThat(SafePaths.validRunDir("C:\\x", id)).isFalse();
        assertThat(SafePaths.validRunDir("/etc", id)).isFalse();
        assertThat(SafePaths.validRunDir("runs/" + id + "/", id)).isFalse();
        assertThat(SafePaths.validRunDir("runs/other", id)).isFalse();
        assertThat(SafePaths.validRunDir("other/" + id, id)).isFalse();
        assertThat(SafePaths.validRunDir(null, id)).isFalse();
        assertThat(SafePaths.validRunDir("runs/x", "x")).isFalse(); // id shape
        assertThat(SafePaths.validRunDir("runs/..", "..")).isFalse();
    }

    @Test
    void realDirectoriesAndFilesAreFoundInsideTheRoot() throws IOException {
        Path root = tmp.toRealPath();
        Path run = Files.createDirectories(root.resolve("runs").resolve("001-c4-shared-r1"));
        Files.writeString(run.resolve("measured.jsonl"), "x");
        assertThat(SafePaths.runDir(root, "001-c4-shared-r1")).contains(run.toRealPath());
        assertThat(SafePaths.file(root, run.toRealPath(), "measured.jsonl")).isPresent();
        assertThat(SafePaths.file(root, run.toRealPath(), "missing.jsonl")).isEmpty();
        assertThat(SafePaths.file(root, run.toRealPath(), "../x")).isEmpty();
        assertThat(SafePaths.file(root, run.toRealPath(), "a/b")).isEmpty();
        assertThat(SafePaths.runDir(root, "009-c4-shared-r9")).isEmpty();
    }

    @Test
    void aDirectoryIsNotAFile() throws IOException {
        Path root = tmp.toRealPath();
        Path run = Files.createDirectories(root.resolve("runs").resolve("001-c4-shared-r1"));
        Files.createDirectory(run.resolve("measured.jsonl"));
        assertThat(SafePaths.file(root, run.toRealPath(), "measured.jsonl")).isEmpty();
    }

    @Test
    void aSymbolicLinkLeavingTheRootIsRefused() throws IOException {
        Path root = Files.createDirectory(tmp.resolve("root")).toRealPath();
        Path outside = Files.createDirectory(tmp.resolve("outside")).toRealPath();
        Files.writeString(outside.resolve("secret.txt"), "secret");
        Path run = Files.createDirectories(root.resolve("runs").resolve("001-c4-shared-r1"));
        try {
            Files.createSymbolicLink(run.resolve("measured.jsonl"), outside.resolve("secret.txt"));
            Files.createSymbolicLink(root.resolve("runs").resolve("002-c4-shared-r2"), outside);
        } catch (UnsupportedOperationException | IOException | SecurityException ex) {
            assumeThat(false).as("symbolic links are not available: " + ex).isTrue();
        }
        assertThat(SafePaths.file(root, run.toRealPath(), "measured.jsonl")).isEmpty();
        assertThat(SafePaths.runDir(root, "002-c4-shared-r2")).isEmpty();
    }
}
