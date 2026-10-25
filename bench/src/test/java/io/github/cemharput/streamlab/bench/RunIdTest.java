package io.github.cemharput.streamlab.bench;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.util.List;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.Timeout.ThreadMode;
import org.junit.jupiter.api.io.TempDir;

@Timeout(value = 30, unit = TimeUnit.SECONDS, threadMode = ThreadMode.SEPARATE_THREAD)
class RunIdTest {

    private static final Instant NOW = Instant.parse("2026-03-04T05:06:07Z");
    private static final GitInfo CLEAN = new GitInfo("0123456789abcdef", false, NOW, List.of(), true, true);
    private static final GitInfo DIRTY = new GitInfo("0123456789abcdef", true, NOW, List.of(" M x"), true, true);

    @TempDir
    Path out;

    @Test
    void formatsTimeCommitAndSuffixes() {
        assertThat(RunId.format(NOW, CLEAN, false)).isEqualTo("20260304T050607Z-0123456");
        assertThat(RunId.format(NOW, DIRTY, false)).isEqualTo("20260304T050607Z-0123456-dirty");
        assertThat(RunId.format(NOW, DIRTY, true)).isEqualTo("20260304T050607Z-0123456-dirty-smoke");
        assertThat(RunId.format(NOW, GitInfo.UNAVAILABLE, true)).isEqualTo("20260304T050607Z-nogit-smoke");
    }

    @Test
    void createsMissingOutputDirectoryAndTheRunDirectory() throws IOException {
        Path root = out.resolve("a").resolve("b");
        Path dir = RunId.createUniqueDirectory(root, NOW, CLEAN, false);
        assertThat(dir).isDirectory().hasParent(root);
        assertThat(dir.getFileName().toString()).isEqualTo("20260304T050607Z-0123456");
    }

    @Test
    void collisionGetsASuffixAndNeverTouchesTheExistingDirectory() throws IOException {
        Path first = RunId.createUniqueDirectory(out, NOW, CLEAN, false);
        Path marker = Files.writeString(first.resolve("manifest.json"), "original");
        Path second = RunId.createUniqueDirectory(out, NOW, CLEAN, false);
        Path third = RunId.createUniqueDirectory(out, NOW, CLEAN, false);
        assertThat(second.getFileName().toString()).endsWith("-2");
        assertThat(third.getFileName().toString()).endsWith("-3");
        assertThat(Files.readString(marker)).isEqualTo("original");
    }

    @Test
    void anExistingFileWithTheNameIsAlsoSkipped() throws IOException {
        Files.writeString(out.resolve("20260304T050607Z-0123456"), "file");
        Path dir = RunId.createUniqueDirectory(out, NOW, CLEAN, false);
        assertThat(dir.getFileName().toString()).endsWith("-2");
    }

    @Test
    void failsAfterNinetyNineAttempts() throws IOException {
        RunId.createUniqueDirectory(out, NOW, CLEAN, false);
        for (int i = 2; i <= 99; i++) {
            RunId.createUniqueDirectory(out, NOW, CLEAN, false);
        }
        assertThatThrownBy(() -> RunId.createUniqueDirectory(out, NOW, CLEAN, false)).isInstanceOf(IOException.class)
                .hasMessageContaining("no free");
    }
}
