package io.github.cemharput.streamlab.report;

import static org.assertj.core.api.Assertions.assertThat;

import io.github.cemharput.streamlab.report.testsupport.Cli;
import io.github.cemharput.streamlab.report.testsupport.Scenarios;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.Timeout.ThreadMode;
import org.junit.jupiter.api.io.TempDir;

/** The tool never modifies the result set: file list, size, modification time and sha256 stay the same. */
@Timeout(value = 60, unit = TimeUnit.SECONDS, threadMode = ThreadMode.SEPARATE_THREAD)
class ResultSetUnchangedTest {

    @TempDir
    Path tmp;

    private void assertUnchanged(Path set, Runnable action) throws Exception {
        Map<String, String> hashes = Cli.hashes(set);
        Map<String, String> stats = Cli.stats(set);
        action.run();
        assertThat(Cli.hashes(set)).isEqualTo(hashes);
        assertThat(Cli.stats(set)).isEqualTo(stats);
    }

    @Test
    void checkLeavesTheSetUntouched() throws Exception {
        Path set = Scenarios.mixed(tmp).build();
        assertUnchanged(set, () -> assertThat(Cli.run(tmp, "--results", set.toString(), "--check").exit()).isZero());
    }

    @Test
    void writingTheReportLeavesTheSetUntouched() throws Exception {
        Path set = Scenarios.mixed(tmp).build();
        Path out = Files.createDirectory(tmp.resolve("out"));
        assertUnchanged(set, () -> assertThat(
                Cli.run(tmp, "--results", set.toString(), "--out", out.toString()).exit()).isZero());
        assertUnchanged(set, () -> assertThat(
                Cli.run(tmp, "--results", set.toString(), "--out", out.toString(), "--overwrite").exit()).isZero());
    }

    @Test
    void failedRunsOfTheToolLeaveTheSetUntouchedToo() throws Exception {
        Path set = Scenarios.mixed(tmp).build();
        Path out = Files.createDirectory(tmp.resolve("out"));
        assertUnchanged(set, () -> {
            assertThat(Cli.run(tmp, "--results", set.toString(), "--out", set.toString()).exit()).isEqualTo(2);
            assertThat(Cli.run(tmp, "--results", set.toString(), "--out", out.toString(), "--bogus").exit())
                    .isEqualTo(2);
        });
    }

    @Test
    void aMalformedSetIsNotRepairedOrTouched() throws Exception {
        Path set = Scenarios.clean(tmp).build();
        Files.writeString(set.resolve("runs").resolve("001-c2-shared-r1").resolve("measured.jsonl"), "garbage\n\n{");
        Path out = Files.createDirectory(tmp.resolve("out"));
        assertUnchanged(set, () -> assertThat(
                Cli.run(tmp, "--results", set.toString(), "--out", out.toString()).exit()).isZero());
    }
}
