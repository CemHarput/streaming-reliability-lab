package io.github.cemharput.streamlab.report;

import static org.assertj.core.api.Assertions.assertThat;

import io.github.cemharput.streamlab.report.testsupport.Cli;
import io.github.cemharput.streamlab.report.testsupport.Cli.Result;
import io.github.cemharput.streamlab.report.testsupport.ResultSetBuilder;
import io.github.cemharput.streamlab.report.testsupport.Scans;
import io.github.cemharput.streamlab.report.testsupport.Scenarios;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.Timeout.ThreadMode;
import org.junit.jupiter.api.io.TempDir;

/** The fixtures contain the user name "fixtureuser" in every place where the runner records a path. */
@Timeout(value = 60, unit = TimeUnit.SECONDS, threadMode = ThreadMode.SEPARATE_THREAD)
class PrivacyTest {

    @TempDir
    Path tmp;

    private void check(ResultSetBuilder b, boolean withFailureTexts) throws IOException {
        Path set = b.build();
        // the fixture really contains what must not appear
        String manifest = Files.readString(set.resolve("manifest.json"));
        assertThat(manifest).contains("fixtureuser").contains("operatorNote");
        Path out = Files.createDirectory(tmp.resolve("out-" + b.root().getFileName() + withFailureTexts));
        for (String mode : new String[] {"--check", "--out"}) {
            Result r = mode.equals("--check") ? Cli.run(tmp, "--results", set.toString(), "--check")
                    : Cli.run(tmp, "--results", set.toString(), "--out", out.toString());
            assertThat(r.exit()).as(r.err()).isZero();
            Scans.assertClean(out, r.out() + r.err(), List.of(tmp, set));
        }
        Scans.assertClean(out, "", List.of(tmp, set));
    }

    @Test
    void aWindowsFlavouredSetLeaksNeitherPathsNorUserNames() throws Exception {
        check(Scenarios.mixed(tmp.resolve("win")), true);
    }

    @Test
    void aLinuxFlavouredSetLeaksNeitherPathsNorUserNames() throws Exception {
        ResultSetBuilder b = Scenarios.mixed(tmp.resolve("lin")).linux();
        check(b, true);
    }

    @Test
    void failureMessagesHarnessFailuresStatusLinesAndOperatorNotesAreNeverPrinted() throws Exception {
        ResultSetBuilder b = Scenarios.mixed(tmp.resolve("texts")).setStatus("failed");
        b.rootKey("operatorNote", "unique-note-text-7f3a");
        Path set = b.build();
        // put statusLines with a distinctive text into the git block
        String manifest = Files.readString(set.resolve("manifest.json"));
        Files.writeString(set.resolve("manifest.json"), manifest.replace("\"statusLines\": []",
                "\"statusLines\": [\" M unique-status-line-9c1d\"]"));
        Path out = Files.createDirectory(tmp.resolve("o"));
        Result r = Cli.run(tmp, "--results", set.toString(), "--out", out.toString());
        assertThat(r.exit()).isZero();
        StringBuilder all = new StringBuilder(r.out());
        for (String name : OutputWriter.ALL_FILES) {
            all.append(Files.readString(out.resolve(name)));
        }
        assertThat(all.toString()).doesNotContain("unique-note-text-7f3a").doesNotContain("unique-status-line-9c1d")
                .doesNotContain("gateway did not start").doesNotContain("failed in ")
                .contains("present (text not reproduced)").contains("Git status lines (count only) | 1");
    }

    @Test
    void artifactsAreShownByFileNameOnly() throws Exception {
        Path set = Scenarios.clean(tmp).linux().build();
        Path out = Files.createDirectory(tmp.resolve("art"));
        assertThat(Cli.run(tmp, "--results", set.toString(), "--out", out.toString()).exit()).isZero();
        String report = Files.readString(out.resolve("benchmark-report.md"));
        assertThat(report).contains("| gatewayJar | gateway-0.1.0-SNAPSHOT.jar |").doesNotContain("streamlab/x/target");
    }

    @Test
    void theJavaHomeCommandLinesAndPidsAreNeverRead() throws Exception {
        Path set = Scenarios.clean(tmp).linux().build();
        Path out = Files.createDirectory(tmp.resolve("home"));
        assertThat(Cli.run(tmp, "--results", set.toString(), "--out", out.toString()).exit()).isZero();
        StringBuilder all = new StringBuilder();
        for (String name : OutputWriter.ALL_FILES) {
            all.append(Files.readString(out.resolve(name)));
        }
        assertThat(all.toString()).doesNotContain("-jdk").doesNotContain("--operator-note")
                .doesNotContain("/jdk/bin/java");
    }
}
