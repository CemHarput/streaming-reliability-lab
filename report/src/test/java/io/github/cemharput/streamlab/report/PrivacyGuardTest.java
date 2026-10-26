package io.github.cemharput.streamlab.report;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.Timeout.ThreadMode;

@Timeout(value = 30, unit = TimeUnit.SECONDS, threadMode = ThreadMode.SEPARATE_THREAD)
class PrivacyGuardTest {

    @Test
    void absolutePathsOfEveryFlavourAreCaught() {
        for (String text : List.of("C:\\Users\\x\\y", "d:/data", "\\\\server\\share", "/home/someone/x",
                "/Users/someone", "/root/x", "/tmp/x", "/mnt/disk", "/var/log")) {
            assertThat(PrivacyGuard.scan("see " + text + " here", List.of())).as(text).isNotEmpty();
        }
    }

    @Test
    void relativePathsUrlsAndMarkdownAreNotAbsolutePaths() {
        for (String text : List.of("docs/benchmark-report.md", "benchmark/runs.csv", "http://127.0.0.1:8080/stream",
                "<svg xmlns=\"http://www.w3.org/2000/svg\">", "`source:code`", "x[ceil(p/100 * n)]", "a:b", "runs/001-c4-shared-r1",
                "java -jar report/target/report-0.1.0-SNAPSHOT.jar --results <dir>", "bench/results/<run-id>")) {
            assertThat(PrivacyGuard.scan(text, List.of())).as(text).isEmpty();
        }
    }

    @Test
    void literalSecretsAreCaughtWithoutEchoingThem() {
        List<String> found = PrivacyGuard.scan("prefix secretplace suffix", List.of("secretplace"));
        assertThat(found).containsExactly("a path of this machine");
        assertThat(found.toString()).doesNotContain("secretplace");
        assertThat(PrivacyGuard.scan("nothing", java.util.Arrays.asList("secretplace", null, ""))).isEmpty();
    }

    @Test
    void aBareUserNameIsNotSearchedBecauseItCollidesWithProse() {
        assertThat(PrivacyGuard.scan("the runner finished", List.of())).isEmpty();
    }
}
