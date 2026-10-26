package io.github.cemharput.streamlab.report;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.Timeout.ThreadMode;

/** The committed {@code docs/benchmark-report.md} (and {@code docs/benchmark/*}, if present) are what the tool writes. */
@Timeout(value = 30, unit = TimeUnit.SECONDS, threadMode = ThreadMode.SEPARATE_THREAD)
class CommittedReportTest {

    private static List<String> secrets() {
        List<String> s = new ArrayList<>();
        s.add(System.getProperty("user.home"));
        s.add(System.getProperty("user.dir"));
        return s;
    }

    @Test
    void theCommittedReportStartsWithTheGenerationMarker() throws IOException {
        String text = QuotedLimitsConsistencyTest.read(QuotedLimitsConsistencyTest.doc("benchmark-report.md"));
        assertThat(text.lines().findFirst().orElseThrow()).startsWith(ReportText.MARKER_PREFIX);
        assertThat(text.lines().findFirst().orElseThrow()).endsWith("-->");
    }

    @Test
    void aCommittedPlaceholderEqualsTheGeneratedPlaceholder() throws IOException {
        String text = QuotedLimitsConsistencyTest.read(QuotedLimitsConsistencyTest.doc("benchmark-report.md"));
        if (text.lines().findFirst().orElseThrow().contains("as a placeholder")) {
            assertThat(text).isEqualTo(ReportRenderer.placeholder());
            Path data = QuotedLimitsConsistencyTest.doc("benchmark");
            assertThat(Files.exists(data)).as("no data files next to a placeholder").isFalse();
        }
    }

    @Test
    void theCommittedFilesPassBothGuards() throws IOException {
        Path report = QuotedLimitsConsistencyTest.doc("benchmark-report.md");
        String text = QuotedLimitsConsistencyTest.read(report);
        assertThat(WordingGuard.scan(text, true)).isEmpty();
        assertThat(PrivacyGuard.scan(text, secrets())).isEmpty();
        Path data = QuotedLimitsConsistencyTest.doc("benchmark");
        if (Files.isDirectory(data)) {
            try (Stream<Path> files = Files.list(data)) {
                for (Path f : (Iterable<Path>) files.filter(Files::isRegularFile)::iterator) {
                    String content = QuotedLimitsConsistencyTest.read(f);
                    assertThat(WordingGuard.scan(content, false)).as(f.getFileName().toString()).isEmpty();
                    assertThat(PrivacyGuard.scan(content, secrets())).as(f.getFileName().toString()).isEmpty();
                }
            }
        }
    }

    @Test
    void aCommittedAnalysisCarriesTheSameQuotedBlock() throws IOException {
        String text = QuotedLimitsConsistencyTest.read(QuotedLimitsConsistencyTest.doc("benchmark-report.md"));
        int b = text.indexOf(WordingGuard.BEGIN);
        int e = text.indexOf(WordingGuard.END);
        assertThat(b).isGreaterThanOrEqualTo(0);
        assertThat(text.substring(b + WordingGuard.BEGIN.length() + 1, e)).isEqualTo(ReportText.QUOTED_LIMITS);
    }
}
