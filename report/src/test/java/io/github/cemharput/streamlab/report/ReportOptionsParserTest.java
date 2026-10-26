package io.github.cemharput.streamlab.report;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.Timeout.ThreadMode;
import org.junit.jupiter.api.io.TempDir;

@Timeout(value = 30, unit = TimeUnit.SECONDS, threadMode = ThreadMode.SEPARATE_THREAD)
class ReportOptionsParserTest {

    @TempDir
    Path tmp;

    Path results;
    Path out;

    @BeforeEach
    void dirs() throws IOException {
        results = Files.createDirectory(tmp.resolve("results"));
        out = Files.createDirectory(tmp.resolve("out"));
    }

    private ReportOptions parse(String... args) throws UsageException {
        return ReportOptionsParser.parse(args, tmp);
    }

    @Test
    void checkWithResults() throws Exception {
        ReportOptions o = parse("--results", "results", "--check");
        assertThat(o.check()).isTrue();
        assertThat(o.results()).isEqualTo(results.toAbsolutePath().normalize());
        assertThat(o.out()).isNull();
    }

    @Test
    void equalsFormAndOut() throws Exception {
        ReportOptions o = parse("--results=results", "--out=out", "--overwrite", "--allow-smoke");
        assertThat(o.out()).isEqualTo(out.toAbsolutePath().normalize());
        assertThat(o.overwrite()).isTrue();
        assertThat(o.allowSmoke()).isTrue();
        assertThat(o.check()).isFalse();
    }

    @Test
    void placeholderNeedsOutAndExcludesResults() throws Exception {
        assertThat(parse("--placeholder", "--out", "out").placeholder()).isTrue();
        assertThatThrownBy(() -> parse("--placeholder")).isInstanceOf(UsageException.class).hasMessageContaining("--out");
        assertThatThrownBy(() -> parse("--placeholder", "--results", "results", "--out", "out"))
                .isInstanceOf(UsageException.class).hasMessageContaining("--placeholder");
        assertThatThrownBy(() -> parse("--placeholder", "--check", "--out", "out")).isInstanceOf(UsageException.class);
    }

    @Test
    void checkAndOutAreExclusive() {
        assertThatThrownBy(() -> parse("--results", "results", "--check", "--out", "out"))
                .isInstanceOf(UsageException.class).hasMessageContaining("--check");
    }

    @Test
    void overwriteOnlyWithOut() {
        assertThatThrownBy(() -> parse("--results", "results", "--check", "--overwrite"))
                .isInstanceOf(UsageException.class).hasMessageContaining("--overwrite");
    }

    @Test
    void resultsOrPlaceholderIsRequiredAndOutOrCheck() {
        assertThatThrownBy(() -> parse()).isInstanceOf(UsageException.class);
        assertThatThrownBy(() -> parse("--results", "results")).isInstanceOf(UsageException.class);
        assertThatThrownBy(() -> parse("--out", "out")).isInstanceOf(UsageException.class);
    }

    @Test
    void directoriesMustExist() {
        assertThatThrownBy(() -> parse("--results", "missing", "--check")).isInstanceOf(UsageException.class)
                .hasMessageContaining("existing directory");
        assertThatThrownBy(() -> parse("--results", "results", "--out", "missing"))
                .isInstanceOf(UsageException.class);
    }

    @Test
    void unknownDuplicateAndMalformedOptionsAreUsageErrors() {
        assertThatThrownBy(() -> parse("--nope")).isInstanceOf(UsageException.class);
        assertThatThrownBy(() -> parse("results")).isInstanceOf(UsageException.class);
        assertThatThrownBy(() -> parse("--results", "results", "--results", "results", "--check"))
                .isInstanceOf(UsageException.class).hasMessageContaining("twice");
        assertThatThrownBy(() -> parse("--results")).isInstanceOf(UsageException.class);
        assertThatThrownBy(() -> parse("--check=yes")).isInstanceOf(UsageException.class);
        assertThatThrownBy(() -> parse("--results", " ", "--check")).isInstanceOf(UsageException.class);
        assertThatThrownBy(() -> parse("--allow-smoke", "--placeholder", "--out", "out"))
                .isInstanceOf(UsageException.class);
    }

    @Test
    void helpWinsOverEverythingElse() throws Exception {
        assertThat(parse("--help").help()).isTrue();
        assertThat(parse("-h", "--bogus").help()).isTrue();
    }

    @Test
    void usageDescribesEveryOptionAndTheExitCodes() {
        for (String option : new String[] {"--results", "--out", "--check", "--overwrite", "--placeholder",
            "--allow-smoke", "--help"}) {
            assertThat(ReportOptionsParser.USAGE).contains(option);
        }
        assertThat(ReportOptionsParser.USAGE).contains("Exit codes").contains("no performance claim");
        for (String file : OutputWriter.ALL_FILES) {
            assertThat(ReportOptionsParser.USAGE).contains(file.substring(file.lastIndexOf('/') + 1));
        }
    }
}
