package io.github.cemharput.streamlab.bench;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.github.cemharput.streamlab.bench.BenchOptions.ChecklistMode;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.Timeout.ThreadMode;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

@Timeout(value = 30, unit = TimeUnit.SECONDS, threadMode = ThreadMode.SEPARATE_THREAD)
class BenchOptionsParserTest {

    @TempDir
    Path root;

    @BeforeEach
    void layout() throws IOException {
        Files.writeString(root.resolve("pom.xml"), "<project/>");
        Files.createDirectories(root.resolve("gateway"));
        Files.createDirectories(root.resolve("loadgen"));
    }

    private BenchOptions parse(String... args) throws UsageException {
        return BenchOptionsParser.parse(args, root);
    }

    @Test
    void defaultsMatchTheProtocol() throws Exception {
        BenchOptions o = parse();
        assertThat(o.preset()).isEqualTo(Preset.DEFAULT);
        assertThat(o.repetitions()).isEqualTo(4);
        assertThat(o.duration()).isEqualTo(Duration.ofSeconds(60));
        assertThat(o.warmup()).isEqualTo(Duration.ofSeconds(20));
        assertThat(o.rampUp()).isEqualTo(Duration.ofSeconds(5));
        assertThat(o.cooldown()).isEqualTo(Duration.ofSeconds(30));
        assertThat(o.concurrency()).containsExactly(50, 100, 150);
        assertThat(o.modeOrder()).containsExactly(ConnectionMode.SHARED, ConnectionMode.PER_REQUEST);
        assertThat(o.gatewayPort()).isEqualTo(8080);
        assertThat(o.simulatorPort()).isEqualTo(50051);
        assertThat(o.checklist()).isEqualTo(ChecklistMode.ASK);
        assertThat(o.outputDir()).isEqualTo(root.resolve("bench").resolve("results"));
        assertThat(o.repoRoot()).isEqualTo(root);
        assertThat(o.dryRun()).isFalse();
        assertThat(o.smoke()).isFalse();
    }

    @Test
    void smokePresetChangesOnlyTheDocumentedValues() throws Exception {
        BenchOptions o = parse("--preset", "smoke");
        assertThat(o.smoke()).isTrue();
        assertThat(o.concurrency()).containsExactly(4);
        assertThat(o.repetitions()).isEqualTo(2);
        assertThat(o.duration()).isEqualTo(Duration.ofSeconds(3));
        assertThat(o.warmup()).isEqualTo(Duration.ofSeconds(2));
        assertThat(o.rampUp()).isEqualTo(Duration.ZERO);
        assertThat(o.cooldown()).isEqualTo(Duration.ofSeconds(1));
        assertThat(o.cpuWindow()).isEqualTo(Duration.ofSeconds(1));
    }

    @Test
    void acceptsBothValueSyntaxesAndExplicitValues() throws Exception {
        BenchOptions o = parse("--preset=smoke", "--repetitions", "3", "--duration=10s", "--warmup", "5s",
                "--ramp-up=1s", "--concurrency", "2,8", "--mode-order=per-request,shared", "--cooldown", "0s",
                "--gateway-port=9090", "--simulator-port", "6000", "--checklist=yes", "--operator-note", "hello",
                "--dry-run");
        assertThat(o.repetitions()).isEqualTo(3);
        assertThat(o.duration()).isEqualTo(Duration.ofSeconds(10));
        assertThat(o.concurrency()).containsExactly(2, 8);
        assertThat(o.modeOrder()).containsExactly(ConnectionMode.PER_REQUEST, ConnectionMode.SHARED);
        assertThat(o.cooldown()).isEqualTo(Duration.ZERO);
        assertThat(o.gatewayPort()).isEqualTo(9090);
        assertThat(o.checklist()).isEqualTo(ChecklistMode.YES);
        assertThat(o.operatorNote()).isEqualTo("hello");
        assertThat(o.dryRun()).isTrue();
    }

    @Test
    void helpWinsOverOtherProblems() throws Exception {
        assertThat(parse("--help").help()).isTrue();
        assertThat(parse("-h", "--dry-run").help())
                .isTrue();
    }

    @ParameterizedTest
    @ValueSource(strings = {
            "positional",
            "--unknown 1",
            "--repetitions",
            "--repetitions 0",
            "--repetitions 21",
            "--repetitions x",
            "--repetitions 2 --repetitions 3",
            "--dry-run=yes",
            "--dry-run --dry-run",
            "--preset big",
            "--duration 0s",
            "--duration 3h",
            "--duration 10",
            "--duration 10x",
            "--warmup 500ms",
            "--ramp-up 60s",
            "--ramp-up 20s",
            "--cooldown 11m",
            "--concurrency 0",
            "--concurrency 10001",
            "--concurrency 5,5",
            "--concurrency 1,2,3,4,5,6,7,8,9,10,11",
            "--concurrency a",
            "--concurrency 5,",
            "--mode-order shared",
            "--mode-order shared,shared",
            "--mode-order shared,bogus",
            "--mode-order shared,per-request,shared",
            "--gateway-port 0",
            "--gateway-port 65536",
            "--gateway-port 8000 --simulator-port 8000",
            "--checklist maybe",
            "--repo-root nonexistent-dir",
    })
    void invalidArgumentsAreUsageErrors(String line) {
        String[] args = line.split(" ");
        assertThatThrownBy(() -> parse(args)).isInstanceOf(UsageException.class);
    }

    @Test
    void rejectsLongNoteAndFileAsOutputDir() throws IOException {
        assertThatThrownBy(() -> parse("--operator-note", "x".repeat(501))).isInstanceOf(UsageException.class);
        Path file = Files.writeString(root.resolve("afile"), "x");
        assertThatThrownBy(() -> parse("--output-dir", file.toString())).isInstanceOf(UsageException.class);
    }

    @Test
    void emptySimulatorClasspathIsRejected() {
        assertThatThrownBy(() -> parse("--simulator-classpath", java.io.File.pathSeparator))
                .isInstanceOf(UsageException.class);
    }

    @Test
    void repoRootMustLookLikeTheRepository() throws IOException {
        Path other = Files.createDirectory(root.resolve("elsewhere"));
        assertThatThrownBy(() -> BenchOptionsParser.parse(new String[0], other)).isInstanceOf(UsageException.class)
                .hasMessageContaining("repository root");
    }

    @Test
    void parsingCreatesNothing() throws Exception {
        parse("--output-dir", root.resolve("new-results").toString());
        assertThat(root.resolve("new-results")).doesNotExist();
    }

    @Test
    void formatsDurationsInTheLoadgenNotation() {
        assertThat(BenchOptionsParser.format(Duration.ofSeconds(60))).isEqualTo("60s");
        assertThat(BenchOptionsParser.format(Duration.ofMillis(1500))).isEqualTo("1500ms");
        assertThat(BenchOptionsParser.format(Duration.ZERO)).isEqualTo("0s");
        assertThat(Preset.fromCliName("smoke")).isEqualTo(Preset.SMOKE);
        assertThat(Preset.fromCliName("huge")).isNull();
    }
}
