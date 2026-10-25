package io.github.cemharput.streamlab.bench;

import static org.assertj.core.api.Assertions.assertThat;

import io.github.cemharput.streamlab.bench.testsupport.FakeLauncher;
import io.github.cemharput.streamlab.bench.testsupport.FakeRepo;
import io.github.cemharput.streamlab.bench.testsupport.FakeTicker;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Stream;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.Timeout.ThreadMode;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

@Timeout(value = 60, unit = TimeUnit.SECONDS, threadMode = ThreadMode.SEPARATE_THREAD)
class BenchMainTest {

    @TempDir
    Path repo;

    private final ByteArrayOutputStream outBytes = new ByteArrayOutputStream();
    private final ByteArrayOutputStream errBytes = new ByteArrayOutputStream();
    private final AtomicBoolean launched = new AtomicBoolean();
    private final AtomicBoolean portBusy = new AtomicBoolean();
    private BenchMain.Context ctx;

    @BeforeEach
    void setUp() {
        FakeLauncher launcher = new FakeLauncher(spec -> {
            launched.set(true);
            throw new AssertionError("a process must not be started: " + spec.name());
        });
        ctx = new BenchMain.Context((command, timeout) -> CommandRunner.CommandResult.failed("no git"),
                new EnvironmentCapture.Platform(false, Map.of("SPRING_PROFILES_ACTIVE", "x", "PATH", "p"), List::of),
                port -> portBusy.get(), new FakeTicker(), launcher, () -> Instant.parse("2026-03-04T05:06:07Z"),
                "/jdk", port -> {
                    throw new AssertionError("no gateway client expected");
                }, port -> {
                    throw new AssertionError("no simulator client expected");
                }, BenchRunner.Timeouts.defaults());
    }

    private int run(OperatorChecklist.Terminal terminal, String... args) {
        PrintStream out = new PrintStream(outBytes, true, StandardCharsets.UTF_8);
        PrintStream err = new PrintStream(errBytes, true, StandardCharsets.UTF_8);
        return BenchMain.run(args, out, err, terminal, repo, ctx);
    }

    private int run(String... args) {
        return run(noTerminal(), args);
    }

    private static OperatorChecklist.Terminal noTerminal() {
        return new OperatorChecklist.Terminal() {
            @Override
            public boolean interactive() {
                return false;
            }

            @Override
            public String readLine(String prompt) {
                return null;
            }
        };
    }

    private String stdout() {
        return outBytes.toString(StandardCharsets.UTF_8);
    }

    private String stderr() {
        return errBytes.toString(StandardCharsets.UTF_8);
    }

    private List<String> tree() throws IOException {
        try (Stream<Path> s = Files.walk(repo)) {
            return s.map(p -> repo.relativize(p).toString()).sorted().toList();
        }
    }

    @Test
    void usageTextCarriesTheDocumentedExitCodeWording() {
        assertThat(BenchOptionsParser.USAGE).contains("Exit codes: 0 all planned runs completed", "result directory exists",
                "result directory cannot be", "created, or the manifest cannot be written", "failed precondition");
    }

    @Test
    void helpPrintsUsageAndExitsZero() throws Exception {
        FakeRepo.layout(repo);
        assertThat(run("--help")).isZero();
        assertThat(stdout()).contains("Usage:", "--checklist", "protocol version 1");
        assertThat(stderr()).isEmpty();
    }

    @ParameterizedTest
    @ValueSource(strings = {"--bogus", "--repetitions=0", "--duration 5s --ramp-up 5s", "--concurrency 5,5",
            "--mode-order shared,shared", "--gateway-port 1 --simulator-port 1", "stray"})
    void usageErrorsExitTwoWithOneLineAndStartNothing(String line) throws Exception {
        FakeRepo.build(repo);
        List<String> before = tree();
        assertThat(run(line.split(" "))).isEqualTo(2);
        assertThat(stderr().lines().toList()).hasSize(2).last().isEqualTo("Try --help.");
        assertThat(launched).isFalse();
        assertThat(tree()).isEqualTo(before);
    }

    @Test
    void dryRunPrintsThePlanAndCommandsAndTouchesNothing() throws Exception {
        FakeRepo.build(repo);
        List<String> before = tree();

        assertThat(run("--preset", "smoke", "--dry-run")).isZero();

        String out = stdout();
        assertThat(out).contains("dry run", "Preset: smoke", "Planned runs: 4", "001-c4-shared-r1",
                "002-c4-per-request-r1", "003-c4-per-request-r2", "004-c4-shared-r2",
                "--lab.connection-mode=shared", "--lab.connection-mode=per-request",
                "SIMULATOR_PORT=50051", "SIMULATOR_SCHEDULER_THREADS=", "-Xms1g -Xmx1g", "-Xms512m -Xmx512m",
                "io.github.cemharput.streamlab.simulator.SimulatorMain", "warm-up loadgen", "measured loadgen",
                "--summary-output measured-summary.json", "Nominal total duration", "SPRING_PROFILES_ACTIVE",
                "makes no performance claim");
        assertThat(out).doesNotContain("MISSING");
        assertThat(launched).isFalse();
        assertThat(tree()).isEqualTo(before);
        assertThat(repo.resolve("bench").resolve("results")).doesNotExist();
    }

    @Test
    void dryRunWithMissingArtifactsWarnsButStillExitsZero() throws Exception {
        FakeRepo.layout(repo);
        assertThat(run("--dry-run")).isZero();
        assertThat(stdout()).contains("  MISSING: gateway jar not found", "Planned runs: 24", "./mvnw -B package -DskipTests");
    }

    @Test
    void dryRunLabelsAnAmbiguousJarSeparately() throws Exception {
        FakeRepo.build(repo);
        Files.writeString(repo.resolve("gateway/target/gateway-0.2.0.jar"), "second");
        assertThat(run("--dry-run")).isZero();
        assertThat(stdout()).contains("AMBIGUOUS: gateway jar is ambiguous").doesNotContain("MISSING:");
    }

    @Test
    void dryRunDistinguishesStaleFromMissingArtifacts() throws Exception {
        FakeRepo.build(repo);
        Files.writeString(repo.resolve("bench/target/lib/a-dep-2.jar"), "newer");
        assertThat(run("--dry-run")).isZero();
        assertThat(stdout()).contains("STALE: stale jars").doesNotContain("MISSING:");
        assertThat(run("--checklist", "yes")).isEqualTo(2);
        assertThat(stderr()).contains("artifact problems: stale jars");
    }

    @Test
    void missingArtifactsAreAPreconditionFailureWithoutADirectory() throws Exception {
        FakeRepo.layout(repo);
        assertThat(run("--checklist", "yes")).isEqualTo(2);
        assertThat(stderr()).contains("artifact problems").contains("Try --help.");
        assertThat(repo.resolve("bench")).doesNotExist();
    }

    @Test
    void aBusyPortIsAPreconditionFailureWithoutADirectory() throws Exception {
        FakeRepo.build(repo);
        portBusy.set(true);
        assertThat(run("--checklist", "yes", "--preset", "smoke")).isEqualTo(2);
        assertThat(stderr()).contains("listener is already present");
        assertThat(repo.resolve("bench").resolve("results")).doesNotExist();
        assertThat(launched).isFalse();
    }

    @Test
    void checklistAskWithoutATerminalIsAPreconditionFailure() throws Exception {
        FakeRepo.build(repo);
        assertThat(run("--preset", "smoke")).isEqualTo(2);
        assertThat(stderr()).contains("--checklist yes|skip");
        assertThat(repo.resolve("bench").resolve("results")).doesNotExist();
    }
}
