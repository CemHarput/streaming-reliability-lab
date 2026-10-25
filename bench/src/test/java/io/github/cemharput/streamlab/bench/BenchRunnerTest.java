package io.github.cemharput.streamlab.bench;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.github.cemharput.streamlab.bench.BenchOptions.ChecklistMode;
import io.github.cemharput.streamlab.bench.ProcessSpecs.ProcessSpec;
import io.github.cemharput.streamlab.bench.json.Json;
import io.github.cemharput.streamlab.bench.testsupport.FakeChildProcess;
import io.github.cemharput.streamlab.bench.testsupport.FakeLauncher;
import io.github.cemharput.streamlab.bench.testsupport.FakeRepo;
import io.github.cemharput.streamlab.bench.testsupport.FakeTicker;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BooleanSupplier;
import java.util.stream.Stream;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.Timeout.ThreadMode;
import org.junit.jupiter.api.io.TempDir;

/** The orchestration with every external effect faked: no real gateway, simulator or loadgen is started. */
@Timeout(value = 60, unit = TimeUnit.SECONDS, threadMode = ThreadMode.SEPARATE_THREAD)
@SuppressWarnings("unchecked")
class BenchRunnerTest {

    private static final String SUMMARY = "{\"type\":\"summary\",\"schemaVersion\":1,\"outcome_COMPLETED\":3,"
            + "\"outcome_REJECTED\":0}";
    private static final String JSONL = "{\"type\":\"header\"}\n{\"type\":\"footer\",\"stopReason\":\"duration\","
            + "\"requestsStarted\":3,\"recordsWritten\":3,\"interrupted\":0}\n";

    @TempDir
    Path tmp;

    private final AtomicLong pids = new AtomicLong(100);
    private final FakeTicker ticker = new FakeTicker();
    private final ByteArrayOutputStream outBytes = new ByteArrayOutputStream();
    private final PrintStream out = new PrintStream(outBytes);
    private final AtomicInteger loadgenExit = new AtomicInteger();
    private volatile boolean gatewayDiesAtStart;
    private volatile boolean modeOk = true;
    private volatile boolean portBusy;
    private volatile BooleanSupplier gatewayReadiness = () -> true;
    private volatile java.util.function.Supplier<Instant> clock = () -> Instant.parse("2026-03-04T05:06:07Z");
    private volatile boolean warmupLoadgenHangs;
    private volatile boolean blockWarmupWait;
    private volatile Runnable gatewayOnWait = () -> { };
    private final AtomicInteger gatewayCloses = new AtomicInteger();

    private FakeLauncher launcher;
    private BenchRunner runner;
    private Path results;

    private FakeChildProcess behave(ProcessSpec spec) {
        long pid = pids.incrementAndGet();
        if (spec.name().endsWith("-loadgen")) {
            String phase = spec.name().replace("-loadgen", "");
            try {
                Files.writeString(spec.workDir().resolve(phase + "-summary.json"), SUMMARY);
                Files.writeString(spec.workDir().resolve(phase + ".jsonl"), JSONL);
            } catch (IOException ex) {
                throw new UncheckedIOException(ex);
            }
            if (phase.equals("warmup") && (warmupLoadgenHangs || blockWarmupWait)) {
                FakeChildProcess hanging = new FakeChildProcess(pid, true);
                return blockWarmupWait ? hanging.blockingWait().afterWait(this::awaitManifestInterrupted) : hanging;
            }
            return new FakeChildProcess(pid, false).exited(loadgenExit.get());
        }
        if (spec.name().equals("gateway") && gatewayDiesAtStart) {
            return new FakeChildProcess(pid, false).exited(1);
        }
        if (spec.name().equals("gateway")) {
            return new FakeChildProcess(pid, true).onWaitFor(() -> gatewayOnWait.run());
        }
        return new FakeChildProcess(pid, true);
    }

    /** Makes the hook win the race deterministically: the runner thread continues only once it is marked. */
    private void awaitManifestInterrupted() {
        org.awaitility.Awaitility.await().atMost(java.time.Duration.ofSeconds(30)).until(() -> {
            try {
                return "interrupted".equals(manifest().get("status"));
            } catch (IOException ex) {
                return false;
            }
        });
    }

    @BeforeEach
    void setUp() throws Exception {
        Files.createDirectories(tmp.resolve("repo"));
        FakeRepo.build(tmp.resolve("repo"));
        results = tmp.resolve("results");
        launcher = new FakeLauncher(this::behave);
    }

    private BenchRunner runner(String... extra) throws Exception {
        String[] base = {"--preset", "smoke", "--repetitions", "1", "--concurrency", "4", "--checklist", "yes",
                "--output-dir", results.toString()};
        String[] args = Stream.concat(Stream.of(base), Stream.of(extra)).toArray(String[]::new);
        BenchOptions o = BenchOptionsParser.parse(args, tmp.resolve("repo"));
        Artifacts.Resolved art = Artifacts.resolve(o);
        ProcessSpecs specs = new ProcessSpecs("JAVA", Map.of(), o, art, 4);
        GatewayApi gateway = new GatewayApi() {
            @Override
            public boolean readiness() {
                return gatewayReadiness.getAsBoolean();
            }

            @Override
            public ModeCheck verifyConnectionMode(String expectedTag) {
                return new ModeCheck(modeOk, List.of(modeOk ? expectedTag : "other"), 1.0, modeOk ? "ok" : "mismatch");
            }

            @Override
            public List<Snapshot.Entry> snapshotEntries() {
                return List.of(new Snapshot.Entry(GatewayProbe.ACTIVE_STREAMS, 200, 0.0, "{\"raw\":1}"));
            }

            @Override
            public IdleResult awaitIdle(java.time.Duration timeout) {
                return new IdleResult(true, 0, "idle");
            }

            @Override
            public void close() {
                gatewayCloses.incrementAndGet();
            }
        };
        SimulatorApi simulator = () -> new SimulatorApi.StatsResult(Map.of("active_generations", 0L), null);
        EnvironmentCapture environment = new EnvironmentCapture((c, t) -> CommandRunner.CommandResult.failed("none"),
                new EnvironmentCapture.Platform(false, Map.of(), List::of));
        BenchRunner.Services services = new BenchRunner.Services(launcher, port -> portBusy, ticker, p -> gateway,
                p -> simulator, environment, new CpuLoadSampler(() -> 0.05, ticker),
                () -> clock.get());
        OperatorChecklist.Result checklist = OperatorChecklist.run(ChecklistMode.YES, null, out);
        runner = new BenchRunner(o, PlanBuilder.build(o), art, specs, GitInfo.UNAVAILABLE, checklist, services,
                BenchRunner.Timeouts.defaults(), out, "test");
        return runner;
    }

    @SuppressWarnings("unchecked")
    private Map<String, Object> manifest() throws IOException {
        Path dir;
        try (Stream<Path> s = Files.list(results)) {
            dir = s.findFirst().orElseThrow();
        }
        return Json.parseObject(Files.readString(dir.resolve("manifest.json")));
    }

    @SuppressWarnings("unchecked")
    private static List<Map<String, Object>> runs(Map<String, Object> manifest) {
        return (List<Map<String, Object>>) manifest.get("runs");
    }

    private Path runDir(String id) throws IOException {
        try (Stream<Path> s = Files.list(results)) {
            return s.findFirst().orElseThrow().resolve("runs").resolve(id);
        }
    }

    @Test
    void completesAllRunsInPlanOrderWithTheDocumentedProcessSequence() throws Exception {
        assertThat(runner().execute()).isZero();

        Map<String, Object> m = manifest();
        assertThat(m).containsEntry("status", "completed");
        assertThat(m.keySet()).containsExactlyElementsOf(Manifest.TOP_LEVEL_KEYS);
        assertThat(runs(m)).extracting(r -> r.get("status")).containsExactly("completed", "completed");
        assertThat(runs(m)).extracting(r -> r.get("mode")).containsExactly("shared", "per-request");
        assertThat(runs(m)).extracting(r -> r.get("orderLabel")).containsExactly("AB", "AB");
        assertThat(launcher.names()).containsExactly("simulator", "gateway", "warmup-loadgen", "measured-loadgen",
                "simulator", "gateway", "warmup-loadgen", "measured-loadgen");
        assertThat(launcher.specs().get(1).command()).contains("--lab.connection-mode=shared");
        assertThat(launcher.specs().get(5).command()).contains("--lab.connection-mode=per-request");
        assertThat(launcher.children()).noneMatch(FakeChildProcess::isAlive);
        for (Map<String, Object> run : runs(m)) {
            assertThat(run.keySet()).containsExactlyElementsOf(Manifest.RUN_KEYS);
            assertThat(run.get("portsFreed")).isEqualTo(true);
            assertThat(new java.util.ArrayList<Object>(((Map<?, ?>) run.get("snapshots")).keySet())).containsExactlyInAnyOrder("ready", "pre", "post");
            Map<?, ?> processes = (Map<?, ?>) run.get("processes");
            assertThat(new java.util.ArrayList<Object>(processes.keySet())).containsExactlyInAnyOrder("simulator", "gateway");
            assertThat(((Map<?, ?>) ((Map<?, ?>) processes.get("gateway")).get("stop")).get("method"))
                    .isEqualTo("destroy");
            assertThat(((Map<?, ?>) run.get("measured")).get("footer")).isNotNull();
            assertThat(((Map<?, ?>) run.get("warmup")).get("warmup")).isEqualTo(true);
        }
        Path dir = runDir("001-c4-shared-r1");
        assertThat(dir.resolve("snapshot-ready.json")).isRegularFile();
        assertThat(dir.resolve("snapshot-pre.json")).isRegularFile();
        assertThat(dir.resolve("snapshot-post.json")).isRegularFile();
        assertThat(Json.parseObject(Files.readString(dir.resolve("snapshot-post.json"))).keySet())
                .containsExactly("capturedAt", "gateway", "simulator");
        try (Stream<Path> files = Files.list(dir.getParent().getParent())) {
            assertThat(files).noneMatch(p -> p.toString().endsWith(".tmp"));
        }
        assertThat(ticker.sleepCount()).isPositive(); // cooldown between the two runs
    }

    @Test
    void gatewayExitBeforeReadyIsARunFailureWithCleanup() throws Exception {
        gatewayDiesAtStart = true;
        BenchRunner r = runner();
        assertThat(r.execute()).isEqualTo(1);
        Map<String, Object> m = manifest();
        assertThat(m).containsEntry("status", "failed");
        assertThat(runs(m)).extracting(x -> x.get("status")).containsExactly("failed", "not_run");
        Map<?, ?> failure = (Map<?, ?>) runs(m).get(0).get("failure");
        assertThat(failure.get("phase")).isEqualTo("gateway-ready");
        assertThat((String) failure.get("message")).contains("exited before it was ready");
        assertThat(launcher.names()).containsExactly("simulator", "gateway");
        assertThat(launcher.children().get(0).isAlive()).isFalse();
        assertThat(r.supervisor().registered()).isEmpty();
    }

    @Test
    void aReadinessTimeoutIsBoundedAndFails() throws Exception {
        gatewayReadiness = () -> false;
        assertThat(runner().execute()).isEqualTo(1);
        Map<?, ?> failure = (Map<?, ?>) runs(manifest()).get(0).get("failure");
        assertThat(failure.get("phase")).isEqualTo("gateway-ready");
        assertThat((String) failure.get("message")).contains("not ready within 90 s");
        assertThat(launcher.children()).noneMatch(FakeChildProcess::isAlive);
    }

    @Test
    void aConnectionModeMismatchFailsTheRunAndIsFlagged() throws Exception {
        modeOk = false;
        assertThat(runner().execute()).isEqualTo(1);
        Map<String, Object> run = runs(manifest()).get(0);
        assertThat((List<Object>) run.get("flags")).contains(Flags.CONNECTION_MODE_MISMATCH);
        assertThat(((Map<?, ?>) run.get("failure")).get("phase")).isEqualTo("connection-mode");
        assertThat(launcher.names()).doesNotContain("warmup-loadgen");
    }

    @Test
    void aFailingLoadgenIsRecordedWithItsFlagAndFailsTheRun() throws Exception {
        loadgenExit.set(1);
        assertThat(runner().execute()).isEqualTo(1);
        Map<String, Object> run = runs(manifest()).get(0);
        assertThat(((Map<?, ?>) run.get("warmup")).get("flags")).isEqualTo(List.of(Flags.LOADGEN_EXIT_NONZERO));
        assertThat(((Map<?, ?>) run.get("failure")).get("phase")).isEqualTo("warmup");
        assertThat(launcher.children()).noneMatch(FakeChildProcess::isAlive);
    }

    @Test
    void aListenerBeforeStartFailsTheRunWithoutStartingAnything() throws Exception {
        portBusy = true;
        assertThat(runner().execute()).isEqualTo(1);
        assertThat(launcher.specs()).isEmpty();
        Map<?, ?> failure = (Map<?, ?>) runs(manifest()).get(0).get("failure");
        assertThat(failure.get("phase")).isEqualTo("port-check");
    }

    @Test
    void theShutdownHookKillsEverythingMarksInterruptedAndForbidsNewProcesses() throws Exception {
        AtomicReference<BenchRunner> ref = new AtomicReference<>();
        gatewayReadiness = () -> {
            BenchMain.shutdownHook(ref.get()).run(); // what Ctrl+C does, here on the runner thread
            return false;
        };
        BenchRunner r = runner();
        ref.set(r);

        assertThat(r.execute()).isEqualTo(1);

        Map<String, Object> m = manifest();
        assertThat(m).containsEntry("status", "interrupted");
        assertThat((List<Object>) m.get("flags")).contains(Flags.INTERRUPTED);
        assertThat(runs(m)).extracting(x -> x.get("status")).containsExactly("interrupted", "not_run");
        assertThat(launcher.children()).noneMatch(FakeChildProcess::isAlive);
        assertThat(r.supervisor().isClosed()).isTrue();
        assertThat(launcher.names()).containsExactly("simulator", "gateway");
        assertThatThrownBy(() -> r.supervisor().start(launcher.specs().get(0)))
                .isInstanceOf(IllegalStateException.class);
    }

    @Test
    void logTruncationIsFlagged() throws Exception {
        launcher = new FakeLauncher(spec -> {
            FakeChildProcess c = behave(spec);
            return spec.name().equals("gateway") ? c.logDropped(5) : c;
        });
        assertThat(runner().execute()).isZero();
        assertThat((List<Object>) runs(manifest()).get(0).get("flags")).contains(Flags.LOG_TRUNCATED);
    }

    @Test
    void theOperatorNoteIsStoredAndNullWhenAbsent() throws Exception {
        assertThat(runner("--operator-note", "quiet machine").execute()).isZero();
        assertThat(manifest()).containsEntry("operatorNote", "quiet machine");
    }

    @Test
    void theOperatorNoteIsNullWithoutTheOption() throws Exception {
        runner().execute();
        assertThat(manifest()).containsEntry("operatorNote", null);
    }

    @Test
    void theGatewayClientIsClosedAfterEveryRun() throws Exception {
        runner().execute();
        assertThat(gatewayCloses.get()).isEqualTo(2);
    }

    @Test
    void loadgenProcessesGetLogCountersAndAGracefulWaitCoveringTheirDrainTimeout() throws Exception {
        warmupLoadgenHangs = true; // never exits on its own: the runner has to stop it
        assertThat(runner().execute()).isEqualTo(1);
        FakeChildProcess hung = launcher.children().stream().filter(c -> c.calls().contains("destroy")
                && launcher.specs().get(launcher.children().indexOf(c)).name().equals("warmup-loadgen")).findFirst()
                .orElseThrow();
        assertThat(hung.waits()).contains(java.time.Duration.ofSeconds(40));
        Map<?, ?> warmup = (Map<?, ?>) runs(manifest()).get(0).get("warmup");
        Map<?, ?> log = (Map<?, ?>) ((Map<?, ?>) warmup.get("loadgen")).get("log");
        assertThat(log.keySet().stream().map(String::valueOf).toList()).containsExactly("bytesWritten", "bytesDropped",
                "bytesLostAfterWriteError", "writeError");
        assertThat(((Number) log.get("bytesWritten")).longValue()).isEqualTo(10);
        assertThat((List<Object>) warmup.get("flags")).contains(Flags.LOADGEN_WAIT_EXCEEDED);
    }

    @Test
    void aLogWriteErrorIsRecordedAndFlagged() throws Exception {
        launcher = new FakeLauncher(spec -> {
            FakeChildProcess c = behave(spec);
            return spec.name().equals("simulator") || spec.name().equals("measured-loadgen")
                    ? c.logWriteError("IOException: No space left on device") : c;
        });
        assertThat(runner().execute()).isZero();
        Map<String, Object> run = runs(manifest()).get(0);
        Map<?, ?> sim = (Map<?, ?>) ((Map<?, ?>) run.get("processes")).get("simulator");
        assertThat(((Map<?, ?>) sim.get("log")).get("writeError")).isEqualTo("IOException: No space left on device");
        assertThat((List<Object>) run.get("flags")).contains(Flags.LOG_WRITE_ERROR);
        Map<?, ?> measured = (Map<?, ?>) run.get("measured");
        assertThat((List<Object>) measured.get("flags")).contains(Flags.LOG_WRITE_ERROR);
        assertThat(((Map<?, ?>) ((Map<?, ?>) measured.get("loadgen")).get("log")).get("writeError"))
                .isEqualTo("IOException: No space left on device");
    }

    @Test
    void aWriteErrorIsNotReportedAsLogTruncation() throws Exception {
        launcher = new FakeLauncher(spec -> {
            FakeChildProcess c = behave(spec);
            return spec.name().equals("gateway") ? c.logWriteError("IOException: disk full").logLostAfterWriteError(777) : c;
        });
        assertThat(runner().execute()).isZero();
        Map<String, Object> run = runs(manifest()).get(0);
        assertThat((List<Object>) run.get("flags")).contains(Flags.LOG_WRITE_ERROR).doesNotContain(Flags.LOG_TRUNCATED);
        Map<?, ?> log = (Map<?, ?>) ((Map<?, ?>) ((Map<?, ?>) run.get("processes")).get("gateway")).get("log");
        assertThat(((Number) log.get("bytesLostAfterWriteError")).longValue()).isEqualTo(777);
        assertThat(((Number) log.get("bytesDropped")).longValue()).isZero();
    }

    @Test
    void theHookKillBoundIsRecordedWithTheOtherTimeouts() throws Exception {
        runner().execute();
        Map<?, ?> timeouts = (Map<?, ?>) ((Map<?, ?>) ((Map<?, ?>) manifest().get("parameters")).get("plan"))
                .get("timeouts");
        assertThat(((Number) timeouts.get("hookKillMs")).longValue()).isEqualTo(10_000);
    }

    @Test
    void manifestWriteRetriesUseTheInjectedTickerAndNeverSleepForReal() throws Exception {
        java.util.concurrent.atomic.AtomicInteger moves = new java.util.concurrent.atomic.AtomicInteger();
        BenchRunner r = runner().withManifestMover((from, to) -> {
            moves.incrementAndGet();
            throw new java.nio.file.AccessDeniedException(to.toString());
        });
        long startNanos = System.nanoTime();

        assertThat(r.execute()).isEqualTo(1);

        assertThat(moves.get()).isEqualTo(ManifestStore.MOVE_ATTEMPTS);
        assertThat(ticker.sleepCount()).isEqualTo(ManifestStore.MOVE_ATTEMPTS - 1); // virtual pauses only
        assertThat(java.time.Duration.ofNanos(System.nanoTime() - startNanos)).isLessThan(java.time.Duration.ofSeconds(5));
        assertThat(launcher.specs()).isEmpty();
    }

    @Test
    void anUnexpectedRuntimeFailureWhileBuildingTheManifestStillLeavesAFailedManifest() throws Exception {
        AtomicInteger calls = new AtomicInteger();
        clock = () -> {
            if (calls.incrementAndGet() >= 2) { // 1st call names the directory, later ones fail
                throw new IllegalStateException("clock broke");
            }
            return Instant.parse("2026-03-04T05:06:07Z");
        };
        assertThat(runner().execute()).isEqualTo(1);
        Map<String, Object> m = manifest();
        assertThat(m).containsEntry("status", "failed");
        assertThat((String) m.get("harnessFailure")).contains("clock broke");
        assertThat(launcher.specs()).isEmpty();
    }

    @Test
    void theHookSharesOneDeadlineAndDoesNotWaitAgainForTheRunnerThread() throws Exception {
        java.util.concurrent.CountDownLatch release = new java.util.concurrent.CountDownLatch(1);
        gatewayReadiness = () -> { // the runner thread is stuck here while the hook runs
            try {
                release.await(60, TimeUnit.SECONDS);
            } catch (InterruptedException ex) {
                Thread.currentThread().interrupt();
            }
            return false;
        };
        Thread hookThread = Thread.currentThread();
        gatewayOnWait = () -> {
            if (Thread.currentThread() == hookThread) {
                ticker.sleep(java.time.Duration.ofSeconds(10)); // the kill wait consumes the whole hook budget
            }
        };
        BenchRunner r = runner();
        java.util.concurrent.ExecutorService executor = java.util.concurrent.Executors.newSingleThreadExecutor();
        try {
            java.util.concurrent.Future<Integer> exit = executor.submit(r::execute);
            org.awaitility.Awaitility.await().atMost(java.time.Duration.ofSeconds(30))
                    .until(() -> launcher.names().contains("gateway"));
            long start = System.nanoTime();

            BenchMain.shutdownHook(r).run();

            // Waiting a second hookKill (10 s) for the stuck runner thread would exceed this by far.
            assertThat(java.time.Duration.ofNanos(System.nanoTime() - start)).isLessThan(java.time.Duration.ofSeconds(5));
            release.countDown();
            assertThat(exit.get(30, TimeUnit.SECONDS)).isEqualTo(1);
        } finally {
            release.countDown();
            executor.shutdownNow();
        }
    }

    /** Normal moves work until run 1 is completed and run 2 has started; from then on they fail permanently. */
    private ManifestStore.Mover failsFromTheSecondRun(java.util.concurrent.atomic.AtomicReference<String> lastGood) {
        AtomicInteger broken = new AtomicInteger();
        return (from, to) -> {
            String content = Files.readString(from);
            boolean failing = broken.get() > 0 || content.contains("\"status\": \"completed\"")
                    && content.indexOf("\"status\": \"running\"") != content.lastIndexOf("\"status\": \"running\"");
            if (failing) {
                broken.set(1); // permanent from now on
                throw new java.nio.file.AccessDeniedException(to.toString());
            }
            Files.move(from, to, java.nio.file.StandardCopyOption.REPLACE_EXISTING);
            lastGood.set(content);
        };
    }

    @Test
    void aPermanentManifestWriteFailureMidRunFallsBackToTheFullStateWithAFailedStatus() throws Exception {
        java.util.concurrent.atomic.AtomicReference<String> lastGood = new java.util.concurrent.atomic.AtomicReference<>();
        BenchRunner r = runner().withManifestMover(failsFromTheSecondRun(lastGood));

        assertThat(r.execute()).isEqualTo(1);

        Map<String, Object> m = manifest();
        assertThat(m).containsEntry("status", "failed");
        assertThat((String) m.get("harnessFailure")).contains("cannot write");
        assertThat(new java.util.ArrayList<Object>(m.keySet())).containsAll(Manifest.TOP_LEVEL_KEYS);
        assertThat(m.get("environment")).isNotNull();
        assertThat(m.get("parameters")).isNotNull();
        assertThat(m.get("operatorChecklist")).isNotNull();
        assertThat(((Map<?, ?>) m.get("artifacts")).get("gatewayJar")).isNotNull();
        assertThat(((Map<?, ?>) ((Map<?, ?>) m.get("artifacts")).get("gatewayJar")).get("sha256")).isNotNull();
        Map<String, Object> first = runs(m).get(0);
        assertThat(first.get("status")).isEqualTo("completed");
        assertThat(first.get("warmup")).isNotNull();
        assertThat(first.get("measured")).isNotNull();
        assertThat((Map<?, ?>) first.get("snapshots")).hasSize(3);
        assertThat(outBytes.toString()).contains("the full manifest state was written directly");
        assertThat(results.resolve(manifestDirName()).resolve("manifest.json.direct.tmp")).doesNotExist();
    }

    @Test
    void whenTheDirectFallbackFailsTooTheManifestFileIsLeftUntouched() throws Exception {
        java.util.concurrent.atomic.AtomicReference<String> lastGood = new java.util.concurrent.atomic.AtomicReference<>();
        BenchRunner r = runner().withManifestMover(failsFromTheSecondRun(lastGood))
                .withDirectManifestMover((from, to) -> {
                    throw new java.nio.file.AccessDeniedException(to.toString());
                });

        assertThat(r.execute()).isEqualTo(1);

        Path file = results.resolve(manifestDirName()).resolve("manifest.json");
        assertThat(Files.readString(file)).isEqualTo(lastGood.get());
        assertThat(outBytes.toString()).contains("manifest.json keeps its last written status")
                .contains(file.resolveSibling("manifest.json.direct.tmp").toString());
        // the fully written direct file is kept: it is the only copy of the final state
        assertThat(Json.parseObject(Files.readString(file.resolveSibling("manifest.json.direct.tmp"))))
                .containsEntry("status", "failed").containsKey("environment");
    }

    @Test
    void whenNotEvenTheDirectTmpCanBeWrittenTheMessageSaysNoCopyExists() throws Exception {
        java.util.concurrent.atomic.AtomicReference<String> lastGood = new java.util.concurrent.atomic.AtomicReference<>();
        BenchRunner r = runner().withManifestMover(failsFromTheSecondRun(lastGood))
                .withDirectManifestWriter((target, content) -> {
                    Files.writeString(target, content.substring(0, 5));
                    throw new IOException("disk full");
                });

        assertThat(r.execute()).isEqualTo(1);

        Path file = results.resolve(manifestDirName()).resolve("manifest.json");
        assertThat(Files.readString(file)).isEqualTo(lastGood.get());
        assertThat(file.resolveSibling("manifest.json.direct.tmp")).doesNotExist();
        assertThat(outBytes.toString()).contains("no usable copy of the final state could be written")
                .doesNotContain("the full state is in");
    }

    @Test
    void aNonAtomicDirectMoveThatDeletesTheTargetThenFailsLeavesTheCompleteStateInTheDirectFile() throws Exception {
        java.util.concurrent.atomic.AtomicReference<String> lastGood = new java.util.concurrent.atomic.AtomicReference<>();
        BenchRunner r = runner().withManifestMover(failsFromTheSecondRun(lastGood))
                .withDirectManifestMover((from, to) -> {
                    Files.deleteIfExists(to); // what a non-atomic replace can do before failing
                    throw new java.nio.file.AccessDeniedException(to.toString());
                });

        assertThat(r.execute()).isEqualTo(1);

        Path file = results.resolve(manifestDirName()).resolve("manifest.json");
        Path direct = file.resolveSibling("manifest.json.direct.tmp");
        assertThat(file).doesNotExist();
        Map<String, Object> state = Json.parseObject(Files.readString(direct));
        assertThat(state).containsEntry("status", "failed");
        assertThat(state.get("environment")).isNotNull();
        assertThat(runs(state).get(0).get("status")).isEqualTo("completed");
        assertThat(outBytes.toString()).contains(direct.toString());
    }

    @Test
    void anInterruptMarkedInMemoryButNeverWrittenIsWrittenDirectlyAtTheEnd() throws Exception {
        java.util.concurrent.CountDownLatch release = new java.util.concurrent.CountDownLatch(1);
        gatewayReadiness = () -> { // the runner thread waits here until the hook released it
            try {
                release.await(60, TimeUnit.SECONDS);
            } catch (InterruptedException ex) {
                Thread.currentThread().interrupt();
            }
            return false;
        };
        Thread hookThread = Thread.currentThread();
        gatewayOnWait = () -> {
            if (Thread.currentThread() == hookThread) {
                release.countDown(); // the hook has marked (and failed to write) already
            }
        };
        AtomicInteger broken = new AtomicInteger();
        BenchRunner r = runner().withManifestMover((from, to) -> {
            if (broken.get() > 0 || Files.readString(from).contains("\"status\": \"interrupted\"")) {
                broken.set(1);
                throw new java.nio.file.AccessDeniedException(to.toString());
            }
            Files.move(from, to, java.nio.file.StandardCopyOption.REPLACE_EXISTING);
        });
        java.util.concurrent.ExecutorService executor = java.util.concurrent.Executors.newSingleThreadExecutor();
        try {
            java.util.concurrent.Future<Integer> exit = executor.submit(r::execute);
            org.awaitility.Awaitility.await().atMost(java.time.Duration.ofSeconds(30))
                    .until(() -> launcher.names().contains("gateway"));

            BenchMain.shutdownHook(r).run();

            assertThat(exit.get(30, TimeUnit.SECONDS)).isEqualTo(1);
        } finally {
            release.countDown();
            executor.shutdownNow();
        }
        Map<String, Object> m = manifest();
        assertThat(m).containsEntry("status", "interrupted");
        assertThat((List<Object>) m.get("flags")).contains(Flags.INTERRUPTED);
        assertThat(runs(m)).extracting(x -> x.get("status")).doesNotContain("running");
        assertThat(m.get("environment")).isNotNull();
        assertThat(outBytes.toString()).contains("the full manifest state was written directly")
                .doesNotContain("bench: interrupted; manifest status interrupted");
    }

    @Test
    void aFailingStopRecordDoesNotStopTheCleanupAndNoRunStaysRunning() throws Exception {
        AtomicInteger broken = new AtomicInteger();
        BenchRunner r = runner().withManifestMover((from, to) -> {
            String content = Files.readString(from);
            if (broken.get() > 0 || content.contains("\"method\": \"destroy\"")) { // the first stop record
                broken.set(1);
                throw new java.nio.file.AccessDeniedException(to.toString());
            }
            Files.move(from, to, java.nio.file.StandardCopyOption.REPLACE_EXISTING);
        });

        assertThat(r.execute()).isEqualTo(1);

        assertThat(launcher.children()).noneMatch(FakeChildProcess::isAlive); // simulator stopped too
        Map<String, Object> m = manifest();
        assertThat(m).containsEntry("status", "failed");
        assertThat(runs(m)).extracting(x -> x.get("status")).doesNotContain("running");
        Map<String, Object> run = runs(m).get(0);
        assertThat(run.get("portsFreed")).isEqualTo(true);
        assertThat(run.get("endedAt")).isNotNull();
        Map<?, ?> processes = (Map<?, ?>) run.get("processes");
        assertThat(((Map<?, ?>) processes.get("simulator")).get("stop")).isNotNull();
        assertThat(((Map<?, ?>) run.get("failure")).get("phase")).isEqualTo("harness");
    }

    @Test
    void theFallbackKeepsAnInterruptedStatusInsteadOfTurningItIntoFailed() throws Exception {
        AtomicInteger moves = new AtomicInteger();
        BenchRunner r = runner().withManifestMover((from, to) -> {
            if (moves.incrementAndGet() > 1) { // creation works, marking the interrupt cannot be written
                throw new java.nio.file.AccessDeniedException(to.toString());
            }
            Files.move(from, to, java.nio.file.StandardCopyOption.REPLACE_EXISTING);
        });
        BenchMain.shutdownHook(r).run(); // an interrupt before execute(): the manifest will be marked interrupted

        assertThat(r.execute()).isEqualTo(1);

        Map<String, Object> m = manifest();
        assertThat(m).containsEntry("status", "interrupted");
        assertThat((List<Object>) m.get("flags")).contains(Flags.INTERRUPTED);
        assertThat(m.get("environment")).isNotNull();
        assertThat(runs(m)).extracting(x -> x.get("status")).containsExactly("not_run", "not_run");
        assertThat(outBytes.toString()).contains("the full manifest state was written directly");
    }

    private String manifestDirName() throws IOException {
        try (Stream<Path> s = Files.list(results)) {
            return s.findFirst().orElseThrow().getFileName().toString();
        }
    }

    @Test
    void anInterruptBeforeTheManifestExistsIsRecordedAsSoonAsItDoes() throws Exception {
        BenchRunner r = runner();
        BenchMain.shutdownHook(r).run(); // the hook fires before execute() has created anything
        assertThat(r.execute()).isEqualTo(1);
        Map<String, Object> m = manifest();
        assertThat(m).containsEntry("status", "interrupted");
        assertThat((List<Object>) m.get("flags")).contains(Flags.INTERRUPTED);
        assertThat(runs(m)).extracting(x -> x.get("status")).containsExactly("not_run", "not_run");
        assertThat(launcher.specs()).isEmpty();
    }

    @Test
    void aConcurrentHookMarksTheManifestBeforeWaitingAndStillRecordsStopResults() throws Exception {
        blockWarmupWait = true; // the runner thread sits in loadgen.waitFor until the hook kills the children
        java.util.concurrent.atomic.AtomicReference<String> statusDuringKillWait = new java.util.concurrent.atomic.AtomicReference<>();
        Thread hookThread = Thread.currentThread();
        gatewayOnWait = () -> {
            if (Thread.currentThread() == hookThread) { // the hook's wait for the killed processes
                try {
                    statusDuringKillWait.set((String) manifest().get("status"));
                } catch (IOException ex) {
                    throw new UncheckedIOException(ex);
                }
            }
        };
        BenchRunner r = runner();
        java.util.concurrent.ExecutorService executor = java.util.concurrent.Executors.newSingleThreadExecutor();
        try {
            java.util.concurrent.Future<Integer> exit = executor.submit(r::execute);
            org.awaitility.Awaitility.await().atMost(java.time.Duration.ofSeconds(30))
                    .until(() -> launcher.names().contains("warmup-loadgen"));

            BenchMain.shutdownHook(r).run(); // runs on this thread, like the JVM's hook thread

            // The hook waited for the runner thread: its records exist already, before we even join the thread.
            Map<String, Object> afterHook = runs(manifest()).get(0);
            Map<?, ?> simulatorAfterHook = (Map<?, ?>) ((Map<?, ?>) afterHook.get("processes")).get("simulator");
            assertThat(simulatorAfterHook.get("stop")).isNotNull();
            assertThat(afterHook.get("portsFreed")).isEqualTo(true);

            assertThat(exit.get(30, TimeUnit.SECONDS)).isEqualTo(1);
        } finally {
            executor.shutdownNow();
        }
        assertThat(statusDuringKillWait.get()).isEqualTo("interrupted");
        Map<String, Object> m = manifest();
        assertThat(m).containsEntry("status", "interrupted");
        Map<String, Object> run = runs(m).get(0);
        assertThat(run.get("status")).isEqualTo("interrupted");
        assertThat(run.get("portsFreed")).isEqualTo(true);
        Map<?, ?> processes = (Map<?, ?>) run.get("processes");
        Map<?, ?> stop = (Map<?, ?>) ((Map<?, ?>) processes.get("simulator")).get("stop");
        assertThat(stop).isNotNull();
        assertThat(stop.get("method")).isEqualTo("already_exited");
        assertThat(((Map<?, ?>) ((Map<?, ?>) processes.get("gateway")).get("log")).get("bytesWritten")).isEqualTo(10L);
        assertThat(launcher.children()).noneMatch(FakeChildProcess::isAlive);
        // the final loadgen record is written even though the hook marked the manifest first
        Map<?, ?> warmup = (Map<?, ?>) run.get("warmup");
        Map<?, ?> loadgen = (Map<?, ?>) warmup.get("loadgen");
        assertThat(((Number) loadgen.get("exitCode")).intValue()).isEqualTo(137);
        assertThat(((Number) ((Map<?, ?>) loadgen.get("log")).get("bytesWritten")).longValue()).isEqualTo(10);
        assertThat(warmup.get("outcomes")).isNotNull();
        assertThat(run.get("measured")).isNull();
    }

    @Test
    void ifTheManifestCannotBeBuiltAMinimalFailedManifestIsLeftBehind() throws Exception {
        BenchRunner r = runner();
        Files.delete(Artifacts.resolve(BenchOptionsParser.parse(new String[] {"--output-dir", results.toString()},
                tmp.resolve("repo"))).gatewayJar()); // hashing the jar will now fail
        assertThat(r.execute()).isEqualTo(1);
        Map<String, Object> m = manifest();
        assertThat(m).containsEntry("status", "failed").containsEntry("tool", "streamlab-bench");
        assertThat((String) m.get("harnessFailure")).contains("NoSuchFile");
        assertThat(launcher.specs()).isEmpty();
    }

    @Test
    void theManifestRecordsPidsAndCommandsOfEveryProcess() throws Exception {
        runner().execute();
        Map<?, ?> gateway = (Map<?, ?>) ((Map<?, ?>) runs(manifest()).get(0).get("processes")).get("gateway");
        assertThat(((Number) gateway.get("pid")).longValue()).isPositive();
        assertThat((List<Object>) gateway.get("command")).contains("--lab.connection-mode=shared");
        assertThat(((Map<?, ?>) gateway.get("log")).get("file")).isEqualTo("gateway.log");
    }
}
