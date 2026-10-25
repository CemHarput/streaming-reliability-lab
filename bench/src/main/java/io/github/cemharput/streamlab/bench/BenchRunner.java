package io.github.cemharput.streamlab.bench;

import io.github.cemharput.streamlab.bench.Artifacts.ArtifactInfo;
import io.github.cemharput.streamlab.bench.LoadgenOutputInspector.LoadgenFacts;
import io.github.cemharput.streamlab.bench.PlanBuilder.PlannedRun;
import io.github.cemharput.streamlab.bench.PlanBuilder.RunPlan;
import io.github.cemharput.streamlab.bench.ProcessSpecs.ProcessSpec;
import io.github.cemharput.streamlab.bench.ProcessSupervisor.StopResult;
import io.github.cemharput.streamlab.bench.json.Json;
import java.io.IOException;
import java.io.PrintStream;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;
import java.util.function.IntFunction;
import java.util.function.Supplier;

/**
 * Orchestrates the run set: for every planned cell fresh simulator and gateway JVMs, bounded readiness waits,
 * warm-up and measured loadgen runs, snapshots, bounded stop and port checks. All external effects go through
 * injected interfaces. It analyses nothing.
 */
public final class BenchRunner {

    public static final int EXIT_OK = 0;
    public static final int EXIT_FAILED = 1;

    /** Collaborators; tests replace all of them. */
    public record Services(ProcessLauncher launcher, PortProbe ports, Ticker ticker, IntFunction<GatewayApi> gateway,
            IntFunction<SimulatorApi> simulator, EnvironmentCapture environment, CpuLoadSampler cpu,
            Supplier<Instant> clock) {
    }

    /** Every wait of the protocol is bounded by one of these. */
    public record Timeouts(Duration simulatorReady, Duration gatewayReady, Duration idle, Duration gatewayGraceful,
            Duration gatewayForced, Duration simulatorGraceful, Duration simulatorForced, Duration portsFreed,
            Duration loadgenExtra, Duration loadgenGraceful, Duration loadgenForced, Duration pollInterval,
            Duration hookKill) {

        public static Timeouts defaults() {
            return new Timeouts(Duration.ofSeconds(30), Duration.ofSeconds(90), Duration.ofSeconds(10),
                    Duration.ofSeconds(45), Duration.ofSeconds(10), Duration.ofSeconds(20), Duration.ofSeconds(10),
                    Duration.ofSeconds(30), Duration.ofSeconds(80),
                    ProcessSpecs.LOADGEN_DRAIN_TIMEOUT.plus(Duration.ofSeconds(10)), Duration.ofSeconds(10),
                    Duration.ofMillis(250), Duration.ofSeconds(10));
        }

        Map<String, Object> toMap() {
            Map<String, Object> m = new LinkedHashMap<>();
            m.put("simulatorReadyMs", simulatorReady.toMillis());
            m.put("gatewayReadyMs", gatewayReady.toMillis());
            m.put("idleMs", idle.toMillis());
            m.put("gatewayGracefulMs", gatewayGraceful.toMillis());
            m.put("gatewayForcedMs", gatewayForced.toMillis());
            m.put("simulatorGracefulMs", simulatorGraceful.toMillis());
            m.put("simulatorForcedMs", simulatorForced.toMillis());
            m.put("portsFreedMs", portsFreed.toMillis());
            m.put("loadgenExtraMs", loadgenExtra.toMillis());
            m.put("loadgenGracefulMs", loadgenGraceful.toMillis());
            m.put("loadgenForcedMs", loadgenForced.toMillis());
            m.put("hookKillMs", hookKill.toMillis());
            return m;
        }
    }

    private static final class RunFailure extends Exception {
        private final String phase;

        RunFailure(String phase, String message) {
            super(message);
            this.phase = phase;
        }
    }

    private final BenchOptions options;
    private final RunPlan plan;
    private final Artifacts.Resolved artifacts;
    private final ProcessSpecs specs;
    private final GitInfo git;
    private final OperatorChecklist.Result checklist;
    private final Services services;
    private final Timeouts timeouts;
    private final PrintStream out;
    private final ProcessSupervisor supervisor;
    private final String toolVersion;

    private final java.util.concurrent.CountDownLatch done = new java.util.concurrent.CountDownLatch(1);
    private volatile boolean running;
    private volatile Thread runnerThread;
    private ManifestStore.Mover manifestMover = ManifestStore.defaultMover();
    private ManifestStore.Mover directManifestMover;
    private ManifestStore.TmpWriter directManifestWriter;
    private volatile boolean aborting;
    private volatile ManifestStore store;

    public BenchRunner(BenchOptions options, RunPlan plan, Artifacts.Resolved artifacts, ProcessSpecs specs,
            GitInfo git, OperatorChecklist.Result checklist, Services services, Timeouts timeouts, PrintStream out,
            String toolVersion) {
        this.options = options;
        this.plan = plan;
        this.artifacts = artifacts;
        this.specs = specs;
        this.git = git;
        this.checklist = checklist;
        this.services = services;
        this.timeouts = timeouts;
        this.out = out;
        this.toolVersion = toolVersion;
        this.supervisor = new ProcessSupervisor(services.launcher(), services.ticker());
    }

    /** Test seam: replaces the write of the direct manifest tmp file. */
    BenchRunner withDirectManifestWriter(ManifestStore.TmpWriter writer) {
        this.directManifestWriter = writer;
        return this;
    }

    /** Test seam: replaces the move of the direct manifest fallback. */
    BenchRunner withDirectManifestMover(ManifestStore.Mover mover) {
        this.directManifestMover = mover;
        return this;
    }

    /** Test seam: replaces the final step of every manifest write. */
    BenchRunner withManifestMover(ManifestStore.Mover mover) {
        this.manifestMover = mover;
        return this;
    }

    public ProcessSupervisor supervisor() {
        return supervisor;
    }

    /**
     * Called from the shutdown hook: forbids new processes, force-kills the registered ones and marks the manifest
     * {@code interrupted}. Safe to call at any time and more than once.
     */
    public void abort() {
        aborting = true;
        long deadline = services.ticker().nanoTime() + timeouts.hookKill().toNanos(); // one deadline for the whole hook
        // Kill first, mark the manifest before the (bounded) wait for the processes to die.
        supervisor.killAll(timeouts.hookKill(), this::markInterruptedQuietly);
        // Give the runner thread a bounded chance to record stop results, log counters and portsFreed.
        if (running && Thread.currentThread() != runnerThread) {
            try {
                long remaining = deadline - services.ticker().nanoTime();
                if (remaining > 0) {
                    done.await(remaining, java.util.concurrent.TimeUnit.NANOSECONDS);
                }
            } catch (InterruptedException ex) {
                Thread.currentThread().interrupt();
            }
        }
    }

    private void markInterruptedQuietly() {
        ManifestStore s = store;
        if (s != null) {
            try {
                s.markInterrupted();
            } catch (UncheckedIOException ex) {
                // nothing more can be done while the JVM is going down
            }
        }
    }

    /** Runs the whole set. Returns 0 when every planned run completed, 1 otherwise. */
    public int execute() {
        runnerThread = Thread.currentThread();
        running = true;
        try {
            return executeSet();
        } finally {
            done.countDown();
        }
    }

    private int executeSet() {
        Path dir;
        try {
            dir = RunId.createUniqueDirectory(options.outputDir(), services.clock().get(), git, options.smoke());
        } catch (IOException ex) {
            out.println("bench: cannot create the result directory: " + ex.getMessage());
            return EXIT_FAILED;
        }
        ResultLayout layout = new ResultLayout(dir);
        String runId = dir.getFileName().toString();
        ManifestStore manifest;
        try {
            manifest = createManifest(layout, runId);
        } catch (IOException | RuntimeException ex) {
            boolean minimal = writeMinimalFailedManifest(layout, runId, ex);
            out.println("bench: cannot create the manifest: " + ex.getMessage()
                    + (minimal ? " (a minimal failed manifest was written)" : " (no manifest could be written)"));
            return EXIT_FAILED;
        }
        store = manifest; // an interrupt that arrived earlier is applied by the end-of-run handling below
        out.println("Result directory: " + dir);
        boolean failed = false;
        String harnessFailure = null;
        try {
            for (int i = 0; i < plan.runs().size() && !aborting; i++) {
                PlannedRun run = plan.runs().get(i);
                boolean ok = executeRun(layout, manifest, run);
                if (!ok) {
                    failed = true;
                    break;
                }
                if (i + 1 < plan.runs().size() && !aborting) {
                    services.ticker().sleep(options.cooldown());
                }
            }
        } catch (InterruptedException ex) {
            Thread.currentThread().interrupt();
            aborting = true;
        } catch (RuntimeException ex) {
            failed = true;
            harnessFailure = ex.toString();
        } finally {
            supervisor.killAll(timeouts.hookKill());
        }
        String message = harnessFailure;
        boolean wasFailed = failed;
        String writeProblem = null;
        try {
            if (aborting) {
                manifest.markInterrupted();
            } else {
                manifest.updateIfRunning(root -> finish(root, wasFailed, message));
            }
        } catch (UncheckedIOException ex) {
            writeProblem = ex.getMessage();
        }
        if (writeProblem != null || manifest.isDirty()) {
            // The normal write path failed (now or earlier, e.g. a swallowed error in the hook). One direct attempt
            // writes the complete in-memory state with its current status (a still running manifest becomes
            // failed). If that fails too, manifest.json is untouched; the full state stays in the direct tmp only if the
            // move failed (a tmp that could not be written is removed, see ManifestStore.DirectResult).
            String why = writeProblem != null ? writeProblem : "an earlier manifest write failed";
            ManifestStore.DirectResult direct = manifest.writeDirect(root -> {
                if (Manifest.STATUS_RUNNING.equals(root.get("status"))) {
                    finish(root, true, why);
                }
            });
            out.println("bench: cannot write the manifest normally: " + why + switch (direct) {
                case WRITTEN -> " (the full manifest state was written directly)";
                case TMP_KEPT -> " (manifest.json keeps its last written status; the full state is in "
                        + manifest.directTmp() + ")";
                case NOTHING -> " (manifest.json keeps its last written status; no usable copy of the final state"
                        + " could be written)";
            });
            return EXIT_FAILED;
        }
        if (Manifest.STATUS_INTERRUPTED.equals(manifest.status())) {
            out.println("bench: interrupted; manifest status interrupted");
            return EXIT_FAILED;
        }
        out.println("bench: " + (failed ? "failed" : "completed") + "; manifest " + layout.manifest());
        return failed ? EXIT_FAILED : EXIT_OK;
    }

    /** Best effort: the normal manifest could not be created, so leave at least a failed marker behind. */
    private boolean writeMinimalFailedManifest(ResultLayout layout, String runId, Exception cause) {
        Map<String, Object> m = new LinkedHashMap<>();
        m.put("schemaVersion", Manifest.SCHEMA_VERSION);
        m.put("protocolVersion", Manifest.PROTOCOL_VERSION);
        m.put("runId", runId);
        m.put("tool", Manifest.TOOL);
        m.put("toolVersion", toolVersion);
        m.put("status", Manifest.STATUS_FAILED);
        String finished;
        try {
            finished = services.clock().get().toString();
        } catch (RuntimeException ex) {
            finished = Instant.now().toString();
        }
        m.put("finishedAt", finished);
        m.put("note", Manifest.NOTE);
        m.put("harnessFailure", String.valueOf(cause));
        try {
            Files.writeString(layout.manifest(), Json.write(m), StandardCharsets.UTF_8);
            return true;
        } catch (IOException | RuntimeException ex) {
            return false;
        }
    }

    @SuppressWarnings("unchecked")
    private void finish(Map<String, Object> root, boolean failed, String harnessFailure) {
        root.put("status", failed ? Manifest.STATUS_FAILED : Manifest.STATUS_COMPLETED);
        root.put("finishedAt", services.clock().get().toString());
        for (Object o : (List<Object>) root.get("runs")) {
            Map<String, Object> run = (Map<String, Object>) o;
            if (Manifest.RUN_PLANNED.equals(run.get("status"))) {
                run.put("status", Manifest.RUN_NOT_RUN);
            } else if ("running".equals(run.get("status"))) { // its end record could not be written
                run.put("status", "failed");
                run.put("endedAt", root.get("finishedAt"));
                if (run.get("failure") == null) {
                    Map<String, Object> fm = new LinkedHashMap<>();
                    fm.put("phase", "harness");
                    fm.put("message", harnessFailure != null ? harnessFailure : "the run did not finish");
                    run.put("failure", fm);
                }
            }
        }
        if (harnessFailure != null) {
            root.put("harnessFailure", harnessFailure);
        }
    }

    // ---- manifest -----------------------------------------------------------------------------------------

    private ManifestStore createManifest(ResultLayout layout, String runId) throws IOException {
        Map<String, Object> root = Manifest.newRoot(runId, toolVersion, services.clock().get().toString());
        root.put("preset", options.preset().cliName());
        root.put("commandLine", options.commandLine());
        root.put("operatorNote", options.operatorNote());
        root.put("operatorChecklist", checklistMap());
        List<String> setFlags = new ArrayList<>(checklist.flags());
        if (options.smoke()) {
            setFlags.add(Flags.SMOKE_PRESET);
        }
        if (!git.available()) {
            setFlags.add(Flags.GIT_UNAVAILABLE);
        } else {
            if (git.dirty()) {
                setFlags.add(Flags.GIT_DIRTY);
            }
            if (git.ignoreKnown() && !git.ignoredOutput()) {
                setFlags.add(Flags.OUTPUT_NOT_GITIGNORED);
            }
        }
        EnvironmentCapture.Captured env = services.environment().environment(options, git, specs.removedEnvironment());
        root.put("environment", env.map());
        setFlags.addAll(env.flags());
        root.put("artifacts", artifactsMap(setFlags));
        root.put("parameters", parametersMap());
        List<Object> runs = new ArrayList<>();
        plan.runs().forEach(r -> runs.add(Manifest.newRun(r)));
        root.put("runs", runs);
        root.put("flags", setFlags);
        ManifestStore s = new ManifestStore(layout.manifest(), root, services.clock(), services.ticker(),
                manifestMover);
        if (directManifestMover != null) {
            s.withDirectMover(directManifestMover);
        }
        if (directManifestWriter != null) {
            s.withDirectWriter(directManifestWriter);
        }
        s.write();
        return s;
    }

    private Map<String, Object> checklistMap() {
        return checklist.toMap();
    }

    private Map<String, Object> artifactsMap(List<String> setFlags) throws IOException {
        Map<String, Object> m = new LinkedHashMap<>();
        List<Path> newest = new ArrayList<>();
        m.put("gatewayJar", ArtifactInfo.of(artifacts.gatewayJar()).toMap());
        m.put("loadgenJar", ArtifactInfo.of(artifacts.loadgenJar()).toMap());
        newest.add(artifacts.gatewayJar());
        newest.add(artifacts.loadgenJar());
        List<Object> lib = new ArrayList<>();
        for (Path p : artifacts.loadgenLib()) {
            lib.add(ArtifactInfo.of(p).toMap());
        }
        m.put("loadgenLib", lib);
        List<Object> cp = new ArrayList<>();
        for (Path p : artifacts.simulatorClasspath()) {
            cp.add(ArtifactInfo.of(p).toMap());
            if (p.getFileName().toString().startsWith("simulator-")) {
                newest.add(p);
            }
        }
        m.put("simulatorClasspath", cp);
        m.put("benchJar", artifacts.benchJar() == null ? null : ArtifactInfo.of(artifacts.benchJar()).toMap());
        if (git.commitTime() != null) {
            for (Path p : newest) {
                if (Files.getLastModifiedTime(p).toInstant().isBefore(git.commitTime())) {
                    setFlags.add(Flags.ARTIFACT_OLDER_THAN_HEAD);
                    break;
                }
            }
        }
        return m;
    }

    private Map<String, Object> parametersMap() {
        Map<String, Object> p = new LinkedHashMap<>();
        Map<String, Object> gateway = new LinkedHashMap<>();
        Map<String, String> props = new LinkedHashMap<>(specs.gatewayProperties(options.modeOrder().get(0)));
        props.put("lab.connection-mode", "<per run: shared | per-request>");
        gateway.put("properties", props);
        p.put("gateway", gateway);
        Map<String, Object> simulator = new LinkedHashMap<>();
        simulator.put("env", specs.simulatorEnv());
        p.put("simulator", simulator);
        Map<String, Object> jvm = new LinkedHashMap<>();
        jvm.put("gateway", specs.gatewayJvmOptions());
        jvm.put("simulator", specs.simulatorJvmOptions());
        jvm.put("loadgen", specs.loadgenJvmOptions("<phase>"));
        p.put("jvm", jvm);
        Map<String, Object> loadgen = new LinkedHashMap<>();
        loadgen.put("target", specs.loadgenTarget());
        loadgen.put("fixedArguments", specs.loadgenFixedArguments());
        p.put("loadgen", loadgen);
        Map<String, Object> pl = new LinkedHashMap<>();
        pl.put("concurrency", options.concurrency());
        pl.put("repetitions", options.repetitions());
        pl.put("durationMs", options.duration().toMillis());
        pl.put("warmupMs", options.warmup().toMillis());
        pl.put("rampUpMs", options.rampUp().toMillis());
        pl.put("cooldownMs", options.cooldown().toMillis());
        pl.put("modeOrder", options.modeOrder().stream().map(ConnectionMode::property).toList());
        pl.put("alternation", PlanBuilder.ALTERNATION);
        pl.put("seed", ProcessSpecs.SEED);
        Map<String, Object> ports = new LinkedHashMap<>();
        ports.put("gateway", options.gatewayPort());
        ports.put("simulator", options.simulatorPort());
        pl.put("ports", ports);
        pl.put("timeouts", timeouts.toMap());
        pl.put("logLimitBytes", ProcessSpecs.LOG_LIMIT_BYTES);
        pl.put("cpuBusyThreshold", Flags.CPU_BUSY_THRESHOLD);
        pl.put("cpuWindowMs", options.cpuWindow().toMillis());
        p.put("plan", pl);
        return p;
    }

    // ---- one run ------------------------------------------------------------------------------------------

    @SuppressWarnings("unchecked")
    private static Map<String, Object> runMap(Map<String, Object> root, PlannedRun run) {
        return (Map<String, Object>) ((List<Object>) root.get("runs")).get(run.index() - 1);
    }

    private void updateRun(ManifestStore manifest, PlannedRun run, Consumer<Map<String, Object>> change) {
        manifest.updateIfRunning(root -> change.accept(runMap(root, run)));
    }

    /** Also applies after an interrupt (status stays as it is): stop results, log counters, portsFreed. */
    private void updateRunAlways(ManifestStore manifest, PlannedRun run, Consumer<Map<String, Object>> change) {
        manifest.update(root -> change.accept(runMap(root, run)));
    }

    /** Returns true when the run completed. */
    private boolean executeRun(ResultLayout layout, ManifestStore manifest, PlannedRun run)
            throws InterruptedException {
        GatewayApi gateway = services.gateway().apply(options.gatewayPort());
        try {
            return executeRun(layout, manifest, run, gateway);
        } finally {
            gateway.close();
        }
    }

    private boolean executeRun(ResultLayout layout, ManifestStore manifest, PlannedRun run, GatewayApi gwApi)
            throws InterruptedException {
        Path dir = layout.runDir(run);
        out.println("Run " + run.index() + "/" + plan.runs().size() + ": " + run.id());
        List<String> runFlags = new ArrayList<>();
        String[] phase = {"setup"};
        RunFailure failure = null;
        ChildProcess simulator = null;
        ChildProcess gateway = null;
        Map<String, Object> processes = new LinkedHashMap<>();
        try {
            Files.createDirectories(dir);
            updateRun(manifest, run, m -> {
                m.put("status", "running");
                m.put("startedAt", services.clock().get().toString());
            });

            phase[0] = "port-check";
            for (int port : List.of(options.gatewayPort(), options.simulatorPort())) {
                if (services.ports().listenerPresent(port)) {
                    throw new RunFailure(phase[0], "a listener is already present on port " + port
                            + " (a leftover process from an earlier run?)");
                }
            }
            Map<String, Object> timeWait = services.environment()
                    .timeWait(List.of(options.gatewayPort(), options.simulatorPort()));
            if (timeWait == null) {
                runFlags.add(Flags.TIME_WAIT_UNAVAILABLE);
            }
            updateRun(manifest, run, m -> m.put("timeWaitBefore", timeWait));

            SimulatorApi simApi = services.simulator().apply(options.simulatorPort());

            phase[0] = "simulator-start";
            ProcessSpec simSpec = specs.simulator(dir);
            simulator = startProcess(manifest, run, processes, simSpec, "simulator");
            ChildProcess simFinal = simulator;
            awaitReady(phase[0] = "simulator-ready", () -> simApi.stats().available(), timeouts.simulatorReady(),
                    simFinal, "simulator");

            phase[0] = "gateway-start";
            ProcessSpec gwSpec = specs.gateway(dir, run.mode());
            gateway = startProcess(manifest, run, processes, gwSpec, "gateway");
            ChildProcess gwFinal = gateway;
            awaitReady(phase[0] = "gateway-ready", gwApi::readiness, timeouts.gatewayReady(), gwFinal, "gateway");
            GatewayApi.ModeCheck mode = gwApi.verifyConnectionMode(run.mode().metricTag());
            if (!mode.ok()) {
                runFlags.add(Flags.CONNECTION_MODE_MISMATCH);
                throw new RunFailure("connection-mode", mode.message());
            }

            phase[0] = "snapshot-ready";
            snapshot(layout, manifest, run, dir, "ready", ResultLayout.SNAPSHOT_READY, gwApi, simApi, null);

            phase[0] = "warmup";
            runLoadgen(manifest, run, dir, "warmup", options.warmup());

            phase[0] = "snapshot-pre";
            GatewayApi.IdleResult idleBefore = gwApi.awaitIdle(timeouts.idle());
            Snapshot pre = snapshot(layout, manifest, run, dir, "pre", ResultLayout.SNAPSHOT_PRE, gwApi, simApi,
                    idleBefore);

            phase[0] = "cpu-sample";
            List<Double> cpu = services.cpu().sample(options.cpuWindow());
            runFlags.addAll(CpuLoadSampler.flags(cpu));
            updateRun(manifest, run, m -> m.put("cpuLoadBefore", new ArrayList<>(cpu)));

            phase[0] = "measured";
            runLoadgen(manifest, run, dir, "measured", options.duration());

            phase[0] = "snapshot-post";
            GatewayApi.IdleResult idleAfter = gwApi.awaitIdle(timeouts.idle());
            awaitSimulatorIdle(simApi);
            Snapshot post = snapshot(layout, manifest, run, dir, "post", ResultLayout.SNAPSHOT_POST, gwApi, simApi,
                    idleAfter);
            runFlags.addAll(RunFlags.state(pre, post));

            if (!simFinal.isAlive()) {
                runFlags.add(Flags.SIMULATOR_EXITED);
                throw new RunFailure(phase[0], "the simulator exited during the run");
            }
            if (!gwFinal.isAlive()) {
                runFlags.add(Flags.GATEWAY_EXITED);
                throw new RunFailure(phase[0], "the gateway exited during the run");
            }
        } catch (RunFailure rf) {
            failure = rf;
        } catch (IOException | RuntimeException ex) {
            failure = new RunFailure(phase[0], ex.toString());
        } catch (InterruptedException ex) {
            aborting = true;
            cleanup(manifest, run, processes, simulator, gateway, runFlags);
            throw ex;
        }
        cleanup(manifest, run, processes, simulator, gateway, runFlags);
        if (aborting) {
            updateRunAlways(manifest, run, m -> m.put("flags", new ArrayList<>(runFlags)));
            return false;
        }
        RunFailure f = failure;
        boolean ok = f == null;
        updateRun(manifest, run, m -> {
            m.put("status", ok ? "completed" : "failed");
            m.put("endedAt", services.clock().get().toString());
            if (f != null) {
                Map<String, Object> fm = new LinkedHashMap<>();
                fm.put("phase", f.phase);
                fm.put("message", f.getMessage());
                m.put("failure", fm);
            }
            m.put("flags", new ArrayList<>(runFlags));
        });
        if (!ok) {
            out.println("  failed in " + f.phase + ": " + f.getMessage());
        }
        return ok;
    }

    /**
     * Stops gateway then simulator (bounded), verifies the ports, records stop results and log statistics. A failing
     * manifest write never prevents the remaining stops and the port check; the first such failure is rethrown at
     * the end.
     */
    private void cleanup(ManifestStore manifest, PlannedRun run, Map<String, Object> processes,
            ChildProcess simulator, ChildProcess gateway, List<String> runFlags) throws InterruptedException {
        RuntimeException[] failure = {null};
        try {
            if (gateway != null) {
                StopResult r = supervisor.stop(gateway, timeouts.gatewayGraceful(), timeouts.gatewayForced());
                recording(failure, () -> recordStop(manifest, run, "gateway", gateway, r, runFlags));
            }
            if (simulator != null) {
                StopResult r = supervisor.stop(simulator, timeouts.simulatorGraceful(), timeouts.simulatorForced());
                recording(failure, () -> recordStop(manifest, run, "simulator", simulator, r, runFlags));
            }
        } finally {
            if (simulator != null || gateway != null) {
                boolean freed = services.ports().awaitNoListener(
                        List.of(options.gatewayPort(), options.simulatorPort()), timeouts.portsFreed(),
                        Duration.ofMillis(500), services.ticker());
                if (!freed) {
                    runFlags.add(Flags.PORT_NOT_FREED);
                }
                recording(failure, () -> updateRunAlways(manifest, run, m -> m.put("portsFreed", freed)));
            }
        }
        if (failure[0] != null) {
            throw failure[0];
        }
    }

    private static void recording(RuntimeException[] firstFailure, Runnable record) {
        try {
            record.run();
        } catch (RuntimeException ex) {
            if (firstFailure[0] == null) {
                firstFailure[0] = ex;
            }
        }
    }

    @SuppressWarnings("unchecked")
    private void recordStop(ManifestStore manifest, PlannedRun run, String name, ChildProcess child, StopResult r,
            List<String> runFlags) {
        Map<String, Object> stop = new LinkedHashMap<>();
        stop.put("method", r.method());
        stop.put("exitCode", r.exitCode());
        stop.put("durationMs", r.durationMs());
        if (child.logBytesDropped() > 0 && !runFlags.contains(Flags.LOG_TRUNCATED)) { // size limit only
            runFlags.add(Flags.LOG_TRUNCATED);
        }
        if (child.logWriteError() != null && !runFlags.contains(Flags.LOG_WRITE_ERROR)) {
            runFlags.add(Flags.LOG_WRITE_ERROR);
        }
        updateRunAlways(manifest, run, m -> {
            Map<String, Object> process = (Map<String, Object>) ((Map<String, Object>) m.get("processes")).get(name);
            if (process != null) {
                process.put("stop", stop);
                fillLog((Map<String, Object>) process.get("log"), child);
            }
        });
    }

    private static void fillLog(Map<String, Object> log, ChildProcess child) {
        log.put("bytesWritten", child.logBytesWritten());
        log.put("bytesDropped", child.logBytesDropped());
        log.put("bytesLostAfterWriteError", child.logBytesLostAfterWriteError());
        log.put("writeError", child.logWriteError());
    }

    private ChildProcess startProcess(ManifestStore manifest, PlannedRun run, Map<String, Object> processes,
            ProcessSpec spec, String name) throws IOException {
        ChildProcess child = supervisor.start(spec);
        Map<String, Object> p = new LinkedHashMap<>();
        p.put("pid", child.pid());
        p.put("command", spec.command());
        p.put("envOverrides", spec.envOverrides());
        p.put("startedAt", services.clock().get().toString());
        p.put("stop", null);
        Map<String, Object> log = new LinkedHashMap<>();
        log.put("file", spec.logFile().getFileName().toString());
        log.put("bytesWritten", null);
        log.put("bytesDropped", null);
        log.put("bytesLostAfterWriteError", null);
        log.put("writeError", null);
        p.put("log", log);
        processes.put(name, p);
        updateRun(manifest, run, m -> {
            @SuppressWarnings("unchecked")
            Map<String, Object> all = (Map<String, Object>) m.get("processes");
            all.put(name, p);
        });
        return child;
    }

    private void awaitReady(String phase, java.util.function.BooleanSupplier probe, Duration timeout,
            ChildProcess child, String what) throws RunFailure, InterruptedException {
        Readiness.Outcome o = Readiness.await(probe, timeout, timeouts.pollInterval(), child::isAlive,
                services.ticker());
        switch (o.result()) {
            case READY -> { }
            case PROCESS_DIED -> throw new RunFailure(phase, "the " + what + " exited before it was ready (exit code "
                    + child.exitValue() + ")");
            default -> throw new RunFailure(phase, "the " + what + " was not ready within " + timeout.toSeconds()
                    + " s");
        }
    }

    private void awaitSimulatorIdle(SimulatorApi simApi) throws InterruptedException {
        long deadline = services.ticker().nanoTime() + timeouts.idle().toNanos();
        while (services.ticker().nanoTime() < deadline) {
            SimulatorApi.StatsResult s = simApi.stats();
            if (!s.available() || Long.valueOf(0).equals(s.values().get("active_generations"))) {
                return;
            }
            services.ticker().sleep(timeouts.pollInterval());
        }
    }

    private Snapshot snapshot(ResultLayout layout, ManifestStore manifest, PlannedRun run, Path dir, String name,
            String file, GatewayApi gwApi, SimulatorApi simApi, GatewayApi.IdleResult idle) throws IOException {
        SimulatorApi.StatsResult stats = simApi.stats();
        Snapshot s = new Snapshot(services.clock().get(), gwApi.snapshotEntries(), stats.values(), stats.error());
        Files.writeString(dir.resolve(file), Json.write(s.toMap()), StandardCharsets.UTF_8, StandardOpenOption.CREATE_NEW,
                StandardOpenOption.WRITE);
        Map<String, Object> ref = new LinkedHashMap<>();
        ref.put("file", file);
        ref.put("capturedAt", s.capturedAt().toString());
        if (idle != null) {
            Map<String, Object> im = new LinkedHashMap<>();
            im.put("idle", idle.idle());
            im.put("waitedMs", idle.waitedMillis());
            im.put("lastObserved", idle.message());
            ref.put("gatewayIdleWait", im);
        }
        updateRun(manifest, run, m -> {
            @SuppressWarnings("unchecked")
            Map<String, Object> snaps = (Map<String, Object>) m.get("snapshots");
            snaps.put(name, ref);
        });
        return s;
    }

    private void runLoadgen(ManifestStore manifest, PlannedRun run, Path dir, String phase, Duration duration)
            throws RunFailure, IOException, InterruptedException {
        ProcessSpec spec = specs.loadgen(dir, phase, run.concurrency(), duration);
        long start = services.ticker().nanoTime();
        ChildProcess child = supervisor.start(spec);
        Map<String, Object> lg = new LinkedHashMap<>();
        lg.put("pid", child.pid());
        lg.put("command", spec.command());
        lg.put("exitCode", null);
        lg.put("durationMs", null);
        Map<String, Object> files = new LinkedHashMap<>();
        files.put("jsonl", ResultLayout.loadgenJsonl(phase));
        files.put("summary", ResultLayout.loadgenSummary(phase));
        files.put("log", ResultLayout.loadgenLog(phase));
        lg.put("files", files);
        Map<String, Object> lgLog = new LinkedHashMap<>();
        lgLog.put("bytesWritten", null);
        lgLog.put("bytesDropped", null);
        lgLog.put("bytesLostAfterWriteError", null);
        lgLog.put("writeError", null);
        lg.put("log", lgLog);
        Map<String, Object> phaseMap = new LinkedHashMap<>();
        phaseMap.put("warmup", phase.equals("warmup"));
        phaseMap.put("loadgen", lg);
        phaseMap.put("outcomes", null);
        phaseMap.put("footer", null);
        phaseMap.put("flags", new ArrayList<String>());
        updateRun(manifest, run, m -> m.put(phase, phaseMap));

        boolean exited = child.waitFor(duration.plus(timeouts.loadgenExtra()));
        boolean waitExceeded = !exited;
        // The graceful wait covers the loadgen drain timeout, so summary and footer are not cut off on POSIX.
        supervisor.stop(child, timeouts.loadgenGraceful(), timeouts.loadgenForced());
        Integer exitCode = child.exitValue();
        long durationMs = (services.ticker().nanoTime() - start) / 1_000_000;
        LoadgenFacts facts = LoadgenOutputInspector.inspect(dir.resolve(ResultLayout.loadgenSummary(phase)),
                dir.resolve(ResultLayout.loadgenJsonl(phase)));
        List<String> flags = new ArrayList<>(RunFlags.loadgen(facts, exitCode, waitExceeded, run.concurrency()));
        if (child.logBytesDropped() > 0) {
            flags.add(Flags.LOG_TRUNCATED);
        }
        if (child.logWriteError() != null) {
            flags.add(Flags.LOG_WRITE_ERROR);
        }
        // Also after an interrupt: only this phase's sub-map changes, never a status.
        updateRunAlways(manifest, run, m -> {
            lg.put("exitCode", exitCode);
            lg.put("durationMs", durationMs);
            fillLog(lgLog, child);
            phaseMap.put("outcomes", new LinkedHashMap<>(facts.outcomes()));
            phaseMap.put("footer", facts.footer());
            phaseMap.put("flags", flags);
            m.put(phase, phaseMap);
        });
        if (waitExceeded) {
            throw new RunFailure(phase, "the loadgen run did not finish in time and was stopped");
        }
        if (exitCode != null && exitCode != 0) {
            throw new RunFailure(phase, "the loadgen exited with code " + exitCode + " (see "
                    + ResultLayout.loadgenLog(phase) + ")");
        }
    }
}
