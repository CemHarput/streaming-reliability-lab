package io.github.cemharput.streamlab.bench;

import java.io.PrintStream;
import java.nio.file.Path;
import java.time.Instant;
import java.util.List;
import java.util.function.Supplier;

/**
 * Command line entry point of the benchmark protocol runner. {@link #run} never calls {@code System.exit}.
 * Exit codes: 0 all planned runs completed, 1 harness or run failure after the result directory exists (the manifest
 * says {@code failed}; exceptions: if the directory itself cannot be created there is neither directory nor
 * manifest, if not even a minimal manifest can be written there is no manifest, and if writing the manifest keeps
 * failing during the run one direct write (second temp file, atomic move) of the complete in-memory manifest with its
 * current status is attempted, and if that fails too the file keeps its last written status, for example
 * {@code running}, while the full state stays in {@code manifest.json.direct.tmp} if that file could be written; a leftover
 * {@code manifest.json.tmp} may remain),
 * 2 invalid arguments or failed precondition (nothing started). An interrupt (Ctrl+C) ends the JVM with the exit
 * status the JVM chooses; the manifest then says {@code interrupted}.
 */
public final class BenchMain {

    public static final int EXIT_OK = 0;
    public static final int EXIT_FAILED = 1;
    public static final int EXIT_USAGE = 2;

    /** System access; tests replace parts of it. */
    public record Context(CommandRunner commands, EnvironmentCapture.Platform platform, PortProbe ports,
            Ticker ticker, ProcessLauncher launcher, Supplier<Instant> clock, String javaHome,
            java.util.function.IntFunction<GatewayApi> gateway, java.util.function.IntFunction<SimulatorApi> simulator,
            BenchRunner.Timeouts timeouts) {

        public static Context system() {
            return new Context(new SystemCommandRunner(), EnvironmentCapture.Platform.current(), PortProbe.connect(),
                    Ticker.SYSTEM, new JdkProcessLauncher(ProcessSpecs.LOG_LIMIT_BYTES), Instant::now,
                    System.getProperty("java.home"), port -> new GatewayProbe(port, Ticker.SYSTEM),
                    SimulatorStatsClient::new, BenchRunner.Timeouts.defaults());
        }
    }

    private BenchMain() {
    }

    public static void main(String[] args) {
        System.exit(run(args, System.out, System.err, OperatorChecklist.Terminal.system(),
                Path.of("").toAbsolutePath(), Context.system()));
    }

    public static int run(String[] args, PrintStream out, PrintStream err, OperatorChecklist.Terminal terminal,
            Path cwd, Context ctx) {
        BenchOptions options;
        try {
            options = BenchOptionsParser.parse(args, cwd);
        } catch (UsageException ex) {
            return usage(err, ex);
        }
        if (options.help()) {
            out.print(BenchOptionsParser.USAGE);
            return EXIT_OK;
        }

        Artifacts.Resolved artifacts = Artifacts.resolve(options);
        EnvironmentCapture environment = new EnvironmentCapture(ctx.commands(), ctx.platform());
        GitInfo git = environment.git(options.repoRoot(), options.outputDir());
        boolean windows = ctx.platform().windows();
        String javaBin = ProcessSpecs.javaBinary(ctx.javaHome(), windows);
        int schedulerThreads = Runtime.getRuntime().availableProcessors();
        PlanBuilder.RunPlan plan = PlanBuilder.build(options);

        if (options.dryRun()) {
            ProcessSpecs display = new ProcessSpecs(javaBin, ctx.platform().env(), options,
                    DryRun.forDisplay(artifacts), schedulerThreads);
            DryRun.print(out, options, artifacts, display, plan, RunId.format(ctx.clock().get(), git, options.smoke()),
                    display.removedEnvironment());
            return EXIT_OK;
        }

        try {
            if (!artifacts.complete()) {
                throw new UsageException("artifact problems: " + String.join("; ", artifacts.problems()));
            }
            for (int port : List.of(options.gatewayPort(), options.simulatorPort())) {
                if (ctx.ports().listenerPresent(port)) {
                    throw new UsageException("a listener is already present on 127.0.0.1:" + port
                            + " (a leftover gateway/simulator from an earlier run? Find it with"
                            + " 'Get-NetTCPConnection -LocalPort " + port + " -State Listen' or 'ss -ltnp')");
                }
            }
            OperatorChecklist.requireTerminal(options.checklist(), terminal);
        } catch (UsageException ex) {
            return usage(err, ex);
        }

        OperatorChecklist.Result checklist;
        try {
            checklist = OperatorChecklist.run(options.checklist(), terminal, out);
        } catch (UsageException ex) {
            return usage(err, ex);
        }

        ProcessSpecs specs = new ProcessSpecs(javaBin, ctx.platform().env(), options, artifacts, schedulerThreads);
        BenchRunner.Services services = new BenchRunner.Services(ctx.launcher(), ctx.ports(), ctx.ticker(),
                ctx.gateway(), ctx.simulator(), environment, new CpuLoadSampler(CpuLoadSampler.osLoad(), ctx.ticker()),
                ctx.clock());
        String version = BenchMain.class.getPackage().getImplementationVersion();
        BenchRunner runner = new BenchRunner(options, plan, artifacts, specs, git, checklist, services,
                ctx.timeouts(), out, version == null ? "dev" : version);
        Thread hook = shutdownHook(runner);
        Runtime.getRuntime().addShutdownHook(hook);
        try {
            return runner.execute();
        } finally {
            try {
                Runtime.getRuntime().removeShutdownHook(hook);
            } catch (IllegalStateException ex) {
                // the JVM is already shutting down: the hook is running
            }
        }
    }

    /** The Ctrl+C / termination hook: kills the registered children, marks the manifest interrupted. */
    public static Thread shutdownHook(BenchRunner runner) {
        return new Thread(runner::abort, "bench-shutdown");
    }

    private static int usage(PrintStream err, UsageException ex) {
        err.println("bench: " + ex.getMessage());
        err.println("Try --help.");
        return EXIT_USAGE;
    }
}
