package io.github.cemharput.streamlab.loadgen;

import java.io.IOException;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

/** Command line entry point. {@link #run} never calls {@code System.exit}, so tests can drive it. */
public final class LoadgenMain {

    public static final int EXIT_OK = 0;
    public static final int EXIT_ERROR = 1;
    public static final int EXIT_USAGE = 2;

    /** The longest stop after the drain: 5 s waiting for workers, 5 s for the HttpClient, 5 s for its executor, margin. */
    private static final long HOOK_EXTRA_WAIT_SECONDS = 20;

    private LoadgenMain() {
    }

    public static void main(String[] args) {
        System.exit(run(args, System.out, System.err));
    }

    /**
     * Runs the load generator.
     *
     * @return 0 when the run completed (whatever the request outcomes were), 1 on an I/O or unexpected error,
     *         2 on invalid arguments
     */
    public static int run(String[] args, PrintStream out, PrintStream err) {
        Options options;
        try {
            options = OptionsParser.parse(args);
        } catch (UsageException ex) {
            err.println("loadgen: " + ex.getMessage());
            err.println("Try --help.");
            return EXIT_USAGE;
        }
        if (options.help()) {
            out.print(OptionsParser.USAGE);
            return EXIT_OK;
        }

        LoadRun run = new LoadRun(options, String.join(" ", args));
        long waitSeconds = options.drainTimeout().toSeconds() + HOOK_EXTRA_WAIT_SECONDS;
        // Counted down after the summary is written, so that a signal cannot end the JVM before the output is complete.
        CountDownLatch outputDone = new CountDownLatch(1);
        Thread hook = shutdownHook(run, outputDone, waitSeconds);
        Runtime.getRuntime().addShutdownHook(hook);
        try {
            LoadRun.Result result = run.run();
            out.print(run.summary().toText(result.facts()));
            if (options.summaryOutput() != null) {
                writeSummary(options.summaryOutput(), options.overwrite(), run.summary().toJson(result.facts()));
            }
            return EXIT_OK;
        } catch (IOException ex) {
            err.println("loadgen: I/O error: " + ex.getMessage());
            return EXIT_ERROR;
        } catch (InterruptedException ex) {
            Thread.currentThread().interrupt();
            err.println("loadgen: interrupted");
            return EXIT_ERROR;
        } catch (RuntimeException ex) {
            err.println("loadgen: unexpected error: " + ex);
            return EXIT_ERROR;
        } finally {
            outputDone.countDown();
            try {
                Runtime.getRuntime().removeShutdownHook(hook);
            } catch (IllegalStateException ex) {
                // JVM is already shutting down: the hook is running
            }
        }
    }

    /**
     * The JVM shutdown hook: asks the run to stop and keeps the JVM alive until {@code outputDone} is counted down
     * (the summary is written) or {@code waitSeconds} have passed.
     */
    static Thread shutdownHook(LoadRun run, CountDownLatch outputDone, long waitSeconds) {
        return new Thread(() -> {
            run.stop("signal");
            try {
                outputDone.await(waitSeconds, TimeUnit.SECONDS);
            } catch (InterruptedException ex) {
                Thread.currentThread().interrupt();
            }
        }, "loadgen-shutdown-hook");
    }

    private static void writeSummary(Path file, boolean overwrite, String json) throws IOException {
        StandardOpenOption[] opts = overwrite
                ? new StandardOpenOption[] {StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING,
                    StandardOpenOption.WRITE}
                : new StandardOpenOption[] {StandardOpenOption.CREATE_NEW, StandardOpenOption.WRITE};
        Files.writeString(file, json + System.lineSeparator(), StandardCharsets.UTF_8, opts);
    }
}
