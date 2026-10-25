package io.github.cemharput.streamlab.loadgen;

import io.github.cemharput.streamlab.loadgen.RecordWriter.RequestRecord;
import io.github.cemharput.streamlab.loadgen.client.Outcome;
import io.github.cemharput.streamlab.loadgen.client.StreamClient;
import io.github.cemharput.streamlab.loadgen.client.StreamParams;
import io.github.cemharput.streamlab.loadgen.client.StreamResult;
import java.io.IOException;
import java.net.http.HttpClient;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

/**
 * The closed-model run: {@code concurrency} workers (virtual threads), each opening one stream after the other.
 * Worker {@code w} starts at {@code rampUp * w / concurrency} (linear ramp). The run ends when the duration is
 * over, when all requests were started and finished, or on {@link #stop}; open streams then get the drain
 * timeout, the rest is interrupted and recorded as {@code INTERRUPTED}.
 */
public final class LoadRun {

    /** Result of a finished run. */
    public record Result(String stopReason, long elapsedNanos, long requestsStarted, long recordsWritten,
            Summary.RunFacts facts) {
    }

    private static final long CLIENT_SHUTDOWN_SECONDS = 5;

    private final Options options;
    private final String commandLine;
    private final Summary summary = new Summary();
    private final CountDownLatch stopSignal = new CountDownLatch(1);
    /** Wakes workers that wait for their ramp-up slot: counted down by {@link #stop} and when the request budget is used up. */
    private final CountDownLatch wake = new CountDownLatch(1);
    private final CountDownLatch finished = new CountDownLatch(1);
    private final AtomicReference<String> stopReason = new AtomicReference<>();
    private final AtomicLong nextIndex = new AtomicLong();
    private final AtomicLong started = new AtomicLong();

    private volatile RecordWriter writer;

    public LoadRun(Options options, String commandLine) {
        this.options = options;
        this.commandLine = commandLine;
    }

    public Summary summary() {
        return summary;
    }

    /** Asks the run to stop starting new requests. The first reason wins. Safe from any thread. */
    public void stop(String reason) {
        stopReason.compareAndSet(null, reason);
        stopSignal.countDown();
        wake.countDown();
    }

    public boolean isStopping() {
        return stopSignal.getCount() == 0;
    }

    /** Waits until {@link #run} has completely finished (also when it failed). */
    public boolean awaitFinished(long timeout, TimeUnit unit) throws InterruptedException {
        return finished.await(timeout, unit);
    }

    /** Runs to completion on the calling thread. Call once. */
    public Result run() throws IOException, InterruptedException {
        try {
            return runInternal();
        } finally {
            finished.countDown();
        }
    }

    private Result runInternal() throws IOException, InterruptedException {
        long runStart = System.nanoTime();
        RecordWriter w = new RecordWriter(options.output(), options.overwrite(), summary);
        writer = w;
        w.onFailure(() -> stop("writer_error")); // a failing disk ends the run instead of letting it idle on
        ScheduledExecutorService timer = null;
        ExecutorService httpExecutor = Executors.newVirtualThreadPerTaskExecutor();
        HttpClient http = null;
        try {
            w.start(RecordWriter.headerLine(options, Instant.now(), toolVersion(), commandLine));
            http = HttpClient.newBuilder()
                    .version(HttpClient.Version.HTTP_1_1)
                    .connectTimeout(options.connectTimeout())
                    .executor(httpExecutor)
                    .build();
            StreamClient client = new StreamClient(http, options.target(), options.idleTimeout(), summary::recordInterChunk);
            RequestVariation variation = RequestVariation.from(options);

            int n = options.concurrency();
            CountDownLatch allDone = new CountDownLatch(n);
            AtomicInteger running = new AtomicInteger(n);
            List<Thread> workers = new ArrayList<>(n);
            Thread.Builder builder = Thread.ofVirtual().name("loadgen-worker-", 0);
            for (int i = 0; i < n; i++) {
                int worker = i;
                workers.add(builder.unstarted(() -> {
                    try {
                        workerLoop(worker, runStart, client, variation);
                    } finally {
                        allDone.countDown();
                        if (running.decrementAndGet() == 0) {
                            stop("requests");
                        }
                    }
                }));
            }
            if (options.duration() != null) {
                timer = Executors.newSingleThreadScheduledExecutor(r -> {
                    Thread t = new Thread(r, "loadgen-duration-timer");
                    t.setDaemon(true);
                    return t;
                });
                timer.schedule(() -> stop("duration"), options.duration().toNanos(), TimeUnit.NANOSECONDS);
            }
            workers.forEach(Thread::start);

            stopSignal.await();
            long drainDeadline = System.nanoTime() + options.drainTimeout().toNanos();
            if (!allDone.await(Math.max(0, drainDeadline - System.nanoTime()), TimeUnit.NANOSECONDS)) {
                workers.forEach(Thread::interrupt);
                allDone.await(CLIENT_SHUTDOWN_SECONDS, TimeUnit.SECONDS);
            }
            long elapsed = System.nanoTime() - runStart;
            shutdown(http, httpExecutor);
            http = null;

            String reason = stopReason.get();
            long startedCount = started.get();
            w.finish(() -> RecordWriter.footerLine(Instant.now(), elapsed, reason, startedCount,
                    w.recordsWritten(), summary.count(Outcome.INTERRUPTED), summary));
            Summary.RunFacts facts = new Summary.RunFacts(options.target().toString(), options.concurrency(),
                    elapsed, reason, startedCount, w.recordsWritten());
            return new Result(reason, elapsed, startedCount, w.recordsWritten(), facts);
        } finally {
            if (timer != null) {
                timer.shutdownNow();
            }
            if (http != null) {
                shutdown(http, httpExecutor);
            }
            w.close();
        }
    }

    private void workerLoop(int worker, long runStart, StreamClient client, RequestVariation variation) {
        long scheduled = options.rampUp().toNanos() * worker / options.concurrency();
        try {
            while (true) {
                long wait = scheduled - (System.nanoTime() - runStart);
                if (wait > 0 && wake.await(wait, TimeUnit.NANOSECONDS) && isStopping()) {
                    return;
                }
                if (isStopping() || writer.failure() != null) {
                    return;
                }
                long index = nextIndex.getAndIncrement();
                if (options.requests() != null && index >= options.requests()) {
                    wake.countDown(); // the budget is used up: workers still waiting for their ramp slot need not wait
                    return;
                }
                StreamParams params = variation.forIndex(index);
                started.incrementAndGet();
                StreamResult result = client.execute(params, runStart);
                writer.submit(new RequestRecord(index, worker, params, scheduled, result));
                if (Thread.currentThread().isInterrupted()) {
                    return;
                }
                scheduled = result.endOffsetNanos();
                if (result.outcome() == Outcome.REJECTED) {
                    scheduled += options.rejectPause().toNanos();
                }
            }
        } catch (InterruptedException ex) {
            Thread.currentThread().interrupt();
        }
    }

    private static void shutdown(HttpClient http, ExecutorService executor) throws InterruptedException {
        http.shutdownNow();
        http.awaitTermination(java.time.Duration.ofSeconds(CLIENT_SHUTDOWN_SECONDS));
        executor.shutdownNow();
        executor.awaitTermination(CLIENT_SHUTDOWN_SECONDS, TimeUnit.SECONDS);
    }

    private static String toolVersion() {
        String v = LoadRun.class.getPackage().getImplementationVersion();
        return v == null ? "unknown" : v;
    }
}
