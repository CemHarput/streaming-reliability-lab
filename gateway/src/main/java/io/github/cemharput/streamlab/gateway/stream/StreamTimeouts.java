package io.github.cemharput.streamlab.gateway.stream;

import io.github.cemharput.streamlab.gateway.LifecyclePhases;
import io.github.cemharput.streamlab.gateway.config.GatewayProperties;
import java.lang.System.Logger;
import java.lang.System.Logger.Level;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import org.springframework.context.SmartLifecycle;
import org.springframework.stereotype.Component;

/**
 * One shared scheduler for all per-stream timeout timers, plus the configured limits.
 *
 * <p>A single daemon thread fires the timers; the expiry path only does non-blocking work
 * (claim, cancel upstream, release capacity) and hands the client write to another thread,
 * so a stuck client can never delay another stream's timer. Cancelled timers leave the
 * queue at once ({@code removeOnCancel}), so {@link #pendingTimers()} equals the number of
 * armed timers and is the leak indicator the tests use.
 *
 * <p>Lifecycle: starts before stream admission and stops after it (see {@link LifecyclePhases}).
 */
@Component
public class StreamTimeouts implements SmartLifecycle {

    static final Duration STOP_WAIT = Duration.ofSeconds(1);
    private static final Logger LOGGER = System.getLogger(StreamTimeouts.class.getName());

    private final Duration firstChunk;
    private final Duration idleBetweenChunks;
    private volatile ScheduledThreadPoolExecutor executor;

    public StreamTimeouts(GatewayProperties properties) {
        this.firstChunk = properties.timeouts().firstChunk();
        this.idleBetweenChunks = properties.timeouts().idleBetweenChunks();
    }

    public Duration firstChunk() {
        return firstChunk;
    }

    public Duration idleBetweenChunks() {
        return idleBetweenChunks;
    }

    /** Throws {@link RejectedExecutionException} when the scheduler is not running. */
    ScheduledFuture<?> schedule(Runnable task, Duration delay) {
        ScheduledThreadPoolExecutor current = executor;
        if (current == null) {
            throw new RejectedExecutionException("stream timeouts scheduler is not running");
        }
        return current.schedule(task, delay.toNanos(), TimeUnit.NANOSECONDS);
    }

    /** Armed timers right now; 0 when stopped. */
    public int pendingTimers() {
        ScheduledThreadPoolExecutor current = executor;
        return current == null ? 0 : current.getQueue().size();
    }

    @Override
    public synchronized void start() {
        if (executor != null) {
            return;
        }
        ScheduledThreadPoolExecutor created = new ScheduledThreadPoolExecutor(1, runnable -> {
            Thread thread = new Thread(runnable, "stream-timeouts");
            thread.setDaemon(true);
            return thread;
        });
        created.setRemoveOnCancelPolicy(true);
        created.setExecuteExistingDelayedTasksAfterShutdownPolicy(false);
        executor = created;
    }

    @Override
    public void stop() {
        ScheduledThreadPoolExecutor stopping;
        synchronized (this) {
            stopping = executor;
            executor = null;
        }
        if (stopping == null) {
            return;
        }
        List<Runnable> dropped = stopping.shutdownNow();
        if (!dropped.isEmpty()) {
            LOGGER.log(Level.WARNING, "{0} stream timer(s) were still armed at stop (leak indicator)", dropped.size());
        }
        try {
            if (!stopping.awaitTermination(STOP_WAIT.toNanos(), TimeUnit.NANOSECONDS)) {
                LOGGER.log(Level.WARNING, "stream timeouts scheduler did not terminate within {0}", STOP_WAIT);
            }
        } catch (InterruptedException ex) {
            Thread.currentThread().interrupt();
        }
    }

    @Override
    public boolean isRunning() {
        return executor != null;
    }

    @Override
    public int getPhase() {
        return LifecyclePhases.STREAM_TIMEOUTS;
    }
}
