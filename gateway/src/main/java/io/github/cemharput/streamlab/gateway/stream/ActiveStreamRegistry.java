package io.github.cemharput.streamlab.gateway.stream;

import io.github.cemharput.streamlab.gateway.LifecyclePhases;
import io.github.cemharput.streamlab.gateway.config.GatewayProperties;
import java.lang.System.Logger;
import java.lang.System.Logger.Level;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;
import org.springframework.context.SmartLifecycle;
import org.springframework.stereotype.Component;

/**
 * Tracks active streams and owns the shutdown sequence: reject new streams,
 * drain existing ones within the drain timeout, force-cancel the rest.
 */
@Component
public class ActiveStreamRegistry implements SmartLifecycle {

    static final Duration FORCED_CANCEL_WAIT = Duration.ofSeconds(2);
    private static final Logger LOGGER = System.getLogger(ActiveStreamRegistry.class.getName());

    private final Duration drainTimeout;
    private final ReentrantLock lock = new ReentrantLock();
    private final Condition emptied = lock.newCondition();
    private final Set<StreamBridge> active = new HashSet<>();
    private boolean accepting;
    private volatile boolean running;

    public ActiveStreamRegistry(GatewayProperties properties) {
        this.drainTimeout = properties.shutdown().drainTimeout();
    }

    /** Admission check and registration are atomic. */
    boolean tryRegister(StreamBridge bridge) {
        lock.lock();
        try {
            if (!accepting) {
                return false;
            }
            active.add(bridge);
            return true;
        } finally {
            lock.unlock();
        }
    }

    void unregister(StreamBridge bridge) {
        lock.lock();
        try {
            active.remove(bridge);
            if (active.isEmpty()) {
                emptied.signalAll();
            }
        } finally {
            lock.unlock();
        }
    }

    public boolean isAccepting() {
        lock.lock();
        try {
            return accepting;
        } finally {
            lock.unlock();
        }
    }

    public int activeCount() {
        lock.lock();
        try {
            return active.size();
        } finally {
            lock.unlock();
        }
    }

    @Override
    public void start() {
        lock.lock();
        try {
            accepting = true;
        } finally {
            lock.unlock();
        }
        running = true;
    }

    @Override
    public void stop() {
        lock.lock();
        try {
            accepting = false;
        } finally {
            lock.unlock();
        }
        boolean interrupted = false;
        try {
            int remaining;
            try {
                remaining = awaitEmpty(drainTimeout);
            } catch (InterruptedException ex) {
                interrupted = true;
                remaining = activeCount();
            }
            if (remaining > 0) {
                LOGGER.log(Level.WARNING, "{0} stream(s) still active after drain timeout {1}; cancelling those that can be cancelled", remaining, drainTimeout);
                CompletableFuture<Void> terminalEvents = cancelAll();
                if (!interrupted) {
                    interrupted = awaitTerminalEvents(terminalEvents);
                }
                // Streams already completing cannot be cancelled; they keep their slot until
                // their final write returns (see README, "Known limits").
                int stillHeld = activeCount();
                if (stillHeld > 0) {
                    LOGGER.log(Level.WARNING, "{0} stream(s) still hold a slot after forced cancel: their final write is in progress and cannot be cancelled", stillHeld);
                }
            }
        } finally {
            running = false;
            if (interrupted) {
                Thread.currentThread().interrupt();
            }
        }
    }

    /** Cancels every remaining stream in parallel; upstream is cancelled before this returns. */
    private CompletableFuture<Void> cancelAll() {
        List<StreamBridge> snapshot;
        lock.lock();
        try {
            snapshot = new ArrayList<>(active);
        } finally {
            lock.unlock();
        }
        return CompletableFuture.allOf(
                snapshot.stream().map(StreamBridge::cancelForShutdown).toArray(CompletableFuture<?>[]::new));
    }

    /** Waits at most {@link #FORCED_CANCEL_WAIT} in total; returns true when interrupted. */
    private boolean awaitTerminalEvents(CompletableFuture<Void> terminalEvents) {
        try {
            terminalEvents.get(FORCED_CANCEL_WAIT.toNanos(), TimeUnit.NANOSECONDS);
        } catch (TimeoutException | ExecutionException ex) {
            LOGGER.log(Level.ERROR, "Terminal events of cancelled streams not delivered within {0}", FORCED_CANCEL_WAIT);
        } catch (InterruptedException ex) {
            return true;
        }
        return false;
    }

    private int awaitEmpty(Duration timeout) throws InterruptedException {
        long nanos = timeout.toNanos();
        lock.lock();
        try {
            while (!active.isEmpty() && nanos > 0) {
                nanos = emptied.awaitNanos(nanos);
            }
            return active.size();
        } finally {
            lock.unlock();
        }
    }

    @Override
    public boolean isRunning() {
        return running;
    }

    @Override
    public int getPhase() {
        return LifecyclePhases.STREAM_ADMISSION;
    }
}
