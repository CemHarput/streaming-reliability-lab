package io.github.cemharput.streamlab.gateway.stream;

import io.github.cemharput.streamlab.gateway.LifecyclePhases;
import io.github.cemharput.streamlab.gateway.config.GatewayProperties;
import io.github.cemharput.streamlab.gateway.config.GatewayProperties.Limits;
import io.github.cemharput.streamlab.gateway.config.GatewayProperties.OverloadPolicy;
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
 * Single source of truth for stream capacity and owner of the shutdown sequence
 * (reject new streams, drain existing ones within the drain timeout, force-cancel the rest).
 *
 * <p>Two sets are kept under one lock. {@code tracked} holds every stream whose final event
 * has not been written yet; shutdown drains and force-cancels this set. {@code admitted}
 * (a subset) holds the streams that own a permit; capacity is checked against it. A
 * stream gives up its permit as soon as its terminal outcome is claimed, so a stuck final
 * write does not leak capacity, but it stays tracked until the write returns.
 *
 * <p>Waiting for a permit is not fair: a new request may overtake a waiting one.
 */
@Component
public class ActiveStreamRegistry implements SmartLifecycle {

    static final Duration FORCED_CANCEL_WAIT = Duration.ofSeconds(2);
    private static final Logger LOGGER = System.getLogger(ActiveStreamRegistry.class.getName());

    private final Duration drainTimeout;
    private final ReentrantLock lock = new ReentrantLock();
    private final Condition emptied = lock.newCondition();
    private final Condition permitFreed = lock.newCondition();
    private final Set<StreamBridge> active = new HashSet<>();
    private final Set<StreamBridge> admitted = new HashSet<>();
    private final Limits limits;
    private boolean accepting;
    private int waiting;
    private volatile boolean running;

    public ActiveStreamRegistry(GatewayProperties properties) {
        this.drainTimeout = properties.shutdown().drainTimeout();
        this.limits = properties.limits();
    }

    enum Admission {
        ADMITTED, REJECTED_LIMIT, REJECTED_QUEUE_TIMEOUT, REJECTED_SHUTDOWN, REJECTED_INTERRUPTED
    }

    /** Non-waiting attempt: admission check and registration are atomic. */
    boolean tryRegister(StreamBridge bridge) {
        lock.lock();
        try {
            if (!accepting || admitted.size() >= limits.maxActiveStreams()) {
                return false;
            }
            addAdmitted(bridge);
            return true;
        } finally {
            lock.unlock();
        }
    }

    /**
     * Applies the overload policy. In WAIT mode this blocks the calling (virtual) request
     * thread for at most {@code queue-wait}; {@link #stop()} wakes waiters immediately.
     */
    Admission admit(StreamBridge bridge) {
        lock.lock();
        try {
            if (!accepting) {
                return Admission.REJECTED_SHUTDOWN;
            }
            if (admitted.size() < limits.maxActiveStreams()) {
                addAdmitted(bridge);
                return Admission.ADMITTED;
            }
            if (limits.overloadPolicy() == OverloadPolicy.REJECT || waiting >= limits.maxWaiting()) {
                return Admission.REJECTED_LIMIT;
            }
            waiting++;
            try {
                long nanos = limits.queueWait().toNanos();
                while (true) {
                    nanos = permitFreed.awaitNanos(nanos);
                    if (!accepting) {
                        return Admission.REJECTED_SHUTDOWN;
                    }
                    if (admitted.size() < limits.maxActiveStreams()) {
                        addAdmitted(bridge);
                        return Admission.ADMITTED;
                    }
                    if (nanos <= 0) {
                        return Admission.REJECTED_QUEUE_TIMEOUT;
                    }
                }
            } catch (InterruptedException ex) {
                Thread.currentThread().interrupt();
                return Admission.REJECTED_INTERRUPTED;
            } finally {
                waiting--;
            }
        } finally {
            lock.unlock();
        }
    }

    private void addAdmitted(StreamBridge bridge) {
        active.add(bridge);
        admitted.add(bridge);
    }

    /** Gives the permit back; idempotent. The stream stays tracked until {@link #unregister}. */
    void releasePermit(StreamBridge bridge) {
        lock.lock();
        try {
            if (admitted.remove(bridge)) {
                permitFreed.signalAll();
            }
        } finally {
            lock.unlock();
        }
    }

    void unregister(StreamBridge bridge) {
        lock.lock();
        try {
            if (admitted.remove(bridge)) {
                permitFreed.signalAll();
            }
            active.remove(bridge);
            if (active.isEmpty()) {
                emptied.signalAll();
            }
        } finally {
            lock.unlock();
        }
    }

    /** Streams that hold a permit. */
    public int admittedCount() {
        lock.lock();
        try {
            return admitted.size();
        } finally {
            lock.unlock();
        }
    }

    /** Requests currently waiting for a permit. */
    public int waitingCount() {
        lock.lock();
        try {
            return waiting;
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
            // Waiters hold no permit, so stop() does not wait for them; they answer 503 now.
            permitFreed.signalAll();
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
                // Streams whose terminal outcome is already claimed cannot be cancelled. They no
                // longer hold a permit or lease (capacity was released at the claim), but they
                // stay tracked until their last event write returns (see README, "Known limits").
                int stillFinishing = activeCount();
                if (stillFinishing > 0) {
                    LOGGER.log(Level.WARNING, "{0} stream(s) still finishing a final event write after forced cancel: they hold no permit or lease any more but stay tracked until the write returns", stillFinishing);
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
