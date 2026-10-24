package io.github.cemharput.streamlab.gateway.stream;

import java.lang.System.Logger;
import java.lang.System.Logger.Level;
import java.time.Duration;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Consumer;

/**
 * The timer of one stream: at most one armed task at a time (first chunk, then idle, or the retry backoff
 * between two upstream calls).
 *
 * <p>All state sits behind a leaf lock. Only {@code schedule} and {@code cancel} run under it:
 * no write, no {@code sendLock}, no registry lock, and the expiry callback runs after the lock
 * was released. A stale task (replaced or cancelled after it was already dequeued) is ignored
 * through its token. The idle timer is armed only while the upstream holds credit, i.e. while
 * silence really is the upstream's doing; it is reset by each chunk <em>received</em>.
 *
 * <p>Every upstream call restarts the cycle ({@link #start}); between two calls only the backoff
 * ({@link #scheduleRetry}) is armed and idle cannot be, because no call is active.
 */
final class StreamTimer {

    private static final Logger LOGGER = System.getLogger(StreamTimer.class.getName());

    /** Identity token of one scheduled task. */
    private static final class Armed {
        final TimeoutKind kind;
        /** Non-null for a retry backoff (then {@code kind} is null); never set for a timeout. */
        final Runnable retryTask;
        ScheduledFuture<?> future;

        Armed(TimeoutKind kind, Runnable retryTask) {
            this.kind = kind;
            this.retryTask = retryTask;
        }
    }

    private final StreamTimeouts timeouts;
    private final Consumer<TimeoutKind> onExpiry;
    private final ReentrantLock timerLock = new ReentrantLock();
    private boolean closed;
    private boolean firstChunkSeen;
    private boolean upstreamDone;
    private boolean callActive;
    private int outstanding;
    private Armed armed;

    StreamTimer(StreamTimeouts timeouts, Consumer<TimeoutKind> onExpiry) {
        this.timeouts = timeouts;
        this.onExpiry = onExpiry;
    }

    /**
     * An upstream call is about to start with {@code credit} messages granted (per call: resets the
     * first-chunk cycle). The first-chunk timer is armed only when there is credit: without credit the
     * upstream cannot send and its silence is not its own doing; it arms on the first grant.
     */
    void start(int credit) {
        timerLock.lock();
        try {
            outstanding = credit;
            firstChunkSeen = false;
            callActive = true;
            disarm();
            if (!closed && !upstreamDone && credit > 0) {
                arm(TimeoutKind.FIRST_CHUNK, timeouts.firstChunk());
            }
        } finally {
            timerLock.unlock();
        }
    }

    /** A chunk arrived from upstream: reset the idle timer if upstream may still send. */
    void chunkReceived(boolean last) {
        timerLock.lock();
        try {
            outstanding--;
            firstChunkSeen = true;
            if (last) {
                upstreamDone = true;
            }
            if (armed != null && armed.retryTask != null) {
                // A stray chunk (no call is active) must not cancel the backoff.
                return;
            }
            disarm();
            if (!closed && !upstreamDone && callActive && outstanding > 0) {
                arm(TimeoutKind.IDLE, timeouts.idleBetweenChunks());
            }
        } finally {
            timerLock.unlock();
        }
    }

    /** One credit is about to be handed back to upstream (call right before {@code request(1)}). */
    void creditGranted() {
        creditGranted(1);
    }

    /** {@code count} credits are about to be handed to the current call. */
    void creditGranted(int count) {
        timerLock.lock();
        try {
            outstanding += count;
            if (!closed && !upstreamDone && callActive && armed == null && outstanding > 0) {
                if (firstChunkSeen) {
                    arm(TimeoutKind.IDLE, timeouts.idleBetweenChunks());
                } else {
                    arm(TimeoutKind.FIRST_CHUNK, timeouts.firstChunk());
                }
            }
        } finally {
            timerLock.unlock();
        }
    }

    /** onError / onCompleted was recorded: there is nothing left to wait for. */
    void upstreamEnded() {
        timerLock.lock();
        try {
            upstreamDone = true;
            disarm();
        } finally {
            timerLock.unlock();
        }
    }

    /** The current call ended with a retryable status: no timeout can fire until the next {@link #start}. */
    void callEnded() {
        timerLock.lock();
        try {
            callActive = false;
            disarm();
        } finally {
            timerLock.unlock();
        }
    }

    /**
     * Occupies the single slot with a backoff of {@code delay}; {@code task} runs on the scheduler thread without
     * any lock and must only hand off. Returns false when the timer is closed or the scheduler is not running.
     */
    boolean scheduleRetry(Duration delay, Runnable task) {
        timerLock.lock();
        try {
            if (closed) {
                return false;
            }
            disarm();
            Armed token = new Armed(null, task);
            try {
                token.future = timeouts.schedule(() -> fire(token), delay);
                armed = token;
                return true;
            } catch (RejectedExecutionException ex) {
                LOGGER.log(Level.DEBUG, "timeout scheduler not running; retry not scheduled");
                return false;
            }
        } finally {
            timerLock.unlock();
        }
    }

    /** Whether the armed task is a retry backoff (tests). */
    boolean retryPending() {
        timerLock.lock();
        try {
            return armed != null && armed.retryTask != null;
        } finally {
            timerLock.unlock();
        }
    }

    /** Terminal outcome claimed: cancel and never arm again. Idempotent. */
    void close() {
        timerLock.lock();
        try {
            closed = true;
            disarm();
        } finally {
            timerLock.unlock();
        }
    }

    boolean isArmed() {
        timerLock.lock();
        try {
            return armed != null;
        } finally {
            timerLock.unlock();
        }
    }

    TimeoutKind armedKind() {
        timerLock.lock();
        try {
            return armed == null ? null : armed.kind;
        } finally {
            timerLock.unlock();
        }
    }

    /** Caller holds the lock. */
    private void arm(TimeoutKind kind, Duration delay) {
        disarm();
        Armed token = new Armed(kind, null);
        try {
            // The task cannot pass its token check before this method released the lock.
            token.future = timeouts.schedule(() -> fire(token), delay);
            armed = token;
        } catch (RejectedExecutionException ex) {
            LOGGER.log(Level.DEBUG, "timeout scheduler not running; {0} timer not armed", kind);
        }
    }

    /** Caller holds the lock. */
    private void disarm() {
        if (armed != null) {
            armed.future.cancel(false);
            armed = null;
        }
    }

    private void fire(Armed token) {
        timerLock.lock();
        try {
            if (armed != token) {
                return;
            }
            armed = null;
        } finally {
            timerLock.unlock();
        }
        if (token.retryTask != null) {
            token.retryTask.run();
        } else {
            onExpiry.accept(token.kind);
        }
    }
}
