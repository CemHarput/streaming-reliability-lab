package io.github.cemharput.streamlab.gateway.stream;

import java.lang.System.Logger;
import java.lang.System.Logger.Level;
import java.time.Duration;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Consumer;

/**
 * The timeout timer of one stream: at most one armed task at a time (first chunk, then idle).
 *
 * <p>All state sits behind a leaf lock. Only {@code schedule} and {@code cancel} run under it:
 * no write, no {@code sendLock}, no registry lock, and the expiry callback runs after the lock
 * was released. A stale task (replaced or cancelled after it was already dequeued) is ignored
 * through its token. The idle timer is armed only while the upstream holds credit, i.e. while
 * silence really is the upstream's doing; it is reset by each chunk <em>received</em>.
 */
final class StreamTimer {

    private static final Logger LOGGER = System.getLogger(StreamTimer.class.getName());

    /** Identity token of one scheduled task. */
    private static final class Armed {
        final TimeoutKind kind;
        ScheduledFuture<?> future;

        Armed(TimeoutKind kind) {
            this.kind = kind;
        }
    }

    private final StreamTimeouts timeouts;
    private final Consumer<TimeoutKind> onExpiry;
    private final ReentrantLock timerLock = new ReentrantLock();
    private boolean closed;
    private boolean firstChunkSeen;
    private boolean upstreamDone;
    private int outstanding;
    private Armed armed;

    StreamTimer(StreamTimeouts timeouts, Consumer<TimeoutKind> onExpiry) {
        this.timeouts = timeouts;
        this.onExpiry = onExpiry;
    }

    /** The upstream call is about to start with {@code credit} messages granted. */
    void start(int credit) {
        timerLock.lock();
        try {
            outstanding = credit;
            if (!closed) {
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
            disarm();
            if (!closed && !upstreamDone && outstanding > 0) {
                arm(TimeoutKind.IDLE, timeouts.idleBetweenChunks());
            }
        } finally {
            timerLock.unlock();
        }
    }

    /** One credit is about to be handed back to upstream (call right before {@code request(1)}). */
    void creditGranted() {
        timerLock.lock();
        try {
            outstanding++;
            if (!closed && !upstreamDone && firstChunkSeen && armed == null && outstanding > 0) {
                arm(TimeoutKind.IDLE, timeouts.idleBetweenChunks());
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
        Armed token = new Armed(kind);
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
        onExpiry.accept(token.kind);
    }
}
