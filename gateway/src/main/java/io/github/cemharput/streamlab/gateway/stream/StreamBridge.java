package io.github.cemharput.streamlab.gateway.stream;

import io.github.cemharput.streamlab.gateway.stream.SseEvents.ChunkEvent;
import io.github.cemharput.streamlab.gateway.stream.SseEvents.CompleteEvent;
import io.github.cemharput.streamlab.gateway.stream.SseEvents.ErrorEvent;
import io.github.cemharput.streamlab.gateway.upstream.ChannelProvider;
import io.github.cemharput.streamlab.tokenstream.v1.GenerateRequest;
import io.github.cemharput.streamlab.tokenstream.v1.TokenChunk;
import io.grpc.Status;
import io.grpc.stub.ClientCallStreamObserver;
import io.grpc.stub.ClientResponseObserver;
import java.io.IOException;
import java.lang.System.Logger;
import java.lang.System.Logger.Level;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.ReentrantLock;
import org.springframework.http.MediaType;
import org.springframework.web.servlet.mvc.method.annotation.SseEmitter;

/**
 * Bridges one upstream gRPC call to one SSE emitter and owns the single terminal
 * outcome of the stream.
 *
 * <p>The terminal outcome is claimed with a compare-and-set, never under a lock
 * that a blocking client write can hold. The winner cancels upstream and frees
 * its permit and lease immediately ({@link #releaseCapacity()}); only the terminal
 * SSE event is written under {@link #sendLock}, which keeps events ordered (nothing
 * is sent after the terminal event). A client that stopped reading therefore delays
 * at most its own terminal event, never shutdown, upstream cancellation or capacity.
 *
 * <p>Upstream chunks go into a bounded per-stream queue ({@code offer} only, the gRPC
 * thread never blocks) and a serial virtual-thread drainer writes them to the client.
 * Upstream flow control is manual: the initial credit equals the queue capacity (minus what is still
 * buffered, for a retry call) and one credit is returned after each chunk the client accepted, so the
 * queue cannot exceed its capacity. The upstream end (error/complete) is handled after the queue
 * drained, so queued chunks always precede it. {@link #finish} removes the stream from the
 * registry's tracked set after the last write.
 *
 * <p>A {@link StreamTimer} bounds upstream silence (first chunk, then idle between chunks while
 * upstream holds credit). Its expiry is just another terminal claim ({@link #timedOut}); every
 * claim goes through {@link #claim} so the timer is closed on every terminal path.
 *
 * <p>Each upstream call is an {@link Attempt}. An upstream {@code UNAVAILABLE} before the last chunk
 * is retried a bounded number of times ({@link RetryPolicy}) within the stream-deadline budget: the
 * call is dead, the stream stays registered (permit and lease are kept), the timer slot holds the
 * backoff and the next call starts on a virtual thread, never inline from a callback. It resumes
 * after the last chunk <em>received</em> (chunks still queued are written first, so the client sees
 * neither a gap nor a duplicate). Callbacks of a dead attempt are ignored. A retry is not a new
 * terminal path: the terminal outcome is still claimed once, exclusively through {@link #claim}.
 */
final class StreamBridge implements ClientResponseObserver<GenerateRequest, TokenChunk> {

    enum StreamOutcome {
        COMPLETED, UPSTREAM_ERROR, CLIENT_GONE, SHUTDOWN_CANCELLED, FIRST_CHUNK_TIMEOUT, IDLE_TIMEOUT
    }

    /** Where an {@link Attempt} is in its life; guarded by {@link #creditLock}. */
    private enum AttemptState {
        NEW, STARTING, OPEN, DEAD
    }

    private static final Logger LOGGER = System.getLogger(StreamBridge.class.getName());
    private static final Executor TERMINAL_EVENT_EXECUTOR =
            command -> Thread.ofVirtual().name("stream-terminal-event").start(command);
    private static final Executor DRAIN_EXECUTOR =
            command -> Thread.ofVirtual().name("stream-drain").start(command);
    private static final Executor RETRY_EXECUTOR =
            command -> Thread.ofVirtual().name("stream-retry").start(command);

    static final String SHUTDOWN_MESSAGE = "gateway shutting down";
    /** How long a terminal event waits for a client write that is stuck in progress. */
    static final Duration SEND_LOCK_WAIT = Duration.ofSeconds(5);

    private final String requestId;
    private final GenerateRequest request;
    private final SseEmitter emitter;
    private final ActiveStreamRegistry registry;
    private final ChannelProvider.Lease lease;
    private final int queueCapacity;
    private final StreamMetrics metrics;
    private final StreamTimer timer;
    private final RetryPolicy policy;
    private final UpstreamStarter starter;
    private final Duration streamDeadline;
    private final ArrayBlockingQueue<TokenChunk> queue;
    private final AtomicInteger wip = new AtomicInteger();
    private final Attempt initialAttempt = new Attempt(1);

    private final ReentrantLock sendLock = new ReentrantLock();
    /**
     * Guards {@link #buffered} and every attempt's state and banked credit. Lock order: creditLock then the
     * timer lock, sendLock then the timer lock; creditLock and sendLock are never held together.
     */
    private final ReentrantLock creditLock = new ReentrantLock();
    /** Chunks accepted from upstream whose credit has not been handed back yet. Guarded by creditLock. */
    private int buffered;
    private final AtomicReference<StreamOutcome> terminal = new AtomicReference<>();
    /** The upstream call whose callbacks count; a dead attempt's late callbacks are ignored. */
    private volatile Attempt current = initialAttempt;
    /** System.nanoTime-style deadline of the whole stream (all attempts). */
    private volatile long budgetDeadline;
    /** Resume position: last chunk <em>received</em> + 1. Written by the current attempt's serial callback thread. */
    private volatile int nextSeq;
    private volatile boolean lastReceived;
    /** The retryable status that put the stream into backoff. */
    private volatile Status lastStatus;
    /** Set before the final upstream end is recorded, so the UPSTREAM_ERROR claim winner counts it once. */
    private volatile boolean retriesExhausted;
    private volatile boolean lastSeen;
    /** Set by the serial gRPC callbacks after every offer; OK means onCompleted. */
    private volatile Status upstreamEnd;
    // Only the drainer touches this (happens-before via the wip counter).
    private int sentChunks;

    StreamBridge(GenerateRequest request, SseEmitter emitter, ActiveStreamRegistry registry,
            ChannelProvider.Lease lease, int queueCapacity, StreamMetrics metrics, StreamTimeouts timeouts,
            RetryPolicy policy, UpstreamStarter starter, Duration streamDeadline) {
        this.request = request;
        this.requestId = request.getRequestId();
        this.emitter = emitter;
        this.registry = registry;
        this.lease = lease;
        this.queueCapacity = queueCapacity;
        this.metrics = metrics;
        this.policy = policy;
        this.starter = starter;
        this.streamDeadline = streamDeadline;
        this.budgetDeadline = policy.nanoTime() + streamDeadline.toNanos();
        this.timer = new StreamTimer(timeouts, this::timedOut);
        this.queue = new ArrayBlockingQueue<>(queueCapacity);
        // A container-side end of the response before our terminal event means the client is gone.
        // These callbacks only use the atomic claim, so they cannot block on a stuck write.
        emitter.onCompletion(this::clientGone);
        emitter.onTimeout(this::clientGone);
        emitter.onError(ex -> clientGone());
    }

    /** Starts the first upstream call (after admission); the retry budget starts here. */
    void start() {
        budgetDeadline = policy.nanoTime() + streamDeadline.toNanos();
        startCall(initialAttempt, streamDeadline);
    }

    private GenerateRequest requestFor(int seq) {
        return seq == 0 ? request : request.toBuilder().setResumeFromSeq(seq).build();
    }

    /** Never throws: a starter failure ends the attempt as a non-retryable INTERNAL error. */
    private void startCall(Attempt attempt, Duration deadline) {
        try {
            starter.start(requestFor(nextSeq), deadline, attempt);
        } catch (RuntimeException ex) {
            attempt.fail(Status.INTERNAL.withDescription("failed to start upstream call").withCause(ex));
            return;
        }
        markStarted(attempt);
    }

    // The four observer methods are the first attempt's callbacks (a caller that starts the call itself, tests);
    // retries use their own Attempt objects.
    @Override
    public void beforeStart(ClientCallStreamObserver<GenerateRequest> stream) {
        initialAttempt.beforeStart(stream);
    }

    @Override
    public void onNext(TokenChunk chunk) {
        initialAttempt.onNext(chunk);
    }

    @Override
    public void onError(Throwable t) {
        initialAttempt.onError(t);
    }

    @Override
    public void onCompleted() {
        initialAttempt.onCompleted();
    }

    /** One upstream call. Its callbacks are serial (gRPC); a dead attempt's late callbacks are ignored. */
    private final class Attempt implements ClientResponseObserver<GenerateRequest, TokenChunk> {

        final int number;
        volatile ClientCallStreamObserver<GenerateRequest> call;
        final AtomicBoolean ended = new AtomicBoolean();
        /** Both guarded by creditLock. */
        AttemptState state = AttemptState.NEW;
        int banked;

        Attempt(int number) {
            this.number = number;
        }

        @Override
        public void beforeStart(ClientCallStreamObserver<GenerateRequest> stream) {
            attemptBeforeStart(this, stream);
        }

        @Override
        public void onNext(TokenChunk chunk) {
            if (current != this || ended.get()) {
                return;
            }
            chunkFromUpstream(chunk);
        }

        @Override
        public void onError(Throwable t) {
            end(Status.fromThrowable(t));
        }

        @Override
        public void onCompleted() {
            end(Status.OK);
        }

        void fail(Status status) {
            end(status);
        }

        private void end(Status status) {
            if (current == this && ended.compareAndSet(false, true)) {
                attemptEnded(this, status);
            }
        }
    }

    private void attemptBeforeStart(Attempt attempt, ClientCallStreamObserver<GenerateRequest> stream) {
        attempt.call = stream;
        int initial;
        creditLock.lock();
        try {
            // Everything buffered already holds credit: the new call may only have what is left.
            initial = queueCapacity - buffered;
            // A retry call cannot take request() before it started (grpc throws "Not started"):
            // credit handed back until markStarted is banked. The first call has no such window.
            attempt.state = attempt.number == 1 ? AttemptState.OPEN : AttemptState.STARTING;
            // Called by the stub right before call.start, i.e. after admission: the first-chunk limit
            // measures upstream time to first token only. A claimed stream has a closed timer.
            timer.start(initial);
        } finally {
            creditLock.unlock();
        }
        // Manual flow control: upstream may have at most `initial` chunks outstanding;
        // every chunk written to the client hands one credit back (see handBackCredit).
        stream.disableAutoRequestWithInitial(initial);
        // Publish the call before reading the outcome: a concurrent claim either
        // sees the call and cancels it, or is seen here.
        StreamOutcome outcome = terminal.get();
        if (outcome != null) {
            stream.cancel(cancelMessage(outcome), null);
        }
    }

    /** The starter returned: the call is started. Flushes credit that was handed back in the meantime. */
    private void markStarted(Attempt attempt) {
        int banked = 0;
        creditLock.lock();
        try {
            if (attempt.state == AttemptState.STARTING) {
                attempt.state = AttemptState.OPEN;
                banked = attempt.banked;
                attempt.banked = 0;
                if (banked > 0) {
                    timer.creditGranted(banked);
                }
            }
        } finally {
            creditLock.unlock();
        }
        if (banked > 0) {
            attempt.call.request(banked);
        }
    }

    /** gRPC callback thread of the current attempt: only enqueues, never writes and never blocks. */
    private void chunkFromUpstream(TokenChunk chunk) {
        if (terminal.get() != null) {
            return;
        }
        // A chunk received (not written) resets the idle timer: the limits measure upstream silence.
        timer.chunkReceived(chunk.getLast());
        // Resume position and last-chunk flag are written before the offer, by the only writer.
        nextSeq = chunk.getSeq() + 1;
        if (chunk.getLast()) {
            lastReceived = true;
        }
        creditLock.lock();
        try {
            buffered++;
        } finally {
            creditLock.unlock();
        }
        // Count before offering: the drainer may poll the chunk (and count it out) before this
        // thread continues, which would let queue_depth dip below zero for an instant.
        metrics.chunkQueued();
        if (!queue.offer(chunk)) {
            metrics.chunkDequeued();
            // Upstream sent more than the credit it was granted.
            overflow();
            return;
        }
        scheduleDrain();
    }

    /**
     * An attempt ended (its serial callback thread). A retryable status schedules the next attempt behind
     * a backoff; everything else is the stream's upstream end, handled by the drainer after the queue.
     */
    private void attemptEnded(Attempt attempt, Status status) {
        if (terminal.get() == null && policy.isRetryable(status) && !lastReceived && retryAfter(attempt, status)) {
            return;
        }
        recordEnd(status);
    }

    /** True when the next attempt was scheduled behind a backoff. */
    private boolean retryAfter(Attempt attempt, Status status) {
        if (attempt.number > policy.maxRetries()) {
            if (policy.maxRetries() > 0) {
                exhausted(attempt.number, "attempts");
            }
            return false;
        }
        if (!registry.isAccepting()) {
            LOGGER.log(Level.INFO, "Stream {0}: retry stopped, gateway shutting down", requestId);
            return false;
        }
        Duration delay = policy.backoff(attempt.number - 1);
        if (delay.toNanos() >= budgetDeadline - policy.nanoTime()) {
            exhausted(attempt.number, "budget");
            return false;
        }
        lastStatus = status;
        creditLock.lock();
        try {
            attempt.state = AttemptState.DEAD;
            timer.callEnded();
        } finally {
            creditLock.unlock();
        }
        LOGGER.log(Level.INFO, "Stream {0}: attempt {1} failed with {2}; retry {3}/{4} in {5} ms from seq {6}",
                requestId, attempt.number, status.getCode(), attempt.number, policy.maxRetries(),
                delay.toMillis(), nextSeq);
        int next = attempt.number + 1;
        return timer.scheduleRetry(delay, () -> {
            try {
                RETRY_EXECUTOR.execute(() -> startAttempt(next));
            } catch (RuntimeException ex) {
                LOGGER.log(Level.ERROR, "cannot start retry thread of " + requestId, ex);
                recordEnd(lastStatus);
            }
        });
    }

    private void exhausted(int attempts, String reason) {
        retriesExhausted = true;
        LOGGER.log(Level.INFO, "Stream {0}: retries exhausted after {1} attempt(s) ({2})", requestId, attempts, reason);
    }

    /** Records the upstream end; the drainer handles it once the queue is empty, so queued chunks come first. */
    private void recordEnd(Status status) {
        if (upstreamEnd == null) {
            upstreamEnd = status;
        }
        timer.upstreamEnded();
        scheduleDrain();
    }

    /** Virtual thread {@code stream-retry} after the backoff. Starts the next upstream call, if still wanted. */
    private void startAttempt(int number) {
        try {
            if (terminal.get() != null) {
                return;
            }
            if (!registry.isAccepting()) {
                LOGGER.log(Level.INFO, "Stream {0}: retry stopped, gateway shutting down", requestId);
                recordEnd(lastStatus);
                return;
            }
            long remaining = budgetDeadline - policy.nanoTime();
            if (remaining <= 0) {
                exhausted(number - 1, "budget");
                recordEnd(lastStatus);
                return;
            }
            Attempt attempt = new Attempt(number);
            current = attempt;
            metrics.retried(nextSeq == 0 ? "restart" : "resume");
            startCall(attempt, Duration.ofNanos(remaining));
        } catch (RuntimeException ex) {
            LOGGER.log(Level.ERROR, "retry of " + requestId + " failed", ex);
            recordEnd(Status.INTERNAL.withDescription("failed to start upstream call").withCause(ex));
        }
    }

    private void scheduleDrain() {
        if (wip.getAndIncrement() == 0) {
            DRAIN_EXECUTOR.execute(this::drainLoop);
        }
    }

    /** At most one drain loop per stream runs at a time (wip counter); no thread is held while idle. */
    private void drainLoop() {
        int missed = 1;
        do {
            try {
                drainOnce();
            } catch (RuntimeException ex) {
                LOGGER.log(Level.ERROR, "drain of " + requestId + " failed", ex);
            }
            missed = wip.addAndGet(-missed);
        } while (missed != 0);
    }

    private void drainOnce() {
        while (true) {
            if (terminal.get() != null) {
                discardQueued();
                return;
            }
            // Read the end first: it is set after every offer, so an empty queue then means drained.
            Status end = upstreamEnd;
            TokenChunk chunk = queue.poll();
            if (chunk != null) {
                metrics.chunkDequeued();
                writeChunk(chunk);
                continue;
            }
            if (end != null) {
                handleUpstreamEnd(end);
            }
            return;
        }
    }

    /** Safe from any thread (also while the drainer is stuck in a write); each chunk is removed once. */
    private void discardQueued() {
        List<TokenChunk> dropped = new ArrayList<>();
        queue.drainTo(dropped);
        metrics.chunksDiscarded(dropped.size());
    }

    private void writeChunk(TokenChunk chunk) {
        boolean requestMore;
        sendLock.lock();
        try {
            requestMore = writeChunkLocked(chunk);
        } finally {
            sendLock.unlock();
        }
        // Outside the lock: hand one credit back after a successful write.
        if (requestMore) {
            handBackCredit();
        }
    }

    /**
     * One buffered chunk was written: give its credit to the current call. While that call is still
     * starting the credit is banked and flushed by {@link #markStarted}; a dead or missing call just
     * returns it to the pool (the next call is sized from {@code buffered}). Never called under sendLock.
     */
    private void handBackCredit() {
        Attempt attempt;
        boolean send = false;
        creditLock.lock();
        try {
            buffered--;
            if (terminal.get() != null) {
                return;
            }
            attempt = current;
            switch (attempt.state) {
                case OPEN -> {
                    // Before request(1): the onNext that credit enables is always counted after it.
                    timer.creditGranted(1);
                    send = true;
                }
                case STARTING -> attempt.banked++;
                case NEW, DEAD -> { }
            }
        } finally {
            creditLock.unlock();
        }
        ClientCallStreamObserver<GenerateRequest> call = attempt.call;
        if (send && call != null) {
            call.request(1);
        }
    }

    /** Must hold {@link #sendLock}. Returns true when a non-final chunk was written successfully. */
    private boolean writeChunkLocked(TokenChunk chunk) {
        if (terminal.get() != null) {
            return false;
        }
        boolean last = chunk.getLast();
        if (last) {
            // Claim COMPLETED before the final chunk is written: a concurrent shutdown
            // then cannot cancel a stream whose last data the client has already received.
            // lastSeen is set before the claim on purpose: if the claim is lost, a terminal
            // outcome already exists and the upstream end is a no-op either way.
            lastSeen = true;
            if (!claim(StreamOutcome.COMPLETED)) {
                return false;
            }
            // The claim winner frees capacity before any write, so a stuck write cannot leak it.
            releaseCapacity();
        }
        try {
            emitter.send(SseEmitter.event()
                    .name(SseEvents.CHUNK)
                    .id(Integer.toString(chunk.getSeq()))
                    .data(new ChunkEvent(requestId, chunk.getSeq(), chunk.getText(), last),
                            MediaType.APPLICATION_JSON));
            sentChunks++;
        } catch (IOException | IllegalStateException ex) {
            if (last) {
                // The upstream call is already complete and capacity is already released.
                terminal.set(StreamOutcome.CLIENT_GONE);
                finish(StreamOutcome.CLIENT_GONE);
            } else {
                clientGone();
            }
            return false;
        }
        if (!last) {
            return true;
        }
        try {
            emitter.send(SseEmitter.event()
                    .name(SseEvents.COMPLETE)
                    .data(new CompleteEvent(requestId, sentChunks), MediaType.APPLICATION_JSON));
        } catch (IOException | IllegalStateException ex) {
            LOGGER.log(Level.DEBUG, "client left before complete event of {0}", requestId);
        }
        completeEmitter();
        finish(StreamOutcome.COMPLETED);
        return false;
    }

    /** Runs on the drainer once the queue is empty; upstream has already ended. */
    private void handleUpstreamEnd(Status status) {
        String code;
        String message;
        if (status.isOk()) {
            if (lastSeen) {
                return;
            }
            code = Status.Code.INTERNAL.name();
            message = "upstream ended without last chunk";
        } else {
            code = status.getCode().name();
            message = status.getDescription() == null ? "" : status.getDescription();
        }
        if (claim(StreamOutcome.UPSTREAM_ERROR)) {
            if (retriesExhausted) {
                metrics.retryExhausted();
            }
            releaseCapacity();
            sendErrorAndComplete(code, message);
            finish(StreamOutcome.UPSTREAM_ERROR);
        }
    }

    /** gRPC thread: upstream violated its credit. One error, upstream cancelled, queue dropped. */
    private void overflow() {
        if (!claim(StreamOutcome.UPSTREAM_ERROR)) {
            return;
        }
        cancelUpstream("per-request queue overflow");
        releaseCapacity();
        discardQueued();
        TERMINAL_EVENT_EXECUTOR.execute(() -> {
            sendErrorAndComplete(Status.Code.RESOURCE_EXHAUSTED.name(), "per-request queue overflow");
            finish(StreamOutcome.UPSTREAM_ERROR);
        });
        scheduleDrain();
    }

    /**
     * Called by the registry when the drain timeout elapsed. Cancels upstream and
     * frees the registry slot before returning; the returned future completes
     * when the terminal event has been written (or given up on).
     */
    CompletableFuture<Void> cancelForShutdown() {
        if (!claim(StreamOutcome.SHUTDOWN_CANCELLED)) {
            return CompletableFuture.completedFuture(null);
        }
        cancelUpstream(StreamOutcome.SHUTDOWN_CANCELLED);
        releaseCapacity();
        finish(StreamOutcome.SHUTDOWN_CANCELLED);
        discardQueued();
        return CompletableFuture.runAsync(
                () -> sendErrorAndComplete(Status.Code.CANCELLED.name(), SHUTDOWN_MESSAGE),
                TERMINAL_EVENT_EXECUTOR);
    }

    /**
     * Timer thread, holds no lock. Same shape as {@link #overflow()}: the claim winner cancels
     * upstream and frees capacity before anything is written; the error goes out on another
     * thread so a stuck client write never blocks the shared scheduler.
     */
    private void timedOut(TimeoutKind kind) {
        if (!claim(kind.outcome())) {
            return;
        }
        cancelUpstream(kind.message());
        releaseCapacity();
        metrics.timedOut(kind);
        discardQueued();
        TERMINAL_EVENT_EXECUTOR.execute(() -> {
            sendErrorAndComplete(Status.Code.DEADLINE_EXCEEDED.name(), kind.message());
            finish(kind.outcome());
        });
        // A chunk offered after the discard above is dropped by the drainer.
        scheduleDrain();
    }

    /**
     * The only place that claims the terminal outcome; closing the timer here leaves no path that leaks it
     * (this also cancels a pending retry backoff).
     */
    private boolean claim(StreamOutcome outcome) {
        if (terminal.compareAndSet(null, outcome)) {
            timer.close();
            return true;
        }
        return false;
    }

    private void clientGone() {
        if (claim(StreamOutcome.CLIENT_GONE)) {
            cancelUpstream(StreamOutcome.CLIENT_GONE);
            releaseCapacity();
            finish(StreamOutcome.CLIENT_GONE);
            discardQueued();
        }
    }

    /**
     * The caller must have won the terminal claim. Waits at most {@link #SEND_LOCK_WAIT}
     * for a client write that is stuck; if it stays stuck the terminal event is given up
     * on and the emitter is left to its own timeout (see README, "Known limits").
     */
    private void sendErrorAndComplete(String code, String message) {
        boolean interrupted = false;
        boolean locked;
        try {
            locked = sendLock.tryLock(SEND_LOCK_WAIT.toNanos(), TimeUnit.NANOSECONDS);
        } catch (InterruptedException ex) {
            // Still deliver the event if the lock is free right now.
            interrupted = true;
            locked = sendLock.tryLock();
        }
        try {
            if (!locked) {
                LOGGER.log(Level.WARNING, "Skipping terminal event of {0}: a client write is stuck", requestId);
                return;
            }
            try {
                try {
                    emitter.send(SseEmitter.event()
                            .name(SseEvents.ERROR)
                            .data(new ErrorEvent(requestId, code, message), MediaType.APPLICATION_JSON));
                } catch (IOException | IllegalStateException ex) {
                    LOGGER.log(Level.DEBUG, "client left before error event of {0}", requestId);
                }
                completeEmitter();
            } finally {
                sendLock.unlock();
            }
        } finally {
            if (interrupted) {
                Thread.currentThread().interrupt();
            }
        }
    }

    /** Chunks waiting for the drainer; never more than the stream queue capacity. */
    int queuedChunks() {
        return queue.size();
    }

    /** Chunks accepted from upstream whose credit has not been handed back yet (tests). */
    int bufferedChunks() {
        creditLock.lock();
        try {
            return buffered;
        } finally {
            creditLock.unlock();
        }
    }

    private void completeEmitter() {
        try {
            emitter.complete();
        } catch (RuntimeException ex) {
            LOGGER.log(Level.DEBUG, "emitter already completed for {0}", requestId);
        }
    }

    private void cancelUpstream(StreamOutcome outcome) {
        cancelUpstream(cancelMessage(outcome));
    }

    private void cancelUpstream(String message) {
        // A dead attempt's call is already closed: cancelling it is a no-op.
        ClientCallStreamObserver<GenerateRequest> call = current.call;
        if (call != null) {
            call.cancel(message, null);
        }
    }

    /** Permit and lease, exactly once: only the terminal claim winner calls this, before any write. */
    private void releaseCapacity() {
        lease.close();
        registry.releasePermit(this);
    }

    /** Leaves the tracked set once the last event has been written (or the stream was cancelled). */
    private void finish(StreamOutcome outcome) {
        registry.unregister(this);
        LOGGER.log(Level.INFO, "Stream {0} finished: {1}", requestId, outcome);
    }

    /** Whether a timeout timer is armed (tests). */
    boolean timerArmed() {
        return timer.isArmed();
    }

    /** Whether the timer slot holds a retry backoff (tests). */
    boolean retryPending() {
        return timer.retryPending();
    }

    private static String cancelMessage(StreamOutcome outcome) {
        return switch (outcome) {
            case SHUTDOWN_CANCELLED -> SHUTDOWN_MESSAGE;
            case FIRST_CHUNK_TIMEOUT -> TimeoutKind.FIRST_CHUNK.message();
            case IDLE_TIMEOUT -> TimeoutKind.IDLE.message();
            case COMPLETED, UPSTREAM_ERROR, CLIENT_GONE -> "client disconnected";
        };
    }
}
