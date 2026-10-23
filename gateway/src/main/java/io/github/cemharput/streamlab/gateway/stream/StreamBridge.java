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
 * Upstream flow control is manual: the initial credit equals the queue capacity and one
 * credit is returned after each chunk the client accepted, so the queue cannot exceed
 * its capacity. The upstream end (error/complete) is handled after the queue drained,
 * so queued chunks always precede it. {@link #finish} removes the stream from the
 * registry's tracked set after the last write.
 */
final class StreamBridge implements ClientResponseObserver<GenerateRequest, TokenChunk> {

    enum StreamOutcome {
        COMPLETED, UPSTREAM_ERROR, CLIENT_GONE, SHUTDOWN_CANCELLED
    }

    private static final Logger LOGGER = System.getLogger(StreamBridge.class.getName());
    private static final Executor TERMINAL_EVENT_EXECUTOR =
            command -> Thread.ofVirtual().name("stream-terminal-event").start(command);
    private static final Executor DRAIN_EXECUTOR =
            command -> Thread.ofVirtual().name("stream-drain").start(command);

    static final String SHUTDOWN_MESSAGE = "gateway shutting down";
    /** How long a terminal event waits for a client write that is stuck in progress. */
    static final Duration SEND_LOCK_WAIT = Duration.ofSeconds(5);

    private final String requestId;
    private final SseEmitter emitter;
    private final ActiveStreamRegistry registry;
    private final ChannelProvider.Lease lease;
    private final int queueCapacity;
    private final StreamMetrics metrics;
    private final ArrayBlockingQueue<TokenChunk> queue;
    private final AtomicInteger wip = new AtomicInteger();

    private final ReentrantLock sendLock = new ReentrantLock();
    private final AtomicReference<StreamOutcome> terminal = new AtomicReference<>();
    private volatile ClientCallStreamObserver<GenerateRequest> requestStream;
    private volatile boolean lastSeen;
    /** Set by the serial gRPC callbacks after every offer; OK means onCompleted. */
    private volatile Status upstreamEnd;
    // Only the drainer touches this (happens-before via the wip counter).
    private int sentChunks;

    StreamBridge(String requestId, SseEmitter emitter, ActiveStreamRegistry registry, ChannelProvider.Lease lease,
            int queueCapacity, StreamMetrics metrics) {
        this.requestId = requestId;
        this.emitter = emitter;
        this.registry = registry;
        this.lease = lease;
        this.queueCapacity = queueCapacity;
        this.metrics = metrics;
        this.queue = new ArrayBlockingQueue<>(queueCapacity);
        // A container-side end of the response before our terminal event means the client is gone.
        // These callbacks only use the atomic claim, so they cannot block on a stuck write.
        emitter.onCompletion(this::clientGone);
        emitter.onTimeout(this::clientGone);
        emitter.onError(ex -> clientGone());
    }

    @Override
    public void beforeStart(ClientCallStreamObserver<GenerateRequest> stream) {
        requestStream = stream;
        // Manual flow control: upstream may have at most `queueCapacity` chunks outstanding;
        // every chunk written to the client hands one credit back (see writeChunk).
        stream.disableAutoRequestWithInitial(queueCapacity);
        // Publish the stream before reading the outcome: a concurrent claim either
        // sees the stream and cancels it, or is seen here.
        StreamOutcome outcome = terminal.get();
        if (outcome != null) {
            stream.cancel(cancelMessage(outcome), null);
        }
    }

    /** gRPC callback thread: only enqueues, never writes and never blocks. */
    @Override
    public void onNext(TokenChunk chunk) {
        if (terminal.get() != null) {
            return;
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

    /** The end is handled by the drainer after the queued chunks, so they are written first. */
    @Override
    public void onError(Throwable t) {
        if (upstreamEnd == null) {
            upstreamEnd = Status.fromThrowable(t);
        }
        scheduleDrain();
    }

    @Override
    public void onCompleted() {
        if (upstreamEnd == null) {
            upstreamEnd = Status.OK;
        }
        scheduleDrain();
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
        if (requestMore && terminal.get() == null) {
            ClientCallStreamObserver<GenerateRequest> stream = requestStream;
            if (stream != null) {
                stream.request(1);
            }
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
            if (!terminal.compareAndSet(null, StreamOutcome.COMPLETED)) {
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
        if (terminal.compareAndSet(null, StreamOutcome.UPSTREAM_ERROR)) {
            releaseCapacity();
            sendErrorAndComplete(code, message);
            finish(StreamOutcome.UPSTREAM_ERROR);
        }
    }

    /** gRPC thread: upstream violated its credit. One error, upstream cancelled, queue dropped. */
    private void overflow() {
        if (!terminal.compareAndSet(null, StreamOutcome.UPSTREAM_ERROR)) {
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
        if (!terminal.compareAndSet(null, StreamOutcome.SHUTDOWN_CANCELLED)) {
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

    private void clientGone() {
        if (terminal.compareAndSet(null, StreamOutcome.CLIENT_GONE)) {
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
        ClientCallStreamObserver<GenerateRequest> stream = requestStream;
        if (stream != null) {
            stream.cancel(message, null);
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

    private static String cancelMessage(StreamOutcome outcome) {
        return outcome == StreamOutcome.SHUTDOWN_CANCELLED ? SHUTDOWN_MESSAGE : "client disconnected";
    }
}
