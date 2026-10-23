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
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
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
 * its registry slot immediately; only the terminal SSE event is written under
 * {@link #sendLock}, which keeps events ordered (nothing is sent after the
 * terminal event). A client that stopped reading therefore delays at most its
 * own terminal event, never shutdown or upstream cancellation.
 */
final class StreamBridge implements ClientResponseObserver<GenerateRequest, TokenChunk> {

    enum StreamOutcome {
        COMPLETED, UPSTREAM_ERROR, CLIENT_GONE, SHUTDOWN_CANCELLED
    }

    private static final Logger LOGGER = System.getLogger(StreamBridge.class.getName());
    private static final Executor TERMINAL_EVENT_EXECUTOR =
            command -> Thread.ofVirtual().name("stream-terminal-event").start(command);

    static final String SHUTDOWN_MESSAGE = "gateway shutting down";
    /** How long a terminal event waits for a client write that is stuck in progress. */
    static final Duration SEND_LOCK_WAIT = Duration.ofSeconds(5);

    private final String requestId;
    private final SseEmitter emitter;
    private final ActiveStreamRegistry registry;
    private final ChannelProvider.Lease lease;

    private final ReentrantLock sendLock = new ReentrantLock();
    private final AtomicReference<StreamOutcome> terminal = new AtomicReference<>();
    private volatile ClientCallStreamObserver<GenerateRequest> requestStream;
    private volatile boolean lastSeen;
    // gRPC delivers onNext serially, so only that callback touches this.
    private int sentChunks;

    StreamBridge(String requestId, SseEmitter emitter, ActiveStreamRegistry registry, ChannelProvider.Lease lease) {
        this.requestId = requestId;
        this.emitter = emitter;
        this.registry = registry;
        this.lease = lease;
        // A container-side end of the response before our terminal event means the client is gone.
        // These callbacks only use the atomic claim, so they cannot block on a stuck write.
        emitter.onCompletion(this::clientGone);
        emitter.onTimeout(this::clientGone);
        emitter.onError(ex -> clientGone());
    }

    @Override
    public void beforeStart(ClientCallStreamObserver<GenerateRequest> stream) {
        requestStream = stream;
        // Publish the stream before reading the outcome: a concurrent claim either
        // sees the stream and cancels it, or is seen here.
        StreamOutcome outcome = terminal.get();
        if (outcome != null) {
            stream.cancel(cancelMessage(outcome), null);
        }
    }

    @Override
    public void onNext(TokenChunk chunk) {
        sendLock.lock();
        try {
            if (terminal.get() != null) {
                return;
            }
            boolean last = chunk.getLast();
            if (last) {
                // Claim COMPLETED before the final chunk is written: a concurrent shutdown
                // then cannot cancel a stream whose last data the client has already received.
                // lastSeen is set before the claim on purpose: if the claim is lost, a terminal
                // outcome already exists and onCompleted() is a no-op either way.
                lastSeen = true;
                if (!terminal.compareAndSet(null, StreamOutcome.COMPLETED)) {
                    return;
                }
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
                    // The upstream call is already complete; only the slot is left to free.
                    terminal.set(StreamOutcome.CLIENT_GONE);
                    release(StreamOutcome.CLIENT_GONE);
                } else {
                    clientGone();
                }
                return;
            }
            if (!last) {
                return;
            }
            try {
                emitter.send(SseEmitter.event()
                        .name(SseEvents.COMPLETE)
                        .data(new CompleteEvent(requestId, sentChunks), MediaType.APPLICATION_JSON));
            } catch (IOException | IllegalStateException ex) {
                LOGGER.log(Level.DEBUG, "client left before complete event of {0}", requestId);
            }
            completeEmitter();
            release(StreamOutcome.COMPLETED);
        } finally {
            sendLock.unlock();
        }
    }

    @Override
    public void onError(Throwable t) {
        Status status = Status.fromThrowable(t);
        if (terminal.compareAndSet(null, StreamOutcome.UPSTREAM_ERROR)) {
            sendErrorAndComplete(status.getCode().name(), status.getDescription() == null ? "" : status.getDescription());
            release(StreamOutcome.UPSTREAM_ERROR);
        }
    }

    @Override
    public void onCompleted() {
        if (lastSeen || !terminal.compareAndSet(null, StreamOutcome.UPSTREAM_ERROR)) {
            return;
        }
        sendErrorAndComplete(Status.Code.INTERNAL.name(), "upstream ended without last chunk");
        release(StreamOutcome.UPSTREAM_ERROR);
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
        release(StreamOutcome.SHUTDOWN_CANCELLED);
        return CompletableFuture.runAsync(
                () -> sendErrorAndComplete(Status.Code.CANCELLED.name(), SHUTDOWN_MESSAGE),
                TERMINAL_EVENT_EXECUTOR);
    }

    private void clientGone() {
        if (terminal.compareAndSet(null, StreamOutcome.CLIENT_GONE)) {
            cancelUpstream(StreamOutcome.CLIENT_GONE);
            release(StreamOutcome.CLIENT_GONE);
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

    private void completeEmitter() {
        try {
            emitter.complete();
        } catch (RuntimeException ex) {
            LOGGER.log(Level.DEBUG, "emitter already completed for {0}", requestId);
        }
    }

    private void cancelUpstream(StreamOutcome outcome) {
        ClientCallStreamObserver<GenerateRequest> stream = requestStream;
        if (stream != null) {
            stream.cancel(cancelMessage(outcome), null);
        }
    }

    private void release(StreamOutcome outcome) {
        lease.close();
        registry.unregister(this);
        LOGGER.log(Level.INFO, "Stream {0} finished: {1}", requestId, outcome);
    }

    private static String cancelMessage(StreamOutcome outcome) {
        return outcome == StreamOutcome.SHUTDOWN_CANCELLED ? SHUTDOWN_MESSAGE : "client disconnected";
    }
}
