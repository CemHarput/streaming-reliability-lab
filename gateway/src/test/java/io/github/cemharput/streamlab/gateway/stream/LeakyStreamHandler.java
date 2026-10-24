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
import java.time.Duration;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReentrantLock;
import org.springframework.http.MediaType;
import org.springframework.web.servlet.mvc.method.annotation.SseEmitter;

/**
 * A deliberately flawed stream handler, test code only. It bridges one upstream call to one emitter like
 * {@link StreamBridge} does (same {@link UpstreamStarter}, same {@link StreamTimer}, same SSE event shapes, manual
 * flow control with the same credit rule) but carries exactly two flaws, marked {@code FLAW (a)} and
 * {@code FLAW (b)}. The stream contract tests ({@link StreamContract}) run against it to prove they detect them.
 *
 * <p>Simplifications that are not flaws and that the contract does not measure: chunks are written synchronously on
 * the gRPC callback thread (no queue, no drainer: the credit rule bounds the buffered chunks), a timeout writes its
 * error on the timer thread, and there is no retry and no overflow path.
 */
final class LeakyStreamHandler implements ClientResponseObserver<GenerateRequest, TokenChunk> {

    /** How long a terminal path waits at the race hook for the other terminal path. */
    static final Duration RACE_HOOK_WAIT = Duration.ofSeconds(5);

    /** A single permit; counts acquisitions and releases so a double release is visible (held goes negative). */
    static final class PermitLedger {

        private final AtomicInteger acquired = new AtomicInteger();
        private final AtomicInteger releases = new AtomicInteger();

        boolean tryAcquire() {
            return acquired.compareAndSet(0, 1);
        }

        void release() {
            releases.incrementAndGet();
        }

        int releases() {
            return releases.get();
        }

        int held() {
            return acquired.get() - releases.get();
        }
    }

    private final GenerateRequest request;
    private final String requestId;
    private final SseEmitter emitter;
    private final ChannelProvider.Lease lease;
    private final PermitLedger permits;
    private final int capacity;
    private final StreamTimer timer;
    private final UpstreamStarter starter;
    private final Duration deadline;
    private final ReentrantLock sendLock = new ReentrantLock();
    private final AtomicInteger received = new AtomicInteger();
    private final AtomicInteger creditReturned = new AtomicInteger();
    private final AtomicInteger sentChunks = new AtomicInteger();

    private volatile ClientCallStreamObserver<GenerateRequest> call;
    private volatile boolean terminated;
    private volatile CountDownLatch raceArrivals;
    private volatile boolean raceHookBroken;

    LeakyStreamHandler(GenerateRequest request, SseEmitter emitter, ChannelProvider.Lease lease, PermitLedger permits,
            int capacity, StreamTimeouts timeouts, UpstreamStarter starter, Duration deadline) {
        this.request = request;
        this.requestId = request.getRequestId();
        this.emitter = emitter;
        this.lease = lease;
        this.permits = permits;
        this.capacity = capacity;
        this.starter = starter;
        this.deadline = deadline;
        this.timer = new StreamTimer(timeouts, this::timedOut);
        emitter.onCompletion(this::clientGone);
        emitter.onTimeout(this::clientGone);
        emitter.onError(ex -> clientGone());
    }

    void start() {
        starter.start(request, deadline, this);
    }

    @Override
    public void beforeStart(ClientCallStreamObserver<GenerateRequest> stream) {
        call = stream;
        stream.disableAutoRequestWithInitial(capacity);
        timer.start(capacity);
    }

    @Override
    public void onNext(TokenChunk chunk) {
        if (terminated) {
            return;
        }
        boolean last = chunk.getLast();
        timer.chunkReceived(last);
        received.incrementAndGet();
        sendLock.lock();
        try {
            if (terminated) {
                return;
            }
            emitter.send(SseEmitter.event()
                    .name(SseEvents.CHUNK)
                    .id(Integer.toString(chunk.getSeq()))
                    .data(new ChunkEvent(requestId, chunk.getSeq(), chunk.getText(), last),
                            MediaType.APPLICATION_JSON));
            sentChunks.incrementAndGet();
        } catch (IOException | IllegalStateException ex) {
            clientGone();
            return;
        } finally {
            sendLock.unlock();
        }
        if (!last) {
            // Credit rule: one credit back per chunk the client accepted, so received - creditReturned <= capacity.
            creditReturned.incrementAndGet();
            timer.creditGranted(1);
            call.request(1);
        }
    }

    @Override
    public void onCompleted() {
        terminal(() -> {
            timer.close();
            release();
            send(SseEvents.COMPLETE, new CompleteEvent(requestId, sentChunks.get()));
            completeEmitter();
        });
    }

    @Override
    public void onError(Throwable t) {
        String code = Status.fromThrowable(t).getCode().name();
        String message = Status.fromThrowable(t).getDescription() == null
                ? "" : Status.fromThrowable(t).getDescription();
        terminal(() -> {
            timer.close();
            release();
            send(SseEvents.ERROR, new ErrorEvent(requestId, code, message));
            completeEmitter();
        });
    }

    void cancelForShutdown() {
        terminal(() -> {
            timer.close();
            cancelUpstream(StreamBridge.SHUTDOWN_MESSAGE);
            release();
            send(SseEvents.ERROR, new ErrorEvent(requestId, Status.Code.CANCELLED.name(),
                    StreamBridge.SHUTDOWN_MESSAGE));
            completeEmitter();
        });
    }

    private void timedOut(TimeoutKind kind) {
        terminal(() -> {
            timer.close();
            cancelUpstream(kind.message());
            release();
            send(SseEvents.ERROR, new ErrorEvent(requestId, Status.Code.DEADLINE_EXCEEDED.name(), kind.message()));
            completeEmitter();
        });
    }

    private void clientGone() {
        // FLAW (a): the client is gone, capacity and timer are freed, but the upstream call is never cancelled
        // (no cancelUpstream here, unlike StreamBridge.clientGone). A generation that never ends on its own
        // (FAULT_TYPE_HANG) stays active in the simulator until the channel dies.
        terminal(() -> {
            timer.close();
            release();
        });
    }

    /**
     * The terminal step. FLAW (b): check-then-act on a plain volatile flag instead of one atomic claim
     * (StreamBridge.claim uses compareAndSet). Two paths that both pass the check each run their action: two
     * terminal events and a double release. {@link #raceHook()} only makes the interleaving deterministic for the
     * tests; the flaw is the check/set gap itself.
     */
    private boolean terminal(Runnable act) {
        if (terminated) {
            return false;
        }
        raceHook();
        terminated = true;
        act.run();
        return true;
    }

    /** Test hook: when armed, a terminal path that passed the check waits here until the other one arrived too. */
    private void raceHook() {
        CountDownLatch latch = raceArrivals;
        if (latch == null) {
            return;
        }
        latch.countDown();
        try {
            if (!latch.await(RACE_HOOK_WAIT.toNanos(), TimeUnit.NANOSECONDS)) {
                raceHookBroken = true;
            }
        } catch (InterruptedException ex) {
            raceHookBroken = true;
            Thread.currentThread().interrupt();
        }
    }

    /** Arms the race hook; call after {@link #start()} and before the competing triggers. */
    void armTerminalRace(CountDownLatch arrivals) {
        this.raceArrivals = arrivals;
    }

    /** True when a terminal path waited the full {@link #RACE_HOOK_WAIT} without the other one arriving. */
    boolean raceHookBroken() {
        return raceHookBroken;
    }

    /** Chunks accepted from upstream whose credit has not been handed back. */
    int bufferedChunks() {
        return received.get() - creditReturned.get();
    }

    private void cancelUpstream(String message) {
        ClientCallStreamObserver<GenerateRequest> current = call;
        if (current != null) {
            current.cancel(message, null);
        }
    }

    private void release() {
        lease.close();
        permits.release();
    }

    private void send(String name, Object payload) {
        sendLock.lock();
        try {
            emitter.send(SseEmitter.event().name(name).data(payload, MediaType.APPLICATION_JSON));
        } catch (IOException | IllegalStateException ex) {
            // client left before the terminal event: nothing to do
        } finally {
            sendLock.unlock();
        }
    }

    private void completeEmitter() {
        try {
            emitter.complete();
        } catch (RuntimeException ex) {
            // already completed
        }
    }
}
