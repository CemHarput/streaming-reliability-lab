package io.github.cemharput.streamlab.simulator;

import io.github.cemharput.streamlab.simulator.SimulatorMetrics.Outcome;
import io.github.cemharput.streamlab.tokenstream.v1.FaultSpec;
import io.github.cemharput.streamlab.tokenstream.v1.FaultType;
import io.github.cemharput.streamlab.tokenstream.v1.GenerateRequest;
import io.github.cemharput.streamlab.tokenstream.v1.StreamProfile;
import io.github.cemharput.streamlab.tokenstream.v1.TokenChunk;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.stub.ServerCallStreamObserver;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

/**
 * State machine for one GenerateTokens call.
 *
 * <p>Events arrive from two threads: the scheduler (timed emits) and the gRPC
 * call executor (ready / cancel callbacks). All state transitions and every
 * observer call happen under {@code this} lock, which also satisfies gRPC's
 * rule that a StreamObserver is not called concurrently.
 *
 * <p>Invariants: exactly one terminal transition per generation, and no task
 * stays scheduled after it. Upstream work stopping on cancel is what the
 * gateway tests assert through {@code active_generations}.
 */
final class Generation {

    private final GenerateRequest request;
    private final ServerCallStreamObserver<TokenChunk> observer;
    private final ScheduledExecutorService scheduler;
    private final SimulatorMetrics metrics;
    private final int chunkCount;

    // Guarded by this.
    private int nextSeq;
    private boolean finished;
    private boolean awaitingReady;
    private ScheduledFuture<?> pending;

    Generation(
            GenerateRequest request,
            ServerCallStreamObserver<TokenChunk> observer,
            ScheduledExecutorService scheduler,
            SimulatorMetrics metrics
    ) {
        this.request = request;
        this.observer = observer;
        this.scheduler = scheduler;
        this.metrics = metrics;
        this.chunkCount = request.getProfile().getChunkCount();
        this.nextSeq = request.getResumeFromSeq();
    }

    /** Must be called from the service method, before it returns. */
    void start() {
        observer.setOnCancelHandler(this::onCancel);
        observer.setOnReadyHandler(this::onReady);
        metrics.generationStarted();
        schedule(request.getProfile().getFirstChunkDelayMs());
    }

    private synchronized void schedule(long delayMs) {
        if (finished) {
            return;
        }
        pending = scheduler.schedule(this::emitNext, delayMs, TimeUnit.MILLISECONDS);
    }

    private synchronized void onReady() {
        if (finished || !awaitingReady) {
            return;
        }
        awaitingReady = false;
        emitNext();
    }

    private synchronized void onCancel() {
        if (!finished) {
            finish(Outcome.CANCELLED);
        }
    }

    private synchronized void emitNext() {
        if (finished) {
            return;
        }
        pending = null;

        FaultSpec fault = request.getFault();
        if (fault.getType() != FaultType.FAULT_TYPE_NONE && fault.getAtSeq() == nextSeq) {
            applyFault(fault.getType());
            return;
        }
        if (!observer.isReady()) {
            // Respect transport flow control instead of buffering unboundedly;
            // onReady resumes the stream.
            awaitingReady = true;
            metrics.flowControlPaused();
            return;
        }

        int seq = nextSeq;
        boolean last = seq == chunkCount - 1;
        try {
            observer.onNext(chunk(seq, last));
        } catch (StatusRuntimeException ex) {
            // The call was cancelled between our check and the write.
            finish(Outcome.CANCELLED);
            return;
        }
        metrics.chunkEmitted();
        nextSeq = seq + 1;

        if (last) {
            finish(Outcome.COMPLETED);
            observer.onCompleted();
        } else {
            schedule(request.getProfile().getChunkIntervalMs());
        }
    }

    private void applyFault(FaultType type) {
        String description = "injected " + type.name() + " at seq " + nextSeq;
        switch (type) {
            case FAULT_TYPE_UNAVAILABLE -> {
                finish(Outcome.FAULTED);
                observer.onError(Status.UNAVAILABLE.withDescription(description).asRuntimeException());
            }
            case FAULT_TYPE_INTERNAL -> {
                finish(Outcome.FAULTED);
                observer.onError(Status.INTERNAL.withDescription(description).asRuntimeException());
            }
            case FAULT_TYPE_HANG -> {
                // Stay active with nothing scheduled: only a client cancel,
                // a deadline or server shutdown ends this generation.
            }
            default -> throw new IllegalStateException("Unhandled fault type " + type);
        }
    }

    private void finish(Outcome outcome) {
        finished = true;
        awaitingReady = false;
        if (pending != null) {
            pending.cancel(false);
            pending = null;
        }
        metrics.generationFinished(outcome);
    }

    private TokenChunk chunk(int seq, boolean last) {
        StreamProfile profile = request.getProfile();
        Instant now = Instant.now();
        return TokenChunk.newBuilder()
                .setRequestId(request.getRequestId())
                .setSeq(seq)
                .setText(SyntheticText.chunk(request.getSeed(), request.getRequestId(), seq, profile.getChunkSizeBytes()))
                .setLast(last)
                .setEmittedAtEpochMicros(ChronoUnit.MICROS.between(Instant.EPOCH, now))
                .build();
    }
}
