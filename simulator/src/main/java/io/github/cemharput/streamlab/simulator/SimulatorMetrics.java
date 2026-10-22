package io.github.cemharput.streamlab.simulator;

import io.github.cemharput.streamlab.tokenstream.v1.SimulatorStats;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.LongAdder;

/** Counters exposed through SimulatorAdminService.GetStats. */
final class SimulatorMetrics {

    enum Outcome { COMPLETED, CANCELLED, FAULTED }

    private final AtomicLong active = new AtomicLong();
    private final LongAdder started = new LongAdder();
    private final LongAdder completed = new LongAdder();
    private final LongAdder cancelled = new LongAdder();
    private final LongAdder faulted = new LongAdder();
    private final LongAdder rejected = new LongAdder();
    private final LongAdder chunksEmitted = new LongAdder();
    private final LongAdder flowControlPauses = new LongAdder();

    void generationStarted() {
        started.increment();
        active.incrementAndGet();
    }

    /** Must be called exactly once per started generation. */
    void generationFinished(Outcome outcome) {
        active.decrementAndGet();
        switch (outcome) {
            case COMPLETED -> completed.increment();
            case CANCELLED -> cancelled.increment();
            case FAULTED -> faulted.increment();
        }
    }

    void requestRejected() {
        rejected.increment();
    }

    void chunkEmitted() {
        chunksEmitted.increment();
    }

    void flowControlPaused() {
        flowControlPauses.increment();
    }

    SimulatorStats snapshot() {
        return SimulatorStats.newBuilder()
                .setActiveGenerations(active.get())
                .setStartedTotal(started.sum())
                .setCompletedTotal(completed.sum())
                .setCancelledTotal(cancelled.sum())
                .setFaultedTotal(faulted.sum())
                .setRejectedTotal(rejected.sum())
                .setChunksEmittedTotal(chunksEmitted.sum())
                .setFlowControlPausesTotal(flowControlPauses.sum())
                .build();
    }
}
