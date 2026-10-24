package io.github.cemharput.streamlab.gateway.stream;

import io.github.cemharput.streamlab.gateway.stream.StreamTestSupport.CountingLease;
import io.github.cemharput.streamlab.gateway.stream.StreamTestSupport.CountingRegistry;
import io.github.cemharput.streamlab.gateway.stream.StreamTestSupport.RecordingEmitter;
import io.github.cemharput.streamlab.tokenstream.v1.GenerateRequest;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import java.time.Duration;
import java.util.concurrent.CountDownLatch;

/**
 * The seam of the stream contract tests: one stream implementation seen from the outside. {@link Kind#REAL} is
 * {@link StreamBridge}, {@link Kind#LEAKY} is {@link LeakyStreamHandler}. The upstream call, the emitter, the lease and
 * the timeout scheduler are the same fakes/real parts for both; only the object under test differs.
 */
interface StreamUnderTest {

    enum Kind {
        REAL, LEAKY
    }

    /** Everything one stream needs. {@code starter} decides what the upstream is (real stub or a fake). */
    record Wiring(GenerateRequest request, RecordingEmitter emitter, CountingLease lease, StreamTimeouts timeouts,
            int capacity, UpstreamStarter starter, Duration deadline, Duration drainTimeout) {
    }

    static StreamUnderTest create(Kind kind, Wiring wiring) {
        return switch (kind) {
            case REAL -> new RealStream(wiring);
            case LEAKY -> new LeakyStream(wiring);
        };
    }

    /** Takes the (single) permit; false means the harness is broken. */
    boolean admit();

    void start();

    /** The container reports that the client's response ended. */
    void clientGone();

    /** Gateway shutdown with a drain timeout that has already elapsed; returns how long it took. */
    Duration shutdown();

    /** Direct shutdown cancel of this one stream (what the registry's force-cancel calls); used by the race. */
    void cancelForShutdown();

    /** Arms the in-class terminal-race hook (LEAKY only); returns false when the implementation has none. */
    boolean armTerminalRaceHook(CountDownLatch arrivals);

    boolean raceHookBroken();

    int leaseCloses();

    int permitReleases();

    /** Permits still held: REAL registry.admittedCount(); LEAKY acquired minus released (negative on a double release). */
    int permitsHeld();

    int pendingTimers();

    /** Chunks accepted from upstream whose credit has not been handed back. */
    int bufferedChunks();

    /** {@link StreamBridge} with a counting registry. */
    final class RealStream implements StreamUnderTest {

        private final Wiring wiring;
        private final CountingRegistry registry;
        private final StreamBridge bridge;

        RealStream(Wiring wiring) {
            this.wiring = wiring;
            this.registry = StreamTestSupport.startedCountingRegistry(wiring.drainTimeout());
            this.bridge = new StreamBridge(wiring.request(), wiring.emitter(), registry, wiring.lease(),
                    wiring.capacity(), new StreamMetrics(new SimpleMeterRegistry(), registry), wiring.timeouts(),
                    RetryPolicy.disabled(), wiring.starter(), wiring.deadline());
        }

        @Override
        public boolean admit() {
            return registry.tryRegister(bridge);
        }

        @Override
        public void start() {
            bridge.start();
        }

        @Override
        public void clientGone() {
            wiring.emitter().simulateClientGone();
        }

        @Override
        public Duration shutdown() {
            long begin = System.nanoTime();
            registry.stop();
            return Duration.ofNanos(System.nanoTime() - begin);
        }

        @Override
        public void cancelForShutdown() {
            bridge.cancelForShutdown();
        }

        @Override
        public boolean armTerminalRaceHook(CountDownLatch arrivals) {
            return false;
        }

        @Override
        public boolean raceHookBroken() {
            return false;
        }

        @Override
        public int leaseCloses() {
            return wiring.lease().closes.get();
        }

        @Override
        public int permitReleases() {
            return registry.releases.get();
        }

        @Override
        public int permitsHeld() {
            return registry.admittedCount();
        }

        @Override
        public int pendingTimers() {
            return wiring.timeouts().pendingTimers();
        }

        @Override
        public int bufferedChunks() {
            return bridge.bufferedChunks();
        }
    }

    /** {@link LeakyStreamHandler} with a one-permit ledger. */
    final class LeakyStream implements StreamUnderTest {

        private final Wiring wiring;
        private final LeakyStreamHandler.PermitLedger permits = new LeakyStreamHandler.PermitLedger();
        private final LeakyStreamHandler handler;

        LeakyStream(Wiring wiring) {
            this.wiring = wiring;
            this.handler = new LeakyStreamHandler(wiring.request(), wiring.emitter(), wiring.lease(), permits,
                    wiring.capacity(), wiring.timeouts(), wiring.starter(), wiring.deadline());
        }

        @Override
        public boolean admit() {
            return permits.tryAcquire();
        }

        @Override
        public void start() {
            handler.start();
        }

        @Override
        public void clientGone() {
            wiring.emitter().simulateClientGone();
        }

        @Override
        public Duration shutdown() {
            long begin = System.nanoTime();
            handler.cancelForShutdown();
            return Duration.ofNanos(System.nanoTime() - begin);
        }

        @Override
        public void cancelForShutdown() {
            handler.cancelForShutdown();
        }

        @Override
        public boolean armTerminalRaceHook(CountDownLatch arrivals) {
            handler.armTerminalRace(arrivals);
            return true;
        }

        @Override
        public boolean raceHookBroken() {
            return handler.raceHookBroken();
        }

        @Override
        public int leaseCloses() {
            return wiring.lease().closes.get();
        }

        @Override
        public int permitReleases() {
            return permits.releases();
        }

        @Override
        public int permitsHeld() {
            return permits.held();
        }

        @Override
        public int pendingTimers() {
            return wiring.timeouts().pendingTimers();
        }

        @Override
        public int bufferedChunks() {
            return handler.bufferedChunks();
        }
    }
}
