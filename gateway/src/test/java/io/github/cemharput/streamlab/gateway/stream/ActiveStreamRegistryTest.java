package io.github.cemharput.streamlab.gateway.stream;

import static io.github.cemharput.streamlab.gateway.stream.StreamTestSupport.NO_LEASE;
import static io.github.cemharput.streamlab.gateway.stream.StreamTestSupport.chunk;
import static io.github.cemharput.streamlab.gateway.stream.StreamTestSupport.limits;
import static io.github.cemharput.streamlab.gateway.stream.StreamTestSupport.startedRegistry;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import io.github.cemharput.streamlab.gateway.config.GatewayProperties.OverloadPolicy;
import io.github.cemharput.streamlab.gateway.stream.ActiveStreamRegistry.Admission;
import io.github.cemharput.streamlab.gateway.stream.StreamTestSupport.CountingLease;
import io.github.cemharput.streamlab.gateway.stream.StreamTestSupport.FakeUpstream;
import io.github.cemharput.streamlab.gateway.stream.StreamTestSupport.RecordingEmitter;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

// SEPARATE_THREAD: the default mode only interrupts the test thread, which a wait on a lock
// (the earlier design) ignores. Here the test fails after 15s even if its thread stays blocked.
@Timeout(value = 15, threadMode = Timeout.ThreadMode.SEPARATE_THREAD)
class ActiveStreamRegistryTest {

    private static final Duration TIMEOUT = Duration.ofSeconds(5);

    private static StreamBridge bridge(ActiveStreamRegistry registry) {
        return StreamTestSupport.newBridge(RecordingEmitter.responsive(), registry, NO_LEASE);
    }

    @Test
    void interruptedStopStillCancelsRemainingStreams() {
        ActiveStreamRegistry registry = startedRegistry(Duration.ofSeconds(30));
        FakeUpstream upstream = new FakeUpstream();
        StreamBridge bridge = bridge(registry);
        bridge.beforeStart(upstream);
        registry.tryRegister(bridge);

        Thread.currentThread().interrupt();
        registry.stop();

        assertThat(upstream.cancelMessage.get()).isEqualTo(StreamBridge.SHUTDOWN_MESSAGE);
        assertThat(registry.activeCount()).isZero();
        assertThat(registry.isRunning()).isFalse();
        assertThat(Thread.interrupted()).as("interrupt flag restored").isTrue();
    }

    @Test
    void streamWithAFinalWriteInProgressKeepsItsSlotButDoesNotDelayStop() {
        ActiveStreamRegistry registry = startedRegistry(Duration.ofSeconds(1));
        RecordingEmitter emitter = RecordingEmitter.stalled();
        FakeUpstream upstream = new FakeUpstream();
        StreamBridge bridge = StreamTestSupport.newBridge(emitter, registry, NO_LEASE);
        bridge.beforeStart(upstream);
        registry.tryRegister(bridge);
        try {
            bridge.onNext(chunk(0, true));
            await().atMost(TIMEOUT).until(() -> emitter.sendInProgress);

            long startedAt = System.nanoTime();
            registry.stop();
            Duration took = Duration.ofNanos(System.nanoTime() - startedAt);

            // The completing stream cannot be cancelled: stop() returns after the drain,
            // nothing is cancelled upstream, and the stream stays tracked until the write returns.
            assertThat(took).isLessThan(Duration.ofSeconds(3));
            assertThat(upstream.cancelMessage.get()).isNull();
            assertThat(registry.activeCount()).isEqualTo(1);

            emitter.gate.countDown();
            await().atMost(TIMEOUT).until(() -> registry.activeCount() == 0);
        } finally {
            emitter.gate.countDown();
        }
    }

    @Test
    void stalledClientsDoNotStretchShutdownBeyondTheForcedCancelWait() {
        ActiveStreamRegistry registry = startedRegistry(Duration.ofSeconds(1));
        List<RecordingEmitter> emitters = List.of(RecordingEmitter.stalled(), RecordingEmitter.stalled());
        List<FakeUpstream> upstreams = List.of(new FakeUpstream(), new FakeUpstream());
        try {
            for (int i = 0; i < emitters.size(); i++) {
                RecordingEmitter emitter = emitters.get(i);
                StreamBridge bridge = StreamTestSupport.newBridge(emitter, registry, NO_LEASE);
                bridge.beforeStart(upstreams.get(i));
                registry.tryRegister(bridge);
                bridge.onNext(chunk(0, false));
                await().atMost(TIMEOUT).until(() -> emitter.sendInProgress);
            }

            long startedAt = System.nanoTime();
            registry.stop();
            Duration took = Duration.ofNanos(System.nanoTime() - startedAt);

            // drain (1s) + forced-cancel wait (2s) + slack; the stuck writes alone would cost
            // StreamBridge.SEND_LOCK_WAIT (5s) each if cancellation waited for them.
            assertThat(took).isLessThan(Duration.ofSeconds(4));
            assertThat(upstreams).allSatisfy(up -> assertThat(up.cancelMessage.get()).isEqualTo(StreamBridge.SHUTDOWN_MESSAGE));
            assertThat(registry.activeCount()).isZero();
        } finally {
            emitters.forEach(emitter -> emitter.gate.countDown());
        }
    }

    @Test
    void rejectPolicyRejectsBeyondMaxActiveStreams() {
        ActiveStreamRegistry registry = startedRegistry(Duration.ofSeconds(1),
                limits(1, OverloadPolicy.REJECT, Duration.ofSeconds(5), 1));
        StreamBridge first = bridge(registry);

        assertThat(registry.admit(first)).isEqualTo(Admission.ADMITTED);
        long startedAt = System.nanoTime();
        assertThat(registry.admit(bridge(registry))).isEqualTo(Admission.REJECTED_LIMIT);
        assertThat(Duration.ofNanos(System.nanoTime() - startedAt)).as("reject does not wait").isLessThan(Duration.ofSeconds(1));
        assertThat(registry.waitingCount()).isZero();

        registry.releasePermit(first);
        assertThat(registry.admittedCount()).isZero();
        assertThat(registry.admit(bridge(registry))).isEqualTo(Admission.ADMITTED);
    }

    @Test
    void waitingRequestIsAdmittedWhenAPermitIsReleased() throws Exception {
        ActiveStreamRegistry registry = startedRegistry(Duration.ofSeconds(1),
                limits(1, OverloadPolicy.WAIT, Duration.ofSeconds(30), 1));
        StreamBridge first = bridge(registry);
        assertThat(registry.admit(first)).isEqualTo(Admission.ADMITTED);
        CompletableFuture<Admission> waiter = CompletableFuture.supplyAsync(() -> registry.admit(bridge(registry)));
        try {
            await().atMost(TIMEOUT).until(() -> registry.waitingCount() == 1);
            assertThat(waiter).isNotDone();

            registry.releasePermit(first);

            assertThat(waiter.get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS)).isEqualTo(Admission.ADMITTED);
            assertThat(registry.waitingCount()).isZero();
            assertThat(registry.admittedCount()).isEqualTo(1);
        } finally {
            registry.stop();
        }
    }

    @Test
    void waitingRequestTimesOutAfterQueueWait() {
        ActiveStreamRegistry registry = startedRegistry(Duration.ofSeconds(1),
                limits(1, OverloadPolicy.WAIT, Duration.ofMillis(500), 1));
        assertThat(registry.admit(bridge(registry))).isEqualTo(Admission.ADMITTED);

        long startedAt = System.nanoTime();
        Admission result = registry.admit(bridge(registry));
        Duration waited = Duration.ofNanos(System.nanoTime() - startedAt);

        assertThat(result).isEqualTo(Admission.REJECTED_QUEUE_TIMEOUT);
        assertThat(waited).isGreaterThanOrEqualTo(Duration.ofMillis(500)).isLessThan(Duration.ofSeconds(5));
        assertThat(registry.waitingCount()).isZero();
    }

    @Test
    void waitersBeyondMaxWaitingAreRejectedImmediately() throws Exception {
        ActiveStreamRegistry registry = startedRegistry(Duration.ofSeconds(1),
                limits(1, OverloadPolicy.WAIT, Duration.ofSeconds(30), 1));
        StreamBridge first = bridge(registry);
        assertThat(registry.admit(first)).isEqualTo(Admission.ADMITTED);
        CompletableFuture<Admission> waiter = CompletableFuture.supplyAsync(() -> registry.admit(bridge(registry)));
        try {
            await().atMost(TIMEOUT).until(() -> registry.waitingCount() == 1);

            long startedAt = System.nanoTime();
            Admission second = registry.admit(bridge(registry));
            Duration took = Duration.ofNanos(System.nanoTime() - startedAt);

            assertThat(second).isEqualTo(Admission.REJECTED_LIMIT);
            assertThat(took).isLessThan(Duration.ofSeconds(2));
            assertThat(registry.waitingCount()).isEqualTo(1);
        } finally {
            registry.releasePermit(first);
            assertThat(waiter.get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS)).isEqualTo(Admission.ADMITTED);
        }
    }

    @Test
    void stopReleasesWaitersWithoutDelayingShutdown() throws Exception {
        ActiveStreamRegistry registry = startedRegistry(Duration.ofSeconds(1),
                limits(1, OverloadPolicy.WAIT, Duration.ofSeconds(30), 1));
        FakeUpstream upstream = new FakeUpstream();
        StreamBridge holder = bridge(registry);
        holder.beforeStart(upstream);
        assertThat(registry.admit(holder)).isEqualTo(Admission.ADMITTED);
        CompletableFuture<Admission> waiter = CompletableFuture.supplyAsync(() -> registry.admit(bridge(registry)));
        await().atMost(TIMEOUT).until(() -> registry.waitingCount() == 1);

        long startedAt = System.nanoTime();
        CompletableFuture<Void> stopping = CompletableFuture.runAsync(registry::stop);
        assertThat(waiter.get(2, TimeUnit.SECONDS)).isEqualTo(Admission.REJECTED_SHUTDOWN);
        stopping.get(4, TimeUnit.SECONDS);
        Duration took = Duration.ofNanos(System.nanoTime() - startedAt);

        // The waiter did not extend stop(): drain (1s) + forced cancel only.
        assertThat(took).isLessThan(Duration.ofSeconds(4));
        assertThat(registry.waitingCount()).isZero();
        assertThat(upstream.cancelMessage.get()).isEqualTo(StreamBridge.SHUTDOWN_MESSAGE);
    }

    @Test
    void stuckFinalWriteDoesNotBlockAdmissionOfANewStream() {
        ActiveStreamRegistry registry = startedRegistry(Duration.ofSeconds(1),
                limits(1, OverloadPolicy.REJECT, Duration.ofSeconds(5), 1));
        RecordingEmitter emitter = RecordingEmitter.stalled();
        CountingLease lease = new CountingLease();
        StreamBridge stuck = StreamTestSupport.newBridge(emitter, registry, lease);
        stuck.beforeStart(new FakeUpstream());
        assertThat(registry.admit(stuck)).isEqualTo(Admission.ADMITTED);
        try {
            stuck.onNext(chunk(0, true));
            await().atMost(TIMEOUT).until(() -> emitter.sendInProgress);

            assertThat(registry.admittedCount()).isZero();
            assertThat(registry.activeCount()).isEqualTo(1);
            assertThat(lease.closes.get()).isEqualTo(1);
            assertThat(registry.admit(bridge(registry))).isEqualTo(Admission.ADMITTED);
        } finally {
            emitter.gate.countDown();
        }
        // The stuck stream leaves the tracked set once its write returns; the new one remains.
        await().atMost(TIMEOUT).until(() -> registry.activeCount() == 1);
        assertThat(lease.closes.get()).isEqualTo(1);
    }
}
