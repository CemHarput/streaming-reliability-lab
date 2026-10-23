package io.github.cemharput.streamlab.gateway.stream;

import static io.github.cemharput.streamlab.gateway.stream.StreamTestSupport.NO_LEASE;
import static io.github.cemharput.streamlab.gateway.stream.StreamTestSupport.chunk;
import static io.github.cemharput.streamlab.gateway.stream.StreamTestSupport.startedRegistry;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import io.github.cemharput.streamlab.gateway.stream.StreamTestSupport.FakeUpstream;
import io.github.cemharput.streamlab.gateway.stream.StreamTestSupport.RecordingEmitter;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

// SEPARATE_THREAD: the default mode only interrupts the test thread, which a wait on a lock
// (the earlier design) ignores. Here the test fails after 15s even if its thread stays blocked.
@Timeout(value = 15, threadMode = Timeout.ThreadMode.SEPARATE_THREAD)
class ActiveStreamRegistryTest {

    private static final Duration TIMEOUT = Duration.ofSeconds(5);

    @Test
    void interruptedStopStillCancelsRemainingStreams() {
        ActiveStreamRegistry registry = startedRegistry(Duration.ofSeconds(30));
        FakeUpstream upstream = new FakeUpstream();
        StreamBridge bridge = new StreamBridge("r", RecordingEmitter.responsive(), registry, NO_LEASE);
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
    void streamWithAFinalWriteInProgressKeepsItsSlotButDoesNotDelayStop() throws Exception {
        ActiveStreamRegistry registry = startedRegistry(Duration.ofSeconds(1));
        RecordingEmitter emitter = RecordingEmitter.stalled();
        FakeUpstream upstream = new FakeUpstream();
        StreamBridge bridge = new StreamBridge("r", emitter, registry, NO_LEASE);
        bridge.beforeStart(upstream);
        registry.tryRegister(bridge);
        try {
            CompletableFuture<Void> finalWrite = CompletableFuture.runAsync(() -> bridge.onNext(chunk(0, true)));
            await().atMost(TIMEOUT).until(() -> emitter.sendInProgress);

            long startedAt = System.nanoTime();
            registry.stop();
            Duration took = Duration.ofNanos(System.nanoTime() - startedAt);

            // The completing stream cannot be cancelled: stop() returns after the drain,
            // nothing is cancelled upstream, and the slot is held until the write returns.
            assertThat(took).isLessThan(Duration.ofSeconds(3));
            assertThat(upstream.cancelMessage.get()).isNull();
            assertThat(registry.activeCount()).isEqualTo(1);

            emitter.gate.countDown();
            finalWrite.get(TIMEOUT.toMillis(), java.util.concurrent.TimeUnit.MILLISECONDS);
            assertThat(registry.activeCount()).isZero();
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
                StreamBridge bridge = new StreamBridge("r" + i, emitter, registry, NO_LEASE);
                bridge.beforeStart(upstreams.get(i));
                registry.tryRegister(bridge);
                CompletableFuture.runAsync(() -> bridge.onNext(chunk(0, false)));
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
}
