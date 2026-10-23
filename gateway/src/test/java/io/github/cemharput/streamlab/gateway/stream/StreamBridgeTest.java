package io.github.cemharput.streamlab.gateway.stream;

import static io.github.cemharput.streamlab.gateway.stream.StreamTestSupport.NO_LEASE;
import io.github.cemharput.streamlab.gateway.stream.StreamTestSupport.CountingLease;
import io.github.cemharput.streamlab.gateway.upstream.ChannelProvider;
import static io.github.cemharput.streamlab.gateway.stream.StreamTestSupport.chunk;
import static io.github.cemharput.streamlab.gateway.stream.StreamTestSupport.startedRegistry;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import io.github.cemharput.streamlab.gateway.stream.StreamTestSupport.FakeUpstream;
import io.github.cemharput.streamlab.gateway.stream.StreamTestSupport.RecordingEmitter;
import io.grpc.Status;
import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

// SEPARATE_THREAD: the default mode only interrupts the test thread, which a wait on a lock
// (the earlier design) ignores. Here the test fails after 10s even if its thread stays blocked.
@Timeout(value = 10, threadMode = Timeout.ThreadMode.SEPARATE_THREAD)
class StreamBridgeTest {

    private static final Duration TIMEOUT = Duration.ofSeconds(5);

    private final ActiveStreamRegistry registry = startedRegistry(Duration.ofSeconds(1));
    private final FakeUpstream upstream = new FakeUpstream();

    private StreamBridge register(RecordingEmitter emitter) {
        return register(emitter, NO_LEASE);
    }

    private StreamBridge register(RecordingEmitter emitter, ChannelProvider.Lease lease) {
        StreamBridge bridge = new StreamBridge("r", emitter, registry, lease);
        bridge.beforeStart(upstream);
        assertThat(registry.tryRegister(bridge)).isTrue();
        return bridge;
    }

    @Test
    void shutdownCancelDoesNotWaitForStalledClientWrite() throws Exception {
        RecordingEmitter emitter = RecordingEmitter.stalled();
        StreamBridge bridge = register(emitter);
        try {
            CompletableFuture<Void> stalledWrite = CompletableFuture.runAsync(() -> bridge.onNext(chunk(0, false)));
            await().atMost(TIMEOUT).until(() -> emitter.sendInProgress);

            long startedAt = System.nanoTime();
            CompletableFuture<Void> terminalEvent = bridge.cancelForShutdown();
            Duration took = Duration.ofNanos(System.nanoTime() - startedAt);

            // Upstream is cancelled and the slot freed while the client write is still stuck.
            assertThat(took).isLessThan(Duration.ofSeconds(1));
            assertThat(upstream.cancelMessage.get()).isEqualTo(StreamBridge.SHUTDOWN_MESSAGE);
            assertThat(registry.activeCount()).isZero();
            assertThat(terminalEvent).isNotDone();

            emitter.gate.countDown();
            terminalEvent.get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
            stalledWrite.get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);

            // The in-flight chunk is delivered first; the single terminal event is last.
            assertThat(emitter.events).hasSize(2);
            assertThat(emitter.events.get(0)).contains("event:chunk");
            assertThat(emitter.events.get(1)).contains("event:error").contains("CANCELLED").contains(StreamBridge.SHUTDOWN_MESSAGE);
            assertThat(emitter.completed).isTrue();
        } finally {
            emitter.gate.countDown();
        }
    }

    @Test
    void shutdownCannotCancelAStreamWhoseFinalChunkIsBeingWritten() throws Exception {
        RecordingEmitter emitter = RecordingEmitter.stalled();
        StreamBridge bridge = register(emitter);
        try {
            CompletableFuture<Void> finalWrite = CompletableFuture.runAsync(() -> bridge.onNext(chunk(0, true)));
            await().atMost(TIMEOUT).until(() -> emitter.sendInProgress);

            // COMPLETED was claimed before the write began, so shutdown loses the race.
            bridge.cancelForShutdown().get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
            assertThat(upstream.cancelMessage.get()).isNull();
            assertThat(registry.activeCount()).isEqualTo(1);

            emitter.gate.countDown();
            finalWrite.get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);

            assertThat(emitter.events).hasSize(2);
            assertThat(emitter.events.get(0)).contains("event:chunk");
            assertThat(emitter.events.get(1)).contains("event:complete");
            assertThat(registry.activeCount()).isZero();
            assertThat(emitter.completed).isTrue();
        } finally {
            emitter.gate.countDown();
        }
    }

    @Test
    void failedWriteOfTheFinalChunkStillFreesTheSlot() {
        RecordingEmitter emitter = RecordingEmitter.disconnected();
        CountingLease lease = new CountingLease();
        StreamBridge bridge = register(emitter, lease);

        bridge.onNext(chunk(0, true));
        // Late upstream callbacks must neither release again nor send anything.
        bridge.onCompleted();
        bridge.onError(Status.CANCELLED.asRuntimeException());

        // The upstream call has already completed, so there is nothing to cancel.
        assertThat(upstream.cancelMessage.get()).isNull();
        assertThat(registry.activeCount()).isZero();
        assertThat(lease.closes.get()).as("lease released exactly once").isEqualTo(1);
        assertThat(emitter.events).isEmpty();
        assertThat(emitter.completed).isFalse();
    }

    @Test
    void nothingIsSentAfterTheTerminalEvent() {
        RecordingEmitter emitter = RecordingEmitter.responsive();
        StreamBridge bridge = register(emitter);

        bridge.cancelForShutdown().orTimeout(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS).join();
        bridge.onNext(chunk(0, false));
        bridge.onError(Status.CANCELLED.asRuntimeException());
        bridge.onCompleted();

        assertThat(emitter.events).hasSize(1);
        assertThat(emitter.events.get(0)).contains("event:error");
    }

    @Test
    void failedClientWriteCancelsUpstreamAndFreesTheSlot() {
        RecordingEmitter emitter = RecordingEmitter.disconnected();
        StreamBridge bridge = register(emitter);

        bridge.onNext(chunk(0, false));

        assertThat(upstream.cancelMessage.get()).isEqualTo("client disconnected");
        assertThat(registry.activeCount()).isZero();
        bridge.onError(Status.CANCELLED.asRuntimeException());
        assertThat(emitter.events).isEmpty();
    }

    @Test
    void upstreamErrorEndsTheStreamWithOneErrorEvent() {
        RecordingEmitter emitter = RecordingEmitter.responsive();
        StreamBridge bridge = register(emitter);

        bridge.onNext(chunk(0, false));
        bridge.onError(Status.UNAVAILABLE.withDescription("boom").asRuntimeException());
        bridge.onCompleted();

        assertThat(emitter.events).hasSize(2);
        assertThat(emitter.events.get(1)).contains("event:error").contains("UNAVAILABLE").contains("boom");
        assertThat(registry.activeCount()).isZero();
        assertThat(emitter.completed).isTrue();
    }
}
