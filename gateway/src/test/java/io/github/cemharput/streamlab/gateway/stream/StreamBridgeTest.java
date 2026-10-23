package io.github.cemharput.streamlab.gateway.stream;

import static io.github.cemharput.streamlab.gateway.stream.StreamTestSupport.NO_LEASE;
import static io.github.cemharput.streamlab.gateway.stream.StreamTestSupport.chunk;
import static io.github.cemharput.streamlab.gateway.stream.StreamTestSupport.gauge;
import static io.github.cemharput.streamlab.gateway.stream.StreamTestSupport.limits;
import static io.github.cemharput.streamlab.gateway.stream.StreamTestSupport.startedRegistry;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import io.github.cemharput.streamlab.gateway.config.GatewayProperties.OverloadPolicy;
import io.github.cemharput.streamlab.gateway.stream.StreamTestSupport.CountingLease;
import io.github.cemharput.streamlab.gateway.stream.StreamTestSupport.FakeUpstream;
import io.github.cemharput.streamlab.gateway.stream.StreamTestSupport.RecordingEmitter;
import io.github.cemharput.streamlab.gateway.stream.StreamTestSupport.SteppingEmitter;
import io.github.cemharput.streamlab.gateway.upstream.ChannelProvider;
import io.grpc.Status;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.springframework.web.servlet.mvc.method.annotation.SseEmitter;

// SEPARATE_THREAD: the default mode only interrupts the test thread, which a wait on a lock
// (the earlier design) ignores. Here the test fails after 10s even if its thread stays blocked.
@Timeout(value = 10, threadMode = Timeout.ThreadMode.SEPARATE_THREAD)
class StreamBridgeTest {

    private static final Duration TIMEOUT = Duration.ofSeconds(5);
    /** For "nothing else happens" assertions: Awaitility keeps the condition true for this long. */
    private static final Duration QUIET = Duration.ofMillis(300);

    private final ActiveStreamRegistry registry = startedRegistry(Duration.ofSeconds(1));
    private final FakeUpstream upstream = new FakeUpstream();
    private final SimpleMeterRegistry meters = new SimpleMeterRegistry();
    private final StreamMetrics metrics = new StreamMetrics(meters, registry);

    private StreamBridge register(SseEmitter emitter) {
        return register(emitter, NO_LEASE, 16);
    }

    private StreamBridge register(SseEmitter emitter, ChannelProvider.Lease lease) {
        return register(emitter, lease, 16);
    }

    private StreamBridge register(SseEmitter emitter, ChannelProvider.Lease lease, int capacity) {
        StreamBridge bridge = StreamTestSupport.newBridge(emitter, registry, lease, capacity, metrics);
        bridge.beforeStart(upstream);
        assertThat(registry.tryRegister(bridge)).isTrue();
        return bridge;
    }

    private double chunksGauge() {
        return gauge(meters, "queue_depth", "chunks");
    }

    @Test
    void beforeStartDisablesAutoRequestWithQueueCapacity() {
        register(RecordingEmitter.responsive(), NO_LEASE, 4);

        assertThat(upstream.initialRequest.get()).isEqualTo(4);
    }

    @Test
    void shutdownCancelDoesNotWaitForStalledClientWrite() throws Exception {
        RecordingEmitter emitter = RecordingEmitter.stalled();
        StreamBridge bridge = register(emitter);
        try {
            bridge.onNext(chunk(0, false));
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
            await().atMost(TIMEOUT).until(() -> emitter.events.size() == 2);

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
            bridge.onNext(chunk(0, true));
            await().atMost(TIMEOUT).until(() -> emitter.sendInProgress);

            // COMPLETED was claimed before the write began, so shutdown loses the race.
            bridge.cancelForShutdown().get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
            assertThat(upstream.cancelMessage.get()).isNull();
            assertThat(registry.activeCount()).isEqualTo(1);

            emitter.gate.countDown();
            await().atMost(TIMEOUT).until(() -> registry.activeCount() == 0);

            assertThat(emitter.events).hasSize(2);
            assertThat(emitter.events.get(0)).contains("event:chunk");
            assertThat(emitter.events.get(1)).contains("event:complete");
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
        await().atMost(TIMEOUT).until(() -> registry.activeCount() == 0);
        // Late upstream callbacks must neither release again nor send anything.
        bridge.onCompleted();
        bridge.onError(Status.CANCELLED.asRuntimeException());

        await().during(QUIET).atMost(TIMEOUT).untilAsserted(() -> {
            // The upstream call has already completed, so there is nothing to cancel.
            assertThat(upstream.cancelMessage.get()).isNull();
            assertThat(registry.activeCount()).isZero();
            assertThat(lease.closes.get()).as("lease released exactly once").isEqualTo(1);
            assertThat(emitter.events).isEmpty();
            assertThat(emitter.completed).isFalse();
        });
    }

    @Test
    void nothingIsSentAfterTheTerminalEvent() {
        RecordingEmitter emitter = RecordingEmitter.responsive();
        StreamBridge bridge = register(emitter);

        bridge.cancelForShutdown().orTimeout(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS).join();
        bridge.onNext(chunk(0, false));
        bridge.onError(Status.CANCELLED.asRuntimeException());
        bridge.onCompleted();

        await().during(QUIET).atMost(TIMEOUT).untilAsserted(() -> {
            assertThat(emitter.events).hasSize(1);
            assertThat(emitter.events.get(0)).contains("event:error");
        });
    }

    @Test
    void failedClientWriteCancelsUpstreamAndFreesTheSlot() {
        RecordingEmitter emitter = RecordingEmitter.disconnected();
        StreamBridge bridge = register(emitter);

        bridge.onNext(chunk(0, false));

        await().atMost(TIMEOUT).untilAsserted(() -> {
            assertThat(upstream.cancelMessage.get()).isEqualTo("client disconnected");
            assertThat(registry.activeCount()).isZero();
        });
        bridge.onError(Status.CANCELLED.asRuntimeException());
        await().during(QUIET).atMost(TIMEOUT).untilAsserted(() -> {
            assertThat(emitter.events).isEmpty();
            // AC4: no credit is handed back once the stream has a terminal outcome.
            assertThat(upstream.requested.get()).isZero();
        });
    }

    @Test
    void upstreamErrorEndsTheStreamWithOneErrorEvent() {
        RecordingEmitter emitter = RecordingEmitter.responsive();
        StreamBridge bridge = register(emitter);

        bridge.onNext(chunk(0, false));
        bridge.onError(Status.UNAVAILABLE.withDescription("boom").asRuntimeException());
        bridge.onCompleted();

        await().atMost(TIMEOUT).untilAsserted(() -> {
            assertThat(emitter.events).hasSize(2);
            assertThat(emitter.events.get(1)).contains("event:error").contains("UNAVAILABLE").contains("boom");
            assertThat(registry.activeCount()).isZero();
            assertThat(emitter.completed).isTrue();
        });
        await().during(QUIET).atMost(TIMEOUT).untilAsserted(() -> assertThat(emitter.events).hasSize(2));
    }

    @Test
    void upstreamErrorIsDeliveredAfterQueuedChunks() {
        RecordingEmitter emitter = RecordingEmitter.stalled();
        StreamBridge bridge = register(emitter, NO_LEASE, 4);
        try {
            upstream.deliver(bridge, chunk(0, false));
            await().atMost(TIMEOUT).until(() -> emitter.sendInProgress);
            upstream.deliver(bridge, chunk(1, false));
            upstream.deliver(bridge, chunk(2, false));
            bridge.onError(Status.UNAVAILABLE.withDescription("boom").asRuntimeException());
            assertThat(bridge.queuedChunks()).isEqualTo(2);

            emitter.gate.countDown();
            await().atMost(TIMEOUT).until(() -> emitter.events.size() == 4);

            assertThat(emitter.events.get(0)).contains("event:chunk").contains("id:0\n");
            assertThat(emitter.events.get(1)).contains("event:chunk").contains("id:1\n");
            assertThat(emitter.events.get(2)).contains("event:chunk").contains("id:2\n");
            assertThat(emitter.events.get(3)).contains("event:error").contains("UNAVAILABLE");
            await().atMost(TIMEOUT).until(() -> registry.activeCount() == 0);
            assertThat(chunksGauge()).isZero();
        } finally {
            emitter.gate.countDown();
        }
    }

    @Test
    void chunkBeyondGrantedCreditFailsWithResourceExhausted() {
        RecordingEmitter emitter = RecordingEmitter.stalled();
        CountingLease lease = new CountingLease();
        StreamBridge bridge = register(emitter, lease, 2);
        try {
            bridge.onNext(chunk(0, false));
            await().atMost(TIMEOUT).until(() -> emitter.sendInProgress);
            bridge.onNext(chunk(1, false));
            bridge.onNext(chunk(2, false));
            assertThat(bridge.queuedChunks()).isEqualTo(2);

            // The queue is full: a further chunk is a credit violation, never queued.
            bridge.onNext(chunk(3, false));
            bridge.onNext(chunk(4, false));
            assertThat(bridge.queuedChunks()).isLessThanOrEqualTo(2);
            assertThat(upstream.cancelMessage.get()).isEqualTo("per-request queue overflow");
            assertThat(lease.closes.get()).isEqualTo(1);

            emitter.gate.countDown();
            await().atMost(TIMEOUT).until(() -> registry.activeCount() == 0);

            long errors = emitter.events.stream().filter(e -> e.contains("event:error")).count();
            assertThat(errors).isEqualTo(1);
            assertThat(emitter.events.get(emitter.events.size() - 1)).contains("RESOURCE_EXHAUSTED");
            assertThat(emitter.completed).isTrue();
            assertThat(bridge.queuedChunks()).isZero();
            assertThat(chunksGauge()).isZero();
            assertThat(lease.closes.get()).isEqualTo(1);
        } finally {
            emitter.gate.countDown();
        }
    }

    @Test
    void shutdownDiscardsQueuedChunks() throws Exception {
        RecordingEmitter emitter = RecordingEmitter.stalled();
        StreamBridge bridge = register(emitter, NO_LEASE, 4);
        try {
            upstream.deliver(bridge, chunk(0, false));
            await().atMost(TIMEOUT).until(() -> emitter.sendInProgress);
            upstream.deliver(bridge, chunk(1, false));
            upstream.deliver(bridge, chunk(2, false));
            assertThat(chunksGauge()).isEqualTo(2);

            CompletableFuture<Void> terminalEvent = bridge.cancelForShutdown();
            await().atMost(TIMEOUT).untilAsserted(() -> {
                assertThat(bridge.queuedChunks()).isZero();
                assertThat(chunksGauge()).isZero();
            });

            emitter.gate.countDown();
            terminalEvent.get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
            await().atMost(TIMEOUT).until(() -> emitter.events.size() == 2);
            assertThat(emitter.events.get(0)).contains("event:chunk").contains("id:0\n");
            assertThat(emitter.events.get(1)).contains("event:error").contains("CANCELLED");
            await().during(QUIET).atMost(TIMEOUT).untilAsserted(() -> assertThat(emitter.events).hasSize(2));
        } finally {
            emitter.gate.countDown();
        }
    }

    @Test
    void slowConsumerQueueNeverExceedsCapacity() {
        int capacity = 4;
        int total = 12;
        SteppingEmitter emitter = new SteppingEmitter();
        StreamBridge bridge = register(emitter, NO_LEASE, capacity);
        try {
            // A well-behaved producer: only sends while it holds credit.
            int next = 0;
            while (next < total && upstream.credit() > 0) {
                upstream.deliver(bridge, chunk(next, next == total - 1));
                next++;
                assertThat(bridge.queuedChunks()).isLessThanOrEqualTo(capacity);
                assertThat(chunksGauge()).isLessThanOrEqualTo(capacity);
            }
            // The consumer has not read anything: credit is exhausted, the producer is stuck.
            // One chunk is in the (blocked) write, the rest are queued.
            assertThat(next).isEqualTo(capacity);
            assertThat(upstream.credit()).isZero();
            await().atMost(TIMEOUT).until(() -> emitter.sendsStarted.get() == 1);
            assertThat(upstream.requested.get()).isZero();

            // The consumer reads one event at a time; each accepted chunk returns one credit.
            for (int written = 1; written <= total; written++) {
                int expected = written;
                emitter.permits.release();
                await().atMost(TIMEOUT).until(() -> emitter.events.size() >= expected);
                if (written < total) {
                    // request(1) happens after the write returned, outside the lock.
                    await().atMost(TIMEOUT).until(() -> upstream.requested.get() >= expected);
                }
                while (next < total && upstream.credit() > 0) {
                    upstream.deliver(bridge, chunk(next, next == total - 1));
                    next++;
                    assertThat(bridge.queuedChunks()).isLessThanOrEqualTo(capacity);
                    assertThat(chunksGauge()).isLessThanOrEqualTo(capacity);
                }
            }
            // The final chunk was written; the complete event needs its own permit.
            emitter.permits.release();
            await().atMost(TIMEOUT).until(() -> emitter.events.size() == total + 1);

            for (int i = 0; i < total; i++) {
                assertThat(emitter.events.get(i)).contains("event:chunk").contains("id:" + i + "\n");
            }
            assertThat(emitter.events.get(total)).contains("event:complete");
            assertThat(emitter.events.stream().filter(e -> e.contains("event:complete") || e.contains("event:error")))
                    .hasSize(1);
            // No credit is requested after the final chunk or the terminal event.
            assertThat(upstream.requested.get()).isEqualTo(total - 1);
            await().atMost(TIMEOUT).until(() -> registry.activeCount() == 0);
            assertThat(chunksGauge()).isZero();
        } finally {
            emitter.permits.release(1000);
        }
    }

    @Test
    void terminalClaimReleasesPermitAndLeaseBeforeTheFinalWrite() throws Exception {
        ActiveStreamRegistry single = startedRegistry(Duration.ofSeconds(1),
                limits(1, OverloadPolicy.REJECT, Duration.ofSeconds(1), 1));
        RecordingEmitter emitter = RecordingEmitter.stalled();
        CountingLease lease = new CountingLease();
        StreamBridge bridge = StreamTestSupport.newBridge(emitter, single, lease, 16, new StreamMetrics(new SimpleMeterRegistry(), single));
        bridge.beforeStart(upstream);
        assertThat(single.tryRegister(bridge)).isTrue();
        StreamBridge second = StreamTestSupport.newBridge(RecordingEmitter.responsive(), single, NO_LEASE);
        second.beforeStart(new FakeUpstream());
        try {
            bridge.onNext(chunk(0, true));
            await().atMost(TIMEOUT).until(() -> emitter.sendInProgress);

            // COMPLETED is claimed, the final write is stuck: capacity is free, the stream is still tracked.
            assertThat(single.admittedCount()).isZero();
            assertThat(single.activeCount()).isEqualTo(1);
            assertThat(lease.closes.get()).isEqualTo(1);
            assertThat(single.tryRegister(second)).isTrue();

            emitter.gate.countDown();
            await().atMost(TIMEOUT).until(() -> single.activeCount() == 1);
            assertThat(lease.closes.get()).as("lease not closed a second time").isEqualTo(1);
            second.cancelForShutdown().get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
            assertThat(single.activeCount()).isZero();
        } finally {
            emitter.gate.countDown();
        }
    }

    @Test
    void stuckUpstreamErrorWriteDoesNotHoldThePermit() {
        ActiveStreamRegistry single = startedRegistry(Duration.ofSeconds(1),
                limits(1, OverloadPolicy.REJECT, Duration.ofSeconds(1), 1));
        RecordingEmitter emitter = RecordingEmitter.stalled();
        CountingLease lease = new CountingLease();
        StreamBridge bridge = StreamTestSupport.newBridge(emitter, single, lease, 16, new StreamMetrics(new SimpleMeterRegistry(), single));
        bridge.beforeStart(upstream);
        assertThat(single.tryRegister(bridge)).isTrue();
        try {
            bridge.onError(Status.UNAVAILABLE.asRuntimeException());
            await().atMost(TIMEOUT).until(() -> emitter.sendInProgress);

            assertThat(single.admittedCount()).isZero();
            assertThat(single.activeCount()).isEqualTo(1);
            assertThat(lease.closes.get()).isEqualTo(1);

            emitter.gate.countDown();
            await().atMost(TIMEOUT).until(() -> single.activeCount() == 0);
            assertThat(lease.closes.get()).isEqualTo(1);
            assertThat(emitter.events).hasSize(1);
        } finally {
            emitter.gate.countDown();
        }
    }
}
