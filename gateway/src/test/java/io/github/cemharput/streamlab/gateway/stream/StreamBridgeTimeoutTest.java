package io.github.cemharput.streamlab.gateway.stream;

import static io.github.cemharput.streamlab.gateway.stream.StreamTestSupport.NO_LEASE;
import static io.github.cemharput.streamlab.gateway.stream.StreamTestSupport.chunk;
import static io.github.cemharput.streamlab.gateway.stream.StreamTestSupport.limits;
import static io.github.cemharput.streamlab.gateway.stream.StreamTestSupport.startedRegistry;
import static io.github.cemharput.streamlab.gateway.stream.StreamTestSupport.startedTimeouts;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import io.github.cemharput.streamlab.gateway.config.GatewayProperties.OverloadPolicy;
import io.github.cemharput.streamlab.gateway.stream.StreamTestSupport.CountingLease;
import io.github.cemharput.streamlab.gateway.stream.StreamTestSupport.FakeUpstream;
import io.github.cemharput.streamlab.gateway.stream.StreamTestSupport.RecordingEmitter;
import io.github.cemharput.streamlab.gateway.upstream.ChannelProvider;
import io.grpc.Status;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import java.time.Duration;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

/** Timeout timers of {@link StreamBridge}: claim, release, ordering and leaks, with a fake upstream. */
@Timeout(value = 30, threadMode = Timeout.ThreadMode.SEPARATE_THREAD)
class StreamBridgeTimeoutTest {

    private static final Duration TIMEOUT = Duration.ofSeconds(5);
    private static final Duration SHORT = Duration.ofMillis(300);
    private static final String FIRST_MESSAGE = "first chunk timeout";
    private static final String IDLE_MESSAGE = "idle timeout between chunks";

    private final FakeUpstream upstream = new FakeUpstream();
    private SimpleMeterRegistry meters;
    private ActiveStreamRegistry registry;
    private StreamMetrics metrics;
    private StreamTimeouts timeouts;

    @BeforeEach
    void setUp() {
        useRegistry(2);
        useTimeouts(SHORT, SHORT);
    }

    @AfterEach
    void tearDown() {
        timeouts.stop();
    }

    private void useRegistry(int maxActive) {
        registry = startedRegistry(Duration.ofSeconds(1), limits(maxActive, OverloadPolicy.REJECT, Duration.ofSeconds(1), 1));
        meters = new SimpleMeterRegistry();
        metrics = new StreamMetrics(meters, registry);
    }

    private void useTimeouts(Duration first, Duration idle) {
        if (timeouts != null) {
            timeouts.stop();
        }
        timeouts = startedTimeouts(first, idle);
    }

    /** Admission first, then the stub's beforeStart: the order of the controller. */
    private StreamBridge register(RecordingEmitter emitter, ChannelProvider.Lease lease, int capacity, FakeUpstream call) {
        StreamBridge bridge = StreamTestSupport.newBridge(emitter, registry, lease, capacity, metrics, timeouts);
        assertThat(registry.tryRegister(bridge)).isTrue();
        bridge.beforeStart(call);
        return bridge;
    }

    private StreamBridge register(RecordingEmitter emitter, ChannelProvider.Lease lease, int capacity) {
        return register(emitter, lease, capacity, upstream);
    }

    private double timeoutCount(String kind) {
        return meters.get("timeouts_total").tag("kind", kind).counter().count();
    }

    @Test
    void firstChunkTimeoutClaimsOnceCancelsUpstreamAndReleasesCapacity() {
        useRegistry(1);
        RecordingEmitter emitter = RecordingEmitter.responsive();
        CountingLease lease = new CountingLease();
        long startedAt = System.nanoTime();
        StreamBridge bridge = register(emitter, lease, 16);
        assertThat(bridge.timerArmed()).isTrue();
        assertThat(timeouts.pendingTimers()).isEqualTo(1);
        assertThat(timeoutCount("first_chunk")).isZero();

        await().atMost(TIMEOUT).until(() -> emitter.events.size() == 1);
        Duration elapsed = Duration.ofNanos(System.nanoTime() - startedAt);

        assertThat(elapsed).isGreaterThanOrEqualTo(SHORT);
        assertThat(emitter.events.get(0)).contains("event:error").contains("DEADLINE_EXCEEDED").contains(FIRST_MESSAGE);
        assertThat(upstream.cancelMessage.get()).isEqualTo(FIRST_MESSAGE);
        await().atMost(TIMEOUT).until(() -> registry.activeCount() == 0);
        assertThat(registry.admittedCount()).isZero();
        assertThat(lease.closes.get()).isEqualTo(1);
        assertThat(timeoutCount("first_chunk")).isEqualTo(1);
        assertThat(timeoutCount("idle")).isZero();
        assertThat(emitter.completed).isTrue();
        assertThat(timeouts.pendingTimers()).isZero();
        // The slot is free again for a new stream.
        StreamBridge next = StreamTestSupport.newBridge(RecordingEmitter.responsive(), registry, NO_LEASE, 16, metrics, timeouts);
        assertThat(registry.tryRegister(next)).isTrue();
        next.cancelForShutdown().orTimeout(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS).join();

        await().during(Duration.ofSeconds(1)).atMost(TIMEOUT).untilAsserted(() -> {
            assertThat(emitter.events).hasSize(1);
            assertThat(lease.closes.get()).isEqualTo(1);
            assertThat(timeoutCount("first_chunk")).isEqualTo(1);
        });
    }

    @Test
    void eachReceivedChunkResetsIdleTimer() {
        useTimeouts(Duration.ofSeconds(2), Duration.ofSeconds(2));
        RecordingEmitter emitter = RecordingEmitter.responsive();
        StreamBridge bridge = register(emitter, NO_LEASE, 16);

        // 8 x 300 ms = 2.4 s in total, longer than the 2 s limit, but every gap is only about
        // a seventh of it, so a slow runner has room before a gap could reach the limit.
        for (int i = 0; i < 8; i++) {
            upstream.deliver(bridge, chunk(i, false));
            await().during(Duration.ofMillis(300)).pollInterval(Duration.ofMillis(20)).atMost(TIMEOUT)
                    .untilAsserted(() -> {
                        assertThat(upstream.cancelMessage.get()).isNull();
                        assertThat(timeoutCount("idle")).isZero();
                    });
        }

        await().atMost(TIMEOUT).until(() -> emitter.events.size() == 9);
        assertThat(emitter.events.get(8)).contains("event:error").contains("DEADLINE_EXCEEDED").contains(IDLE_MESSAGE);
        assertThat(upstream.cancelMessage.get()).isEqualTo(IDLE_MESSAGE);
        assertThat(timeoutCount("idle")).isEqualTo(1);
        assertThat(timeoutCount("first_chunk")).isZero();
    }

    @Test
    void idleTimerPausedWhileCreditExhausted() {
        useTimeouts(Duration.ofMillis(500), Duration.ofMillis(500));
        RecordingEmitter emitter = RecordingEmitter.stalled();
        StreamBridge bridge = register(emitter, NO_LEASE, 2);
        try {
            upstream.deliver(bridge, chunk(0, false));
            upstream.deliver(bridge, chunk(1, false));
            assertThat(upstream.credit()).isZero();

            // Credit 0: upstream may not send, so its silence is not a timeout.
            await().during(Duration.ofMillis(1600)).atMost(TIMEOUT).untilAsserted(() -> {
                assertThat(upstream.cancelMessage.get()).isNull();
                assertThat(timeoutCount("idle")).isZero();
                assertThat(timeoutCount("first_chunk")).isZero();
                assertThat(bridge.timerArmed()).isFalse();
                assertThat(timeouts.pendingTimers()).isZero();
            });

            emitter.gate.countDown();

            // The client reads again: credit comes back and the idle timer is armed again.
            // The bridge arms the timer (creditGranted) just before it calls request(1), so both
            // conditions are awaited together instead of assuming request(1) already ran.
            await().atMost(TIMEOUT).pollInterval(Duration.ofMillis(5)).untilAsserted(() -> {
                assertThat(bridge.timerArmed()).isTrue();
                assertThat(upstream.requested.get()).isGreaterThanOrEqualTo(1);
            });
            // Upstream sends nothing more, so the idle timeout fires after the two chunks.
            await().atMost(TIMEOUT).until(() -> emitter.events.size() == 3);
            assertThat(emitter.events.get(0)).contains("event:chunk");
            assertThat(emitter.events.get(1)).contains("event:chunk");
            assertThat(emitter.events.get(2)).contains("event:error").contains(IDLE_MESSAGE);
            assertThat(timeoutCount("idle")).isEqualTo(1);
        } finally {
            emitter.gate.countDown();
        }
    }

    @ParameterizedTest
    @ValueSource(strings = {"error", "last"})
    void upstreamEndAndLastChunkDisarmTimer(String scenario) {
        RecordingEmitter emitter = RecordingEmitter.stalled();
        StreamBridge bridge = register(emitter, NO_LEASE, 4);
        try {
            upstream.deliver(bridge, chunk(0, false));
            // The 300 ms idle timer is already running here, so poll fast: the default 100 ms
            // poll would eat a third of the margin before the upstream end disarms it.
            await().atMost(TIMEOUT).pollInterval(Duration.ofMillis(5)).until(() -> emitter.sendInProgress);
            if (scenario.equals("error")) {
                bridge.onError(Status.UNAVAILABLE.withDescription("boom").asRuntimeException());
            } else {
                upstream.deliver(bridge, chunk(1, true));
            }

            // Upstream is done; a slow client must not turn into a timeout.
            await().during(Duration.ofSeconds(1)).atMost(TIMEOUT).untilAsserted(() -> {
                assertThat(upstream.cancelMessage.get()).isNull();
                assertThat(bridge.timerArmed()).isFalse();
                assertThat(timeouts.pendingTimers()).isZero();
                assertThat(timeoutCount("idle")).isZero();
                assertThat(timeoutCount("first_chunk")).isZero();
            });

            emitter.gate.countDown();
            if (scenario.equals("error")) {
                await().atMost(TIMEOUT).until(() -> emitter.events.size() == 2);
                assertThat(emitter.events.get(0)).contains("event:chunk");
                assertThat(emitter.events.get(1)).contains("event:error").contains("UNAVAILABLE");
            } else {
                await().atMost(TIMEOUT).until(() -> emitter.events.size() == 3);
                assertThat(emitter.events.get(0)).contains("event:chunk");
                assertThat(emitter.events.get(1)).contains("event:chunk");
                assertThat(emitter.events.get(2)).contains("event:complete");
            }
            await().atMost(TIMEOUT).until(() -> registry.activeCount() == 0);
        } finally {
            emitter.gate.countDown();
        }
    }

    @Test
    void timeoutDoesNotWaitForStalledClientWrite() {
        RecordingEmitter emitter = RecordingEmitter.stalled();
        CountingLease lease = new CountingLease();
        StreamBridge bridge = register(emitter, lease, 16);
        try {
            upstream.deliver(bridge, chunk(0, false));
            await().atMost(TIMEOUT).until(() -> emitter.sendInProgress);

            // The chunk write is stuck; the idle timeout still cancels upstream and frees capacity.
            // timedOut() cancels upstream first and releases capacity / counts the metric after it,
            // so every assertion waits instead of racing the scheduler thread.
            await().atMost(TIMEOUT).untilAsserted(() -> {
                assertThat(upstream.cancelMessage.get()).isEqualTo(IDLE_MESSAGE);
                assertThat(lease.closes.get()).isEqualTo(1);
                assertThat(registry.admittedCount()).isZero();
                assertThat(registry.activeCount()).isEqualTo(1);
                assertThat(timeoutCount("idle")).isEqualTo(1);
            });

            emitter.gate.countDown();
            await().atMost(TIMEOUT).until(() -> emitter.events.size() == 2);
            assertThat(emitter.events.get(0)).contains("event:chunk");
            assertThat(emitter.events.get(1)).contains("event:error").contains("DEADLINE_EXCEEDED");
            await().atMost(TIMEOUT).until(() -> registry.activeCount() == 0);
            await().during(Duration.ofSeconds(1)).atMost(TIMEOUT).untilAsserted(() -> {
                assertThat(emitter.events).hasSize(2);
                assertThat(lease.closes.get()).isEqualTo(1);
            });
        } finally {
            emitter.gate.countDown();
        }
    }

    @Test
    void timersOfOtherStreamsFireWhileOneTerminalWriteIsStuck() {
        RecordingEmitter stuck = RecordingEmitter.stalled();
        register(stuck, NO_LEASE, 16);
        try {
            // The first stream's error write is stuck on the client.
            await().atMost(TIMEOUT).until(() -> stuck.sendInProgress);
            assertThat(timeoutCount("first_chunk")).isEqualTo(1);

            FakeUpstream otherCall = new FakeUpstream();
            RecordingEmitter other = RecordingEmitter.responsive();
            register(other, NO_LEASE, 16, otherCall);

            // The shared scheduler thread was not blocked by the stuck write.
            await().atMost(TIMEOUT).until(() -> other.events.size() == 1);
            assertThat(other.events.get(0)).contains("event:error").contains(FIRST_MESSAGE);
            assertThat(otherCall.cancelMessage.get()).isEqualTo(FIRST_MESSAGE);
            assertThat(timeoutCount("first_chunk")).isEqualTo(2);
        } finally {
            stuck.gate.countDown();
        }
    }

    @Test
    void timeoutThenShutdownCancelGivesOneTerminal() {
        RecordingEmitter emitter = RecordingEmitter.responsive();
        CountingLease lease = new CountingLease();
        StreamBridge bridge = register(emitter, lease, 16);
        await().atMost(TIMEOUT).until(() -> FIRST_MESSAGE.equals(upstream.cancelMessage.get()));

        // The timeout already holds the claim: shutdown has nothing left to cancel.
        assertThat(bridge.cancelForShutdown()).isDone();

        await().atMost(TIMEOUT).until(() -> emitter.events.size() == 1);
        await().during(Duration.ofMillis(600)).atMost(TIMEOUT).untilAsserted(() -> {
            assertThat(emitter.events).hasSize(1);
            assertThat(emitter.events.get(0)).contains("event:error").contains("DEADLINE_EXCEEDED").doesNotContain("CANCELLED");
            assertThat(upstream.cancelMessage.get()).isEqualTo(FIRST_MESSAGE);
            assertThat(lease.closes.get()).isEqualTo(1);
        });
    }

    @Test
    void shutdownCancelThenTimerNeverFires() {
        RecordingEmitter emitter = RecordingEmitter.responsive();
        CountingLease lease = new CountingLease();
        StreamBridge bridge = register(emitter, lease, 16);

        bridge.cancelForShutdown().orTimeout(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS).join();

        assertThat(bridge.timerArmed()).isFalse();
        assertThat(timeouts.pendingTimers()).isZero();
        await().during(Duration.ofSeconds(1)).atMost(TIMEOUT).untilAsserted(() -> {
            assertThat(emitter.events).hasSize(1);
            assertThat(emitter.events.get(0)).contains("event:error").contains("CANCELLED");
            assertThat(upstream.cancelMessage.get()).isEqualTo(StreamBridge.SHUTDOWN_MESSAGE);
            assertThat(timeoutCount("first_chunk")).isZero();
            assertThat(timeoutCount("idle")).isZero();
            assertThat(lease.closes.get()).isEqualTo(1);
        });
    }

    @Test
    void lateUpstreamCallbacksAfterTimeoutSendNothing() {
        RecordingEmitter emitter = RecordingEmitter.responsive();
        CountingLease lease = new CountingLease();
        StreamBridge bridge = register(emitter, lease, 16);
        await().atMost(TIMEOUT).until(() -> emitter.events.size() == 1);
        await().atMost(TIMEOUT).until(() -> registry.activeCount() == 0);

        bridge.onNext(chunk(0, false));
        bridge.onNext(chunk(1, true));
        bridge.onError(Status.CANCELLED.asRuntimeException());
        bridge.onCompleted();

        await().during(Duration.ofSeconds(1)).atMost(TIMEOUT).untilAsserted(() -> {
            assertThat(emitter.events).hasSize(1);
            assertThat(lease.closes.get()).isEqualTo(1);
            assertThat(upstream.requested.get()).isZero();
            assertThat(bridge.queuedChunks()).isZero();
            assertThat(bridge.timerArmed()).isFalse();
            assertThat(timeoutCount("first_chunk")).isEqualTo(1);
        });
    }

    @ParameterizedTest
    @ValueSource(strings = {"COMPLETED", "UPSTREAM_ERROR", "CLIENT_GONE", "SHUTDOWN", "OVERFLOW"})
    void noTimerLeftAfterAnyTerminalOutcome(String outcome) {
        RecordingEmitter emitter = switch (outcome) {
            case "CLIENT_GONE" -> RecordingEmitter.disconnected();
            case "OVERFLOW" -> RecordingEmitter.stalled();
            default -> RecordingEmitter.responsive();
        };
        StreamBridge bridge = register(emitter, new CountingLease(), outcome.equals("OVERFLOW") ? 1 : 16);
        try {
            // Positive control: the first-chunk timer is armed while the stream is live.
            assertThat(bridge.timerArmed()).isTrue();
            assertThat(timeouts.pendingTimers()).isEqualTo(1);

            switch (outcome) {
                case "COMPLETED" -> bridge.onNext(chunk(0, true));
                case "UPSTREAM_ERROR" -> bridge.onError(Status.UNAVAILABLE.asRuntimeException());
                case "CLIENT_GONE" -> bridge.onNext(chunk(0, false));
                case "SHUTDOWN" -> bridge.cancelForShutdown();
                case "OVERFLOW" -> {
                    bridge.onNext(chunk(0, false));
                    await().atMost(TIMEOUT).until(() -> emitter.sendInProgress);
                    bridge.onNext(chunk(1, false));
                    bridge.onNext(chunk(2, false));
                }
                default -> throw new IllegalArgumentException(outcome);
            }

            await().atMost(TIMEOUT).until(() -> !bridge.timerArmed());
            assertThat(timeouts.pendingTimers()).isZero();
            await().during(Duration.ofSeconds(1)).atMost(TIMEOUT).untilAsserted(() -> {
                assertThat(timeoutCount("first_chunk")).isZero();
                assertThat(timeoutCount("idle")).isZero();
                assertThat(timeouts.pendingTimers()).isZero();
            });
        } finally {
            emitter.gate.countDown();
        }
    }
}
