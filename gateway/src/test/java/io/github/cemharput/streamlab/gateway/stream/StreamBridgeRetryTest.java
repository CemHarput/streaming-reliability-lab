package io.github.cemharput.streamlab.gateway.stream;

import static io.github.cemharput.streamlab.gateway.stream.StreamTestSupport.chunk;
import static io.github.cemharput.streamlab.gateway.stream.StreamTestSupport.startedRegistry;
import static io.github.cemharput.streamlab.gateway.stream.StreamTestSupport.startedTimeouts;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import io.github.cemharput.streamlab.gateway.stream.StreamTestSupport.CountingLease;
import io.github.cemharput.streamlab.gateway.stream.StreamTestSupport.FakeUpstream;
import io.github.cemharput.streamlab.gateway.stream.StreamTestSupport.FakeUpstreamStarter;
import io.github.cemharput.streamlab.gateway.stream.StreamTestSupport.FakeUpstreamStarter.Call;
import io.github.cemharput.streamlab.gateway.stream.StreamTestSupport.ManualClock;
import io.github.cemharput.streamlab.gateway.stream.StreamTestSupport.RecordingEmitter;
import io.github.cemharput.streamlab.gateway.upstream.ChannelProvider;
import io.grpc.Status;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.function.LongSupplier;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

/** Retry, resume, budget and stop rules of {@link StreamBridge}, driven by a fake upstream and an injected clock. */
@Timeout(value = 30, threadMode = Timeout.ThreadMode.SEPARATE_THREAD)
class StreamBridgeRetryTest {

    private static final Duration TIMEOUT = Duration.ofSeconds(10);
    private static final Duration NO_BACKOFF = Duration.ofMillis(1);
    private static final Duration BACKOFF = Duration.ofMillis(500);
    /** At least five times the backoff: a start that should not happen would have happened by then. */
    private static final Duration QUIET = Duration.ofMillis(2500);

    private final FakeUpstreamStarter starter = new FakeUpstreamStarter();
    private final CountingLease lease = new CountingLease();
    private SimpleMeterRegistry meters;
    private ActiveStreamRegistry registry;
    private StreamMetrics metrics;
    private StreamTimeouts timeouts;
    private CountDownLatch release;

    @BeforeEach
    void setUp() {
        useRegistry(Duration.ofSeconds(1));
        useTimeouts(Duration.ofSeconds(30), Duration.ofSeconds(30));
        release = new CountDownLatch(1);
    }

    @AfterEach
    void tearDown() {
        release.countDown();
        timeouts.stop();
    }

    private void useRegistry(Duration drain) {
        registry = startedRegistry(drain);
        meters = new SimpleMeterRegistry();
        metrics = new StreamMetrics(meters, registry);
    }

    private void useTimeouts(Duration first, Duration idle) {
        if (timeouts != null) {
            timeouts.stop();
        }
        timeouts = startedTimeouts(first, idle);
    }

    private static RetryPolicy policy(int retries, Duration initial, Duration max, RetryPolicy.Jitter jitter) {
        return policy(retries, initial, max, jitter, System::nanoTime);
    }

    private static RetryPolicy policy(int retries, Duration initial, Duration max, RetryPolicy.Jitter jitter,
            LongSupplier clock) {
        return new RetryPolicy(retries, initial, max, jitter, clock);
    }

    /** Immediate retries: jitter 0 means no wait. */
    private static RetryPolicy immediate(int retries) {
        return policy(retries, NO_BACKOFF, NO_BACKOFF, b -> 0);
    }

    /** A fixed backoff of exactly 500 ms. */
    private static RetryPolicy fixedBackoff(int retries) {
        return policy(retries, BACKOFF, BACKOFF, b -> b);
    }

    private StreamBridge startBridge(RecordingEmitter emitter, int capacity, RetryPolicy policy, Duration deadline) {
        StreamBridge bridge = StreamTestSupport.newRetryingBridge(emitter, registry, lease, capacity, metrics,
                timeouts, policy, starter, deadline);
        assertThat(registry.tryRegister(bridge)).isTrue();
        bridge.start();
        return bridge;
    }

    private StreamBridge startBridge(RecordingEmitter emitter, int capacity, RetryPolicy policy) {
        return startBridge(emitter, capacity, policy, Duration.ofSeconds(60));
    }

    private static Throwable unavailable(String description) {
        return Status.UNAVAILABLE.withDescription(description).asRuntimeException();
    }

    private double retries(String reason) {
        return meters.get("retries_total").tag("reason", reason).counter().count();
    }

    private double exhausted() {
        return meters.get("retry_exhausted_total").counter().count();
    }

    private double timeoutCount(String kind) {
        return meters.get("timeouts_total").tag("kind", kind).counter().count();
    }

    private Call call(int index) {
        return starter.calls.get(index);
    }

    private static long count(List<String> events, String name) {
        return events.stream().filter(e -> e.contains("event:" + name)).count();
    }

    private static void awaitQuietly(CountDownLatch latch) {
        try {
            latch.await();
        } catch (InterruptedException ex) {
            Thread.currentThread().interrupt();
        }
    }

    @Test
    void unavailableBeforeFirstChunkRestartsWithoutResume() {
        RecordingEmitter emitter = RecordingEmitter.responsive();
        startBridge(emitter, 4, immediate(3));
        assertThat(starter.calls).hasSize(1);

        call(0).observer().onError(unavailable("boom-1"));

        await().atMost(TIMEOUT).untilAsserted(() -> {
            assertThat(starter.calls).hasSize(2);
            assertThat(call(1).upstream().initialRequest).hasValue(4);
        });
        assertThat(call(1).request().getResumeFromSeq()).isZero();
        assertThat(call(1).request().getRequestId()).isEqualTo(call(0).request().getRequestId());
        assertThat(call(1).request().getSeed()).isEqualTo(call(0).request().getSeed()).isEqualTo(7);

        call(1).upstream().deliver(call(1).observer(), chunk(0, true));

        await().atMost(TIMEOUT).untilAsserted(() -> {
            assertThat(emitter.completed).isTrue();
            assertThat(emitter.events).hasSize(2);
            assertThat(lease.closes).hasValue(1);
            assertThat(retries("restart")).isEqualTo(1);
            assertThat(timeouts.pendingTimers()).isZero();
            assertThat(registry.admittedCount()).isZero();
        });
        assertThat(emitter.events.get(0)).contains("event:chunk").contains("id:0\n");
        assertThat(emitter.events.get(1)).contains("event:complete");
        assertThat(retries("resume")).isZero();
        assertThat(exhausted()).isZero();
    }

    @Test
    void immediateFailureInsideStartIsRetried() {
        List<String> threads = new CopyOnWriteArrayList<>();
        starter.onStart = call -> {
            threads.add(Thread.currentThread().getName());
            if (starter.calls.size() < 3) {
                call.observer().onError(unavailable("refused-" + starter.calls.size()));
            } else {
                call.upstream().deliver(call.observer(), chunk(0, true));
            }
        };
        RecordingEmitter emitter = RecordingEmitter.responsive();
        Thread caller = Thread.currentThread();

        startBridge(emitter, 4, immediate(3));

        await().atMost(TIMEOUT).untilAsserted(() -> {
            assertThat(emitter.completed).isTrue();
            assertThat(starter.calls).hasSize(3);
            assertThat(retries("restart")).isEqualTo(2);
            assertThat(lease.closes).hasValue(1);
            assertThat(timeouts.pendingTimers()).isZero();
        });
        // The first call starts on the caller; a retry never starts inside a callback or on the scheduler.
        assertThat(threads.get(0)).isEqualTo(caller.getName());
        assertThat(threads.subList(1, 3)).containsOnly("stream-retry");
        assertThat(count(emitter.events, "chunk")).isEqualTo(1);
        assertThat(count(emitter.events, "complete")).isEqualTo(1);
        assertThat(count(emitter.events, "error")).isZero();
    }

    @Test
    void midStreamResumeStartsAfterLastReceivedNotLastWritten() {
        RecordingEmitter emitter = RecordingEmitter.stalled();
        try {
            StreamBridge bridge = startBridge(emitter, 8, immediate(3));
            Call first = call(0);
            first.upstream().deliver(first.observer(), chunk(0, false));
            await().atMost(TIMEOUT).until(() -> emitter.sendInProgress);
            first.upstream().deliver(first.observer(), chunk(1, false));
            first.upstream().deliver(first.observer(), chunk(2, false));
            await().atMost(TIMEOUT).untilAsserted(() -> {
                assertThat(bridge.queuedChunks()).isEqualTo(2);
                assertThat(bridge.bufferedChunks()).isEqualTo(3);
            });

            first.observer().onError(unavailable("cut"));

            await().atMost(TIMEOUT).untilAsserted(() -> {
                assertThat(starter.calls).hasSize(2);
                // 8 capacity minus the 3 chunks that still hold credit.
                assertThat(call(1).upstream().initialRequest).hasValue(5);
            });
            assertThat(call(1).request().getResumeFromSeq()).isEqualTo(3);
            assertThat(call(1).request().getRequestId()).isEqualTo(call(0).request().getRequestId());
            call(1).upstream().deliver(call(1).observer(), chunk(3, false));
            call(1).upstream().deliver(call(1).observer(), chunk(4, false));
            call(1).upstream().deliver(call(1).observer(), chunk(5, true));
        } finally {
            emitter.gate.countDown();
        }

        await().atMost(TIMEOUT).untilAsserted(() -> {
            assertThat(emitter.completed).isTrue();
            assertThat(emitter.events).hasSize(7);
            assertThat(retries("resume")).isEqualTo(1);
            assertThat(lease.closes).hasValue(1);
        });
        for (int i = 0; i < 6; i++) {
            assertThat(emitter.events.get(i)).contains("event:chunk").contains("id:" + i + "\n");
        }
        assertThat(emitter.events.get(6)).contains("event:complete");
        assertThat(retries("restart")).isZero();
    }

    @Test
    void creditFreedWhileTheNewCallStartsIsBankedThenFlushed() {
        RecordingEmitter emitter = RecordingEmitter.stalled();
        CountDownLatch starting = new CountDownLatch(1);
        starter.onStart = call -> {
            if (starter.calls.size() == 2) {
                starting.countDown();
                awaitQuietly(release);
            }
        };
        try {
            StreamBridge bridge = startBridge(emitter, 4, immediate(3));
            Call first = call(0);
            first.upstream().deliver(first.observer(), chunk(0, false));
            await().atMost(TIMEOUT).until(() -> emitter.sendInProgress);
            first.upstream().deliver(first.observer(), chunk(1, false));
            first.observer().onError(unavailable("cut"));
            // The retry call is inside start(): beforeStart ran, the call is not started yet.
            await().atMost(TIMEOUT).until(() -> starting.getCount() == 0);
            FakeUpstream second = call(1).upstream();
            assertThat(second.initialRequest).hasValue(2);

            // The client reads both queued chunks while the new call is still starting.
            emitter.gate.countDown();
            await().atMost(TIMEOUT).until(() -> emitter.events.size() == 2);
            await().during(Duration.ofMillis(500)).atMost(TIMEOUT).untilAsserted(
                    () -> assertThat(second.requested).as("request() before start would throw in grpc").hasValue(0));

            release.countDown();

            await().atMost(TIMEOUT).untilAsserted(() -> {
                assertThat(second.requested).hasValue(2);
                assertThat(bridge.bufferedChunks()).isZero();
                assertThat(second.initialRequest.get() + second.requested.get())
                        .isEqualTo(4 - bridge.bufferedChunks());
            });
        } finally {
            release.countDown();
            emitter.gate.countDown();
        }
    }

    @Test
    void lateCallbacksOfAFailedAttemptAreIgnored() {
        RecordingEmitter emitter = RecordingEmitter.responsive();
        StreamBridge bridge = startBridge(emitter, 4, immediate(3));
        Call first = call(0);
        first.observer().onError(unavailable("cut"));
        await().atMost(TIMEOUT).until(() -> starter.calls.size() == 2);

        first.upstream().deliver(first.observer(), chunk(9, false));
        first.observer().onError(unavailable("late"));
        first.observer().onCompleted();

        await().during(Duration.ofMillis(500)).atMost(TIMEOUT).untilAsserted(() -> {
            assertThat(emitter.events).isEmpty();
            assertThat(bridge.bufferedChunks()).isZero();
            assertThat(bridge.queuedChunks()).isZero();
            assertThat(starter.calls).hasSize(2);
            assertThat(retries("restart")).isEqualTo(1);
        });
        // The live attempt is unaffected.
        call(1).upstream().deliver(call(1).observer(), chunk(0, true));
        await().atMost(TIMEOUT).until(() -> emitter.completed);
        assertThat(count(emitter.events, "complete")).isEqualTo(1);
    }

    @Test
    void exhaustionSendsOneErrorWithTheLastStatus() {
        starter.onStart = call -> call.observer().onError(unavailable("boom-" + starter.calls.size()));
        RecordingEmitter emitter = RecordingEmitter.responsive();

        startBridge(emitter, 4, immediate(2));

        await().atMost(TIMEOUT).untilAsserted(() -> {
            assertThat(emitter.completed).isTrue();
            assertThat(exhausted()).isEqualTo(1);
            assertThat(lease.closes).hasValue(1);
        });
        assertThat(emitter.events).hasSize(1);
        assertThat(emitter.events.get(0)).contains("event:error").contains("UNAVAILABLE").contains("boom-3");
        assertThat(retries("restart")).isEqualTo(2);
        await().during(Duration.ofSeconds(1)).atMost(TIMEOUT).untilAsserted(() -> {
            assertThat(starter.calls).hasSize(3);
            assertThat(emitter.events).hasSize(1);
            assertThat(exhausted()).isEqualTo(1);
            assertThat(lease.closes).hasValue(1);
            assertThat(timeouts.pendingTimers()).isZero();
            assertThat(registry.admittedCount()).isZero();
        });
    }

    @ParameterizedTest
    @EnumSource(value = Status.Code.class,
            names = {"INTERNAL", "INVALID_ARGUMENT", "DEADLINE_EXCEEDED", "RESOURCE_EXHAUSTED", "CANCELLED", "UNKNOWN"})
    void nonRetryableCodesAreNotRetried(Status.Code code) {
        RecordingEmitter emitter = RecordingEmitter.responsive();
        startBridge(emitter, 4, immediate(3));

        call(0).observer().onError(Status.fromCode(code).withDescription("nope").asRuntimeException());

        await().atMost(TIMEOUT).untilAsserted(() -> {
            assertThat(emitter.completed).isTrue();
            assertThat(lease.closes).hasValue(1);
        });
        assertThat(emitter.events).hasSize(1);
        assertThat(emitter.events.get(0)).contains("event:error").contains(code.name());
        await().during(Duration.ofMillis(500)).atMost(TIMEOUT).untilAsserted(() -> {
            assertThat(starter.calls).hasSize(1);
            assertThat(retries("restart")).isZero();
            assertThat(retries("resume")).isZero();
            assertThat(exhausted()).isZero();
        });
    }

    @Test
    void okWithoutLastChunkIsNotRetried() {
        RecordingEmitter emitter = RecordingEmitter.responsive();
        startBridge(emitter, 4, immediate(3));

        call(0).observer().onCompleted();

        await().atMost(TIMEOUT).until(() -> emitter.completed);
        assertThat(emitter.events.get(0)).contains("event:error").contains("INTERNAL");
        await().during(Duration.ofMillis(500)).atMost(TIMEOUT).untilAsserted(
                () -> assertThat(starter.calls).hasSize(1));
        assertThat(exhausted()).isZero();
    }

    @Test
    void firstChunkTimeoutIsNotRetried() {
        useTimeouts(Duration.ofMillis(300), Duration.ofMillis(300));
        RecordingEmitter emitter = RecordingEmitter.responsive();

        startBridge(emitter, 4, immediate(3));

        await().atMost(TIMEOUT).until(() -> emitter.completed);
        assertThat(emitter.events).hasSize(1);
        assertThat(emitter.events.get(0)).contains("event:error").contains("DEADLINE_EXCEEDED")
                .contains("first chunk timeout");
        // 5x the timeout: a retry would have started long ago.
        await().during(Duration.ofMillis(1500)).atMost(TIMEOUT).untilAsserted(() -> {
            assertThat(starter.calls).hasSize(1);
            assertThat(timeoutCount("first_chunk")).isEqualTo(1);
            assertThat(retries("restart")).isZero();
            assertThat(exhausted()).isZero();
            assertThat(timeouts.pendingTimers()).isZero();
        });
        assertThat(lease.closes).hasValue(1);
    }

    @Test
    void lastChunkReceivedIsNeverRetried() {
        RecordingEmitter emitter = RecordingEmitter.stalled();
        try {
            StreamBridge bridge = startBridge(emitter, 4, immediate(3));
            Call first = call(0);
            first.upstream().deliver(first.observer(), chunk(0, false));
            await().atMost(TIMEOUT).until(() -> emitter.sendInProgress);
            first.upstream().deliver(first.observer(), chunk(1, true));
            await().atMost(TIMEOUT).until(() -> bridge.queuedChunks() == 1);

            first.observer().onError(unavailable("after last"));
            await().during(Duration.ofMillis(500)).atMost(TIMEOUT).untilAsserted(() -> {
                assertThat(starter.calls).hasSize(1);
                assertThat(retries("restart")).isZero();
                assertThat(retries("resume")).isZero();
            });
        } finally {
            emitter.gate.countDown();
        }

        // The stream completes from its queue: the late UNAVAILABLE changes nothing.
        await().atMost(TIMEOUT).untilAsserted(() -> {
            assertThat(emitter.completed).isTrue();
            assertThat(emitter.events).hasSize(3);
        });
        assertThat(emitter.events.get(2)).contains("event:complete");
        assertThat(count(emitter.events, "error")).isZero();
        assertThat(exhausted()).isZero();
        assertThat(starter.calls).hasSize(1);
    }

    @Test
    void zeroMaxAttemptsDisablesRetry() {
        RecordingEmitter emitter = RecordingEmitter.responsive();
        startBridge(emitter, 4, immediate(0));

        call(0).observer().onError(unavailable("boom"));

        await().atMost(TIMEOUT).until(() -> emitter.completed);
        assertThat(emitter.events).hasSize(1);
        assertThat(emitter.events.get(0)).contains("event:error").contains("UNAVAILABLE").contains("boom");
        await().during(Duration.ofMillis(500)).atMost(TIMEOUT).untilAsserted(() -> {
            assertThat(starter.calls).hasSize(1);
            assertThat(exhausted()).as("disabled retry is not an exhaustion").isZero();
        });
    }

    @Test
    void starterFailureIsInternalAndNotRetried() {
        RecordingEmitter emitter = RecordingEmitter.responsive();
        UpstreamStarter failing = (request, deadline, observer) -> {
            throw new IllegalStateException("channel closed");
        };
        StreamBridge bridge = StreamTestSupport.newRetryingBridge(emitter, registry, lease, 4, metrics, timeouts,
                immediate(3), failing, Duration.ofSeconds(60));
        assertThat(registry.tryRegister(bridge)).isTrue();

        bridge.start();

        await().atMost(TIMEOUT).until(() -> emitter.completed);
        assertThat(emitter.events).hasSize(1);
        assertThat(emitter.events.get(0)).contains("event:error").contains("INTERNAL")
                .contains("failed to start upstream call");
        assertThat(lease.closes).hasValue(1);
        assertThat(retries("restart")).isZero();
        assertThat(exhausted()).isZero();
    }

    @Test
    void eachRetryGetsTheRemainingBudgetAsDeadline() {
        ManualClock clock = new ManualClock();
        RecordingEmitter emitter = RecordingEmitter.responsive();
        startBridge(emitter, 4, policy(3, NO_BACKOFF, NO_BACKOFF, b -> 0, clock), Duration.ofSeconds(60));
        assertThat(call(0).deadline()).isEqualTo(Duration.ofSeconds(60));

        clock.advance(Duration.ofSeconds(4));
        call(0).observer().onError(unavailable("cut"));

        await().atMost(TIMEOUT).until(() -> starter.calls.size() == 2);
        // The clock is frozen, so the remaining budget is exact.
        assertThat(call(1).deadline()).isEqualTo(Duration.ofSeconds(56));
    }

    @Test
    void nextBackoffBeyondTheBudgetStopsRetrying() {
        ManualClock clock = new ManualClock();
        RecordingEmitter emitter = RecordingEmitter.responsive();
        startBridge(emitter, 4, policy(3, Duration.ofMillis(100), Duration.ofMillis(100), b -> b, clock),
                Duration.ofSeconds(60));

        clock.advance(Duration.ofMillis(59_950));
        call(0).observer().onError(unavailable("late failure"));

        await().atMost(TIMEOUT).untilAsserted(() -> {
            assertThat(emitter.completed).isTrue();
            assertThat(exhausted()).isEqualTo(1);
            assertThat(lease.closes).hasValue(1);
        });
        assertThat(emitter.events).hasSize(1);
        assertThat(emitter.events.get(0)).contains("event:error").contains("UNAVAILABLE").contains("late failure");
        assertThat(starter.calls).hasSize(1);
        assertThat(retries("restart")).isZero();
    }

    @Test
    void budgetSpentDuringBackoffStartsNoCall() {
        ManualClock clock = new ManualClock();
        RecordingEmitter emitter = RecordingEmitter.responsive();
        // A 1 s backoff leaves plenty of room to move the clock before it ends.
        startBridge(emitter, 4, policy(3, Duration.ofSeconds(1), Duration.ofSeconds(1), b -> b, clock),
                Duration.ofSeconds(60));

        call(0).observer().onError(unavailable("cut"));
        await().atMost(TIMEOUT).until(() -> timeouts.pendingTimers() == 1);
        clock.advance(Duration.ofSeconds(60));

        await().atMost(TIMEOUT).untilAsserted(() -> {
            assertThat(emitter.completed).isTrue();
            assertThat(exhausted()).isEqualTo(1);
            assertThat(lease.closes).hasValue(1);
        });
        assertThat(emitter.events).hasSize(1);
        assertThat(emitter.events.get(0)).contains("event:error").contains("UNAVAILABLE").contains("cut");
        assertThat(starter.calls).hasSize(1);
        assertThat(retries("restart")).isZero();
        assertThat(timeouts.pendingTimers()).isZero();
    }

    @Test
    void shutdownCancelDuringBackoffStartsNoCall() {
        RecordingEmitter emitter = RecordingEmitter.responsive();
        StreamBridge bridge = startBridge(emitter, 4, fixedBackoff(3));
        call(0).observer().onError(unavailable("cut"));
        await().atMost(TIMEOUT).untilAsserted(() -> {
            assertThat(bridge.retryPending()).isTrue();
            assertThat(timeouts.pendingTimers()).isEqualTo(1);
            assertThat(bridge.timerArmed()).isTrue();
        });
        assertThat(registry.admittedCount()).as("the permit is held during the backoff").isEqualTo(1);
        assertThat(lease.closes).hasValue(0);

        bridge.cancelForShutdown().join();

        await().during(QUIET).atMost(TIMEOUT).untilAsserted(() -> {
            assertThat(starter.calls).hasSize(1);
            assertThat(lease.closes).hasValue(1);
            assertThat(registry.admittedCount()).isZero();
            assertThat(timeouts.pendingTimers()).isZero();
            assertThat(emitter.events).hasSize(1);
        });
        assertThat(emitter.events.get(0)).contains("event:error").contains("CANCELLED");
        assertThat(retries("restart")).isZero();
        assertThat(exhausted()).isZero();
    }

    @Test
    void clientGoneDuringBackoffStartsNoCall() {
        RecordingEmitter emitter = RecordingEmitter.responsive();
        StreamBridge bridge = startBridge(emitter, 4, fixedBackoff(3));
        call(0).observer().onError(unavailable("cut"));
        await().atMost(TIMEOUT).until(bridge::retryPending);

        emitter.simulateClientGone();

        await().during(QUIET).atMost(TIMEOUT).untilAsserted(() -> {
            assertThat(starter.calls).hasSize(1);
            assertThat(lease.closes).hasValue(1);
            assertThat(registry.admittedCount()).isZero();
            assertThat(timeouts.pendingTimers()).isZero();
            assertThat(emitter.events).isEmpty();
        });
        assertThat(retries("restart")).isZero();
    }

    @Test
    void failedWriteOfAQueuedChunkDuringBackoffStopsRetry() {
        RecordingEmitter emitter = RecordingEmitter.stalled();
        try {
            StreamBridge bridge = startBridge(emitter, 4, fixedBackoff(3));
            Call first = call(0);
            first.upstream().deliver(first.observer(), chunk(0, false));
            await().atMost(TIMEOUT).until(() -> emitter.sendInProgress);
            first.upstream().deliver(first.observer(), chunk(1, false));
            first.observer().onError(unavailable("cut"));
            await().atMost(TIMEOUT).until(bridge::retryPending);

            // The write of chunk 0 completes, the write of the queued chunk 1 fails.
            emitter.failSends = true;
            emitter.gate.countDown();

            await().during(QUIET).atMost(TIMEOUT).untilAsserted(() -> {
                assertThat(starter.calls).hasSize(1);
                assertThat(lease.closes).hasValue(1);
                assertThat(registry.admittedCount()).isZero();
                assertThat(timeouts.pendingTimers()).isZero();
                assertThat(emitter.events).hasSize(1);
            });
        } finally {
            emitter.gate.countDown();
        }
        assertThat(emitter.events.get(0)).contains("event:chunk").contains("id:0\n");
        assertThat(retries("resume")).isZero();
    }

    @Test
    void shutdownBeginningStopsRetries() {
        useRegistry(Duration.ofSeconds(10));
        RecordingEmitter emitter = RecordingEmitter.responsive();
        startBridge(emitter, 4, immediate(3));
        CompletableFuture<Void> stop = CompletableFuture.runAsync(registry::stop);
        try {
            await().atMost(TIMEOUT).until(() -> !registry.isAccepting());

            call(0).observer().onError(unavailable("going away"));

            await().atMost(TIMEOUT).untilAsserted(() -> {
                assertThat(emitter.completed).isTrue();
                // Far below the 10 s drain timeout: the stream ended by itself and the registry drained.
                assertThat(stop).isDone();
            });
        } finally {
            stop.join();
        }
        assertThat(emitter.events).hasSize(1);
        assertThat(emitter.events.get(0)).contains("event:error").contains("UNAVAILABLE").contains("going away");
        assertThat(starter.calls).hasSize(1);
        assertThat(exhausted()).isZero();
        assertThat(lease.closes).hasValue(1);
        assertThat(timeouts.pendingTimers()).isZero();
    }

    @Test
    void shutdownDuringBackoffEndsWithLastStatus() {
        useRegistry(Duration.ofSeconds(10));
        RecordingEmitter emitter = RecordingEmitter.responsive();
        StreamBridge bridge = startBridge(emitter, 4, fixedBackoff(3));
        call(0).observer().onError(unavailable("cut"));
        await().atMost(TIMEOUT).until(bridge::retryPending);

        CompletableFuture<Void> stop = CompletableFuture.runAsync(registry::stop);
        try {
            await().atMost(TIMEOUT).until(() -> !registry.isAccepting());

            // The backoff ends after ~500 ms: the retry sees the shutdown and reports the last status.
            await().atMost(Duration.ofSeconds(5)).untilAsserted(() -> {
                assertThat(emitter.completed).isTrue();
                assertThat(stop).isDone();
            });
        } finally {
            stop.join();
        }
        assertThat(emitter.events).hasSize(1);
        assertThat(emitter.events.get(0)).contains("event:error").contains("UNAVAILABLE").contains("cut");
        assertThat(starter.calls).hasSize(1);
        assertThat(exhausted()).isZero();
        assertThat(retries("restart")).isZero();
        assertThat(lease.closes).hasValue(1);
        assertThat(timeouts.pendingTimers()).isZero();
    }

    @Test
    void clientGoneCancelsTheLiveRetryCallNotTheDeadOne() {
        RecordingEmitter emitter = RecordingEmitter.responsive();
        StreamBridge bridge = startBridge(emitter, 4, immediate(3));
        call(0).observer().onError(unavailable("cut"));
        await().atMost(TIMEOUT).until(() -> starter.calls.size() == 2);

        emitter.simulateClientGone();

        await().atMost(TIMEOUT).untilAsserted(() -> {
            assertThat(call(1).upstream().cancelMessage.get()).isEqualTo("client disconnected");
            assertThat(lease.closes).hasValue(1);
            assertThat(registry.admittedCount()).isZero();
            assertThat(timeouts.pendingTimers()).isZero();
        });
        // The call that already failed is not cancelled, and no further call is started.
        assertThat(call(0).upstream().cancelMessage.get()).isNull();
        await().during(Duration.ofSeconds(1)).atMost(TIMEOUT).untilAsserted(() -> {
            assertThat(starter.calls).hasSize(2);
            assertThat(bridge.retryPending()).isFalse();
        });
    }

    @Test
    void claimWhileTheRetryCallIsStartingCancelsIt() {
        RecordingEmitter emitter = RecordingEmitter.responsive();
        CountDownLatch starting = new CountDownLatch(1);
        starter.onStart = call -> {
            if (starter.calls.size() == 2) {
                starting.countDown();
                awaitQuietly(release);
            }
        };
        try {
            StreamBridge bridge = startBridge(emitter, 4, immediate(3));
            call(0).observer().onError(unavailable("cut"));
            // The retry call is inside start(): beforeStart ran, markStarted has not.
            await().atMost(TIMEOUT).until(() -> starting.getCount() == 0);

            emitter.simulateClientGone();

            await().atMost(TIMEOUT).untilAsserted(() -> {
                assertThat(call(1).upstream().cancelMessage.get()).isEqualTo("client disconnected");
                assertThat(lease.closes).hasValue(1);
                assertThat(registry.admittedCount()).isZero();
            });
            release.countDown();
            await().during(Duration.ofSeconds(1)).atMost(TIMEOUT).untilAsserted(() -> {
                assertThat(starter.calls).hasSize(2);
                assertThat(call(1).upstream().requested).as("no credit for a cancelled call").hasValue(0);
                assertThat(timeouts.pendingTimers()).isZero();
                assertThat(bridge.timerArmed()).isFalse();
            });
        } finally {
            release.countDown();
        }
    }

    @Test
    void claimBeforeTheRetryCallsBeforeStartCancelsItInsideBeforeStart() {
        RecordingEmitter emitter = RecordingEmitter.responsive();
        starter.beforeStartHook = call -> {
            if (starter.calls.size() == 2) {
                // The stream is claimed after the attempt became current but before it published its call.
                emitter.simulateClientGone();
            }
        };
        StreamBridge bridge = startBridge(emitter, 4, immediate(3));
        call(0).observer().onError(unavailable("cut"));

        await().atMost(TIMEOUT).untilAsserted(() -> {
            assertThat(starter.calls).hasSize(2);
            assertThat(call(1).upstream().cancelMessage.get()).isEqualTo("client disconnected");
            assertThat(lease.closes).hasValue(1);
            assertThat(registry.admittedCount()).isZero();
            assertThat(timeouts.pendingTimers()).isZero();
        });
        await().during(Duration.ofSeconds(1)).atMost(TIMEOUT).untilAsserted(() -> {
            assertThat(starter.calls).hasSize(2);
            assertThat(bridge.timerArmed()).isFalse();
        });
    }

    @Test
    void idleCannotFireDuringBackoff() {
        useTimeouts(Duration.ofSeconds(30), Duration.ofMillis(100));
        RecordingEmitter emitter = RecordingEmitter.responsive();
        StreamBridge bridge = startBridge(emitter, 4,
                policy(3, Duration.ofSeconds(3), Duration.ofSeconds(3), b -> b));
        // The idle timer is armed by the delivery; the error follows on the same thread, microseconds later,
        // far inside the 100 ms limit. nextSeq is written before the offer, so the resume position is exact.
        call(0).upstream().deliver(call(0).observer(), chunk(0, false));
        call(0).observer().onError(unavailable("cut"));

        await().atMost(TIMEOUT).untilAsserted(() -> {
            assertThat(bridge.timerArmed()).isTrue();
            assertThat(bridge.retryPending()).isTrue();
            assertThat(timeouts.pendingTimers()).isEqualTo(1);
        });
        // The idle limit is 100 ms; the backoff is 3 s. 600 ms is six times the idle limit and a fifth of the backoff.
        await().during(Duration.ofMillis(600)).atMost(TIMEOUT).untilAsserted(() -> {
            assertThat(timeoutCount("idle")).isZero();
            assertThat(timeoutCount("first_chunk")).isZero();
            assertThat(starter.calls).hasSize(1);
            assertThat(bridge.retryPending()).isTrue();
        });

        await().atMost(Duration.ofSeconds(15)).untilAsserted(() -> {
            assertThat(starter.calls).hasSize(2);
            assertThat(call(1).request().getResumeFromSeq()).isEqualTo(1);
            assertThat(bridge.timerArmed()).isTrue();
            assertThat(bridge.retryPending()).isFalse();
            assertThat(timeouts.pendingTimers()).isEqualTo(1);
        });
        assertThat(retries("resume")).isEqualTo(1);
        assertThat(timeoutCount("idle")).isZero();
    }
}
