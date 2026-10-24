package io.github.cemharput.streamlab.gateway.stream;

import static io.github.cemharput.streamlab.gateway.stream.StreamTestSupport.startedTimeouts;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.awaitility.Awaitility.await;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

/** The per-stream timer and its shared scheduler, without a bridge. */
@Timeout(value = 15, threadMode = Timeout.ThreadMode.SEPARATE_THREAD)
class StreamTimerTest {

    private static final Duration TIMEOUT = Duration.ofSeconds(5);
    private static final Duration SHORT = Duration.ofMillis(200);
    private static final Duration LONG = Duration.ofSeconds(30);

    private final List<TimeoutKind> fired = new CopyOnWriteArrayList<>();
    private StreamTimeouts timeouts;

    @AfterEach
    void tearDown() {
        if (timeouts != null) {
            timeouts.stop();
        }
    }

    private StreamTimer timer(Duration first, Duration idle) {
        timeouts = startedTimeouts(first, idle);
        return new StreamTimer(timeouts, fired::add);
    }

    /** Records every scheduled task so a test can run a stale one by hand; the real tasks never fire. */
    private static final class CapturingTimeouts extends StreamTimeouts {

        final List<Runnable> tasks = new CopyOnWriteArrayList<>();

        CapturingTimeouts() {
            super(StreamTestSupport.properties(Duration.ofSeconds(1), StreamTestSupport.defaultLimits(),
                    new io.github.cemharput.streamlab.gateway.config.GatewayProperties.Timeouts(LONG, LONG)));
        }

        @Override
        ScheduledFuture<?> schedule(Runnable task, Duration delay) {
            tasks.add(task);
            return super.schedule(() -> { }, LONG);
        }
    }

    @Test
    void firstChunkArmedOnlyOnStart() {
        StreamTimer timer = timer(LONG, LONG);

        assertThat(timer.isArmed()).isFalse();
        assertThat(timeouts.pendingTimers()).isZero();

        timer.start(4);

        assertThat(timer.armedKind()).isEqualTo(TimeoutKind.FIRST_CHUNK);
        assertThat(timeouts.pendingTimers()).isEqualTo(1);
    }

    @Test
    void firstChunkReplacedByIdleOnFirstChunk() {
        StreamTimer timer = timer(LONG, LONG);
        timer.start(4);

        timer.chunkReceived(false);

        assertThat(timer.armedKind()).isEqualTo(TimeoutKind.IDLE);
        assertThat(timeouts.pendingTimers()).as("replaced, not added").isEqualTo(1);
        assertThat(fired).isEmpty();
    }

    @Test
    void exhaustedCreditDisarmsAndGrantedCreditRearms() {
        StreamTimer timer = timer(LONG, LONG);
        timer.start(2);
        timer.chunkReceived(false);
        assertThat(timer.armedKind()).isEqualTo(TimeoutKind.IDLE);

        timer.chunkReceived(false);

        // Credit is 0: upstream may not send, so silence is not its fault.
        assertThat(timer.isArmed()).isFalse();
        assertThat(timeouts.pendingTimers()).isZero();

        timer.creditGranted();
        assertThat(timer.armedKind()).isEqualTo(TimeoutKind.IDLE);
        assertThat(timeouts.pendingTimers()).isEqualTo(1);

        // A second credit while armed does not re-arm (no reset on a mere credit).
        timer.creditGranted();
        assertThat(timeouts.pendingTimers()).isEqualTo(1);
    }

    @Test
    void lastChunkAndUpstreamEndLeaveNothingArmed() {
        StreamTimer afterLast = timer(LONG, LONG);
        afterLast.start(4);
        afterLast.chunkReceived(true);
        afterLast.creditGranted();
        assertThat(afterLast.isArmed()).isFalse();

        StreamTimer afterEnd = new StreamTimer(timeouts, fired::add);
        afterEnd.start(4);
        afterEnd.chunkReceived(false);
        afterEnd.upstreamEnded();
        afterEnd.creditGranted();
        assertThat(afterEnd.isArmed()).isFalse();
        assertThat(timeouts.pendingTimers()).isZero();
    }

    @Test
    void closeIsIdempotentAndPreventsRearm() {
        StreamTimer timer = timer(LONG, LONG);
        timer.start(2);

        timer.close();
        timer.close();
        timer.start(2);
        timer.chunkReceived(false);
        timer.creditGranted();

        assertThat(timer.isArmed()).isFalse();
        assertThat(timeouts.pendingTimers()).isZero();
    }

    @Test
    void expiryFiresOnceWithTheArmedKind() {
        StreamTimer timer = timer(SHORT, LONG);

        timer.start(4);

        await().atMost(TIMEOUT).until(() -> !fired.isEmpty());
        assertThat(fired).containsExactly(TimeoutKind.FIRST_CHUNK);
        assertThat(timer.isArmed()).isFalse();
        await().during(SHORT.multipliedBy(3)).atMost(TIMEOUT).untilAsserted(() -> assertThat(fired).hasSize(1));
    }

    @Test
    void staleExpiryIsIgnored() {
        CapturingTimeouts capturing = new CapturingTimeouts();
        timeouts = capturing;
        capturing.start();
        StreamTimer timer = new StreamTimer(capturing, fired::add);
        timer.start(4);
        timer.chunkReceived(false);
        assertThat(capturing.tasks).hasSize(2);

        // The first-chunk task was replaced: running it anyway (it was already dequeued) is a no-op.
        capturing.tasks.get(0).run();
        assertThat(fired).isEmpty();
        assertThat(timer.armedKind()).isEqualTo(TimeoutKind.IDLE);

        capturing.tasks.get(1).run();
        assertThat(fired).containsExactly(TimeoutKind.IDLE);
        assertThat(timer.isArmed()).isFalse();

        // A task of a closed timer is stale too.
        timer.start(4);
        timer.close();
        capturing.tasks.get(2).run();
        assertThat(fired).hasSize(1);
    }

    @Test
    void armOnStoppedSchedulerIsIgnored() {
        timeouts = startedTimeouts(LONG, LONG);
        timeouts.stop();
        StreamTimer timer = new StreamTimer(timeouts, fired::add);

        assertThatCode(() -> {
            timer.start(4);
            timer.chunkReceived(false);
            timer.creditGranted();
            timer.close();
        }).doesNotThrowAnyException();
        assertThat(timer.isArmed()).isFalse();
        assertThat(timeouts.pendingTimers()).isZero();
    }

    @Test
    void stopTerminatesExecutorWithinOneSecond() {
        StreamTimer timer = timer(LONG, LONG);
        timer.start(4);
        assertThat(timeouts.isRunning()).isTrue();
        assertThat(timeouts.pendingTimers()).isEqualTo(1);

        long startedAt = System.nanoTime();
        timeouts.stop();
        Duration took = Duration.ofNanos(System.nanoTime() - startedAt);

        assertThat(took).isLessThan(Duration.ofSeconds(1));
        assertThat(timeouts.isRunning()).isFalse();
        assertThat(timeouts.pendingTimers()).isZero();
        assertThatCode(() -> timer.chunkReceived(false)).doesNotThrowAnyException();
        assertThatCode(timeouts::stop).as("stop is idempotent").doesNotThrowAnyException();
    }

    @Test
    void scheduleRetryOccupiesTheSingleSlotAndCloseCancelsIt() {
        StreamTimer timer = timer(LONG, LONG);
        timer.start(4);
        assertThat(timer.armedKind()).isEqualTo(TimeoutKind.FIRST_CHUNK);
        AtomicInteger ran = new AtomicInteger();

        boolean scheduled = timer.scheduleRetry(LONG, ran::incrementAndGet);

        assertThat(scheduled).isTrue();
        assertThat(timer.isArmed()).isTrue();
        assertThat(timer.retryPending()).isTrue();
        assertThat(timer.armedKind()).as("a backoff is not a timeout kind").isNull();
        assertThat(timeouts.pendingTimers()).as("replaced, not added").isEqualTo(1);

        timer.close();

        assertThat(timer.isArmed()).isFalse();
        assertThat(timer.retryPending()).isFalse();
        assertThat(timeouts.pendingTimers()).isZero();
        assertThat(ran).hasValue(0);
    }

    @Test
    void scheduleRetryAfterCloseIsRefused() {
        StreamTimer timer = timer(LONG, LONG);
        timer.close();

        assertThat(timer.scheduleRetry(SHORT, () -> fired.add(TimeoutKind.IDLE))).isFalse();
        assertThat(timeouts.pendingTimers()).isZero();
    }

    @Test
    void scheduleRetryOnStoppedSchedulerIsRefused() {
        timeouts = startedTimeouts(LONG, LONG);
        timeouts.stop();
        StreamTimer timer = new StreamTimer(timeouts, fired::add);

        assertThat(timer.scheduleRetry(SHORT, () -> { })).isFalse();
        assertThat(timer.retryPending()).isFalse();
    }

    @Test
    void retryTaskFiresOnceAndIsNotATimeout() {
        StreamTimer timer = timer(LONG, LONG);
        AtomicInteger ran = new AtomicInteger();

        assertThat(timer.scheduleRetry(SHORT, ran::incrementAndGet)).isTrue();

        await().atMost(TIMEOUT).until(() -> ran.get() == 1);
        assertThat(timer.isArmed()).isFalse();
        assertThat(fired).as("the expiry callback is for timeouts only").isEmpty();
        await().during(SHORT.multipliedBy(5)).atMost(TIMEOUT).untilAsserted(() -> assertThat(ran).hasValue(1));
        assertThat(timeouts.pendingTimers()).isZero();
    }

    @Test
    void startWithZeroCreditArmsFirstChunkOnFirstGrant() {
        StreamTimer timer = timer(LONG, LONG);

        timer.start(0);

        assertThat(timer.isArmed()).as("no credit, no first-chunk clock").isFalse();
        assertThat(timeouts.pendingTimers()).isZero();

        timer.creditGranted(2);

        assertThat(timer.armedKind()).isEqualTo(TimeoutKind.FIRST_CHUNK);
        assertThat(timeouts.pendingTimers()).isEqualTo(1);

        // A further grant while armed does not re-arm.
        timer.creditGranted(1);
        assertThat(timeouts.pendingTimers()).isEqualTo(1);
    }

    @Test
    void startBeginsANewFirstChunkCycleForEveryCall() {
        StreamTimer timer = timer(LONG, LONG);
        timer.start(4);
        timer.chunkReceived(false);
        assertThat(timer.armedKind()).isEqualTo(TimeoutKind.IDLE);

        timer.callEnded();
        assertThat(timer.isArmed()).isFalse();
        timer.start(3);

        assertThat(timer.armedKind()).isEqualTo(TimeoutKind.FIRST_CHUNK);
        assertThat(timeouts.pendingTimers()).isEqualTo(1);
    }

    @Test
    void callEndedPreventsCreditFromArmingIdle() {
        StreamTimer timer = timer(LONG, LONG);
        timer.start(2);
        timer.chunkReceived(false);
        timer.chunkReceived(false);
        assertThat(timer.isArmed()).isFalse();

        timer.callEnded();
        timer.creditGranted(1);
        timer.creditGranted(5);

        assertThat(timer.isArmed()).as("no call, no idle timer").isFalse();
        assertThat(timeouts.pendingTimers()).isZero();
    }

    @Test
    void callEndedKeepsTheBackoffOutOfReachOfCredit() {
        StreamTimer timer = timer(LONG, LONG);
        timer.start(4);
        timer.chunkReceived(false);
        timer.callEnded();
        assertThat(timer.scheduleRetry(LONG, () -> { })).isTrue();

        timer.creditGranted(1);
        timer.chunkReceived(false);

        assertThat(timer.retryPending()).as("backoff stays the only task").isTrue();
        assertThat(timeouts.pendingTimers()).isEqualTo(1);
    }

    @Test
    void retryTaskRunsOutsideTheTimerLock() {
        timeouts = startedTimeouts(LONG, LONG);
        StreamTimer timer = new StreamTimer(timeouts, fired::add);
        AtomicBoolean ranWithoutLock = new AtomicBoolean();
        AtomicReference<Throwable> probeFailure = new AtomicReference<>();

        // The lock is reentrant, so re-entering the timer from the scheduler thread proves nothing. Another
        // thread asking for the timer's state does: it blocks while the firing thread still holds the lock.
        assertThat(timer.scheduleRetry(Duration.ofMillis(50), () -> {
            try {
                CompletableFuture.supplyAsync(timer::isArmed).get(2, TimeUnit.SECONDS);
                ranWithoutLock.set(true);
            } catch (Exception ex) {
                probeFailure.set(ex);
            }
        })).isTrue();

        await().atMost(TIMEOUT).untilAsserted(() -> {
            assertThat(probeFailure.get()).as("the task ran while the timer lock was still held").isNull();
            assertThat(ranWithoutLock).isTrue();
        });
        timer.close();
    }
}
