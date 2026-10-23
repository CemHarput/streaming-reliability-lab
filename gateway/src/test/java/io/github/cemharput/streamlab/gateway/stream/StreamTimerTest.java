package io.github.cemharput.streamlab.gateway.stream;

import static io.github.cemharput.streamlab.gateway.stream.StreamTestSupport.startedTimeouts;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.awaitility.Awaitility.await;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ScheduledFuture;
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
}
