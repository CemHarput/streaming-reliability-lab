package io.github.cemharput.streamlab.bench;

import static org.assertj.core.api.Assertions.assertThat;

import io.github.cemharput.streamlab.bench.Readiness.Result;
import io.github.cemharput.streamlab.bench.testsupport.FakeTicker;
import java.time.Duration;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.Timeout.ThreadMode;

@Timeout(value = 30, unit = TimeUnit.SECONDS, threadMode = ThreadMode.SEPARATE_THREAD)
class ReadinessTest {

    @Test
    void becomesReadyAfterSeveralPolls() throws Exception {
        AtomicInteger calls = new AtomicInteger();
        Readiness.Outcome o = Readiness.await(() -> calls.incrementAndGet() == 3, Duration.ofSeconds(30),
                Duration.ofMillis(250), () -> true, new FakeTicker());
        assertThat(o.result()).isEqualTo(Result.READY);
        assertThat(o.waitedMillis()).isEqualTo(500);
    }

    @Test
    void timesOutWithinTheBound() throws Exception {
        Readiness.Outcome o = Readiness.await(() -> false, Duration.ofSeconds(30), Duration.ofSeconds(1), () -> true,
                new FakeTicker());
        assertThat(o.result()).isEqualTo(Result.TIMEOUT);
        assertThat(o.waitedMillis()).isBetween(30_000L, 31_000L);
    }

    @Test
    void aDeadProcessEndsTheWaitImmediately() throws Exception {
        FakeTicker ticker = new FakeTicker();
        Readiness.Outcome o = Readiness.await(() -> false, Duration.ofSeconds(90), Duration.ofSeconds(1), () -> false,
                ticker);
        assertThat(o.result()).isEqualTo(Result.PROCESS_DIED);
        assertThat(ticker.sleepCount()).isZero();
    }

    @Test
    void probeExceptionsCountAsNotReady() throws Exception {
        AtomicInteger calls = new AtomicInteger();
        Readiness.Outcome o = Readiness.await(() -> {
            if (calls.incrementAndGet() < 3) {
                throw new IllegalStateException("boom");
            }
            return true;
        }, Duration.ofSeconds(30), Duration.ofMillis(100), () -> true, new FakeTicker());
        assertThat(o.result()).isEqualTo(Result.READY);
    }
}
