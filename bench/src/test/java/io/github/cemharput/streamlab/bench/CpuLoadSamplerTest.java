package io.github.cemharput.streamlab.bench;

import static org.assertj.core.api.Assertions.assertThat;

import io.github.cemharput.streamlab.bench.testsupport.FakeTicker;
import java.time.Duration;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.Timeout.ThreadMode;

@Timeout(value = 30, unit = TimeUnit.SECONDS, threadMode = ThreadMode.SEPARATE_THREAD)
class CpuLoadSamplerTest {

    @Test
    void primesThenSamplesEveryHalfSecondOverTheWindow() throws Exception {
        AtomicInteger reads = new AtomicInteger();
        FakeTicker ticker = new FakeTicker();
        CpuLoadSampler sampler = new CpuLoadSampler(() -> reads.incrementAndGet() == 1 ? 0.99 : 0.1, ticker);
        List<Double> samples = sampler.sample(Duration.ofSeconds(3));
        assertThat(samples).hasSize(6).containsOnly(0.1);
        assertThat(reads.get()).isEqualTo(7);
        assertThat(ticker.sleepCount()).isEqualTo(6);
    }

    @Test
    void negativeValuesAreUnavailable() throws Exception {
        CpuLoadSampler sampler = new CpuLoadSampler(() -> -1.0, new FakeTicker());
        List<Double> samples = sampler.sample(Duration.ofSeconds(1));
        assertThat(samples).containsOnlyNulls();
        assertThat(CpuLoadSampler.flags(samples)).containsExactly(Flags.CPU_LOAD_UNAVAILABLE);
    }

    @Test
    void busyFlagUsesTheMeanAgainstTheFixedThreshold() {
        assertThat(CpuLoadSampler.flags(Arrays.asList(0.5, 0.4, null))).containsExactly(Flags.CPU_BUSY_BEFORE_RUN);
        assertThat(CpuLoadSampler.flags(Arrays.asList(0.1, 0.2, null))).isEmpty();
        assertThat(Flags.CPU_BUSY_THRESHOLD).isEqualTo(0.25);
    }
}
