package io.github.cemharput.streamlab.gateway.stream;

import static org.assertj.core.api.Assertions.assertThat;

import io.github.cemharput.streamlab.gateway.config.GatewayProperties;
import io.grpc.Status;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

@Timeout(value = 30, threadMode = Timeout.ThreadMode.SEPARATE_THREAD)
class RetryPolicyTest {

    private static RetryPolicy policy(RetryPolicy.Jitter jitter) {
        return new RetryPolicy(3, Duration.ofMillis(100), Duration.ofSeconds(2), jitter, () -> 0L);
    }

    @Test
    void ceilingsDoubleUpToMax() {
        RetryPolicy policy = policy(b -> b);

        List<Long> ceilingsMs = java.util.stream.IntStream.range(0, 7)
                .mapToObj(k -> Duration.ofNanos(policy.ceilingNanos(k)).toMillis())
                .toList();

        assertThat(ceilingsMs).containsExactly(100L, 200L, 400L, 800L, 1600L, 2000L, 2000L);
    }

    @Test
    void jitterStaysWithinZeroAndCeiling() {
        RetryPolicy policy = RetryPolicy.from(new GatewayProperties.Retry(10, Duration.ofMillis(100), Duration.ofSeconds(2)));

        for (int k = 0; k < 10; k++) {
            long ceiling = policy.ceilingNanos(k);
            for (int i = 0; i < 1000; i++) {
                assertThat(policy.backoff(k).toNanos()).isBetween(0L, ceiling);
            }
        }
    }

    @Test
    void injectedJitterReceivesTheCeiling() {
        AtomicLong seen = new AtomicLong(-1);
        RetryPolicy policy = policy(bound -> {
            seen.set(bound);
            return bound / 2;
        });

        Duration delay = policy.backoff(2);

        assertThat(seen.get()).isEqualTo(Duration.ofMillis(400).toNanos());
        assertThat(delay).isEqualTo(Duration.ofMillis(200));
    }

    @Test
    void hugeExponentSaturatesAtMax() {
        RetryPolicy policy = policy(b -> b);

        assertThat(policy.ceilingNanos(100)).isEqualTo(Duration.ofSeconds(2).toNanos());
        assertThat(policy.ceilingNanos(62)).isEqualTo(Duration.ofSeconds(2).toNanos());
        assertThat(policy.ceilingNanos(63)).isEqualTo(Duration.ofSeconds(2).toNanos());
        assertThat(policy.ceilingNanos(Integer.MAX_VALUE)).isEqualTo(Duration.ofSeconds(2).toNanos());
    }

    @ParameterizedTest
    @EnumSource(Status.Code.class)
    void onlyUnavailableIsRetryable(Status.Code code) {
        assertThat(policy(b -> b).isRetryable(Status.fromCode(code)))
                .isEqualTo(code == Status.Code.UNAVAILABLE);
    }

    @Test
    void disabledHasNoRetries() {
        RetryPolicy disabled = RetryPolicy.disabled();

        assertThat(disabled.maxRetries()).isZero();
    }

    @Test
    void fromPropertiesCopiesTheSettings() {
        RetryPolicy policy = RetryPolicy.from(new GatewayProperties.Retry(5, Duration.ofMillis(50), Duration.ofMillis(300)));

        assertThat(policy.maxRetries()).isEqualTo(5);
        assertThat(policy.ceilingNanos(0)).isEqualTo(Duration.ofMillis(50).toNanos());
        assertThat(policy.ceilingNanos(5)).isEqualTo(Duration.ofMillis(300).toNanos());
    }
}
