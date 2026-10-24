package io.github.cemharput.streamlab.gateway.stream;

import io.github.cemharput.streamlab.gateway.config.GatewayProperties;
import io.grpc.Status;
import java.time.Duration;
import java.util.concurrent.ThreadLocalRandom;
import java.util.function.LongSupplier;

/**
 * When and how long to wait before the next upstream attempt: exponential ceiling with full jitter,
 * {@code delay = random[0, min(max, initial * 2^k)]}. Only {@code UNAVAILABLE} is retryable.
 * Randomness and clock are injectable.
 */
final class RetryPolicy {

    /** Draws a value in {@code [0, boundInclusiveNanos]}. */
    @FunctionalInterface
    interface Jitter {
        Jitter RANDOM = bound -> ThreadLocalRandom.current().nextLong(bound + 1);

        long draw(long boundInclusiveNanos);
    }

    private final int maxRetries;
    private final long initialNanos;
    private final long maxNanos;
    private final Jitter jitter;
    private final LongSupplier nanoTime;

    RetryPolicy(int maxRetries, Duration initial, Duration max, Jitter jitter, LongSupplier nanoTime) {
        this.maxRetries = maxRetries;
        this.initialNanos = initial.toNanos();
        this.maxNanos = max.toNanos();
        this.jitter = jitter;
        this.nanoTime = nanoTime;
    }

    static RetryPolicy from(GatewayProperties.Retry retry) {
        return new RetryPolicy(retry.maxAttempts(), retry.initialBackoff(), retry.maxBackoff(),
                Jitter.RANDOM, System::nanoTime);
    }

    static RetryPolicy disabled() {
        GatewayProperties.Retry retry = GatewayProperties.Retry.disabled();
        return new RetryPolicy(0, retry.initialBackoff(), retry.maxBackoff(), Jitter.RANDOM, System::nanoTime);
    }

    boolean isRetryable(Status status) {
        return status.getCode() == Status.Code.UNAVAILABLE;
    }

    /** Ceiling of the k-th retry's delay (k = 0 is the first retry); saturates at max, never overflows. */
    long ceilingNanos(int k) {
        if (k < 0 || k >= Long.SIZE - 1 || initialNanos > (maxNanos >> k)) {
            return k < 0 ? initialNanos : maxNanos;
        }
        return Math.min(initialNanos << k, maxNanos);
    }

    Duration backoff(int k) {
        return Duration.ofNanos(jitter.draw(ceilingNanos(k)));
    }

    int maxRetries() {
        return maxRetries;
    }

    long nanoTime() {
        return nanoTime.getAsLong();
    }
}
