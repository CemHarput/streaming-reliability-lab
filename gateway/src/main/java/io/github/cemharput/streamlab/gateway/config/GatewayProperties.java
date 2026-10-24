package io.github.cemharput.streamlab.gateway.config;

import java.time.Duration;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.context.properties.bind.DefaultValue;

@ConfigurationProperties(prefix = "lab")
public record GatewayProperties(
        @DefaultValue("shared") ConnectionMode connectionMode,
        @DefaultValue Upstream upstream,
        @DefaultValue Shutdown shutdown,
        @DefaultValue Limits limits,
        @DefaultValue Timeouts timeouts,
        @DefaultValue Retry retry) {

    public GatewayProperties {
        if (upstream != null && retry != null) {
            requireShorterThanDeadline(retry.maxBackoff(), "lab.retry.max-backoff", upstream.streamDeadline());
        }
        if (upstream != null && timeouts != null) {
            requireShorterThanDeadline(timeouts.firstChunk(), "lab.timeouts.first-chunk", upstream.streamDeadline());
            requireShorterThanDeadline(timeouts.idleBetweenChunks(), "lab.timeouts.idle-between-chunks",
                    upstream.streamDeadline());
        }
    }

    /** Only the shared mode exists in this feature; per-request arrives later. */
    public enum ConnectionMode {
        SHARED
    }

    public record Upstream(
            @DefaultValue("localhost:50051") String target,
            @DefaultValue("60s") Duration streamDeadline) {

        public Upstream {
            requirePositive(streamDeadline, "lab.upstream.stream-deadline");
            if (target == null || target.isBlank()) {
                throw new IllegalArgumentException("lab.upstream.target must not be blank");
            }
        }
    }

    public record Shutdown(
            @DefaultValue("10s") Duration drainTimeout,
            @DefaultValue("5s") Duration channelTerminationTimeout) {

        public Shutdown {
            requirePositive(drainTimeout, "lab.shutdown.drain-timeout");
            requirePositive(channelTerminationTimeout, "lab.shutdown.channel-termination-timeout");
        }
    }

    /** What happens to a request that finds no free permit. */
    public enum OverloadPolicy {
        REJECT, WAIT
    }

    public record Limits(
            @DefaultValue("100") int maxActiveStreams,
            @DefaultValue("reject") OverloadPolicy overloadPolicy,
            @DefaultValue("5s") Duration queueWait,
            @DefaultValue("100") int maxWaiting,
            @DefaultValue("16") int streamQueueCapacity) {

        public static final int MAX_STREAM_QUEUE_CAPACITY = 4096;

        public Limits {
            if (maxActiveStreams < 1) {
                throw new IllegalArgumentException("lab.limits.max-active-streams must be at least 1");
            }
            if (overloadPolicy == null) {
                throw new IllegalArgumentException("lab.limits.overload-policy must be reject or wait");
            }
            requirePositive(queueWait, "lab.limits.queue-wait");
            if (maxWaiting < 1) {
                throw new IllegalArgumentException("lab.limits.max-waiting must be at least 1");
            }
            if (streamQueueCapacity < 1 || streamQueueCapacity > MAX_STREAM_QUEUE_CAPACITY) {
                throw new IllegalArgumentException(
                        "lab.limits.stream-queue-capacity must be between 1 and " + MAX_STREAM_QUEUE_CAPACITY);
            }
        }

        public static Limits defaults() {
            return new Limits(100, OverloadPolicy.REJECT, Duration.ofSeconds(5), 100, 16);
        }
    }

    /** Upstream silence limits; both must stay below the call deadline or they could never fire. */
    public record Timeouts(
            @DefaultValue("30s") Duration firstChunk,
            @DefaultValue("30s") Duration idleBetweenChunks) {

        public Timeouts {
            requirePositive(firstChunk, "lab.timeouts.first-chunk");
            requirePositive(idleBetweenChunks, "lab.timeouts.idle-between-chunks");
        }

        public static Timeouts defaults() {
            return new Timeouts(Duration.ofSeconds(30), Duration.ofSeconds(30));
        }
    }

    /**
     * Bounded reconnect. {@code maxAttempts} counts <em>retries</em> (extra upstream calls after the first),
     * so at most {@code maxAttempts + 1} calls are made; 0 disables retrying.
     */
    public record Retry(
            @DefaultValue("3") int maxAttempts,
            @DefaultValue("100ms") Duration initialBackoff,
            @DefaultValue("2s") Duration maxBackoff) {

        public static final int MAX_ATTEMPTS = 10;

        public Retry {
            if (maxAttempts < 0 || maxAttempts > MAX_ATTEMPTS) {
                throw new IllegalArgumentException("lab.retry.max-attempts must be between 0 and " + MAX_ATTEMPTS);
            }
            requirePositive(initialBackoff, "lab.retry.initial-backoff");
            requirePositive(maxBackoff, "lab.retry.max-backoff");
            if (initialBackoff.compareTo(maxBackoff) > 0) {
                throw new IllegalArgumentException("lab.retry.initial-backoff (" + initialBackoff
                        + ") must not exceed lab.retry.max-backoff (" + maxBackoff + ")");
            }
        }

        public static Retry defaults() {
            return new Retry(3, Duration.ofMillis(100), Duration.ofSeconds(2));
        }

        public static Retry disabled() {
            return new Retry(0, Duration.ofMillis(100), Duration.ofSeconds(2));
        }
    }

    private static void requireShorterThanDeadline(Duration value, String name, Duration deadline) {
        if (value != null && deadline != null && value.compareTo(deadline) >= 0) {
            throw new IllegalArgumentException(name + " (" + value + ") must be shorter than "
                    + "lab.upstream.stream-deadline (" + deadline + ")");
        }
    }

    private static void requirePositive(Duration value, String name) {
        if (value == null || value.isZero() || value.isNegative()) {
            throw new IllegalArgumentException(name + " must be positive");
        }
    }
}
