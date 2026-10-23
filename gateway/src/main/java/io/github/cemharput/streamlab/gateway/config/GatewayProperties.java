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
        @DefaultValue Timeouts timeouts) {

    public GatewayProperties {
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
