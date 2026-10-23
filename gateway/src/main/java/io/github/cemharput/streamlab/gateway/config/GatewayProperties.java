package io.github.cemharput.streamlab.gateway.config;

import java.time.Duration;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.context.properties.bind.DefaultValue;

@ConfigurationProperties(prefix = "lab")
public record GatewayProperties(
        @DefaultValue("shared") ConnectionMode connectionMode,
        @DefaultValue Upstream upstream,
        @DefaultValue Shutdown shutdown) {

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

    private static void requirePositive(Duration value, String name) {
        if (value == null || value.isZero() || value.isNegative()) {
            throw new IllegalArgumentException(name + " must be positive");
        }
    }
}
