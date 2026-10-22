package io.github.cemharput.streamlab.simulator;

import java.time.Duration;
import java.util.Map;

/**
 * Process-level settings, read from environment variables so the same image
 * runs unchanged under Docker Compose and in benchmarks.
 */
public record SimulatorConfig(int port, Duration shutdownGrace, int schedulerThreads, RequestLimits limits) {

    public static final int DEFAULT_PORT = 50051;

    public SimulatorConfig {
        if (port < 0 || port > 65535) {
            throw new IllegalArgumentException("port out of range: " + port);
        }
        if (shutdownGrace.isNegative()) {
            throw new IllegalArgumentException("shutdownGrace must not be negative");
        }
        if (schedulerThreads < 1) {
            throw new IllegalArgumentException("schedulerThreads must be >= 1");
        }
    }

    public static SimulatorConfig fromEnvironment(Map<String, String> env) {
        return new SimulatorConfig(
                intValue(env, "SIMULATOR_PORT", DEFAULT_PORT),
                Duration.ofSeconds(intValue(env, "SIMULATOR_SHUTDOWN_GRACE_SECONDS", 10)),
                intValue(env, "SIMULATOR_SCHEDULER_THREADS", Runtime.getRuntime().availableProcessors()),
                RequestLimits.DEFAULT
        );
    }

    private static int intValue(Map<String, String> env, String key, int defaultValue) {
        String raw = env.get(key);
        if (raw == null || raw.isBlank()) {
            return defaultValue;
        }
        try {
            return Integer.parseInt(raw.trim());
        } catch (NumberFormatException ex) {
            throw new IllegalArgumentException(key + " is not an integer: " + raw, ex);
        }
    }
}
