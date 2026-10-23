package io.github.cemharput.streamlab.gateway;

import org.springframework.context.SmartLifecycle;

/**
 * Shutdown ordering. Higher phases start first and stop first.
 *
 * <p>Stop order: stream admission (reject new, drain, force-cancel), then the web
 * server, then the upstream channel.
 */
public final class LifecyclePhases {

    /** Starts before the web server and stops after it. */
    public static final int UPSTREAM_CHANNEL = SmartLifecycle.DEFAULT_PHASE - 4096;

    /** Stops before the web server graceful shutdown, so draining still answers 503. */
    public static final int STREAM_ADMISSION = SmartLifecycle.DEFAULT_PHASE - 512;

    private LifecyclePhases() {
    }
}
