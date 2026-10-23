package io.github.cemharput.streamlab.gateway;

import org.springframework.context.SmartLifecycle;

/**
 * Lifecycle ordering. In Spring a lower phase starts first and a higher phase stops first.
 *
 * <p>Stop order: stream admission (reject new, drain, force-cancel), then the stream
 * timeouts scheduler, then the web server, then the upstream channel.
 */
public final class LifecyclePhases {

    /** Starts before the web server and stops after it. */
    public static final int UPSTREAM_CHANNEL = SmartLifecycle.DEFAULT_PHASE - 4096;

    /** Stops before the web server graceful shutdown, so draining still answers 503. */
    public static final int STREAM_ADMISSION = SmartLifecycle.DEFAULT_PHASE - 512;

    /** Starts before stream admission and stops after it, so no admitted stream sees a stopped scheduler. */
    public static final int STREAM_TIMEOUTS = SmartLifecycle.DEFAULT_PHASE - 768;

    private LifecyclePhases() {
    }
}
