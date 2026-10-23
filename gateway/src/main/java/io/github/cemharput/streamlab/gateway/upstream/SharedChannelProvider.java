package io.github.cemharput.streamlab.gateway.upstream;

import io.github.cemharput.streamlab.gateway.LifecyclePhases;
import io.github.cemharput.streamlab.gateway.config.GatewayProperties;
import io.grpc.Channel;
import io.grpc.ConnectivityState;
import io.grpc.ManagedChannel;
import java.lang.System.Logger;
import java.lang.System.Logger.Level;
import java.util.concurrent.TimeUnit;
import org.springframework.context.SmartLifecycle;

/** One lifecycle-managed channel shared by all streams. */
public class SharedChannelProvider implements ChannelProvider, SmartLifecycle {

    private static final Logger LOGGER = System.getLogger(SharedChannelProvider.class.getName());

    private final ManagedChannelFactory factory;
    private final GatewayProperties properties;
    private volatile ManagedChannel channel;
    private volatile boolean running;

    public SharedChannelProvider(ManagedChannelFactory factory, GatewayProperties properties) {
        this.factory = factory;
        this.properties = properties;
    }

    @Override
    public synchronized void start() {
        if (running) {
            return;
        }
        ManagedChannel created = factory.create(properties.upstream().target());
        channel = created;
        running = true;
        LOGGER.log(Level.INFO, "Upstream channel created for {0}", properties.upstream().target());
        watch(created, created.getState(true));
    }

    /** Keeps the connection warm at channel level; this is not stream retry. */
    private void watch(ManagedChannel watched, ConnectivityState last) {
        if (!running) {
            return;
        }
        watched.notifyWhenStateChanged(last, () -> {
            ConnectivityState now = watched.getState(false);
            LOGGER.log(Level.INFO, "Upstream channel state {0} -> {1}", last, now);
            if (!running) {
                return;
            }
            if (now == ConnectivityState.IDLE) {
                now = watched.getState(true);
            }
            watch(watched, now);
        });
    }

    @Override
    public void stop() {
        ManagedChannel current;
        synchronized (this) {
            running = false;
            current = channel;
        }
        if (current == null) {
            return;
        }
        current.shutdown();
        try {
            if (!current.awaitTermination(properties.shutdown().channelTerminationTimeout().toMillis(), TimeUnit.MILLISECONDS)) {
                LOGGER.log(Level.WARNING, "Upstream channel did not terminate in time; forcing");
                current.shutdownNow();
                if (!current.awaitTermination(2, TimeUnit.SECONDS)) {
                    LOGGER.log(Level.ERROR, "Upstream channel did not terminate after forced shutdown");
                }
            }
        } catch (InterruptedException ex) {
            current.shutdownNow();
            Thread.currentThread().interrupt();
        }
    }

    @Override
    public boolean isRunning() {
        return running;
    }

    @Override
    public int getPhase() {
        return LifecyclePhases.UPSTREAM_CHANNEL;
    }

    @Override
    public Lease acquire() {
        ManagedChannel current = channel;
        if (!running || current == null) {
            throw new IllegalStateException("Upstream channel is not running");
        }
        return new SharedLease(current);
    }

    @Override
    public ConnectivityState state(boolean requestConnection) {
        ManagedChannel current = channel;
        if (!running || current == null) {
            return ConnectivityState.SHUTDOWN;
        }
        return current.getState(requestConnection);
    }

    public boolean isTerminated() {
        ManagedChannel current = channel;
        return current != null && current.isTerminated();
    }

    private record SharedLease(Channel channel) implements Lease {

        @Override
        public void close() {
            // shared channel: nothing to release
        }
    }
}
