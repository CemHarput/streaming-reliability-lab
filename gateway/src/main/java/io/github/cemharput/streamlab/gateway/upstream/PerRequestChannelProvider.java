package io.github.cemharput.streamlab.gateway.upstream;

import io.github.cemharput.streamlab.gateway.LifecyclePhases;
import io.github.cemharput.streamlab.gateway.config.GatewayProperties;
import io.grpc.Channel;
import io.grpc.ConnectivityState;
import io.grpc.ManagedChannel;
import java.lang.System.Logger;
import java.lang.System.Logger.Level;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import org.springframework.context.SmartLifecycle;

/**
 * One channel per stream. A lease owns at most one channel: it is created by the first {@link Lease#channel()} call
 * (so never for a request that is rejected or still waiting) and shut down by {@link Lease#close()}, which the stream
 * calls exactly once when it reaches its terminal state.
 *
 * <p>{@code close()} only calls {@link ManagedChannel#shutdown()}: it is non-blocking, lets a call that is finishing
 * complete, and is safe on paths that hold the stream's send lock. Waiting for termination happens only in
 * {@link #stop()}. The provider starts no thread of its own.
 */
public class PerRequestChannelProvider implements ChannelProvider, SmartLifecycle {

    private static final Logger LOGGER = System.getLogger(PerRequestChannelProvider.class.getName());

    private final ManagedChannelFactory factory;
    private final GatewayProperties properties;
    private volatile boolean running;
    /** Leases whose channel exists and has not been shut down yet. */
    private final Set<PerRequestLease> open = ConcurrentHashMap.newKeySet();
    /** Channels that were shut down and whose termination has not been seen yet. */
    private final Set<ManagedChannel> closing = ConcurrentHashMap.newKeySet();

    public PerRequestChannelProvider(ManagedChannelFactory factory, GatewayProperties properties) {
        this.factory = factory;
        this.properties = properties;
    }

    @Override
    public synchronized void start() {
        if (running) {
            return;
        }
        running = true;
        LOGGER.log(Level.INFO, "Upstream connection mode per-request: one channel per stream to {0}",
                properties.upstream().target());
    }

    @Override
    public void stop() {
        running = false;
        // Backstop: normally every stream already released its lease before this phase, so this is empty.
        List<PerRequestLease> leftover = new ArrayList<>(open);
        if (!leftover.isEmpty()) {
            LOGGER.log(Level.WARNING, "Closing {0} per-request channel(s) that were still open at stop",
                    leftover.size());
            leftover.forEach(PerRequestLease::close);
        }
        List<ManagedChannel> pending = new ArrayList<>(closing);
        long deadline = System.nanoTime() + properties.shutdown().channelTerminationTimeout().toNanos();
        try {
            List<ManagedChannel> stuck = new ArrayList<>();
            for (ManagedChannel channel : pending) {
                long remaining = deadline - System.nanoTime();
                if (!channel.awaitTermination(Math.max(remaining, 0), TimeUnit.NANOSECONDS)) {
                    stuck.add(channel);
                }
            }
            if (!stuck.isEmpty()) {
                LOGGER.log(Level.WARNING, "{0} per-request channel(s) did not terminate in time; forcing",
                        stuck.size());
                stuck.forEach(ManagedChannel::shutdownNow);
                int unterminated = 0;
                long forcedDeadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(2);
                for (ManagedChannel channel : stuck) {
                    long remaining = forcedDeadline - System.nanoTime();
                    if (!channel.awaitTermination(Math.max(remaining, 0), TimeUnit.NANOSECONDS)) {
                        unterminated++;
                    }
                }
                if (unterminated > 0) {
                    LOGGER.log(Level.ERROR, "{0} per-request channel(s) did not terminate after forced shutdown",
                            unterminated);
                }
            }
        } catch (InterruptedException ex) {
            pending.forEach(ManagedChannel::shutdownNow);
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
        if (!running) {
            throw new IllegalStateException("Upstream channel provider is not running");
        }
        return new PerRequestLease();
    }

    /** There is no long-lived channel to observe, so readiness reflects the lifecycle only. */
    @Override
    public ConnectivityState state(boolean requestConnection) {
        return running ? ConnectivityState.READY : ConnectivityState.SHUTDOWN;
    }

    /** Number of leases that own a channel which has not been shut down yet. */
    public int openChannels() {
        return open.size();
    }

    /** True when stopped and every channel this provider shut down has terminated. */
    public boolean isTerminated() {
        return !running && open.isEmpty() && closing.stream().allMatch(ManagedChannel::isTerminated);
    }

    private sealed interface State permits Empty, Open, Closed {
    }

    private record Empty() implements State {
    }

    private record Open(ManagedChannel channel) implements State {
    }

    private record Closed() implements State {
    }

    private static final State EMPTY = new Empty();
    private static final State CLOSED = new Closed();

    private final class PerRequestLease implements Lease {

        private final AtomicReference<State> state = new AtomicReference<>(EMPTY);

        /** Creates the channel on the first call; no lock is held while the factory runs. */
        @Override
        public Channel channel() {
            while (true) {
                State current = state.get();
                if (current instanceof Open opened) {
                    return opened.channel();
                }
                if (current instanceof Closed || !running) {
                    throw new IllegalStateException("Upstream channel lease is closed");
                }
                // Pruning happens here, never on the close path.
                closing.removeIf(ManagedChannel::isTerminated);
                ManagedChannel fresh = factory.create(properties.upstream().target());
                Open created = new Open(fresh);
                if (state.compareAndSet(current, created)) {
                    open.add(this);
                    if (state.get() != created) {
                        // close() won between the CAS and the add: its remove ran before the add and found nothing,
                        // so this call removes the entry itself; the channel was already asked to shut down.
                        open.remove(this);
                        throw new IllegalStateException("Upstream channel lease is closed");
                    }
                    if (!running) {
                        close();
                        throw new IllegalStateException("Upstream channel provider stopped");
                    }
                    return fresh;
                }
                // Lost a race with close() (or another channel() call): the new channel was never used.
                fresh.shutdownNow();
            }
        }

        /** Idempotent and non-blocking: the one caller that wins the CAS asks the channel to shut down. */
        @Override
        public void close() {
            while (true) {
                State current = state.get();
                if (current instanceof Closed) {
                    return;
                }
                if (state.compareAndSet(current, CLOSED)) {
                    if (current instanceof Open opened) {
                        open.remove(this);
                        closing.add(opened.channel());
                        opened.channel().shutdown();
                    }
                    return;
                }
            }
        }
    }
}
