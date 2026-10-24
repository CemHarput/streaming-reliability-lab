package io.github.cemharput.streamlab.testsupport;

import io.github.cemharput.streamlab.gateway.upstream.ManagedChannelFactory;
import io.grpc.CallOptions;
import io.grpc.ClientCall;
import io.grpc.ConnectivityState;
import io.grpc.ManagedChannel;
import io.grpc.MethodDescriptor;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Wraps a {@link ManagedChannelFactory} and counts what happens to each channel it creates: how often
 * {@code shutdown()} and {@code shutdownNow()} were called. Creation can be made to fail for the next N calls.
 */
public final class CountingChannelFactory implements ManagedChannelFactory {

    private final ManagedChannelFactory delegate;
    private final List<CountingChannel> channels = new CopyOnWriteArrayList<>();
    private final AtomicInteger failNext = new AtomicInteger();

    public CountingChannelFactory(ManagedChannelFactory delegate) {
        this.delegate = delegate;
    }

    @Override
    public ManagedChannel create(String target) {
        while (true) {
            int pending = failNext.get();
            if (pending <= 0) {
                break;
            }
            if (failNext.compareAndSet(pending, pending - 1)) {
                throw new IllegalStateException("injected channel creation failure");
            }
        }
        CountingChannel channel = new CountingChannel(delegate.create(target));
        channels.add(channel);
        return channel;
    }

    /** Number of channels successfully created. */
    public int created() {
        return channels.size();
    }

    public List<CountingChannel> channels() {
        return List.copyOf(channels);
    }

    /** The next {@code n} calls to {@link #create} throw instead of creating a channel. */
    public void failNextCreations(int n) {
        failNext.set(n);
    }

    /** A channel that counts shutdown calls and forwards everything else. */
    public static final class CountingChannel extends ManagedChannel {

        private final ManagedChannel delegate;
        private final AtomicInteger shutdowns = new AtomicInteger();
        private final AtomicInteger shutdownNows = new AtomicInteger();

        CountingChannel(ManagedChannel delegate) {
            this.delegate = delegate;
        }

        public int shutdownCalls() {
            return shutdowns.get();
        }

        public int shutdownNowCalls() {
            return shutdownNows.get();
        }

        @Override
        public ManagedChannel shutdown() {
            shutdowns.incrementAndGet();
            delegate.shutdown();
            return this;
        }

        @Override
        public boolean isShutdown() {
            return delegate.isShutdown();
        }

        @Override
        public boolean isTerminated() {
            return delegate.isTerminated();
        }

        @Override
        public ManagedChannel shutdownNow() {
            shutdownNows.incrementAndGet();
            delegate.shutdownNow();
            return this;
        }

        @Override
        public boolean awaitTermination(long timeout, TimeUnit unit) throws InterruptedException {
            return delegate.awaitTermination(timeout, unit);
        }

        @Override
        public <RequestT, ResponseT> ClientCall<RequestT, ResponseT> newCall(
                MethodDescriptor<RequestT, ResponseT> methodDescriptor, CallOptions callOptions) {
            return delegate.newCall(methodDescriptor, callOptions);
        }

        @Override
        public String authority() {
            return delegate.authority();
        }

        @Override
        public ConnectivityState getState(boolean requestConnection) {
            return delegate.getState(requestConnection);
        }

        @Override
        public void notifyWhenStateChanged(ConnectivityState source, Runnable callback) {
            delegate.notifyWhenStateChanged(source, callback);
        }

        @Override
        public void resetConnectBackoff() {
            delegate.resetConnectBackoff();
        }

        @Override
        public void enterIdle() {
            delegate.enterIdle();
        }
    }
}
