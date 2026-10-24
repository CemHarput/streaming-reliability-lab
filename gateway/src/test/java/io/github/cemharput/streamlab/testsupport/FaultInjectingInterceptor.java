package io.github.cemharput.streamlab.testsupport;

import io.github.cemharput.streamlab.tokenstream.v1.TokenStreamServiceGrpc;
import io.grpc.CallOptions;
import io.grpc.Channel;
import io.grpc.ClientCall;
import io.grpc.ClientInterceptor;
import io.grpc.ForwardingClientCall;
import io.grpc.ForwardingClientCallListener;
import io.grpc.Metadata;
import io.grpc.MethodDescriptor;
import io.grpc.Status;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Makes the first N {@code GenerateTokens} calls fail with {@code UNAVAILABLE} without touching the simulator.
 * Two plans: refuse a call outright (it never reaches the simulator) or cut a call after K delivered chunks
 * (the simulator sees the call and its cancellation). Without a plan every call passes through.
 */
public final class FaultInjectingInterceptor implements ClientInterceptor {

    public static final String REFUSED = "injected by test interceptor: call refused";

    private final AtomicInteger refuseCalls = new AtomicInteger();
    private final AtomicInteger cutCalls = new AtomicInteger();
    private volatile int cutAfterChunks;
    private final AtomicInteger intercepted = new AtomicInteger();

    /** The next {@code n} calls fail with UNAVAILABLE before reaching the simulator. */
    public void failNextCalls(int n) {
        refuseCalls.set(n);
    }

    /** The next {@code n} calls are cut (UNAVAILABLE) after {@code afterChunks} chunks were delivered. */
    public void cutNextCalls(int n, int afterChunks) {
        cutAfterChunks = afterChunks;
        cutCalls.set(n);
    }

    /** Number of GenerateTokens calls that went through this interceptor. */
    public int interceptedCalls() {
        return intercepted.get();
    }

    public void reset() {
        refuseCalls.set(0);
        cutCalls.set(0);
        intercepted.set(0);
    }

    private static boolean take(AtomicInteger counter) {
        while (true) {
            int current = counter.get();
            if (current <= 0) {
                return false;
            }
            if (counter.compareAndSet(current, current - 1)) {
                return true;
            }
        }
    }

    @Override
    public <ReqT, RespT> ClientCall<ReqT, RespT> interceptCall(MethodDescriptor<ReqT, RespT> method,
            CallOptions callOptions, Channel next) {
        if (!TokenStreamServiceGrpc.getGenerateTokensMethod().getFullMethodName().equals(method.getFullMethodName())) {
            return next.newCall(method, callOptions);
        }
        intercepted.incrementAndGet();
        if (take(refuseCalls)) {
            return new RefusedCall<>();
        }
        if (take(cutCalls)) {
            return new CutCall<>(next.newCall(method, callOptions), cutAfterChunks);
        }
        return next.newCall(method, callOptions);
    }

    /** Fails on start; never reaches the transport. */
    private static final class RefusedCall<ReqT, RespT> extends ClientCall<ReqT, RespT> {

        @Override
        public void start(Listener<RespT> listener, Metadata headers) {
            // Like a channel that is not ready: the failure is delivered asynchronously.
            Thread.ofVirtual().name("test-refused-call").start(() ->
                    listener.onClose(Status.UNAVAILABLE.withDescription(REFUSED), new Metadata()));
        }

        @Override
        public void request(int numMessages) {
        }

        @Override
        public void cancel(String message, Throwable cause) {
        }

        @Override
        public void halfClose() {
        }

        @Override
        public void sendMessage(ReqT message) {
        }
    }

    /** Forwards until K messages were delivered, then cancels the real call and reports UNAVAILABLE. */
    private static final class CutCall<ReqT, RespT> extends ForwardingClientCall.SimpleForwardingClientCall<ReqT, RespT> {

        private final int afterChunks;

        CutCall(ClientCall<ReqT, RespT> delegate, int afterChunks) {
            super(delegate);
            this.afterChunks = afterChunks;
        }

        @Override
        public void start(Listener<RespT> listener, Metadata headers) {
            ClientCall<ReqT, RespT> real = delegate();
            super.start(new ForwardingClientCallListener.SimpleForwardingClientCallListener<RespT>(listener) {
                private int delivered;
                private volatile boolean cut;

                @Override
                public void onMessage(RespT message) {
                    if (cut) {
                        return;
                    }
                    super.onMessage(message);
                    if (++delivered >= afterChunks) {
                        cut = true;
                        real.cancel("test cut", null);
                    }
                }

                @Override
                public void onClose(Status status, Metadata trailers) {
                    if (cut) {
                        super.onClose(Status.UNAVAILABLE.withDescription(
                                "injected by test interceptor: cut after " + afterChunks + " chunks"), new Metadata());
                    } else {
                        super.onClose(status, trailers);
                    }
                }
            }, headers);
        }
    }
}
