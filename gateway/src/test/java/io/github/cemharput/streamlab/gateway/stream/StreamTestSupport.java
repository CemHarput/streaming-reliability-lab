package io.github.cemharput.streamlab.gateway.stream;

import io.github.cemharput.streamlab.gateway.config.GatewayProperties;
import io.github.cemharput.streamlab.gateway.config.GatewayProperties.ConnectionMode;
import io.github.cemharput.streamlab.gateway.config.GatewayProperties.Shutdown;
import io.github.cemharput.streamlab.gateway.config.GatewayProperties.Upstream;
import io.github.cemharput.streamlab.gateway.upstream.ChannelProvider;
import io.github.cemharput.streamlab.tokenstream.v1.GenerateRequest;
import io.github.cemharput.streamlab.tokenstream.v1.TokenChunk;
import io.grpc.Channel;
import io.grpc.stub.ClientCallStreamObserver;
import java.io.IOException;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;
import org.springframework.web.servlet.mvc.method.annotation.SseEmitter;

/** Fakes for driving {@link StreamBridge} and {@link ActiveStreamRegistry} without a server. */
final class StreamTestSupport {

    private StreamTestSupport() {
    }

    static ActiveStreamRegistry startedRegistry(Duration drainTimeout) {
        ActiveStreamRegistry registry = new ActiveStreamRegistry(new GatewayProperties(
                ConnectionMode.SHARED,
                new Upstream("localhost:1", Duration.ofSeconds(60)),
                new Shutdown(drainTimeout, Duration.ofSeconds(1))));
        registry.start();
        return registry;
    }

    static TokenChunk chunk(int seq, boolean last) {
        return TokenChunk.newBuilder().setRequestId("r").setSeq(seq).setText("alpha").setLast(last).build();
    }

    static final ChannelProvider.Lease NO_LEASE = new ChannelProvider.Lease() {
        @Override
        public Channel channel() {
            throw new UnsupportedOperationException();
        }

        @Override
        public void close() {
        }
    };

    /** Counts how often the lease was released, to catch a double release. */
    static final class CountingLease implements ChannelProvider.Lease {

        final java.util.concurrent.atomic.AtomicInteger closes = new java.util.concurrent.atomic.AtomicInteger();

        @Override
        public Channel channel() {
            throw new UnsupportedOperationException();
        }

        @Override
        public void close() {
            closes.incrementAndGet();
        }
    }

    /** Records each sent event as text; every send blocks until {@link #gate} opens (a client that stopped reading). */
    static final class RecordingEmitter extends SseEmitter {

        final List<String> events = new CopyOnWriteArrayList<>();
        final CountDownLatch gate;
        final boolean failSends;
        volatile boolean sendInProgress;
        volatile boolean completed;

        RecordingEmitter(CountDownLatch gate, boolean failSends) {
            this.gate = gate;
            this.failSends = failSends;
        }

        static RecordingEmitter responsive() {
            return new RecordingEmitter(new CountDownLatch(0), false);
        }

        static RecordingEmitter stalled() {
            return new RecordingEmitter(new CountDownLatch(1), false);
        }

        static RecordingEmitter disconnected() {
            return new RecordingEmitter(new CountDownLatch(0), true);
        }

        @Override
        public void send(SseEventBuilder builder) throws IOException {
            if (failSends) {
                throw new IOException("client gone");
            }
            sendInProgress = true;
            try {
                gate.await();
            } catch (InterruptedException ex) {
                Thread.currentThread().interrupt();
                throw new IOException(ex);
            }
            events.add(builder.build().stream()
                    .map(part -> String.valueOf(part.getData()))
                    .collect(Collectors.joining()));
        }

        @Override
        public void complete() {
            completed = true;
        }
    }

    /** Stands in for the upstream gRPC call; remembers how it was cancelled. */
    static final class FakeUpstream extends ClientCallStreamObserver<GenerateRequest> {

        final AtomicReference<String> cancelMessage = new AtomicReference<>();

        @Override
        public void cancel(String message, Throwable cause) {
            cancelMessage.compareAndSet(null, message);
        }

        @Override
        public boolean isReady() {
            return true;
        }

        @Override
        public void setOnReadyHandler(Runnable onReadyHandler) {
        }

        @Override
        public void request(int count) {
        }

        @Override
        public void setMessageCompression(boolean enable) {
        }

        @Override
        public void disableAutoInboundFlowControl() {
        }

        @Override
        public void onNext(GenerateRequest value) {
        }

        @Override
        public void onError(Throwable t) {
        }

        @Override
        public void onCompleted() {
        }
    }
}
