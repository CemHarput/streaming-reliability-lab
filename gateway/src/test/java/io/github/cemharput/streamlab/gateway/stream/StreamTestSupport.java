package io.github.cemharput.streamlab.gateway.stream;

import io.github.cemharput.streamlab.gateway.config.GatewayProperties;
import io.github.cemharput.streamlab.gateway.config.GatewayProperties.ConnectionMode;
import io.github.cemharput.streamlab.gateway.config.GatewayProperties.Shutdown;
import io.github.cemharput.streamlab.gateway.config.GatewayProperties.Limits;
import io.github.cemharput.streamlab.gateway.config.GatewayProperties.OverloadPolicy;
import io.github.cemharput.streamlab.gateway.config.GatewayProperties.Upstream;
import io.github.cemharput.streamlab.gateway.upstream.ChannelProvider;
import io.github.cemharput.streamlab.tokenstream.v1.GenerateRequest;
import io.github.cemharput.streamlab.tokenstream.v1.TokenChunk;
import io.grpc.Channel;
import io.grpc.stub.ClientCallStreamObserver;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import java.io.IOException;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;
import org.springframework.web.servlet.mvc.method.annotation.SseEmitter;

/** Fakes for driving {@link StreamBridge} and {@link ActiveStreamRegistry} without a server. */
final class StreamTestSupport {

    private StreamTestSupport() {
    }

    static ActiveStreamRegistry startedRegistry(Duration drainTimeout) {
        return startedRegistry(drainTimeout, defaultLimits());
    }

    static ActiveStreamRegistry startedRegistry(Duration drainTimeout, Limits limits) {
        ActiveStreamRegistry registry = new ActiveStreamRegistry(new GatewayProperties(
                ConnectionMode.SHARED,
                new Upstream("localhost:1", Duration.ofSeconds(60)),
                new Shutdown(drainTimeout, Duration.ofSeconds(1)),
                limits));
        registry.start();
        return registry;
    }

    static Limits defaultLimits() {
        return Limits.defaults();
    }

    /** Current value of a gauge; {@code queueTag} may be null for gauges without a tag. */
    static double gauge(io.micrometer.core.instrument.MeterRegistry meters, String name, String queueTag) {
        var search = meters.get(name);
        if (queueTag != null) {
            search = search.tag("queue", queueTag);
        }
        return search.gauge().value();
    }

    static Limits limits(int maxActive, OverloadPolicy policy, Duration queueWait, int maxWaiting) {
        return new Limits(maxActive, policy, queueWait, maxWaiting, 16);
    }

    static StreamBridge newBridge(SseEmitter emitter, ActiveStreamRegistry registry, ChannelProvider.Lease lease) {
        return newBridge(emitter, registry, lease, 16, new StreamMetrics(new SimpleMeterRegistry(), registry));
    }

    static StreamBridge newBridge(SseEmitter emitter, ActiveStreamRegistry registry, ChannelProvider.Lease lease,
            int capacity, StreamMetrics metrics) {
        return new StreamBridge("r", emitter, registry, lease, capacity, metrics);
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

    /** Every send needs a permit: a client that reads one event at a time. */
    static final class SteppingEmitter extends SseEmitter {

        final List<String> events = new CopyOnWriteArrayList<>();
        final Semaphore permits = new Semaphore(0);
        final AtomicInteger sendsStarted = new AtomicInteger();
        volatile boolean completed;

        @Override
        public void send(SseEventBuilder builder) throws IOException {
            sendsStarted.incrementAndGet();
            try {
                permits.acquire();
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
        final AtomicInteger initialRequest = new AtomicInteger(-1);
        final AtomicInteger requested = new AtomicInteger();
        final AtomicInteger delivered = new AtomicInteger();

        /** Credit left: what a well-behaved upstream may still send. */
        int credit() {
            return Math.max(0, initialRequest.get()) + requested.get() - delivered.get();
        }

        /** Delivers a chunk the way gRPC would, counting it against the credit. */
        void deliver(StreamBridge bridge, TokenChunk chunk) {
            delivered.incrementAndGet();
            bridge.onNext(chunk);
        }

        @Override
        public void disableAutoRequestWithInitial(int request) {
            initialRequest.set(request);
        }

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
            requested.addAndGet(count);
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
