package io.github.cemharput.streamlab.gateway.stream;

import io.github.cemharput.streamlab.gateway.config.GatewayProperties;
import io.github.cemharput.streamlab.gateway.config.GatewayProperties.ConnectionMode;
import io.github.cemharput.streamlab.gateway.config.GatewayProperties.Shutdown;
import io.github.cemharput.streamlab.gateway.config.GatewayProperties.Limits;
import io.github.cemharput.streamlab.gateway.config.GatewayProperties.Retry;
import io.github.cemharput.streamlab.gateway.config.GatewayProperties.OverloadPolicy;
import io.github.cemharput.streamlab.gateway.config.GatewayProperties.Timeouts;
import io.github.cemharput.streamlab.gateway.config.GatewayProperties.Upstream;
import io.github.cemharput.streamlab.gateway.upstream.ChannelProvider;
import io.github.cemharput.streamlab.tokenstream.v1.GenerateRequest;
import io.github.cemharput.streamlab.tokenstream.v1.TokenChunk;
import io.grpc.Channel;
import io.grpc.stub.ClientCallStreamObserver;
import io.grpc.stub.ClientResponseObserver;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import java.io.IOException;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;
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
        ActiveStreamRegistry registry = new ActiveStreamRegistry(properties(drainTimeout, limits, Timeouts.defaults()));
        registry.start();
        return registry;
    }

    static GatewayProperties properties(Duration drainTimeout, Limits limits, Timeouts timeouts) {
        return new GatewayProperties(
                ConnectionMode.SHARED,
                new Upstream("localhost:1", Duration.ofSeconds(60)),
                new Shutdown(drainTimeout, Duration.ofSeconds(1)),
                limits,
                timeouts,
                Retry.defaults());
    }

    /** A started scheduler with the given limits; the caller must stop it. */
    static StreamTimeouts startedTimeouts(Duration firstChunk, Duration idleBetweenChunks) {
        StreamTimeouts timeouts = new StreamTimeouts(
                properties(Duration.ofSeconds(1), defaultLimits(), new Timeouts(firstChunk, idleBetweenChunks)));
        timeouts.start();
        return timeouts;
    }

    private static StreamTimeouts longTimeouts;

    /** Shared 30s/30s scheduler for tests that do not care about timeouts (daemon thread, never fires in time). */
    private static synchronized StreamTimeouts longTimeouts() {
        if (longTimeouts == null) {
            longTimeouts = startedTimeouts(Duration.ofSeconds(30), Duration.ofSeconds(30));
        }
        return longTimeouts;
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
        return newBridge(emitter, registry, lease, capacity, metrics, longTimeouts());
    }

    static StreamBridge newBridge(SseEmitter emitter, ActiveStreamRegistry registry, ChannelProvider.Lease lease,
            int capacity, StreamMetrics metrics, StreamTimeouts timeouts) {
        return new StreamBridge(GenerateRequest.newBuilder().setRequestId("r").build(), emitter, registry, lease,
                capacity, metrics, timeouts, RetryPolicy.disabled(), NO_STARTER, Duration.ofSeconds(60));
    }

    /** For bridges whose calls are driven by direct observer calls: starting a call is a bug in the test. */
    static final UpstreamStarter NO_STARTER = (request, deadline, observer) -> {
        throw new UnsupportedOperationException("no upstream in this test");
    };

    /** A bridge that retries through {@code starter}; call {@code start()} after registering it. */
    static StreamBridge newRetryingBridge(SseEmitter emitter, ActiveStreamRegistry registry,
            ChannelProvider.Lease lease, int capacity, StreamMetrics metrics, StreamTimeouts timeouts,
            RetryPolicy policy, UpstreamStarter starter, Duration deadline) {
        GenerateRequest request = GenerateRequest.newBuilder().setRequestId("r").setSeed(7).build();
        return new StreamBridge(request, emitter, registry, lease, capacity, metrics, timeouts, policy, starter,
                deadline);
    }

    /** A clock the test moves by hand. */
    static final class ManualClock implements java.util.function.LongSupplier {

        private final AtomicLong nanos = new AtomicLong(1_000_000_000L);

        @Override
        public long getAsLong() {
            return nanos.get();
        }

        void advance(Duration by) {
            nanos.addAndGet(by.toNanos());
        }
    }

    /**
     * Starts fake upstream calls: every start creates a new {@link FakeUpstream}, runs the observer's
     * {@code beforeStart} synchronously (like the stub does) and then the {@link #onStart} hook, which may
     * deliver chunks, fail the call or wait on a latch.
     */
    static final class FakeUpstreamStarter implements UpstreamStarter {

        record Call(GenerateRequest request, Duration deadline,
                ClientResponseObserver<GenerateRequest, TokenChunk> observer, FakeUpstream upstream) {
        }

        final List<Call> calls = new CopyOnWriteArrayList<>();
        volatile Consumer<Call> onStart = call -> { };
        /** Runs after the call is recorded and before `beforeStart`: lets a test claim the stream at that instant. */
        volatile Consumer<Call> beforeStartHook = call -> { };

        @Override
        public void start(GenerateRequest request, Duration deadline,
                ClientResponseObserver<GenerateRequest, TokenChunk> observer) {
            FakeUpstream upstream = new FakeUpstream();
            Call call = new Call(request, deadline, observer, upstream);
            calls.add(call);
            beforeStartHook.accept(call);
            observer.beforeStart(upstream);
            onStart.accept(call);
        }
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
        volatile boolean failSends;
        volatile boolean sendInProgress;
        volatile boolean completed;
        private volatile Runnable completionCallback = () -> { };

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

        @Override
        public void onCompletion(Runnable callback) {
            super.onCompletion(callback);
            completionCallback = callback;
        }

        /** The container reports that the response ended (client disconnected). */
        void simulateClientGone() {
            completionCallback.run();
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
            deliver((ClientResponseObserver<GenerateRequest, TokenChunk>) bridge, chunk);
        }

        /** Delivers to a specific attempt's observer. */
        void deliver(ClientResponseObserver<?, TokenChunk> observer, TokenChunk chunk) {
            delivered.incrementAndGet();
            observer.onNext(chunk);
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
