package io.github.cemharput.streamlab.gateway.stream;

import static io.github.cemharput.streamlab.gateway.stream.StreamTestSupport.NO_LEASE;
import static io.github.cemharput.streamlab.gateway.stream.StreamTestSupport.startedRegistry;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import io.github.cemharput.streamlab.gateway.stream.StreamTestSupport.RecordingEmitter;
import io.github.cemharput.streamlab.simulator.RequestLimits;
import io.github.cemharput.streamlab.simulator.SimulatorConfig;
import io.github.cemharput.streamlab.simulator.SimulatorServer;
import io.github.cemharput.streamlab.tokenstream.v1.GenerateRequest;
import io.github.cemharput.streamlab.tokenstream.v1.GetStatsRequest;
import io.github.cemharput.streamlab.tokenstream.v1.SimulatorAdminServiceGrpc;
import io.github.cemharput.streamlab.tokenstream.v1.SimulatorStats;
import io.github.cemharput.streamlab.tokenstream.v1.StreamProfile;
import io.github.cemharput.streamlab.tokenstream.v1.TokenStreamServiceGrpc;
import io.grpc.ManagedChannel;
import io.grpc.inprocess.InProcessChannelBuilder;
import io.grpc.inprocess.InProcessServerBuilder;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import java.time.Duration;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

/** The bridge against the real simulator over an in-process channel: no Spring, no HTTP. */
@Timeout(value = 30, threadMode = Timeout.ThreadMode.SEPARATE_THREAD)
class UpstreamFlowControlTest {

    private static final Duration TIMEOUT = Duration.ofSeconds(10);

    private SimulatorServer server;
    private ManagedChannel channel;
    private SimulatorAdminServiceGrpc.SimulatorAdminServiceBlockingStub admin;

    @BeforeEach
    void setUp() throws Exception {
        String name = InProcessServerBuilder.generateName();
        server = SimulatorServer.start(
                InProcessServerBuilder.forName(name),
                new SimulatorConfig(0, Duration.ofSeconds(2), 2, RequestLimits.DEFAULT));
        channel = InProcessChannelBuilder.forName(name).build();
        admin = SimulatorAdminServiceGrpc.newBlockingStub(channel);
    }

    @AfterEach
    void tearDown() throws Exception {
        channel.shutdownNow().awaitTermination(5, TimeUnit.SECONDS);
        server.stop();
    }

    private SimulatorStats stats() {
        return admin.getStats(GetStatsRequest.getDefaultInstance());
    }

    @Test
    void realUpstreamPausesWhileClientIsStalled() {
        int capacity = 4;
        int chunks = 50;
        ActiveStreamRegistry registry = startedRegistry(Duration.ofSeconds(1));
        SimpleMeterRegistry meters = new SimpleMeterRegistry();
        RecordingEmitter emitter = RecordingEmitter.stalled();
        StreamBridge bridge = StreamTestSupport.newBridge(emitter, registry, NO_LEASE, capacity,
                new StreamMetrics(meters, registry));
        assertThat(registry.tryRegister(bridge)).isTrue();
        GenerateRequest request = GenerateRequest.newBuilder()
                .setRequestId("r-flow")
                .setSeed(42)
                .setProfile(StreamProfile.newBuilder().setChunkCount(chunks).setFirstChunkDelayMs(1).setChunkIntervalMs(1))
                .build();
        try {
            TokenStreamServiceGrpc.newStub(channel)
                    .withDeadlineAfter(30, TimeUnit.SECONDS)
                    .generateTokens(request, bridge);

            // The client reads nothing: the simulator must stop once the granted credit is used up.
            await().atMost(TIMEOUT).until(() -> stats().getFlowControlPausesTotal() >= 1);
            assertThat(stats().getChunksEmittedTotal()).isLessThanOrEqualTo(capacity);
            assertThat(bridge.queuedChunks()).isLessThanOrEqualTo(capacity);
            assertThat(StreamTestSupport.gauge(meters, "queue_depth", "chunks")).isLessThanOrEqualTo(capacity);
            assertThat(registry.activeCount()).isEqualTo(1);

            emitter.gate.countDown();

            await().atMost(TIMEOUT).until(() -> emitter.completed);
            // 50 chunks, in order, then exactly one complete event.
            assertThat(emitter.events).hasSize(chunks + 1);
            for (int i = 0; i < chunks; i++) {
                assertThat(emitter.events.get(i)).contains("event:chunk").contains("id:" + i + "\n");
            }
            assertThat(emitter.events.get(chunks)).contains("event:complete");
            await().atMost(TIMEOUT).until(() -> registry.activeCount() == 0);
            await().atMost(TIMEOUT).until(() -> stats().getActiveGenerations() == 0);
            assertThat(StreamTestSupport.gauge(meters, "queue_depth", "chunks")).isZero();
        } finally {
            emitter.gate.countDown();
        }
    }
}
