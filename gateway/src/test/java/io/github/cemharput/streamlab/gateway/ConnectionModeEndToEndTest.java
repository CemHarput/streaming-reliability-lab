package io.github.cemharput.streamlab.gateway;

import static io.github.cemharput.streamlab.testsupport.SseAssertions.assertSingleTerminal;
import static io.github.cemharput.streamlab.testsupport.SseAssertions.chunkIds;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import io.github.cemharput.streamlab.gateway.config.GatewayProperties.ConnectionMode;
import io.github.cemharput.streamlab.gateway.upstream.ChannelProvider;
import io.github.cemharput.streamlab.gateway.upstream.PerRequestChannelProvider;
import io.github.cemharput.streamlab.testsupport.CountingChannelFactory;
import io.github.cemharput.streamlab.testsupport.CountingChannelFactory.CountingChannel;
import io.github.cemharput.streamlab.testsupport.InProcessSimulatorConfig;
import io.github.cemharput.streamlab.testsupport.SseClient;
import io.github.cemharput.streamlab.testsupport.SseClient.SseEvent;
import io.github.cemharput.streamlab.tokenstream.v1.SimulatorStats;
import io.micrometer.core.instrument.MeterRegistry;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.context.ConfigurableApplicationContext;

/**
 * The gateway in each connection mode against the real in-process simulator. The channel factory of the test
 * configuration counts what happens to every channel the gateway creates. In-process channels have no sockets,
 * threads or connect backoff, so this proves the channel life cycle, not network behaviour.
 */
@Timeout(value = 60, threadMode = Timeout.ThreadMode.SEPARATE_THREAD)
class ConnectionModeEndToEndTest {

    private static final Duration WAIT = Duration.ofSeconds(30);

    private ConfigurableApplicationContext context;
    private InProcessSimulatorConfig simulator;
    private CountingChannelFactory channels;
    private MeterRegistry meters;

    private void start(ConnectionMode mode, String... extraArgs) {
        List<String> args = new ArrayList<>(List.of(
                "--lab.connection-mode=" + (mode == ConnectionMode.SHARED ? "shared" : "per-request"),
                "--lab.shutdown.drain-timeout=1s"));
        args.addAll(List.of(extraArgs));
        context = new SpringApplicationBuilder(GatewayApplication.class, InProcessSimulatorConfig.class)
                .properties("server.port=0")
                .run(args.toArray(String[]::new));
        simulator = context.getBean(InProcessSimulatorConfig.class);
        channels = simulator.channelFactory();
        meters = context.getBean(MeterRegistry.class);
    }

    @AfterEach
    void tearDown() {
        if (context != null) {
            context.close();
        }
    }

    private String url(String path) {
        return "http://localhost:" + context.getEnvironment().getProperty("local.server.port", Integer.class) + path;
    }

    private SseClient finishedStream(String query) {
        SseClient client = SseClient.open(url("/stream?" + query));
        await().atMost(WAIT).until(client::isFinished);
        return client;
    }

    private double activeStreams() {
        return meters.get("active_streams").gauge().value();
    }

    private double retries(String reason) {
        return meters.get("retries_total").tag("reason", reason).counter().count();
    }

    private void awaitAllChannelsShutOnceAndTerminated() {
        await().atMost(WAIT).untilAsserted(() -> {
            for (CountingChannel channel : channels.channels()) {
                assertThat(channel.shutdownCalls()).isEqualTo(1);
                assertThat(channel.shutdownNowCalls()).isZero();
                assertThat(channel.isTerminated()).isTrue();
            }
        });
    }

    private void assertSharedChannelUntouched() {
        assertThat(channels.created()).isEqualTo(1);
        assertThat(channels.channels().get(0).shutdownCalls()).isZero();
        assertThat(channels.channels().get(0).shutdownNowCalls()).isZero();
        assertThat(channels.channels().get(0).isShutdown()).isFalse();
    }

    @ParameterizedTest
    @EnumSource(ConnectionMode.class)
    void consecutiveStreamsGetOneChannelEachOrShareOne(ConnectionMode mode) {
        start(mode);
        for (int i = 0; i < 3; i++) {
            SseClient client = finishedStream("chunks=5&intervalMs=5");
            assertThat(chunkIds(client.events())).containsExactly("0", "1", "2", "3", "4");
            assertThat(assertSingleTerminal(client.events()).name()).isEqualTo("complete");
        }
        await().atMost(WAIT).untilAsserted(() -> assertThat(activeStreams()).isZero());

        if (mode == ConnectionMode.PER_REQUEST) {
            assertThat(channels.created()).isEqualTo(3);
            awaitAllChannelsShutOnceAndTerminated();
            assertThat(context.getBean(PerRequestChannelProvider.class).openChannels()).isZero();
        } else {
            assertSharedChannelUntouched();
        }
    }

    @ParameterizedTest
    @EnumSource(ConnectionMode.class)
    void resumeKeepsTheStreamsChannel(ConnectionMode mode) {
        start(mode, "--lab.retry.initial-backoff=50ms", "--lab.retry.max-backoff=200ms");
        simulator.faults().cutNextCalls(2, 3);

        SseClient client = finishedStream("chunks=10&intervalMs=10");

        assertThat(chunkIds(client.events())).containsExactly("0", "1", "2", "3", "4", "5", "6", "7", "8", "9");
        assertThat(assertSingleTerminal(client.events()).name()).isEqualTo("complete");
        await().atMost(WAIT).untilAsserted(() -> {
            assertThat(retries("resume")).isEqualTo(2);
            assertThat(activeStreams()).isZero();
        });
        assertThat(channels.created()).isEqualTo(1);
        if (mode == ConnectionMode.PER_REQUEST) {
            awaitAllChannelsShutOnceAndTerminated();
        } else {
            assertSharedChannelUntouched();
        }
    }

    @ParameterizedTest
    @EnumSource(ConnectionMode.class)
    void rejectedRequestOpensNoChannel(ConnectionMode mode) throws Exception {
        start(mode, "--lab.limits.max-active-streams=1");
        SseClient hanging = SseClient.open(url("/stream?chunks=10&intervalMs=10&fault=HANG&faultAtSeq=2"));
        await().atMost(WAIT).until(() -> hanging.events().size() >= 2);
        int createdBefore = channels.created();
        assertThat(createdBefore).isEqualTo(1);

        HttpResponse<String> rejected = SseClient.get(url("/stream?chunks=3"));

        assertThat(rejected.statusCode()).isEqualTo(503);
        assertThat(channels.created()).isEqualTo(createdBefore);
        assertThat(channels.channels().get(0).isShutdown()).isFalse();
    }

    @ParameterizedTest
    @EnumSource(ConnectionMode.class)
    void waitingRequestOpensItsChannelOnlyAfterAdmission(ConnectionMode mode) throws Exception {
        start(mode, "--lab.limits.max-active-streams=1", "--lab.limits.overload-policy=wait",
                "--lab.limits.queue-wait=30s");
        // A long stream, not a hanging one: the disconnect of the holder is noticed by a failed write.
        SseClient holder = SseClient.open(url("/stream?chunks=100000&intervalMs=20"));
        await().atMost(WAIT).until(() -> holder.events().size() >= 2);
        SseClient waiter;
        try {
            waiter = SseClient.open(url("/stream?chunks=3&intervalMs=10"));
            // Only the admission queue: the "chunks" gauge of the long-running holder can also read 1.
            await().atMost(WAIT).until(() -> meters.get("queue_depth").tag("queue", "admission").gauge().value() == 1);
            assertThat(waiter.response()).isNotDone();
            // The waiting request holds a lease but has not created a channel yet.
            assertThat(channels.created()).isEqualTo(1);
        } finally {
            holder.disconnect();
        }

        await().atMost(WAIT).until(waiter::isFinished);
        assertThat(assertSingleTerminal(waiter.events()).name()).isEqualTo("complete");
        assertThat(channels.created()).isEqualTo(mode == ConnectionMode.PER_REQUEST ? 2 : 1);
        if (mode == ConnectionMode.PER_REQUEST) {
            awaitAllChannelsShutOnceAndTerminated();
        } else {
            assertSharedChannelUntouched();
        }
    }

    @Test
    void channelCreationFailureEndsOnlyThatStream() {
        start(ConnectionMode.PER_REQUEST);
        SimulatorStats before = simulator.stats();
        channels.failNextCreations(1);

        SseClient failed = finishedStream("chunks=5&intervalMs=5");

        assertThat(chunkIds(failed.events())).isEmpty();
        SseEvent terminal = assertSingleTerminal(failed.events());
        assertThat(terminal.name()).isEqualTo("error");
        assertThat(terminal.data()).contains("\"code\":\"INTERNAL\"").contains("failed to start upstream call");
        await().atMost(WAIT).untilAsserted(() -> assertThat(activeStreams()).isZero());
        assertThat(simulator.stats().getStartedTotal()).isEqualTo(before.getStartedTotal());
        assertThat(channels.created()).isZero();
        assertThat(context.getBean(PerRequestChannelProvider.class).openChannels()).isZero();

        SseClient next = finishedStream("chunks=5&intervalMs=5");
        assertThat(assertSingleTerminal(next.events()).name()).isEqualTo("complete");
        assertThat(channels.created()).isEqualTo(1);
        awaitAllChannelsShutOnceAndTerminated();
    }

    @ParameterizedTest
    @EnumSource(ConnectionMode.class)
    void shutdownCancelsHangingStreamAndLeavesNoChannel(ConnectionMode mode) {
        start(mode);
        ChannelProvider provider = context.getBean(ChannelProvider.class);
        SseClient hanging = SseClient.open(url("/stream?chunks=10&intervalMs=10&fault=HANG&faultAtSeq=2"));
        await().atMost(WAIT).until(() -> hanging.events().size() >= 2);

        CompletableFuture<Duration> closing = CompletableFuture.supplyAsync(() -> {
            long startedAt = System.nanoTime();
            context.close();
            return Duration.ofNanos(System.nanoTime() - startedAt);
        });
        await().atMost(WAIT).until(() -> closing.isDone() && hanging.isFinished());

        SseEvent terminal = assertSingleTerminal(hanging.events());
        assertThat(terminal.name()).isEqualTo("error");
        assertThat(terminal.data()).contains("\"code\":\"CANCELLED\"");
        // Worst case the code allows: drain 1s + 2s forced-cancel wait + channel termination 5s + 2s.
        assertThat(closing.join()).isLessThan(Duration.ofSeconds(10));
        assertThat(channels.created()).isEqualTo(1);
        for (CountingChannel channel : channels.channels()) {
            assertThat(channel.isTerminated()).isTrue();
            assertThat(channel.shutdownCalls()).isEqualTo(1);
            assertThat(channel.shutdownNowCalls()).isZero();
        }
        if (provider instanceof PerRequestChannelProvider perRequest) {
            assertThat(perRequest.openChannels()).isZero();
            assertThat(perRequest.isTerminated()).isTrue();
        }
    }

    @ParameterizedTest
    @EnumSource(ConnectionMode.class)
    void modeIsExposedAsOneGauge(ConnectionMode mode) throws Exception {
        start(mode);

        var gauges = meters.find("connection_mode").gauges();
        assertThat(gauges).hasSize(1);
        var gauge = gauges.iterator().next();
        assertThat(gauge.getId().getTag("mode")).isEqualTo(mode.tag());
        assertThat(gauge.getId().getTags()).hasSize(1);
        assertThat(gauge.value()).isEqualTo(1.0);
        assertThat(SseClient.get(url("/actuator/metrics/connection_mode")).statusCode()).isEqualTo(200);
    }
}
