package io.github.cemharput.streamlab.gateway;

import static io.github.cemharput.streamlab.testsupport.SseAssertions.assertSingleTerminal;
import static io.github.cemharput.streamlab.testsupport.SseAssertions.chunkIds;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import io.github.cemharput.streamlab.gateway.stream.ActiveStreamRegistry;
import io.github.cemharput.streamlab.gateway.stream.StreamTimeouts;
import io.github.cemharput.streamlab.testsupport.InProcessSimulatorConfig;
import io.github.cemharput.streamlab.testsupport.SseClient;
import io.github.cemharput.streamlab.testsupport.SseClient.SseEvent;
import io.github.cemharput.streamlab.tokenstream.v1.SimulatorStats;
import io.micrometer.core.instrument.MeterRegistry;
import java.time.Duration;
import java.util.List;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.context.ConfigurableApplicationContext;

/**
 * Timeouts and cancellation end to end: the real simulator runs in-process and its own
 * counters (read over a fresh channel) prove that a cancellation reached the upstream.
 */
@Timeout(value = 60, threadMode = Timeout.ThreadMode.SEPARATE_THREAD)
class TimeoutAndCancelTest {

    private static final Duration TIMEOUT = Duration.ofSeconds(15);
    private static final String HANG_AT_0 = "/stream?chunks=3&fault=HANG&faultAtSeq=0";

    private ConfigurableApplicationContext context;
    private MeterRegistry meters;
    private StreamTimeouts timeouts;
    private InProcessSimulatorConfig simulator;

    private void start(String... args) {
        context = new SpringApplicationBuilder(GatewayApplication.class, InProcessSimulatorConfig.class)
                .properties("server.port=0")
                .run(args);
        meters = context.getBean(MeterRegistry.class);
        timeouts = context.getBean(StreamTimeouts.class);
        simulator = context.getBean(InProcessSimulatorConfig.class);
    }

    private String url(String path) {
        return "http://localhost:" + context.getEnvironment().getProperty("local.server.port", Integer.class) + path;
    }

    private SimulatorStats stats() {
        return simulator.stats();
    }

    private double timeoutCount(String kind) {
        return meters.get("timeouts_total").tag("kind", kind).counter().count();
    }

    private double activeStreams() {
        return meters.get("active_streams").gauge().value();
    }

    @AfterEach
    void tearDown() {
        if (context != null) {
            context.close();
        }
    }

    private void assertUpstreamCancelled(SimulatorStats before) {
        await().atMost(TIMEOUT).untilAsserted(() -> {
            SimulatorStats now = stats();
            assertThat(now.getActiveGenerations()).isZero();
            assertThat(now.getCancelledTotal()).isEqualTo(before.getCancelledTotal() + 1);
        });
    }

    private static void assertTimeoutError(SseClient client, String message) {
        SseEvent terminal = assertSingleTerminal(client.events());
        assertThat(terminal.name()).isEqualTo("error");
        assertThat(terminal.data()).contains("\"code\":\"DEADLINE_EXCEEDED\"").contains("\"message\":\"" + message + "\"");
    }

    private void assertNextStreamCompletes() {
        SseClient next = SseClient.open(url("/stream?chunks=3&intervalMs=10"));
        await().atMost(TIMEOUT).until(next::isFinished);
        assertThat(next.response().join().statusCode()).isEqualTo(200);
        assertThat(assertSingleTerminal(next.events()).name()).isEqualTo("complete");
    }

    @Test
    void firstChunkTimeoutOnHangCancelsUpstream() {
        start("--lab.timeouts.first-chunk=1s", "--lab.limits.max-active-streams=1");
        SimulatorStats before = stats();

        long startedAt = System.nanoTime();
        SseClient client = SseClient.open(url(HANG_AT_0));
        await().atMost(TIMEOUT).until(() -> stats().getStartedTotal() == before.getStartedTotal() + 1);
        await().atMost(TIMEOUT).until(client::isFinished);
        Duration elapsed = Duration.ofNanos(System.nanoTime() - startedAt);

        assertThat(elapsed).isGreaterThanOrEqualTo(Duration.ofSeconds(1)).isLessThan(Duration.ofSeconds(10));
        assertThat(chunkIds(client.events())).isEmpty();
        assertTimeoutError(client, "first chunk timeout");
        assertUpstreamCancelled(before);
        assertThat(timeoutCount("first_chunk")).isEqualTo(1);
        assertThat(timeoutCount("idle")).isZero();
        await().atMost(TIMEOUT).until(() -> activeStreams() == 0);
        assertThat(timeouts.pendingTimers()).isZero();
        // max-active-streams=1: the capacity came back, the next stream is admitted.
        assertNextStreamCompletes();
    }

    @Test
    void slowFirstChunkTimesOut() {
        start("--lab.timeouts.first-chunk=1s");
        SimulatorStats before = stats();

        SseClient client = SseClient.open(url("/stream?chunks=3&firstDelayMs=20000"));
        await().atMost(TIMEOUT).until(client::isFinished);

        assertThat(chunkIds(client.events())).isEmpty();
        assertTimeoutError(client, "first chunk timeout");
        assertUpstreamCancelled(before);
        assertThat(timeoutCount("first_chunk")).isEqualTo(1);
        await().atMost(TIMEOUT).until(() -> timeouts.pendingTimers() == 0);
    }

    @Test
    void idleTimeoutOnHangMidStreamCancelsUpstream() {
        start("--lab.timeouts.idle-between-chunks=1s");
        SimulatorStats before = stats();

        SseClient client = SseClient.open(url("/stream?chunks=10&intervalMs=10&fault=HANG&faultAtSeq=3"));
        await().atMost(TIMEOUT).until(client::isFinished);

        // Chunks received so far are written in order, then the single error.
        assertThat(chunkIds(client.events())).containsExactly("0", "1", "2");
        assertTimeoutError(client, "idle timeout between chunks");
        assertUpstreamCancelled(before);
        assertThat(timeoutCount("idle")).isEqualTo(1);
        assertThat(timeoutCount("first_chunk")).isZero();
        await().atMost(TIMEOUT).until(() -> timeouts.pendingTimers() == 0);
    }

    @Test
    void slowIntervalTimesOut() {
        start("--lab.timeouts.idle-between-chunks=1s");
        SimulatorStats before = stats();

        SseClient client = SseClient.open(url("/stream?chunks=5&intervalMs=20000"));
        await().atMost(TIMEOUT).until(client::isFinished);

        assertThat(chunkIds(client.events())).containsExactly("0");
        assertTimeoutError(client, "idle timeout between chunks");
        assertUpstreamCancelled(before);
        assertThat(timeoutCount("idle")).isEqualTo(1);
    }

    @Test
    void hangWithoutCancelStaysActiveUntilShutdownCancels() {
        start("--lab.shutdown.drain-timeout=1s");
        SimulatorStats before = stats();
        ActiveStreamRegistry registry = context.getBean(ActiveStreamRegistry.class);

        SseClient client = SseClient.open(url("/stream?chunks=10&intervalMs=10&fault=HANG&faultAtSeq=2"));
        await().atMost(TIMEOUT).until(() -> client.events().size() >= 2);
        // Positive control for the leak checks below: a live stream holds exactly one armed timer.
        assertThat(timeouts.pendingTimers()).isEqualTo(1);

        // Negative control: nothing cancels this stream, so the simulator keeps generating.
        await().during(Duration.ofSeconds(2)).atMost(Duration.ofSeconds(8)).untilAsserted(() -> {
            assertThat(stats().getActiveGenerations()).isEqualTo(1);
            assertThat(stats().getCancelledTotal()).isEqualTo(before.getCancelledTotal());
        });

        // Shutdown (the registry stops before the channel and the simulator): drain 1s, then cancel.
        registry.stop();

        await().atMost(TIMEOUT).until(client::isFinished);
        SseEvent terminal = assertSingleTerminal(client.events());
        assertThat(terminal.name()).isEqualTo("error");
        assertThat(terminal.data()).contains("\"code\":\"CANCELLED\"").contains("gateway shutting down");
        assertUpstreamCancelled(before);
        assertThat(timeouts.pendingTimers()).isZero();
        assertThat(timeoutCount("idle")).isZero();
    }

    @Test
    void clientDisconnectCancelsUpstream() {
        start();
        SimulatorStats before = stats();

        SseClient client = SseClient.open(url("/stream?chunks=100000&intervalMs=20"));
        await().atMost(TIMEOUT).until(() -> !client.events().isEmpty());
        await().atMost(TIMEOUT).until(() -> stats().getActiveGenerations() == 1);

        client.disconnect();

        assertUpstreamCancelled(before);
        await().atMost(TIMEOUT).until(() -> activeStreams() == 0);
        await().atMost(TIMEOUT).until(() -> timeouts.pendingTimers() == 0);
        assertThat(timeoutCount("idle")).isZero();
        assertThat(timeoutCount("first_chunk")).isZero();
    }

    @Test
    void timeoutsCounterIsPreRegisteredAndExposed() throws Exception {
        start();

        assertThat(timeoutCount("first_chunk")).isZero();
        assertThat(timeoutCount("idle")).isZero();
        assertThat(SseClient.get(url("/actuator/metrics/timeouts_total")).statusCode()).isEqualTo(200);
    }

    @Test
    void streamWithinTimeoutsCompletes() {
        start("--lab.timeouts.first-chunk=1s", "--lab.timeouts.idle-between-chunks=1s");
        SimulatorStats before = stats();

        SseClient client = SseClient.open(url("/stream?chunks=10&firstDelayMs=200&intervalMs=100"));
        await().atMost(TIMEOUT).until(client::isFinished);

        List<SseEvent> events = client.events();
        assertThat(assertSingleTerminal(events).name()).isEqualTo("complete");
        assertThat(chunkIds(events)).hasSize(10);
        assertThat(timeoutCount("first_chunk")).isZero();
        assertThat(timeoutCount("idle")).isZero();
        await().atMost(TIMEOUT).untilAsserted(() -> {
            SimulatorStats now = stats();
            assertThat(now.getCompletedTotal()).isEqualTo(before.getCompletedTotal() + 1);
            assertThat(now.getCancelledTotal()).isEqualTo(before.getCancelledTotal());
            assertThat(now.getActiveGenerations()).isZero();
        });
        await().atMost(TIMEOUT).until(() -> timeouts.pendingTimers() == 0);
    }
}
