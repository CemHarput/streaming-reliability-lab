package io.github.cemharput.streamlab.gateway;

import static io.github.cemharput.streamlab.testsupport.SseAssertions.assertSingleTerminal;
import static io.github.cemharput.streamlab.testsupport.SseAssertions.chunkIds;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import io.github.cemharput.streamlab.gateway.stream.StreamTimeouts;
import io.github.cemharput.streamlab.testsupport.FaultInjectingInterceptor;
import io.github.cemharput.streamlab.testsupport.InProcessSimulatorConfig;
import io.github.cemharput.streamlab.testsupport.SseClient;
import io.github.cemharput.streamlab.testsupport.SseClient.SseEvent;
import io.github.cemharput.streamlab.tokenstream.v1.SimulatorStats;
import io.micrometer.core.instrument.MeterRegistry;
import java.time.Duration;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.context.ConfigurableApplicationContext;

/**
 * Bounded reconnect end to end. The simulator is untouched: a test interceptor on the gateway channel makes calls
 * fail or cuts them, and the simulator's own counters (read over a fresh channel) prove which calls reached it.
 * Backoff is fully jittered, so these tests assert counts and content, never durations.
 */
@Timeout(value = 60, threadMode = Timeout.ThreadMode.SEPARATE_THREAD)
class RetryAndResumeTest {

    private static final Duration TIMEOUT = Duration.ofSeconds(30);
    private static final Pattern TEXT = Pattern.compile("\"text\":\"([^\"]*)\"");

    private ConfigurableApplicationContext context;
    private MeterRegistry meters;
    private StreamTimeouts timeouts;
    private InProcessSimulatorConfig simulator;
    private FaultInjectingInterceptor faults;

    private void start(String... args) {
        context = new SpringApplicationBuilder(GatewayApplication.class, InProcessSimulatorConfig.class)
                .properties("server.port=0")
                .run(args);
        meters = context.getBean(MeterRegistry.class);
        timeouts = context.getBean(StreamTimeouts.class);
        simulator = context.getBean(InProcessSimulatorConfig.class);
        faults = simulator.faults();
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

    private SimulatorStats stats() {
        return simulator.stats();
    }

    private double retries(String reason) {
        return meters.get("retries_total").tag("reason", reason).counter().count();
    }

    private double exhausted() {
        return meters.get("retry_exhausted_total").counter().count();
    }

    private double activeStreams() {
        return meters.get("active_streams").gauge().value();
    }

    private static List<String> texts(List<SseEvent> events) {
        return events.stream()
                .filter(e -> "chunk".equals(e.name()))
                .map(e -> {
                    Matcher matcher = TEXT.matcher(e.data());
                    assertThat(matcher.find()).as(e.data()).isTrue();
                    return matcher.group(1);
                })
                .toList();
    }

    @Test
    void cutStreamResumesWithoutDuplicateOrGap() {
        start("--lab.retry.initial-backoff=50ms", "--lab.retry.max-backoff=200ms");
        SimulatorStats before = stats();
        faults.cutNextCalls(2, 3);

        SseClient client = SseClient.open(url("/stream?chunks=10&intervalMs=10"));
        await().atMost(TIMEOUT).until(client::isFinished);

        assertThat(chunkIds(client.events())).containsExactly("0", "1", "2", "3", "4", "5", "6", "7", "8", "9");
        SseEvent terminal = assertSingleTerminal(client.events());
        assertThat(terminal.name()).isEqualTo("complete");
        assertThat(terminal.data()).contains("\"chunks\":10");
        String requestId = client.response().join().headers().firstValue("X-Request-Id").orElseThrow();
        await().atMost(TIMEOUT).untilAsserted(() -> {
            SimulatorStats now = stats();
            assertThat(now.getStartedTotal()).isEqualTo(before.getStartedTotal() + 3);
            assertThat(now.getCancelledTotal()).isEqualTo(before.getCancelledTotal() + 2);
            assertThat(now.getCompletedTotal()).isEqualTo(before.getCompletedTotal() + 1);
            assertThat(now.getActiveGenerations()).isZero();
            assertThat(retries("resume")).isEqualTo(2);
            assertThat(retries("restart")).isZero();
            assertThat(exhausted()).isZero();
            assertThat(activeStreams()).isZero();
            assertThat(timeouts.pendingTimers()).isZero();
        });
        // Read last: the reference call itself is counted by the simulator. Byte for byte what an
        // uninterrupted call returns for the same request id and seed.
        assertThat(texts(client.events())).isEqualTo(simulator.referenceTexts(requestId, 0, 10));
    }

    @Test
    void refusedCallsAreRetriedAndTheStreamCompletes() {
        start();
        SimulatorStats before = stats();
        faults.failNextCalls(2);

        SseClient client = SseClient.open(url("/stream?chunks=5&intervalMs=10"));
        await().atMost(TIMEOUT).until(client::isFinished);

        assertThat(chunkIds(client.events())).containsExactly("0", "1", "2", "3", "4");
        assertThat(assertSingleTerminal(client.events()).name()).isEqualTo("complete");
        await().atMost(TIMEOUT).untilAsserted(() -> {
            // The refused calls never reached the simulator.
            assertThat(stats().getStartedTotal()).isEqualTo(before.getStartedTotal() + 1);
            assertThat(stats().getActiveGenerations()).isZero();
            assertThat(retries("restart")).isEqualTo(2);
            assertThat(retries("resume")).isZero();
            assertThat(exhausted()).isZero();
            assertThat(activeStreams()).isZero();
            assertThat(timeouts.pendingTimers()).isZero();
        });
        assertThat(faults.interceptedCalls()).isEqualTo(3);
    }

    @Test
    void simulatorUnavailableFaultExhaustsRetries() {
        start();
        SimulatorStats before = stats();

        SseClient client = SseClient.open(url("/stream?chunks=10&intervalMs=10&fault=UNAVAILABLE&faultAtSeq=3"));
        await().atMost(TIMEOUT).until(client::isFinished);

        // Each resumed call fires the fault again at seq 3; the client sees every chunk exactly once.
        assertThat(chunkIds(client.events())).containsExactly("0", "1", "2");
        SseEvent terminal = assertSingleTerminal(client.events());
        assertThat(terminal.name()).isEqualTo("error");
        assertThat(terminal.data())
                .contains("\"code\":\"UNAVAILABLE\"")
                .contains("injected FAULT_TYPE_UNAVAILABLE at seq 3");
        await().atMost(TIMEOUT).untilAsserted(() -> {
            SimulatorStats now = stats();
            assertThat(now.getStartedTotal()).isEqualTo(before.getStartedTotal() + 4);
            assertThat(now.getFaultedTotal()).isEqualTo(before.getFaultedTotal() + 4);
            assertThat(now.getCancelledTotal()).isEqualTo(before.getCancelledTotal());
            assertThat(now.getActiveGenerations()).isZero();
            assertThat(retries("resume")).isEqualTo(3);
            assertThat(exhausted()).isEqualTo(1);
            assertThat(activeStreams()).isZero();
            assertThat(timeouts.pendingTimers()).isZero();
        });
    }

    @Test
    void refusedCallsExhaustTheConfiguredAttempts() {
        start("--lab.retry.max-attempts=2");
        SimulatorStats before = stats();
        faults.failNextCalls(100);

        SseClient client = SseClient.open(url("/stream?chunks=5&intervalMs=10"));
        await().atMost(TIMEOUT).until(client::isFinished);

        assertThat(chunkIds(client.events())).isEmpty();
        SseEvent terminal = assertSingleTerminal(client.events());
        assertThat(terminal.name()).isEqualTo("error");
        assertThat(terminal.data())
                .contains("\"code\":\"UNAVAILABLE\"")
                .contains(FaultInjectingInterceptor.REFUSED);
        await().atMost(TIMEOUT).untilAsserted(() -> {
            assertThat(stats().getStartedTotal()).isEqualTo(before.getStartedTotal());
            assertThat(retries("restart")).isEqualTo(2);
            assertThat(exhausted()).isEqualTo(1);
            assertThat(activeStreams()).isZero();
            assertThat(timeouts.pendingTimers()).isZero();
        });
        assertThat(faults.interceptedCalls()).as("1 call + 2 retries").isEqualTo(3);
    }

    @Test
    void internalFaultIsNotRetried() {
        start();
        SimulatorStats before = stats();

        SseClient client = SseClient.open(url("/stream?chunks=10&intervalMs=10&fault=INTERNAL&faultAtSeq=0"));
        await().atMost(TIMEOUT).until(client::isFinished);

        assertThat(chunkIds(client.events())).isEmpty();
        SseEvent terminal = assertSingleTerminal(client.events());
        assertThat(terminal.name()).isEqualTo("error");
        assertThat(terminal.data()).contains("\"code\":\"INTERNAL\"");
        await().atMost(TIMEOUT).untilAsserted(() -> {
            assertThat(stats().getStartedTotal()).isEqualTo(before.getStartedTotal() + 1);
            assertThat(retries("restart")).isZero();
            assertThat(retries("resume")).isZero();
            assertThat(exhausted()).isZero();
            assertThat(activeStreams()).isZero();
            assertThat(timeouts.pendingTimers()).isZero();
        });
    }

    @Test
    void retryMetricsArePreRegisteredAndExposed() throws Exception {
        start();

        assertThat(retries("restart")).isZero();
        assertThat(retries("resume")).isZero();
        assertThat(exhausted()).isZero();
        assertThat(SseClient.get(url("/actuator/metrics/retries_total")).statusCode()).isEqualTo(200);
        assertThat(SseClient.get(url("/actuator/metrics/retry_exhausted_total")).statusCode()).isEqualTo(200);
    }
}
