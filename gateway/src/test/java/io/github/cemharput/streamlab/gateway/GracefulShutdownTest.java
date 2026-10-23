package io.github.cemharput.streamlab.gateway;

import static io.github.cemharput.streamlab.testsupport.SseAssertions.assertSingleTerminal;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import io.github.cemharput.streamlab.gateway.stream.ActiveStreamRegistry;
import io.github.cemharput.streamlab.gateway.upstream.SharedChannelProvider;
import io.github.cemharput.streamlab.testsupport.InProcessSimulatorConfig;
import io.github.cemharput.streamlab.testsupport.SseClient;
import io.github.cemharput.streamlab.testsupport.SseClient.SseEvent;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.context.ConfigurableApplicationContext;

class GracefulShutdownTest {

    private ConfigurableApplicationContext context;

    private ConfigurableApplicationContext start(String drainTimeout) {
        context = new SpringApplicationBuilder(GatewayApplication.class, InProcessSimulatorConfig.class)
                .properties("server.port=0")
                .run("--lab.shutdown.drain-timeout=" + drainTimeout);
        return context;
    }

    private String base() {
        return "http://localhost:" + context.getEnvironment().getProperty("local.server.port", Integer.class);
    }

    @AfterEach
    void tearDown() {
        if (context != null) {
            context.close();
        }
    }

    @Test
    void activeStreamDrains() {
        start("5s");
        SharedChannelProvider provider = context.getBean(SharedChannelProvider.class);
        ActiveStreamRegistry registry = context.getBean(ActiveStreamRegistry.class);
        SseClient client = SseClient.open(base() + "/stream?chunks=20&intervalMs=50");
        await().atMost(Duration.ofSeconds(5)).until(() -> !client.events().isEmpty());
        // 20 chunks take about a second, so the stream is still active when shutdown starts.
        assertThat(registry.activeCount()).isEqualTo(1);

        CompletableFuture<Void> closing = CompletableFuture.runAsync(context::close);

        await().atMost(Duration.ofSeconds(10)).until(() -> closing.isDone() && client.isFinished());
        SseEvent terminal = assertSingleTerminal(client.events());
        assertThat(terminal.name()).isEqualTo("complete");
        assertThat(client.events().stream().filter(e -> "chunk".equals(e.name()))).hasSize(20);
        assertThat(provider.isTerminated()).isTrue();
    }

    @Test
    void newRequestsRejectedAndHangingStreamCancelled() throws Exception {
        start("3s");
        SharedChannelProvider provider = context.getBean(SharedChannelProvider.class);
        ActiveStreamRegistry registry = context.getBean(ActiveStreamRegistry.class);
        String base = base();
        SseClient hanging = SseClient.open(base + "/stream?chunks=10&intervalMs=10&fault=HANG&faultAtSeq=2");
        await().atMost(Duration.ofSeconds(5)).until(() -> hanging.events().size() >= 2);

        // The future carries the duration, or the exception if close() fails.
        CompletableFuture<Duration> closing = CompletableFuture.supplyAsync(() -> {
            long startedAt = System.nanoTime();
            context.close();
            return Duration.ofNanos(System.nanoTime() - startedAt);
        });
        await().atMost(Duration.ofSeconds(5)).until(() -> !registry.isAccepting());

        HttpResponse<String> rejected = SseClient.get(base + "/stream?chunks=3");
        assertThat(rejected.statusCode()).isEqualTo(503);
        assertThat(rejected.headers().firstValue("Retry-After")).isPresent();
        assertThat(rejected.headers().firstValue("Content-Type").orElse("")).doesNotContain("text/event-stream");

        await().atMost(Duration.ofSeconds(15)).until(() -> closing.isDone() && hanging.isFinished());
        SseEvent terminal = assertSingleTerminal(hanging.events());
        assertThat(terminal.name()).isEqualTo("error");
        assertThat(terminal.data())
                .contains("\"code\":\"CANCELLED\"")
                .contains("\"message\":\"gateway shutting down\"");
        assertThat(provider.isTerminated()).isTrue();

        // The hanging stream holds the drain for its full 3s, then is cancelled promptly.
        // Worst case the code allows is drain + 2s forced-cancel wait + channel termination (5s) + 2s.
        Duration closeTook = closing.join();
        assertThat(closeTook).isGreaterThanOrEqualTo(Duration.ofMillis(2_900)).isLessThan(Duration.ofSeconds(7));
    }
}
