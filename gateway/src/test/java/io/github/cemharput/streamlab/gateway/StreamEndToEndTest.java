package io.github.cemharput.streamlab.gateway;

import static io.github.cemharput.streamlab.testsupport.SseAssertions.assertSingleTerminal;
import static io.github.cemharput.streamlab.testsupport.SseAssertions.chunkIds;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import io.github.cemharput.streamlab.gateway.stream.ActiveStreamRegistry;
import io.github.cemharput.streamlab.testsupport.InProcessSimulatorConfig;
import io.github.cemharput.streamlab.testsupport.SseClient;
import io.github.cemharput.streamlab.testsupport.SseClient.SseEvent;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Import;

@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT,
        properties = "management.endpoint.health.show-components=always")
@Import(InProcessSimulatorConfig.class)
class StreamEndToEndTest {

    @Value("${local.server.port}")
    int port;

    @Autowired
    ActiveStreamRegistry registry;

    private String url(String path) {
        return "http://localhost:" + port + path;
    }

    private SseClient stream(String query) {
        SseClient client = SseClient.open(url("/stream?" + query));
        await().atMost(Duration.ofSeconds(10)).until(client::isFinished);
        return client;
    }

    @Test
    void successStream() throws Exception {
        SseClient client = stream("chunks=5&intervalMs=10");

        HttpResponse<?> response = client.response().get();
        assertThat(response.statusCode()).isEqualTo(200);
        assertThat(response.headers().firstValue("Content-Type").orElseThrow()).startsWith("text/event-stream");
        String requestId = response.headers().firstValue("X-Request-Id").orElseThrow();
        assertThat(java.util.UUID.fromString(requestId)).isNotNull();

        List<SseEvent> events = client.events();
        assertThat(chunkIds(events)).containsExactly("0", "1", "2", "3", "4");
        events.stream().filter(e -> "chunk".equals(e.name()))
                .forEach(e -> assertThat(e.data()).contains("\"requestId\":\"" + requestId + "\""));
        SseEvent terminal = assertSingleTerminal(events);
        assertThat(terminal.name()).isEqualTo("complete");
        assertThat(terminal.data()).contains("\"chunks\":5").contains("\"requestId\":\"" + requestId + "\"");
    }

    @Test
    void unavailableMidStream() {
        SseClient client = stream("chunks=10&intervalMs=10&fault=UNAVAILABLE&faultAtSeq=3");

        assertThat(chunkIds(client.events())).containsExactly("0", "1", "2");
        SseEvent terminal = assertSingleTerminal(client.events());
        assertThat(terminal.name()).isEqualTo("error");
        assertThat(terminal.data())
                .contains("\"code\":\"UNAVAILABLE\"")
                .contains("injected FAULT_TYPE_UNAVAILABLE at seq 3");
    }

    @Test
    void internalAtFirstChunk() {
        SseClient client = stream("chunks=10&intervalMs=10&fault=INTERNAL&faultAtSeq=0");

        assertThat(chunkIds(client.events())).isEmpty();
        SseEvent terminal = assertSingleTerminal(client.events());
        assertThat(terminal.name()).isEqualTo("error");
        assertThat(terminal.data()).contains("\"code\":\"INTERNAL\"");
    }

    @Test
    void invalidChunkCount() {
        SseClient client = stream("chunks=0");

        assertThat(chunkIds(client.events())).isEmpty();
        SseEvent terminal = assertSingleTerminal(client.events());
        assertThat(terminal.name()).isEqualTo("error");
        assertThat(terminal.data())
                .contains("\"code\":\"INVALID_ARGUMENT\"")
                .contains("chunk_count");
    }

    @Test
    void badParameterRejected() throws Exception {
        assertThat(SseClient.get(url("/stream?fault=FOO")).statusCode()).isEqualTo(400);
        assertThat(SseClient.get(url("/stream?chunks=abc")).statusCode()).isEqualTo(400);
        // A real SSE client sends Accept: text/event-stream; the error must still be a 400.
        assertThat(SseClient.getAcceptingEventStream(url("/stream?fault=FOO")).statusCode()).isEqualTo(400);
    }

    @Test
    void negativeFaultAtSeqRejectedInsteadOfSilentlyIgnored() throws Exception {
        assertThat(SseClient.get(url("/stream?fault=UNAVAILABLE&faultAtSeq=-1")).statusCode()).isEqualTo(400);
        assertThat(SseClient.getAcceptingEventStream(url("/stream?fault=UNAVAILABLE&faultAtSeq=-1")).statusCode())
                .isEqualTo(400);
    }

    @Test
    void clientDisconnectFreesTheStreamSlot() {
        SseClient client = SseClient.open(url("/stream?chunks=1000&intervalMs=20"));
        await().atMost(Duration.ofSeconds(10)).until(() -> !client.events().isEmpty());
        assertThat(registry.activeCount()).isPositive();

        client.disconnect();

        await().atMost(Duration.ofSeconds(10)).until(() -> registry.activeCount() == 0);
    }

    @Test
    void readinessUp() {
        await().atMost(Duration.ofSeconds(10)).untilAsserted(() -> {
            HttpResponse<String> response = SseClient.get(url("/actuator/health/readiness"));
            assertThat(response.statusCode()).isEqualTo(200);
            assertThat(response.body()).contains("upstreamChannel");
        });
    }
}
