package io.github.cemharput.streamlab.bench;

import static io.github.cemharput.streamlab.bench.testsupport.FakeActuatorServer.metric;
import static org.assertj.core.api.Assertions.assertThat;

import io.github.cemharput.streamlab.bench.GatewayApi.IdleResult;
import io.github.cemharput.streamlab.bench.GatewayApi.ModeCheck;
import io.github.cemharput.streamlab.bench.testsupport.FakeActuatorServer;
import io.github.cemharput.streamlab.bench.testsupport.FakeTicker;
import java.net.ServerSocket;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.Timeout.ThreadMode;

@Timeout(value = 60, unit = TimeUnit.SECONDS, threadMode = ThreadMode.SEPARATE_THREAD)
class GatewayProbeTest {

    private FakeActuatorServer server;
    private FakeTicker ticker;
    private GatewayProbe probe;

    @BeforeEach
    void setUp() throws Exception {
        server = new FakeActuatorServer();
        ticker = new FakeTicker();
        probe = new GatewayProbe(server.port(), ticker);
    }

    @AfterEach
    void tearDown() {
        server.close();
    }

    @Test
    void readinessNeedsHttp200AndStatusUp() {
        assertThat(probe.readiness()).isFalse(); // 404
        server.reply(GatewayProbe.READINESS, 200, "{\"status\":\"UP\"}");
        assertThat(probe.readiness()).isTrue();
        server.reply(GatewayProbe.READINESS, 503, "{\"status\":\"OUT_OF_SERVICE\"}");
        assertThat(probe.readiness()).isFalse();
        server.reply(GatewayProbe.READINESS, 200, "{\"status\":\"DOWN\"}");
        assertThat(probe.readiness()).isFalse();
        server.reply(GatewayProbe.READINESS, 200, "not json");
        assertThat(probe.readiness()).isFalse();
    }

    @Test
    void readinessIsFalseWhenNothingListens() throws Exception {
        int port;
        try (ServerSocket s = new ServerSocket(0)) {
            port = s.getLocalPort();
        }
        assertThat(new GatewayProbe(port, ticker).readiness()).isFalse();
    }

    @Test
    void connectionModeMustBeExactlyTheExpectedTagWithValueOne() {
        server.reply(GatewayProbe.CONNECTION_MODE, 200, metric("connection_mode", 1.0, "mode", "per_request"));
        assertThat(probe.verifyConnectionMode("per_request").ok()).isTrue();
        ModeCheck wrong = probe.verifyConnectionMode("shared");
        assertThat(wrong.ok()).isFalse();
        assertThat(wrong.observedTags()).containsExactly("per_request");

        server.reply(GatewayProbe.CONNECTION_MODE, 200, metric("connection_mode", 1.0, "mode", "shared", "per_request"));
        assertThat(probe.verifyConnectionMode("shared").ok()).isFalse();

        server.reply(GatewayProbe.CONNECTION_MODE, 200, metric("connection_mode", 0.0, "mode", "shared"));
        assertThat(probe.verifyConnectionMode("shared").ok()).isFalse();

        server.reply(GatewayProbe.CONNECTION_MODE, 404, "{}");
        assertThat(probe.verifyConnectionMode("shared").message()).contains("404");
    }

    @Test
    void snapshotRecordsEveryPathIncludingNotFoundAndKeepsRawBodies() {
        server.reply(GatewayProbe.READINESS, 200, "{\"status\":\"UP\"}");
        server.reply(GatewayProbe.ACTIVE_STREAMS, 200, metric("active_streams", 3.0, null));
        server.reply(GatewayProbe.rejectedPath("limit"), 200, metric("rejected_total", 5.0, null));

        List<Snapshot.Entry> entries = probe.snapshotEntries();

        assertThat(entries.stream().map(Snapshot.Entry::path).toList()).isEqualTo(GatewayProbe.snapshotPaths());
        assertThat(entries).hasSize(GatewayProbe.snapshotPaths().size());
        Snapshot snapshot = new Snapshot(java.time.Instant.EPOCH, entries, null, "n/a");
        assertThat(snapshot.metric(GatewayProbe.ACTIVE_STREAMS)).isEqualTo(3.0);
        assertThat(snapshot.metric(GatewayProbe.rejectedPath("limit"))).isEqualTo(5.0);
        Snapshot.Entry missing = entries.stream().filter(e -> e.path().equals(GatewayProbe.QUEUE_CHUNKS)).findFirst()
                .orElseThrow();
        assertThat(missing.status()).isEqualTo(404);
        assertThat(missing.value()).isNull();
        assertThat(entries.get(0).value()).isEqualTo("UP");
        assertThat(entries.get(1).raw()).contains("active_streams");
        assertThat(snapshot.toMap().get("simulator").toString()).contains("n/a");
    }

    @Test
    void awaitIdleSucceedsWhenAllSeriesAreZero() throws Exception {
        server.reply(GatewayProbe.ACTIVE_STREAMS, 200, metric("active_streams", 0.0, null));
        server.reply(GatewayProbe.QUEUE_CHUNKS, 200, metric("queue_depth", 0.0, "queue", "chunks"));
        server.reply(GatewayProbe.QUEUE_ADMISSION, 200, metric("queue_depth", 0.0, "queue", "admission"));
        IdleResult r = probe.awaitIdle(Duration.ofSeconds(10));
        assertThat(r.idle()).isTrue();
        assertThat(ticker.sleepCount()).isZero();
    }

    @Test
    void awaitIdleIsBoundedAndReportsTheLastObservation() throws Exception {
        server.reply(GatewayProbe.ACTIVE_STREAMS, 200, metric("active_streams", 2.0, null));
        server.reply(GatewayProbe.QUEUE_CHUNKS, 200, metric("queue_depth", 0.0, "queue", "chunks"));
        server.reply(GatewayProbe.QUEUE_ADMISSION, 200, metric("queue_depth", 0.0, "queue", "admission"));
        IdleResult r = probe.awaitIdle(Duration.ofSeconds(10));
        assertThat(r.idle()).isFalse();
        assertThat(r.waitedMillis()).isBetween(10_000L, 10_500L);
        assertThat(r.message()).contains("active_streams=2.0");
    }

    @Test
    void metricValueIgnoresOtherStatisticsOrder() {
        var body = io.github.cemharput.streamlab.bench.json.Json.parseObject(
                "{\"measurements\":[{\"statistic\":\"MAX\",\"value\":9},{\"statistic\":\"COUNT\",\"value\":4}]}");
        assertThat(GatewayProbe.metricValue(body)).isEqualTo(4.0);
    }
}
