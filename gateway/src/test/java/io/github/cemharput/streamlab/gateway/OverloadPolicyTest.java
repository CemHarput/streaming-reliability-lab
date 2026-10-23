package io.github.cemharput.streamlab.gateway;

import static io.github.cemharput.streamlab.testsupport.SseAssertions.assertSingleTerminal;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import io.github.cemharput.streamlab.testsupport.InProcessSimulatorConfig;
import io.github.cemharput.streamlab.testsupport.SseClient;
import io.github.cemharput.streamlab.testsupport.SseClient.SseEvent;
import io.github.cemharput.streamlab.tokenstream.v1.GetStatsRequest;
import io.github.cemharput.streamlab.tokenstream.v1.SimulatorAdminServiceGrpc;
import io.github.cemharput.streamlab.tokenstream.v1.SimulatorStats;
import io.grpc.ManagedChannel;
import io.grpc.inprocess.InProcessChannelBuilder;
import io.micrometer.core.instrument.MeterRegistry;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.context.ConfigurableApplicationContext;

/** Overload policies and metrics end to end, on a manually built context (see GracefulShutdownTest). */
@Timeout(value = 60, threadMode = Timeout.ThreadMode.SEPARATE_THREAD)
class OverloadPolicyTest {

    private static final Duration TIMEOUT = Duration.ofSeconds(15);
    /** Long enough to still be running when the test disconnects it. */
    private static final String LONG_STREAM = "/stream?chunks=100000&intervalMs=20";

    private ConfigurableApplicationContext context;
    private MeterRegistry meters;

    private void start(String... args) {
        context = new SpringApplicationBuilder(GatewayApplication.class, InProcessSimulatorConfig.class)
                .properties("server.port=0")
                .run(args);
        meters = context.getBean(MeterRegistry.class);
    }

    private String url(String path) {
        return "http://localhost:" + context.getEnvironment().getProperty("local.server.port", Integer.class) + path;
    }

    private double gauge(String name, String queue) {
        var search = meters.get(name);
        if (queue != null) {
            search = search.tag("queue", queue);
        }
        return search.gauge().value();
    }

    private double rejected(String reason) {
        return meters.get("rejected_total").tag("reason", reason).counter().count();
    }

    /** Reads the simulator's own counters, to prove what did (not) reach the upstream. */
    private SimulatorStats simulatorStats() {
        ManagedChannel channel = InProcessChannelBuilder
                .forName(context.getBean(InProcessSimulatorConfig.class).serverName()).build();
        try {
            return SimulatorAdminServiceGrpc.newBlockingStub(channel).getStats(GetStatsRequest.getDefaultInstance());
        } finally {
            channel.shutdownNow();
        }
    }

    private SseClient openAndHold() {
        SseClient holder = SseClient.open(url(LONG_STREAM));
        await().atMost(TIMEOUT).until(() -> !holder.events().isEmpty());
        await().atMost(TIMEOUT).until(() -> gauge("active_streams", null) == 1);
        return holder;
    }

    @AfterEach
    void tearDown() {
        if (context != null) {
            context.close();
        }
    }

    @Test
    void rejectPolicyAnswers503AndCountsRejection() throws Exception {
        start("--lab.limits.max-active-streams=1");
        SseClient holder = openAndHold();
        try {
            assertThat(rejected("limit")).isZero();
            long startedBefore = simulatorStats().getStartedTotal();

            HttpResponse<String> rejectedResponse = SseClient.get(url("/stream?chunks=3"));

            assertThat(rejectedResponse.statusCode()).isEqualTo(503);
            // AC1: a rejected request never reaches the upstream.
            assertThat(simulatorStats().getStartedTotal()).isEqualTo(startedBefore);
            assertThat(rejectedResponse.headers().firstValue("Retry-After")).contains("1");
            assertThat(rejectedResponse.headers().firstValue("Content-Type").orElse("")).doesNotContain("text/event-stream");
            assertThat(rejected("limit")).isEqualTo(1);
            assertThat(gauge("active_streams", null)).isEqualTo(1);
            assertThat(gauge("queue_depth", "admission")).isZero();
            assertThat(SseClient.get(url("/actuator/metrics/rejected_total")).statusCode()).isEqualTo(200);
        } finally {
            holder.disconnect();
        }

        await().atMost(TIMEOUT).until(() -> gauge("active_streams", null) == 0);
        SseClient next = SseClient.open(url("/stream?chunks=3&intervalMs=10"));
        await().atMost(TIMEOUT).until(next::isFinished);
        assertThat(next.response().get().statusCode()).isEqualTo(200);
        assertThat(assertSingleTerminal(next.events()).name()).isEqualTo("complete");
        await().atMost(TIMEOUT).until(() -> gauge("active_streams", null) == 0);
        assertThat(gauge("queue_depth", "chunks")).isZero();
        assertThat(rejected("limit")).isEqualTo(1);
    }

    @Test
    void waitPolicyAdmitsWhenSlotFrees() throws Exception {
        start("--lab.limits.max-active-streams=1", "--lab.limits.overload-policy=wait",
                "--lab.limits.queue-wait=30s");
        SseClient holder = openAndHold();
        SseClient waiter;
        try {
            waiter = SseClient.open(url("/stream?chunks=3&intervalMs=10"));
            await().atMost(TIMEOUT).until(() -> gauge("queue_depth", "admission") == 1);
            assertThat(waiter.response()).isNotDone();
        } finally {
            holder.disconnect();
        }

        await().atMost(TIMEOUT).until(waiter::isFinished);
        assertThat(waiter.response().get().statusCode()).isEqualTo(200);
        SseEvent terminal = assertSingleTerminal(waiter.events());
        assertThat(terminal.name()).isEqualTo("complete");
        await().atMost(TIMEOUT).until(() -> gauge("active_streams", null) == 0);
        assertThat(gauge("queue_depth", "admission")).isZero();
        assertThat(rejected("limit")).isZero();
        assertThat(rejected("queue_timeout")).isZero();
    }

    @Test
    void waitPolicyTimesOutWith503() throws Exception {
        start("--lab.limits.max-active-streams=1", "--lab.limits.overload-policy=wait",
                "--lab.limits.queue-wait=4s");
        SseClient holder = openAndHold();
        try {
            long startedBefore = simulatorStats().getStartedTotal();
            long startedAt = System.nanoTime();
            // A generous wait leaves room for a slow runner to observe the waiting request.
            SseClient waiter = SseClient.open(url("/stream?chunks=3"));
            await().atMost(TIMEOUT).until(() -> gauge("queue_depth", "admission") == 1);

            HttpResponse<?> response = waiter.response().get();
            Duration waited = Duration.ofNanos(System.nanoTime() - startedAt);

            assertThat(response.statusCode()).isEqualTo(503);
            assertThat(response.headers().firstValue("Retry-After")).isPresent();
            assertThat(waited).isGreaterThanOrEqualTo(Duration.ofSeconds(4));
            assertThat(simulatorStats().getStartedTotal()).isEqualTo(startedBefore);
            assertThat(rejected("queue_timeout")).isEqualTo(1);
            assertThat(rejected("limit")).isZero();
            await().atMost(TIMEOUT).until(() -> gauge("queue_depth", "admission") == 0);
            assertThat(gauge("active_streams", null)).isEqualTo(1);
        } finally {
            holder.disconnect();
        }
    }

    @Test
    void shutdownReleasesWaitingRequests() {
        start("--lab.limits.max-active-streams=1", "--lab.limits.overload-policy=wait",
                "--lab.limits.queue-wait=30s", "--lab.shutdown.drain-timeout=2s");
        SseClient hanging = SseClient.open(url("/stream?chunks=10&intervalMs=10&fault=HANG&faultAtSeq=2"));
        await().atMost(TIMEOUT).until(() -> hanging.events().size() >= 2);
        SseClient waiter = SseClient.open(url("/stream?chunks=3"));
        await().atMost(TIMEOUT).until(() -> gauge("queue_depth", "admission") == 1);

        CompletableFuture<Duration> closing = CompletableFuture.supplyAsync(() -> {
            long startedAt = System.nanoTime();
            context.close();
            return Duration.ofNanos(System.nanoTime() - startedAt);
        });

        // The waiter is answered when shutdown starts, not after its 30s queue-wait.
        HttpResponse<?> response = waiter.response().orTimeout(10, java.util.concurrent.TimeUnit.SECONDS).join();
        assertThat(response.statusCode()).isEqualTo(503);
        assertThat(rejected("shutdown")).isEqualTo(1);
        assertThat(rejected("queue_timeout")).isZero();

        await().atMost(TIMEOUT).until(() -> closing.isDone() && hanging.isFinished());
        SseEvent terminal = assertSingleTerminal(hanging.events());
        assertThat(terminal.name()).isEqualTo("error");
        assertThat(terminal.data()).contains("\"code\":\"CANCELLED\"");
        assertThat(closing.join()).isLessThan(Duration.ofSeconds(11));
    }
}
