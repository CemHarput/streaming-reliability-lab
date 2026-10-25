package io.github.cemharput.streamlab.loadgen;

import static io.github.cemharput.streamlab.loadgen.testsupport.SseTestServer.chunk;
import static io.github.cemharput.streamlab.loadgen.testsupport.SseTestServer.complete;
import static io.github.cemharput.streamlab.loadgen.testsupport.SseTestServer.startSse;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import io.github.cemharput.streamlab.loadgen.json.FlatJson;
import io.github.cemharput.streamlab.loadgen.testsupport.SseTestServer;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Base64;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import org.HdrHistogram.Histogram;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.Timeout.ThreadMode;
import org.junit.jupiter.api.io.TempDir;

@Timeout(value = 90, unit = TimeUnit.SECONDS, threadMode = ThreadMode.SEPARATE_THREAD)
class LoadRunTest {

    @TempDir
    Path dir;

    private final List<CountDownLatch> gates = new CopyOnWriteArrayList<>();
    private ExecutorService runner;
    private SseTestServer server;
    private Path output;

    @BeforeEach
    void setUp() throws IOException {
        runner = Executors.newCachedThreadPool();
        server = new SseTestServer();
        output = dir.resolve("out.jsonl");
    }

    @AfterEach
    void tearDown() {
        gates.forEach(CountDownLatch::countDown);
        runner.shutdownNow();
        server.close();
    }

    private CountDownLatch gate(int count) {
        CountDownLatch latch = new CountDownLatch(count);
        gates.add(latch);
        return latch;
    }

    private static void awaitLatch(CountDownLatch latch) throws InterruptedException {
        latch.await(30, TimeUnit.SECONDS);
    }

    private Options options(String... extra) throws UsageException {
        List<String> args = new ArrayList<>(List.of("--target", server.uri().toString(), "--output",
                output.toString(), "--seed", "1", "--chunks", "3", "--interval-ms", "0"));
        args.addAll(List.of(extra));
        return OptionsParser.parse(args.toArray(String[]::new));
    }

    private Future<LoadRun.Result> start(LoadRun run) {
        return runner.submit(run::run);
    }

    private static LoadRun.Result get(Future<LoadRun.Result> future) throws Exception {
        return future.get(60, TimeUnit.SECONDS);
    }

    private List<Map<String, Object>> lines() throws IOException {
        List<Map<String, Object>> result = new ArrayList<>();
        for (String line : Files.readAllLines(output)) {
            result.add(FlatJson.parse(line));
        }
        return result;
    }

    private List<Map<String, Object>> requests() throws IOException {
        return lines().stream().filter(m -> "request".equals(m.get("type"))).toList();
    }

    @Test
    void requestsModeProducesExactlyTheRequestedRecordsWithinTheConcurrencyLimit() throws Exception {
        CountDownLatch firstEight = gate(8);
        server.handler((ex, n) -> {
            if (n < 8) {
                firstEight.countDown();
                awaitLatch(firstEight); // proves that eight streams really are open at the same time
            }
            SseTestServer.fullStream(ex, 3);
        });
        LoadRun run = new LoadRun(options("--concurrency", "8", "--requests", "50"), "test");

        LoadRun.Result result = get(start(run));

        assertThat(server.maxOpen()).isEqualTo(8);
        assertThat(result.stopReason()).isEqualTo("requests");
        assertThat(result.requestsStarted()).isEqualTo(50);
        assertThat(result.recordsWritten()).isEqualTo(50);
        List<Map<String, Object>> all = lines();
        assertThat(all.get(0)).containsEntry("type", "header");
        assertThat(all.get(all.size() - 1)).containsEntry("type", "footer");
        List<Map<String, Object>> reqs = requests();
        assertThat(reqs).hasSize(50);
        Set<Long> indexes = new HashSet<>();
        for (Map<String, Object> r : reqs) {
            indexes.add((Long) r.get("index"));
            assertThat((Long) r.get("worker")).isBetween(0L, 7L);
            assertThat(r).containsEntry("outcome", "COMPLETED").containsEntry("chunks", 3L)
                    .containsEntry("protocolViolations", 0L);
            assertThat((Long) r.get("startOffsetNanos")).isGreaterThanOrEqualTo((Long) r.get("scheduledOffsetNanos"));
            assertThat((Long) r.get("endOffsetNanos")).isGreaterThan((Long) r.get("startOffsetNanos"));
            assertThat((Long) r.get("ttfbNanos")).isLessThanOrEqualTo((Long) r.get("ttfcNanos"));
            assertThat((Long) r.get("ttfcNanos")).isLessThanOrEqualTo((Long) r.get("totalNanos"));
        }
        assertThat(indexes).hasSize(50).allMatch(i -> i >= 0 && i < 50);
        // the parameters of each record are the pure function of (seed, index)
        RequestVariation variation = RequestVariation.from(options("--concurrency", "8", "--requests", "50",
                "--overwrite"));
        for (Map<String, Object> r : reqs) {
            assertThat(r.get("paramSeed")).isEqualTo(variation.forIndex((Long) r.get("index")).seed());
        }
        assertThat(run.summary().count(io.github.cemharput.streamlab.loadgen.client.Outcome.COMPLETED)).isEqualTo(50);

        Map<String, Object> footer = all.get(all.size() - 1);
        assertThat(footer).containsEntry("recordsWritten", 50L).containsEntry("requestsStarted", 50L)
                .containsEntry("stopReason", "requests").containsEntry("interrupted", 0L)
                .containsEntry("interChunkCount", 100L).containsEntry("interChunkHistogramUnit", "ns");
        Histogram decoded = Histogram.decodeFromCompressedByteBuffer(
                ByteBuffer.wrap(Base64.getDecoder().decode((String) footer.get("interChunkHistogram"))), 0);
        assertThat(decoded.getTotalCount()).isEqualTo(100);
    }

    @Test
    void headerAndFooterCarryTheDocumentedFieldsInOrder() throws Exception {
        server.handler((ex, n) -> SseTestServer.fullStream(ex, 3));
        LoadRun run = new LoadRun(options("--requests", "1"), "cmd line here");

        get(start(run));

        List<Map<String, Object>> all = lines();
        assertThat(List.copyOf(all.get(0).keySet())).containsExactly("type", "schemaVersion", "tool", "toolVersion",
                "startedAt", "target", "loadModel", "concurrency", "durationMs", "requests", "rampUpMs", "chunks",
                "intervalMs", "firstDelayMs", "faultAtSeq", "fault", "faultRate", "seed", "connectTimeoutMs",
                "idleTimeoutMs", "drainTimeoutMs", "rejectPauseMs", "httpVersion", "javaVersion", "javaVendor",
                "javaVmName", "osName", "osArch", "osVersion", "availableProcessors", "maxHeapBytes", "timeUnit",
                "clock", "coordinatedOmission", "commandLine");
        assertThat(all.get(0)).containsEntry("loadModel", "closed").containsEntry("durationMs", null)
                .containsEntry("requests", 1L).containsEntry("commandLine", "cmd line here")
                .containsEntry("schemaVersion", 1L).containsEntry("httpVersion", "HTTP/1.1")
                .containsEntry("chunks", "3");
        assertThat(List.copyOf(requests().get(0).keySet())).containsExactly("type", "index", "worker", "paramChunks",
                "paramIntervalMs", "paramFirstDelayMs", "paramFault", "paramFaultAtSeq", "paramSeed",
                "scheduledOffsetNanos", "startOffsetNanos", "endOffsetNanos", "outcome", "status", "retryAfter",
                "requestId", "ttfbNanos", "firstBodyByteNanos", "ttfcNanos", "totalNanos", "chunks", "lastSeq",
                "interChunkMinNanos", "interChunkMaxNanos", "terminal", "errorCode", "errorMessage", "completeChunks",
                "protocolViolations", "timeoutKind", "errorPhase", "errorClass", "errorDetail", "bodyEnd");
        assertThat(List.copyOf(all.get(all.size() - 1).keySet())).containsExactly("type", "endedAt", "elapsedNanos",
                "stopReason", "requestsStarted", "recordsWritten", "interrupted", "interChunkCount",
                "interChunkHistogramUnit", "interChunkHistogram");
    }

    @Test
    void rampStartsWorkersAtLinearlySpacedPlannedOffsets() throws Exception {
        CountDownLatch allFour = gate(4);
        server.handler((ex, n) -> {
            allFour.countDown();
            awaitLatch(allFour); // every worker holds exactly one request until all four have started
            SseTestServer.fullStream(ex, 3);
        });
        LoadRun run = new LoadRun(options("--concurrency", "4", "--requests", "4", "--ramp-up", "400ms"), "test");

        get(start(run));

        List<Map<String, Object>> reqs = new ArrayList<>(requests());
        assertThat(reqs).hasSize(4);
        reqs.sort((a, b) -> Long.compare((Long) a.get("worker"), (Long) b.get("worker")));
        for (int w = 0; w < 4; w++) {
            Map<String, Object> r = reqs.get(w);
            assertThat(r.get("worker")).isEqualTo((long) w);
            assertThat(r.get("scheduledOffsetNanos")).isEqualTo(w * 100_000_000L);
            assertThat((Long) r.get("startOffsetNanos")).isGreaterThanOrEqualTo(w * 100_000_000L);
        }
    }

    @Test
    void durationEndStopsNewRequestsAndDrainsOpenStreams() throws Exception {
        CountDownLatch release = gate(1);
        server.handler((ex, n) -> {
            OutputStream os = startSse(ex);
            chunk(os, 0);
            awaitLatch(release);
            chunk(os, 1);
            complete(os, 2);
        });
        LoadRun run = new LoadRun(options("--concurrency", "3", "--duration", "500ms", "--drain-timeout", "30s"),
                "test");
        Future<LoadRun.Result> future = start(run);

        await().atMost(Duration.ofSeconds(30)).until(run::isStopping);
        assertThat(future.isDone()).as("run waits for the open streams").isFalse();
        release.countDown();
        LoadRun.Result result = get(future);

        assertThat(result.stopReason()).isEqualTo("duration");
        assertThat(result.requestsStarted()).isEqualTo(3);
        assertThat(server.requestCount()).as("no request starts after the stop").isEqualTo(3);
        List<Map<String, Object>> reqs = requests();
        assertThat(reqs).hasSize(3).allSatisfy(r -> assertThat(r).containsEntry("outcome", "COMPLETED")
                .containsEntry("chunks", 2L));
        assertThat(result.elapsedNanos()).isGreaterThanOrEqualTo(Duration.ofMillis(500).toNanos());
    }

    @Test
    void openStreamsAreInterruptedWhenTheDrainTimeoutExpires() throws Exception {
        CountDownLatch never = gate(1);
        server.handler((ex, n) -> {
            OutputStream os = startSse(ex);
            chunk(os, 0);
            awaitLatch(never);
        });
        LoadRun run = new LoadRun(options("--concurrency", "2", "--duration", "500ms", "--drain-timeout", "300ms"),
                "test");

        LoadRun.Result result = get(start(run));

        assertThat(result.stopReason()).isEqualTo("duration");
        assertThat(result.elapsedNanos()).isLessThan(Duration.ofSeconds(20).toNanos());
        List<Map<String, Object>> reqs = requests();
        assertThat(reqs).hasSize(2).allSatisfy(r -> assertThat(r).containsEntry("outcome", "INTERRUPTED")
                .containsEntry("chunks", 1L).containsEntry("terminal", "none").containsEntry("bodyEnd", "interrupted"));
        List<Map<String, Object>> all = lines();
        assertThat(all.get(all.size() - 1)).containsEntry("interrupted", 2L).containsEntry("recordsWritten", 2L);
    }

    @Test
    void stopSignalEndsTheRunLikeADurationEnd() throws Exception {
        CountDownLatch never = gate(1);
        server.handler((ex, n) -> {
            OutputStream os = startSse(ex);
            chunk(os, 0);
            awaitLatch(never);
        });
        LoadRun run = new LoadRun(options("--concurrency", "2", "--duration", "60m", "--drain-timeout", "300ms"),
                "test");
        Future<LoadRun.Result> future = start(run);

        await().atMost(Duration.ofSeconds(30)).until(() -> server.openCount() == 2);
        run.stop("signal");
        LoadRun.Result result = get(future);

        assertThat(result.stopReason()).isEqualTo("signal");
        assertThat(requests()).hasSize(2).allSatisfy(r -> assertThat(r).containsEntry("outcome", "INTERRUPTED"));
        assertThat(run.awaitFinished(1, TimeUnit.SECONDS)).isTrue();
    }

    @Test
    void requestBudgetEndsTheWaitOfWorkersStillInTheRampUp() throws Exception {
        server.handler((ex, n) -> SseTestServer.fullStream(ex, 3));
        // 4 workers over a 10 minute ramp: worker 0 uses the single request, the others must not wait for their slot
        LoadRun run = new LoadRun(options("--concurrency", "4", "--requests", "1", "--ramp-up", "10m"), "test");

        LoadRun.Result result = get(start(run));

        assertThat(result.stopReason()).isEqualTo("requests");
        assertThat(result.recordsWritten()).isEqualTo(1);
        assertThat(result.elapsedNanos()).isLessThan(Duration.ofSeconds(30).toNanos());
    }

    @Test
    void rejectPauseEndsEarlyWhenTheRunIsStopped() throws Exception {
        server.handler((ex, n) -> {
            ex.getResponseHeaders().add("Retry-After", "1");
            ex.sendResponseHeaders(503, -1);
        });
        LoadRun run = new LoadRun(options("--concurrency", "1", "--duration", "500ms", "--reject-pause", "10m"),
                "test");

        LoadRun.Result result = get(start(run));

        assertThat(result.stopReason()).isEqualTo("duration");
        assertThat(result.elapsedNanos()).isLessThan(Duration.ofSeconds(30).toNanos());
        assertThat(requests()).hasSize(1).allSatisfy(r -> assertThat(r).containsEntry("outcome", "REJECTED")
                .containsEntry("status", 503L).containsEntry("retryAfter", "1").containsEntry("ttfcNanos", null));
    }

    @Test
    void rejectedRequestsAreStillRecordedWhenThePauseIsZero() throws Exception {
        server.handler((ex, n) -> ex.sendResponseHeaders(503, -1));
        LoadRun run = new LoadRun(options("--concurrency", "2", "--requests", "10", "--reject-pause", "0ms"), "test");

        LoadRun.Result result = get(start(run));

        assertThat(result.recordsWritten()).isEqualTo(10);
        assertThat(requests()).allSatisfy(r -> assertThat(r).containsEntry("outcome", "REJECTED"));
        assertThat(run.summary().toText(result.facts())).contains("REJECTED=10");
    }
}
