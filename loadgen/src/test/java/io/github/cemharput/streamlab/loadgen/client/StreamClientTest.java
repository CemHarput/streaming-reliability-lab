package io.github.cemharput.streamlab.loadgen.client;

import static io.github.cemharput.streamlab.loadgen.testsupport.SseTestServer.chunk;
import static io.github.cemharput.streamlab.loadgen.testsupport.SseTestServer.complete;
import static io.github.cemharput.streamlab.loadgen.testsupport.SseTestServer.error;
import static io.github.cemharput.streamlab.loadgen.testsupport.SseTestServer.raw;
import static io.github.cemharput.streamlab.loadgen.testsupport.SseTestServer.startSse;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import com.sun.net.httpserver.HttpExchange;
import io.github.cemharput.streamlab.loadgen.testsupport.RawSocketServer;
import io.github.cemharput.streamlab.loadgen.testsupport.RawSocketServer.Ending;
import io.github.cemharput.streamlab.loadgen.testsupport.SseTestServer;
import java.io.IOException;
import java.io.OutputStream;
import java.net.ServerSocket;
import java.net.URI;
import java.net.http.HttpClient;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.Timeout.ThreadMode;

@Timeout(value = 60, unit = TimeUnit.SECONDS, threadMode = ThreadMode.SEPARATE_THREAD)
class StreamClientTest {

    private static final StreamParams PARAMS = new StreamParams(3, 10, 0, "NONE", 0, 42);
    private static final Duration IDLE = Duration.ofSeconds(30);
    private static final Duration SHORT_IDLE = Duration.ofMillis(300);

    private final List<CountDownLatch> gates = new CopyOnWriteArrayList<>();
    private final List<Long> intervals = new CopyOnWriteArrayList<>();
    private final List<AutoCloseable> closeables = new ArrayList<>();
    private ExecutorService httpExecutor;
    private HttpClient http;
    private SseTestServer server;

    @BeforeEach
    void setUp() throws IOException {
        httpExecutor = Executors.newVirtualThreadPerTaskExecutor();
        http = HttpClient.newBuilder().version(HttpClient.Version.HTTP_1_1).connectTimeout(Duration.ofSeconds(5))
                .executor(httpExecutor).build();
        server = new SseTestServer();
        closeables.add(server);
    }

    @AfterEach
    void tearDown() throws Exception {
        gates.forEach(CountDownLatch::countDown);
        http.shutdownNow();
        httpExecutor.shutdownNow();
        for (AutoCloseable c : closeables) {
            c.close();
        }
    }

    private CountDownLatch gate() {
        CountDownLatch latch = new CountDownLatch(1);
        gates.add(latch);
        return latch;
    }

    private static void awaitLatch(CountDownLatch latch) throws InterruptedException {
        latch.await(30, TimeUnit.SECONDS);
    }

    private StreamResult execute(URI uri, Duration idle) {
        return new StreamClient(http, uri, idle, intervals::add).execute(PARAMS, System.nanoTime());
    }

    private StreamResult execute(Duration idle) {
        return execute(server.uri(), idle);
    }

    private static void assertTimeOrder(StreamResult r) {
        assertThat(r.ttfbNanos()).isNotNull();
        assertThat(r.firstBodyByteNanos()).isNotNull().isGreaterThanOrEqualTo(r.ttfbNanos());
        assertThat(r.ttfcNanos()).isNotNull().isGreaterThanOrEqualTo(r.firstBodyByteNanos());
        assertThat(r.totalNanos()).isGreaterThanOrEqualTo(r.ttfcNanos());
        assertThat(r.endOffsetNanos() - r.startOffsetNanos()).isGreaterThanOrEqualTo(r.totalNanos());
    }

    @Test
    void completedStream() {
        server.handler((ex, n) -> SseTestServer.fullStream(ex, 3));

        StreamResult r = execute(IDLE);

        assertThat(r.outcome()).isEqualTo(Outcome.COMPLETED);
        assertThat(r.status()).isEqualTo(200);
        assertThat(r.requestId()).isNotNull();
        assertThat(r.chunks()).isEqualTo(3);
        assertThat(r.lastSeq()).isEqualTo(2L);
        assertThat(r.terminal()).isEqualTo("complete");
        assertThat(r.completeChunks()).isEqualTo(3L);
        assertThat(r.protocolViolations()).isZero();
        assertThat(r.bodyEnd()).isEqualTo("eof");
        assertThat(r.timeoutKind()).isNull();
        assertThat(r.errorClass()).isNull();
        assertTimeOrder(r);
        assertThat(r.interChunkMinNanos()).isNotNull().isLessThanOrEqualTo(r.interChunkMaxNanos());
        assertThat(intervals).hasSize(2);
        assertThat(server.queries()).containsExactly("chunks=3&firstDelayMs=0&intervalMs=10&fault=NONE&faultAtSeq=0&seed=42");
    }

    @Test
    void chunksSentInSeparateWritesAreTimedSeparately() throws Exception {
        CountDownLatch firstSent = gate();
        CountDownLatch second = gate();
        server.handler((ex, n) -> {
            OutputStream os = startSse(ex);
            chunk(os, 0);
            firstSent.countDown();
            awaitLatch(second);
            chunk(os, 1);
            complete(os, 2);
        });
        ExecutorService runner = Executors.newSingleThreadExecutor();
        closeables.add(runner::shutdownNow);
        Future<StreamResult> future = runner.submit(() -> execute(IDLE));

        awaitLatch(firstSent);
        second.countDown();
        StreamResult r = future.get(30, TimeUnit.SECONDS);

        assertThat(r.outcome()).isEqualTo(Outcome.COMPLETED);
        assertThat(r.chunks()).isEqualTo(2);
        assertThat(intervals).hasSize(1);
    }

    @Test
    void errorEvent() {
        server.handler((ex, n) -> {
            OutputStream os = startSse(ex);
            chunk(os, 0);
            error(os, "UNAVAILABLE", "upstream gone");
        });

        StreamResult r = execute(IDLE);

        assertThat(r.outcome()).isEqualTo(Outcome.ERROR_EVENT);
        assertThat(r.terminal()).isEqualTo("error");
        assertThat(r.errorCode()).isEqualTo("UNAVAILABLE");
        assertThat(r.errorMessage()).isEqualTo("upstream gone");
        assertThat(r.chunks()).isEqualTo(1);
        assertThat(r.completeChunks()).isNull();
        assertThat(r.protocolViolations()).isZero();
    }

    @Test
    void rejectedWithRetryAfter() {
        server.handler((ex, n) -> {
            ex.getResponseHeaders().add("Retry-After", "1");
            ex.sendResponseHeaders(503, -1);
        });

        StreamResult r = execute(IDLE);

        assertThat(r.outcome()).isEqualTo(Outcome.REJECTED);
        assertThat(r.status()).isEqualTo(503);
        assertThat(r.retryAfter()).isEqualTo("1");
        assertThat(r.ttfbNanos()).isNotNull();
        assertThat(r.ttfcNanos()).isNull();
        assertThat(r.firstBodyByteNanos()).isNull();
        assertThat(r.terminal()).isEqualTo("none");
        assertThat(r.chunks()).isZero();
    }

    @Test
    void otherStatusIsHttpError() {
        server.handler((ex, n) -> {
            byte[] body = "bad".getBytes();
            ex.sendResponseHeaders(400, body.length);
            ex.getResponseBody().write(body);
        });

        StreamResult r = execute(IDLE);

        assertThat(r.outcome()).isEqualTo(Outcome.HTTP_ERROR);
        assertThat(r.status()).isEqualTo(400);
        assertThat(r.retryAfter()).isNull();
        assertThat(r.bodyEnd()).isEqualTo("eof");
        // the response body arrives after the headers: the moment of the outcome is not before it
        assertThat(r.firstBodyByteNanos()).isNotNull();
        assertThat(r.totalNanos()).isGreaterThanOrEqualTo(r.firstBodyByteNanos());
    }

    @Test
    void hugeErrorBodyIsCutOff() {
        server.handler((ex, n) -> {
            ex.sendResponseHeaders(500, 0);
            OutputStream os = ex.getResponseBody();
            byte[] block = new byte[16 * 1024];
            for (int i = 0; i < 64; i++) {
                os.write(block);
            }
        });

        StreamResult r = execute(IDLE);

        assertThat(r.outcome()).isEqualTo(Outcome.HTTP_ERROR);
        assertThat(r.bodyEnd()).isEqualTo("limit");
    }

    @Test
    void wrongContentTypeIsProtocolError() {
        server.handler((ex, n) -> {
            ex.getResponseHeaders().add("Content-Type", "text/plain");
            byte[] body = "hello".getBytes();
            ex.sendResponseHeaders(200, body.length);
            ex.getResponseBody().write(body);
        });

        StreamResult r = execute(IDLE);

        assertThat(r.outcome()).isEqualTo(Outcome.PROTOCOL_ERROR);
        assertThat(r.status()).isEqualTo(200);
        assertThat(r.chunks()).isZero();
    }

    @Test
    void sseLineLimitIsProtocolError() {
        server.handler((ex, n) -> {
            OutputStream os = startSse(ex);
            chunk(os, 0);
            byte[] block = new byte[64 * 1024];
            java.util.Arrays.fill(block, (byte) 'x');
            for (int i = 0; i < 40; i++) {
                os.write(block);
            }
        });

        StreamResult r = execute(IDLE);

        assertThat(r.outcome()).isEqualTo(Outcome.PROTOCOL_ERROR);
        assertThat(r.chunks()).isEqualTo(1);
        assertThat(r.bodyEnd()).isEqualTo("limit");
    }

    @Test
    void resetInTheMiddleOfTheBody() throws Exception {
        RawSocketServer raw = new RawSocketServer(Ending.RESET);
        closeables.add(raw);

        StreamResult r = new StreamClient(http, raw.uri(), IDLE, nanos -> raw.release()).execute(PARAMS,
                System.nanoTime());

        assertThat(r.outcome()).isEqualTo(Outcome.RESET);
        assertThat(r.errorPhase()).isEqualTo("body");
        assertThat(r.chunks()).isEqualTo(2);
        assertThat(r.terminal()).isEqualTo("none");
        assertThat(r.bodyEnd()).isEqualTo("reset");
        assertThat(r.errorClass()).isNotNull();
        assertThat(r.ttfcNanos()).isNotNull();
    }

    @Test
    void truncatedChunkedBodyIsAReset() throws Exception {
        RawSocketServer raw = new RawSocketServer(Ending.TRUNCATED_CHUNKED);
        closeables.add(raw);

        StreamResult r = new StreamClient(http, raw.uri(), IDLE, nanos -> raw.release()).execute(PARAMS,
                System.nanoTime());

        assertThat(r.outcome()).isEqualTo(Outcome.RESET);
        assertThat(r.errorPhase()).isEqualTo("body");
        assertThat(r.chunks()).isEqualTo(2);
        assertThat(r.errorClass()).isNotNull();
    }

    @Test
    void bodyFailureAfterTheTerminalEventKeepsTheOutcomeAndLeavesTheErrorFieldsEmpty() throws Exception {
        RawSocketServer raw = new RawSocketServer(Ending.TRUNCATED_CHUNKED_AFTER_COMPLETE);
        closeables.add(raw);

        StreamResult r = new StreamClient(http, raw.uri(), IDLE, nanos -> raw.release()).execute(PARAMS,
                System.nanoTime());

        assertThat(r.outcome()).isEqualTo(Outcome.COMPLETED);
        assertThat(r.terminal()).isEqualTo("complete");
        assertThat(r.bodyEnd()).isEqualTo("reset");
        assertThat(r.errorPhase()).isNull();
        assertThat(r.errorClass()).isNull();
        assertThat(r.errorDetail()).isNull();
    }

    @Test
    void cleanEofWithoutTerminalEvent() throws Exception {
        RawSocketServer raw = new RawSocketServer(Ending.CLEAN_EOF_NO_TERMINAL);
        closeables.add(raw);

        StreamResult r = new StreamClient(http, raw.uri(), IDLE, nanos -> raw.release()).execute(PARAMS,
                System.nanoTime());

        assertThat(r.outcome()).isEqualTo(Outcome.NO_TERMINAL);
        assertThat(r.chunks()).isEqualTo(2);
        assertThat(r.bodyEnd()).isEqualTo("eof");
        assertThat(r.errorPhase()).isNull();
    }

    @Test
    void eventsAfterTheTerminalEventAreViolationsAndDoNotChangeTheOutcome() {
        server.handler((ex, n) -> {
            OutputStream os = startSse(ex);
            chunk(os, 0);
            complete(os, 1);
            chunk(os, 1);
            complete(os, 2);
            error(os, "INTERNAL", "late");
        });

        StreamResult r = execute(IDLE);

        assertThat(r.outcome()).isEqualTo(Outcome.COMPLETED);
        assertThat(r.chunks()).isEqualTo(1);
        assertThat(r.completeChunks()).isEqualTo(1L);
        assertThat(r.protocolViolations()).isEqualTo(3);
    }

    @Test
    void contractDeviationsAreCounted() {
        server.handler((ex, n) -> {
            OutputStream os = startSse(ex);
            chunk(os, 0);
            chunk(os, 5); // not consecutive
            raw(os, "event: chunk\nid: abc\ndata: {}\n\n"); // id not a number
            raw(os, "event: weird\ndata: x\n\n"); // unknown event name
            raw(os, "event: error\ndata: {\"requestId\":\"r\"}\n\n"); // error without code
        });

        StreamResult r = execute(IDLE);

        assertThat(r.outcome()).isEqualTo(Outcome.ERROR_EVENT);
        assertThat(r.protocolViolations()).isEqualTo(4);
        assertThat(r.errorCode()).isNull();
        assertThat(r.chunks()).isEqualTo(3);
    }

    @Test
    void refusedConnection() throws IOException {
        int port;
        try (ServerSocket s = new ServerSocket(0)) {
            port = s.getLocalPort();
        }

        StreamResult r = execute(URI.create("http://127.0.0.1:" + port + "/stream"), IDLE);

        assertThat(r.outcome()).isEqualTo(Outcome.CONNECT_FAILURE);
        assertThat(r.errorPhase()).isEqualTo("before_headers");
        assertThat(r.errorClass()).isNotNull();
        assertThat(r.status()).isNull();
        assertThat(r.ttfbNanos()).isNull();
    }

    @Test
    void stallBeforeTheHeadersIsAHeadersTimeout() {
        CountDownLatch never = gate();
        server.handler((ex, n) -> {
            awaitLatch(never);
            ex.sendResponseHeaders(503, -1);
        });

        StreamResult r = execute(SHORT_IDLE);

        assertThat(r.outcome()).isEqualTo(Outcome.TIMEOUT);
        assertThat(r.timeoutKind()).isEqualTo("headers");
        assertThat(r.errorPhase()).isEqualTo("before_headers");
        assertThat(r.status()).isNull();
        assertThat(r.totalNanos()).isGreaterThanOrEqualTo(SHORT_IDLE.toNanos());
    }

    @Test
    void stallAfterOneChunkIsAnIdleTimeoutAndCancelClosesTheConnection() throws Exception {
        CountDownLatch stall = gate();
        CountDownLatch connectionClosed = gate();
        AtomicBoolean probeFailed = new AtomicBoolean();
        server.handler((ex, n) -> {
            OutputStream os = startSse(ex);
            chunk(os, 0);
            awaitLatch(stall);
            // after the client gave up, keep writing small comments until the broken connection is noticed
            try {
                for (int i = 0; i < 2000; i++) {
                    raw(os, ": probe\n\n");
                    if (connectionClosed.await(5, TimeUnit.MILLISECONDS)) {
                        return;
                    }
                }
            } catch (IOException ex2) {
                probeFailed.set(true);
                connectionClosed.countDown();
            }
        });

        StreamResult r = execute(SHORT_IDLE);

        assertThat(r.outcome()).isEqualTo(Outcome.TIMEOUT);
        assertThat(r.timeoutKind()).isEqualTo("idle");
        assertThat(r.chunks()).isEqualTo(1);
        assertThat(r.errorPhase()).isNull();
        assertThat(r.bodyEnd()).isEqualTo("timeout");
        stall.countDown();
        assertThat(connectionClosed.await(20, TimeUnit.SECONDS)).as("server noticed the closed connection").isTrue();
        assertThat(probeFailed).isTrue();
    }

    @Test
    void stallAfterTheTerminalEventKeepsTheOutcome() {
        CountDownLatch never = gate();
        server.handler((ex, n) -> {
            OutputStream os = startSse(ex);
            chunk(os, 0);
            complete(os, 1);
            awaitLatch(never); // body never ends
        });

        StreamResult r = execute(SHORT_IDLE);

        assertThat(r.outcome()).isEqualTo(Outcome.COMPLETED);
        assertThat(r.bodyEnd()).isEqualTo("timeout");
        assertThat(r.totalNanos()).isLessThan(r.endOffsetNanos() - r.startOffsetNanos());
    }

    @Test
    void interruptCancelsTheRequestAndKeepsPartialData() throws Exception {
        CountDownLatch firstSent = gate();
        CountDownLatch never = gate();
        server.handler((ex, n) -> {
            OutputStream os = startSse(ex);
            chunk(os, 0);
            chunk(os, 1);
            firstSent.countDown();
            awaitLatch(never);
        });
        AtomicReference<StreamResult> result = new AtomicReference<>();
        AtomicBoolean flagAfter = new AtomicBoolean();
        Thread worker = Thread.ofVirtual().start(() -> {
            result.set(execute(IDLE));
            flagAfter.set(Thread.currentThread().isInterrupted());
        });

        awaitLatch(firstSent);
        await().atMost(Duration.ofSeconds(30)).until(() -> !intervals.isEmpty());
        worker.interrupt();
        worker.join(Duration.ofSeconds(30));

        assertThat(worker.isAlive()).isFalse();
        assertThat(result.get().outcome()).isEqualTo(Outcome.INTERRUPTED);
        assertThat(result.get().chunks()).isEqualTo(2);
        assertThat(result.get().bodyEnd()).isEqualTo("interrupted");
        assertThat(flagAfter).isTrue();
    }
}
