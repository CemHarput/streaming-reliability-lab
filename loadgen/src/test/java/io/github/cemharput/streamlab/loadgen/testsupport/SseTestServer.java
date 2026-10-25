package io.github.cemharput.streamlab.loadgen.testsupport;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import java.io.IOException;
import java.io.OutputStream;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Small {@code /stream} server on 127.0.0.1 with a test-supplied handler per request. It counts open handlers
 * (and their maximum) so tests can check the concurrency limit from the server's point of view.
 */
public final class SseTestServer implements AutoCloseable {

    @FunctionalInterface
    public interface Handler {
        void handle(HttpExchange exchange, int requestNumber) throws Exception;
    }

    private final HttpServer server;
    private final ExecutorService executor = Executors.newVirtualThreadPerTaskExecutor();
    private final AtomicInteger requests = new AtomicInteger();
    private final AtomicInteger open = new AtomicInteger();
    private final AtomicInteger maxOpen = new AtomicInteger();
    private final List<String> queries = new CopyOnWriteArrayList<>();
    private volatile Handler handler = (ex, n) -> {
        throw new IllegalStateException("no handler");
    };

    public SseTestServer() throws IOException {
        server = HttpServer.create(new InetSocketAddress(InetAddress.getLoopbackAddress(), 0), 0);
        server.setExecutor(executor);
        server.createContext("/stream", exchange -> {
            int number = requests.getAndIncrement();
            queries.add(exchange.getRequestURI().getRawQuery());
            int now = open.incrementAndGet();
            maxOpen.accumulateAndGet(now, Math::max);
            try {
                handler.handle(exchange, number);
            } catch (IOException ex) {
                // the client went away; expected in several tests
            } catch (Exception ex) {
                ex.printStackTrace();
            } finally {
                open.decrementAndGet();
                exchange.close();
            }
        });
        server.start();
    }

    public SseTestServer handler(Handler h) {
        this.handler = h;
        return this;
    }

    public URI uri() {
        return URI.create("http://127.0.0.1:" + server.getAddress().getPort() + "/stream");
    }

    public int port() {
        return server.getAddress().getPort();
    }

    public int requestCount() {
        return requests.get();
    }

    public int openCount() {
        return open.get();
    }

    public int maxOpen() {
        return maxOpen.get();
    }

    public List<String> queries() {
        return queries;
    }

    @Override
    public void close() {
        server.stop(0);
        executor.shutdownNow();
    }

    // ---- helpers for handlers ----

    /** Sends 200 + text/event-stream with a chunked body and returns the body stream. */
    public static OutputStream startSse(HttpExchange ex) throws IOException {
        ex.getResponseHeaders().add("Content-Type", "text/event-stream");
        ex.getResponseHeaders().add("X-Request-Id", "req-" + ex.getRequestURI().getRawQuery().hashCode());
        ex.sendResponseHeaders(200, 0);
        OutputStream os = ex.getResponseBody();
        os.flush();
        return os;
    }

    public static void chunk(OutputStream os, int seq) throws IOException {
        raw(os, "event: chunk\nid: " + seq + "\ndata: {\"requestId\":\"r\",\"seq\":" + seq + ",\"text\":\"t" + seq
                + "\",\"last\":false}\n\n");
    }

    public static void complete(OutputStream os, int chunks) throws IOException {
        raw(os, "event: complete\ndata: {\"requestId\":\"r\",\"chunks\":" + chunks + "}\n\n");
    }

    public static void error(OutputStream os, String code, String message) throws IOException {
        raw(os, "event: error\ndata: {\"requestId\":\"r\",\"code\":\"" + code + "\",\"message\":\"" + message
                + "\"}\n\n");
    }

    public static void raw(OutputStream os, String text) throws IOException {
        os.write(text.getBytes(StandardCharsets.UTF_8));
        os.flush();
    }

    /** A complete, well-formed stream of {@code chunks} chunks. */
    public static void fullStream(HttpExchange ex, int chunks) throws IOException {
        OutputStream os = startSse(ex);
        for (int i = 0; i < chunks; i++) {
            chunk(os, i);
        }
        complete(os, chunks);
    }
}
