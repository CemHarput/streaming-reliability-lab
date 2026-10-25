package io.github.cemharput.streamlab.bench.testsupport;

import com.sun.net.httpserver.HttpServer;
import java.io.IOException;
import java.io.OutputStream;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/** A tiny actuator stand-in on 127.0.0.1:0. Responses are looked up by request URI including the query. */
public final class FakeActuatorServer implements AutoCloseable {

    private record Reply(int status, String body) {
    }

    private final HttpServer server;
    private final Map<String, Reply> replies = new ConcurrentHashMap<>();

    public FakeActuatorServer() throws IOException {
        server = HttpServer.create(new InetSocketAddress(InetAddress.getLoopbackAddress(), 0), 0);
        server.createContext("/", exchange -> {
            Reply reply = replies.getOrDefault(exchange.getRequestURI().toString(), new Reply(404, "{}"));
            byte[] bytes = reply.body().getBytes(StandardCharsets.UTF_8);
            exchange.getResponseHeaders().add("Content-Type", "application/json");
            exchange.sendResponseHeaders(reply.status(), bytes.length);
            try (OutputStream out = exchange.getResponseBody()) {
                out.write(bytes);
            }
        });
        server.start();
    }

    public int port() {
        return server.getAddress().getPort();
    }

    public FakeActuatorServer reply(String uri, int status, String body) {
        replies.put(uri, new Reply(status, body));
        return this;
    }

    /** A metric body in the shape of Spring Boot's metrics endpoint. */
    public static String metric(String name, double value, String tag, String... tagValues) {
        StringBuilder tags = new StringBuilder();
        for (int i = 0; i < tagValues.length; i++) {
            tags.append(i > 0 ? "," : "").append('"').append(tagValues[i]).append('"');
        }
        return "{\"name\":\"" + name + "\",\"measurements\":[{\"statistic\":\"VALUE\",\"value\":" + value
                + "}],\"availableTags\":" + (tag == null ? "[]" : "[{\"tag\":\"" + tag + "\",\"values\":["
                + tags + "]}]") + "}";
    }

    @Override
    public void close() {
        server.stop(0);
    }
}
