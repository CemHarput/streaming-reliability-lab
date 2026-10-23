package io.github.cemharput.streamlab.testsupport;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;

/** Minimal SSE client on the JDK HttpClient; collects parsed events. */
public final class SseClient {

    public record SseEvent(String name, String id, String data) {
    }

    private static final HttpClient HTTP = HttpClient.newBuilder().version(HttpClient.Version.HTTP_1_1).build();

    private final List<SseEvent> events = new CopyOnWriteArrayList<>();
    private final CompletableFuture<HttpResponse<?>> response = new CompletableFuture<>();
    private final CompletableFuture<Void> finished;
    private volatile java.util.stream.Stream<String> body;

    private SseClient(URI uri) {
        HttpRequest request = HttpRequest.newBuilder(uri).header("Accept", "text/event-stream").GET().build();
        finished = HTTP.sendAsync(request, HttpResponse.BodyHandlers.ofLines()).thenAccept(resp -> {
            body = resp.body();
            response.complete(resp);
            parse(resp.body());
        });
        finished.exceptionally(ex -> {
            response.completeExceptionally(ex);
            return null;
        });
    }

    public static SseClient open(String url) {
        return new SseClient(URI.create(url));
    }

    public static HttpResponse<String> get(String url) throws Exception {
        return HTTP.send(HttpRequest.newBuilder(URI.create(url)).GET().build(), HttpResponse.BodyHandlers.ofString());
    }

    /** Same as {@link #get(String)} but announces that it wants an event stream, like a real SSE client. */
    public static HttpResponse<String> getAcceptingEventStream(String url) throws Exception {
        return HTTP.send(HttpRequest.newBuilder(URI.create(url)).header("Accept", "text/event-stream").GET().build(),
                HttpResponse.BodyHandlers.ofString());
    }

    /** Drops the connection without waiting for the stream to end (a client that went away). */
    public void disconnect() {
        java.util.stream.Stream<String> lines = body;
        if (lines != null) {
            lines.close();
        }
    }

    private void parse(java.util.stream.Stream<String> lines) {
        String[] fields = new String[3];
        lines.forEach(line -> {
            if (line.isEmpty()) {
                if (fields[0] != null || fields[2] != null) {
                    events.add(new SseEvent(fields[0], fields[1], fields[2]));
                }
                fields[0] = null;
                fields[1] = null;
                fields[2] = null;
                return;
            }
            int colon = line.indexOf(':');
            if (colon < 0) {
                return;
            }
            String key = line.substring(0, colon);
            String value = line.substring(colon + 1);
            if (value.startsWith(" ")) {
                value = value.substring(1);
            }
            switch (key) {
                case "event" -> fields[0] = value;
                case "id" -> fields[1] = value;
                case "data" -> fields[2] = value;
                default -> { }
            }
        });
    }

    public List<SseEvent> events() {
        return events;
    }

    /** Completes when headers arrive. */
    public CompletableFuture<HttpResponse<?>> response() {
        return response;
    }

    /** Completes when the response body ended. */
    public CompletableFuture<Void> finished() {
        return finished;
    }

    public boolean isFinished() {
        return finished.isDone();
    }
}
