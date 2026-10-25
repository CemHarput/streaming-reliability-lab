package io.github.cemharput.streamlab.bench;

import io.github.cemharput.streamlab.bench.json.Json;
import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/** Actuator client over the JDK {@link HttpClient} (HTTP/1.1, connect 2 s, request 5 s). */
public final class GatewayProbe implements GatewayApi {

    public static final String READINESS = "/actuator/health/readiness";
    public static final String ACTIVE_STREAMS = "/actuator/metrics/active_streams";
    public static final String QUEUE_CHUNKS = "/actuator/metrics/queue_depth?tag=queue:chunks";
    public static final String QUEUE_ADMISSION = "/actuator/metrics/queue_depth?tag=queue:admission";
    public static final String REJECTED_TOTAL = "/actuator/metrics/rejected_total";
    public static final String RETRY_EXHAUSTED = "/actuator/metrics/retry_exhausted_total";
    public static final String CONNECTION_MODE = "/actuator/metrics/connection_mode";

    public static final List<String> REJECTED_REASONS = List.of("limit", "queue_timeout", "shutdown",
            "upstream_unavailable", "interrupted");
    public static final List<String> TIMEOUT_KINDS = List.of("first_chunk", "idle");
    public static final List<String> RETRY_REASONS = List.of("restart", "resume");

    private static final Duration IDLE_POLL = Duration.ofMillis(200);

    private final HttpClient client;
    private final String base;
    private final Ticker ticker;

    public GatewayProbe(int port, Ticker ticker) {
        this.base = "http://127.0.0.1:" + port;
        this.ticker = ticker;
        this.client = HttpClient.newBuilder().version(HttpClient.Version.HTTP_1_1)
                .connectTimeout(Duration.ofSeconds(2)).build();
    }

    @Override
    public void close() {
        client.close();
    }

    public static String rejectedPath(String reason) {
        return REJECTED_TOTAL + "?tag=reason:" + reason;
    }

    public static String timeoutsPath(String kind) {
        return "/actuator/metrics/timeouts_total?tag=kind:" + kind;
    }

    public static String retriesPath(String reason) {
        return "/actuator/metrics/retries_total?tag=reason:" + reason;
    }

    /** Every path of a snapshot, in the order they are requested and recorded. */
    public static List<String> snapshotPaths() {
        List<String> paths = new ArrayList<>();
        paths.add(READINESS);
        paths.add(ACTIVE_STREAMS);
        paths.add(QUEUE_CHUNKS);
        paths.add(QUEUE_ADMISSION);
        paths.add(REJECTED_TOTAL);
        REJECTED_REASONS.forEach(r -> paths.add(rejectedPath(r)));
        TIMEOUT_KINDS.forEach(k -> paths.add(timeoutsPath(k)));
        RETRY_REASONS.forEach(r -> paths.add(retriesPath(r)));
        paths.add(RETRY_EXHAUSTED);
        paths.add(CONNECTION_MODE);
        paths.add("/actuator/metrics/jvm.memory.used?tag=area:heap");
        paths.add("/actuator/metrics/jvm.threads.live");
        paths.add("/actuator/metrics/process.cpu.usage");
        paths.add("/actuator/metrics/system.cpu.usage");
        return paths;
    }

    private record Response(int status, String body) {
    }

    private Response get(String path) throws IOException {
        HttpRequest request = HttpRequest.newBuilder(URI.create(base + path)).timeout(Duration.ofSeconds(5)).GET()
                .build();
        try {
            HttpResponse<String> response = client.send(request, HttpResponse.BodyHandlers.ofString());
            return new Response(response.statusCode(), response.body());
        } catch (InterruptedException ex) {
            Thread.currentThread().interrupt();
            throw new IOException("interrupted", ex);
        }
    }

    @Override
    public boolean readiness() {
        try {
            Response r = get(READINESS);
            return r.status() == 200 && "UP".equals(Json.parseObject(r.body()).get("status"));
        } catch (IOException | RuntimeException ex) {
            return false;
        }
    }

    @Override
    public ModeCheck verifyConnectionMode(String expectedTag) {
        try {
            Response r = get(CONNECTION_MODE);
            if (r.status() != 200) {
                return new ModeCheck(false, List.of(), null, "connection_mode answered HTTP " + r.status());
            }
            Map<String, Object> body = Json.parseObject(r.body());
            List<String> observed = tagValues(body, "mode");
            Double value = metricValue(body);
            boolean ok = observed.equals(List.of(expectedTag)) && value != null && value == 1.0;
            return new ModeCheck(ok, observed, value,
                    ok ? "ok" : "expected mode tag [" + expectedTag + "] with value 1.0, observed " + observed
                            + " value " + value);
        } catch (IOException | RuntimeException ex) {
            return new ModeCheck(false, List.of(), null, "connection_mode check failed: " + ex);
        }
    }

    @Override
    public List<Snapshot.Entry> snapshotEntries() {
        List<Snapshot.Entry> entries = new ArrayList<>();
        for (String path : snapshotPaths()) {
            entries.add(entry(path));
        }
        return entries;
    }

    private Snapshot.Entry entry(String path) {
        try {
            Response r = get(path);
            Object value = null;
            if (r.status() == 200) {
                try {
                    Map<String, Object> body = Json.parseObject(r.body());
                    value = path.equals(READINESS) ? body.get("status") : metricValue(body);
                } catch (RuntimeException ex) {
                    value = null;
                }
            }
            return new Snapshot.Entry(path, r.status(), value, r.body());
        } catch (IOException ex) {
            return new Snapshot.Entry(path, -1, null, "request failed: " + ex);
        }
    }

    @Override
    public IdleResult awaitIdle(Duration timeout) throws InterruptedException {
        long start = ticker.nanoTime();
        long deadline = start + timeout.toNanos();
        String last = "no answer";
        while (true) {
            Double active = value(ACTIVE_STREAMS);
            Double chunks = value(QUEUE_CHUNKS);
            Double admission = value(QUEUE_ADMISSION);
            last = "active_streams=" + active + " queue_depth{chunks}=" + chunks + " queue_depth{admission}="
                    + admission;
            if (isZero(active) && isZero(chunks) && isZero(admission)) {
                return new IdleResult(true, (ticker.nanoTime() - start) / 1_000_000, last);
            }
            if (ticker.nanoTime() >= deadline) {
                return new IdleResult(false, (ticker.nanoTime() - start) / 1_000_000, last);
            }
            ticker.sleep(IDLE_POLL);
        }
    }

    private static boolean isZero(Double d) {
        return d != null && d == 0.0;
    }

    private Double value(String path) {
        Snapshot.Entry e = entry(path);
        return e.value() instanceof Double d ? d : null;
    }

    /** First VALUE/COUNT measurement of an actuator metric body. */
    @SuppressWarnings("unchecked")
    static Double metricValue(Map<String, Object> body) {
        if (!(body.get("measurements") instanceof List<?> list)) {
            return null;
        }
        Double first = null;
        for (Object o : list) {
            if (o instanceof Map<?, ?> m && m.get("value") instanceof Number n) {
                Object statistic = ((Map<String, Object>) m).get("statistic");
                if ("VALUE".equals(statistic) || "COUNT".equals(statistic)) {
                    return n.doubleValue();
                }
                if (first == null) {
                    first = n.doubleValue();
                }
            }
        }
        return first;
    }

    static List<String> tagValues(Map<String, Object> body, String tag) {
        List<String> result = new ArrayList<>();
        if (body.get("availableTags") instanceof List<?> tags) {
            for (Object o : tags) {
                if (o instanceof Map<?, ?> m && tag.equals(m.get("tag")) && m.get("values") instanceof List<?> vs) {
                    vs.forEach(v -> result.add(String.valueOf(v)));
                }
            }
        }
        return result;
    }
}
