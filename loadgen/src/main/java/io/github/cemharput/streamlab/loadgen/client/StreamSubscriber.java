package io.github.cemharput.streamlab.loadgen.client;

import io.github.cemharput.streamlab.loadgen.json.FlatJson;
import io.github.cemharput.streamlab.loadgen.sse.SseEvent;
import io.github.cemharput.streamlab.loadgen.sse.SseParser;
import io.github.cemharput.streamlab.loadgen.sse.SseProtocolException;
import java.net.http.HttpResponse;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.Flow;
import java.util.function.LongConsumer;

/**
 * Push-style body subscriber of one response. It never blocks: bytes go straight into the incremental SSE
 * parser (200 + text/event-stream) or are counted and discarded (anything else, at most 64 KiB). Timestamps are
 * taken when the buffer that completes an event arrives, so several events in one buffer share one timestamp.
 * State is guarded by the instance monitor because the HttpClient delivers callbacks on its own threads while the
 * worker thread takes snapshots.
 */
final class StreamSubscriber implements HttpResponse.BodySubscriber<Void> {

    static final int DISCARD_LIMIT_BYTES = 64 * 1024;

    /** Immutable view of the state at one moment. Times are nanoseconds from the request start. */
    record Snapshot(
            int status, String retryAfter, String requestId, boolean sse, long headersAt,
            Long firstBodyByteAt, Long firstChunkAt, long chunks, Long lastSeq,
            Long interChunkMin, Long interChunkMax,
            String terminal, Long terminalAt, String errorCode, String errorMessage, Long completeChunks,
            int violations, boolean parserLimitHit, boolean discardLimitHit) {
    }

    private final long startNanos;
    private final LongConsumer interChunkSink;
    private final CompletableFuture<Void> done = new CompletableFuture<>();
    private final SseParser parser;

    private final int status;
    private final String retryAfter;
    private final String requestId;
    private final boolean sse;
    private final long headersAt;

    private volatile long lastProgressNanos;
    private volatile Flow.Subscription subscription;

    private Long firstBodyByteAt;
    private Long firstChunkAt;
    private long chunks;
    private Long lastSeq;
    private long previousChunkAt = -1;
    private Long interChunkMin;
    private Long interChunkMax;
    private String terminal = "none";
    private Long terminalAt;
    private String errorCode;
    private String errorMessage;
    private Long completeChunks;
    private int violations;
    private boolean parserLimitHit;
    private boolean discardLimitHit;
    private long discarded;
    private boolean finished;

    StreamSubscriber(HttpResponse.ResponseInfo info, long startNanos, LongConsumer interChunkSink) {
        long now = System.nanoTime();
        this.startNanos = startNanos;
        this.interChunkSink = interChunkSink;
        this.headersAt = now - startNanos;
        this.lastProgressNanos = now;
        this.status = info.statusCode();
        this.retryAfter = info.headers().firstValue("Retry-After").orElse(null);
        this.requestId = info.headers().firstValue("X-Request-Id").orElse(null);
        String contentType = info.headers().firstValue("Content-Type").orElse("").trim().toLowerCase();
        this.sse = status == 200 && contentType.startsWith("text/event-stream");
        this.parser = new SseParser(this::onEvent);
    }

    long lastProgressNanos() {
        return lastProgressNanos;
    }

    int status() {
        return status;
    }

    boolean sse() {
        return sse;
    }

    long headersAt() {
        return headersAt;
    }

    /** Cancels the upstream subscription (closes the connection). Safe to call more than once. */
    void cancel() {
        Flow.Subscription s = subscription;
        if (s != null) {
            s.cancel();
        }
        synchronized (this) {
            finished = true;
        }
    }

    @Override
    public void onSubscribe(Flow.Subscription s) {
        this.subscription = s;
        s.request(1);
    }

    @Override
    public void onNext(List<ByteBuffer> buffers) {
        long now = System.nanoTime();
        lastProgressNanos = now;
        boolean more;
        synchronized (this) {
            if (finished) {
                return;
            }
            long rel = now - startNanos;
            for (ByteBuffer buf : buffers) {
                if (!buf.hasRemaining()) {
                    continue;
                }
                if (firstBodyByteAt == null) {
                    firstBodyByteAt = rel;
                }
                if (sse) {
                    try {
                        parser.feed(buf, rel);
                    } catch (SseProtocolException ex) {
                        parserLimitHit = true;
                        finished = true;
                        break;
                    }
                } else {
                    discarded += buf.remaining();
                    buf.position(buf.limit());
                    if (discarded > DISCARD_LIMIT_BYTES) {
                        discardLimitHit = true;
                        finished = true;
                        break;
                    }
                }
            }
            more = !finished;
        }
        if (more) {
            subscription.request(1);
        } else {
            subscription.cancel();
            done.complete(null);
        }
    }

    @Override
    public void onError(Throwable throwable) {
        lastProgressNanos = System.nanoTime();
        synchronized (this) {
            finished = true;
        }
        done.completeExceptionally(throwable);
    }

    @Override
    public void onComplete() {
        lastProgressNanos = System.nanoTime();
        synchronized (this) {
            parser.end();
            finished = true;
        }
        done.complete(null);
    }

    @Override
    public CompletionStage<Void> getBody() {
        return done;
    }

    synchronized Snapshot snapshot() {
        return new Snapshot(status, retryAfter, requestId, sse, headersAt, firstBodyByteAt, firstChunkAt, chunks,
                lastSeq, interChunkMin, interChunkMax, terminal, terminalAt, errorCode, errorMessage,
                completeChunks, violations, parserLimitHit, discardLimitHit);
    }

    /** Runs inside {@code onNext} with the instance monitor held. */
    private void onEvent(SseEvent event, long rel) {
        boolean afterTerminal = !"none".equals(terminal);
        switch (event.type()) {
            case "chunk" -> {
                if (afterTerminal) {
                    violations++;
                    return;
                }
                chunks++;
                if (firstChunkAt == null) {
                    firstChunkAt = rel;
                }
                if (previousChunkAt >= 0) {
                    long delta = rel - previousChunkAt;
                    interChunkMin = interChunkMin == null ? delta : Math.min(interChunkMin, delta);
                    interChunkMax = interChunkMax == null ? delta : Math.max(interChunkMax, delta);
                    interChunkSink.accept(delta);
                }
                previousChunkAt = rel;
                recordSeq(event.id());
            }
            case "complete" -> {
                if (afterTerminal) {
                    violations++;
                    return;
                }
                terminal = "complete";
                terminalAt = rel;
                Map<String, Object> json = parseData(event.data());
                if (json != null && json.get("chunks") instanceof Long count) {
                    completeChunks = count;
                } else {
                    violations++;
                }
            }
            case "error" -> {
                if (afterTerminal) {
                    violations++;
                    return;
                }
                terminal = "error";
                terminalAt = rel;
                Map<String, Object> json = parseData(event.data());
                if (json != null && json.get("code") instanceof String code) {
                    errorCode = code;
                } else {
                    violations++;
                }
                if (json != null && json.get("message") instanceof String message) {
                    errorMessage = message;
                }
            }
            default -> violations++;
        }
    }

    private void recordSeq(String id) {
        long seq;
        try {
            seq = Long.parseLong(id == null ? "" : id);
        } catch (NumberFormatException ex) {
            violations++;
            return;
        }
        if (lastSeq != null && seq != lastSeq + 1) {
            violations++;
        }
        lastSeq = seq;
    }

    private static Map<String, Object> parseData(String data) {
        try {
            return FlatJson.parse(data);
        } catch (IllegalArgumentException ex) {
            return null;
        }
    }
}
