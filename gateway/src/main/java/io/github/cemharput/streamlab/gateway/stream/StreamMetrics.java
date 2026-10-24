package io.github.cemharput.streamlab.gateway.stream;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import org.springframework.stereotype.Component;

/**
 * Gateway resource metrics: {@code active_streams}, {@code queue_depth{queue=chunks|admission}},
 * {@code rejected_total{reason}}, {@code timeouts_total{kind}}, {@code retries_total{reason}} and
 * {@code retry_exhausted_total}. The registry does not depend on this class.
 */
@Component
public class StreamMetrics {

    /** Reasons a request can be turned away with 503 (400 responses are not counted). */
    static final List<String> REJECT_REASONS =
            List.of("limit", "queue_timeout", "shutdown", "upstream_unavailable", "interrupted");

    /** Upstream silence timeouts, by {@link TimeoutKind#tag()}. */
    static final List<String> TIMEOUT_KINDS = List.of(TimeoutKind.FIRST_CHUNK.tag(), TimeoutKind.IDLE.tag());

    /** Whether a retry restarted the stream from the beginning or resumed after received chunks. */
    static final List<String> RETRY_REASONS = List.of("restart", "resume");

    private final AtomicInteger queuedChunks = new AtomicInteger();
    private final Map<String, Counter> rejections = new ConcurrentHashMap<>();
    private final Map<String, Counter> timeouts = new ConcurrentHashMap<>();
    private final Map<String, Counter> retries = new ConcurrentHashMap<>();
    private final Counter retryExhausted;
    private final MeterRegistry meters;

    public StreamMetrics(MeterRegistry meters, ActiveStreamRegistry registry) {
        this.meters = meters;
        Gauge.builder("active_streams", registry, ActiveStreamRegistry::admittedCount)
                .description("Streams holding a permit")
                .register(meters);
        Gauge.builder("queue_depth", queuedChunks, AtomicInteger::get)
                .tag("queue", "chunks")
                .description("Chunks queued across all streams")
                .register(meters);
        Gauge.builder("queue_depth", registry, ActiveStreamRegistry::waitingCount)
                .tag("queue", "admission")
                .description("Requests waiting for a permit")
                .register(meters);
        REJECT_REASONS.forEach(this::counter);
        TIMEOUT_KINDS.forEach(this::timeoutCounter);
        RETRY_REASONS.forEach(this::retryCounter);
        retryExhausted = Counter.builder("retry_exhausted_total")
                .description("Streams that ended with an upstream error after their retries or budget ran out")
                .register(meters);
    }

    /** Counts one extra upstream call that is about to start. */
    void retried(String reason) {
        retryCounter(reason).increment();
    }

    /** Called by the UPSTREAM_ERROR claim winner only, so a stream counts at most once. */
    void retryExhausted() {
        retryExhausted.increment();
    }

    void rejected(String reason) {
        counter(reason).increment();
    }

    /** Called by the terminal claim winner only, so a stream counts at most once. */
    void timedOut(TimeoutKind kind) {
        timeoutCounter(kind.tag()).increment();
    }

    void chunkQueued() {
        queuedChunks.incrementAndGet();
    }

    void chunkDequeued() {
        queuedChunks.decrementAndGet();
    }

    void chunksDiscarded(int count) {
        if (count > 0) {
            queuedChunks.addAndGet(-count);
        }
    }

    private Counter counter(String reason) {
        return rejections.computeIfAbsent(reason,
                r -> Counter.builder("rejected_total").tag("reason", r)
                        .description("Requests answered with 503").register(meters));
    }

    private Counter retryCounter(String reason) {
        return retries.computeIfAbsent(reason,
                r -> Counter.builder("retries_total").tag("reason", r)
                        .description("Extra upstream calls started after an UNAVAILABLE").register(meters));
    }

    private Counter timeoutCounter(String kind) {
        return timeouts.computeIfAbsent(kind,
                k -> Counter.builder("timeouts_total").tag("kind", k)
                        .description("Streams ended by the first-chunk or idle timeout").register(meters));
    }
}
