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
 * Gateway resource metrics: {@code active_streams}, {@code queue_depth{queue=chunks|admission}}
 * and {@code rejected_total{reason}}. The registry does not depend on this class.
 */
@Component
public class StreamMetrics {

    /** Reasons a request can be turned away with 503 (400 responses are not counted). */
    static final List<String> REJECT_REASONS =
            List.of("limit", "queue_timeout", "shutdown", "upstream_unavailable", "interrupted");

    private final AtomicInteger queuedChunks = new AtomicInteger();
    private final Map<String, Counter> rejections = new ConcurrentHashMap<>();
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
    }

    void rejected(String reason) {
        counter(reason).increment();
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
}
