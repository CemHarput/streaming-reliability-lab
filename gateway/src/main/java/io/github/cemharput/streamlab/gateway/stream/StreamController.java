package io.github.cemharput.streamlab.gateway.stream;

import io.github.cemharput.streamlab.gateway.config.GatewayProperties;
import io.github.cemharput.streamlab.gateway.upstream.ChannelProvider;
import io.github.cemharput.streamlab.tokenstream.v1.TokenStreamServiceGrpc;
import io.grpc.Status;
import java.time.Duration;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.servlet.mvc.method.annotation.SseEmitter;

@RestController
public class StreamController {

    static final String REQUEST_ID_HEADER = "X-Request-Id";
    /** Keeps the gRPC deadline firing before the emitter timeout. */
    private static final long EMITTER_TIMEOUT_MARGIN_MS = 5_000;

    private final ChannelProvider channels;
    private final ActiveStreamRegistry registry;
    private final Duration streamDeadline;
    private final StreamMetrics metrics;
    private final StreamTimeouts timeouts;
    private final int streamQueueCapacity;

    public StreamController(ChannelProvider channels, ActiveStreamRegistry registry, StreamMetrics metrics,
            StreamTimeouts timeouts, GatewayProperties properties) {
        this.channels = channels;
        this.registry = registry;
        this.metrics = metrics;
        this.timeouts = timeouts;
        this.streamQueueCapacity = properties.limits().streamQueueCapacity();
        this.streamDeadline = properties.upstream().streamDeadline();
    }

    @GetMapping(path = "/stream", produces = MediaType.TEXT_EVENT_STREAM_VALUE)
    public ResponseEntity<SseEmitter> stream(
            @RequestParam(name = "chunks", defaultValue = "10") int chunks,
            @RequestParam(name = "firstDelayMs", defaultValue = "0") int firstDelayMs,
            @RequestParam(name = "intervalMs", defaultValue = "50") int intervalMs,
            @RequestParam(name = "fault", defaultValue = "NONE") FaultParam fault,
            @RequestParam(name = "faultAtSeq", defaultValue = "0") int faultAtSeq,
            @RequestParam(name = "seed", defaultValue = "0") long seed) {
        if (faultAtSeq < 0) {
            // The proto field is unsigned: a negative value would wrap and silently never fire.
            return ResponseEntity.badRequest().build();
        }
        StreamParameters params = new StreamParameters(chunks, firstDelayMs, intervalMs, fault, faultAtSeq, seed);
        String requestId = UUID.randomUUID().toString();

        ChannelProvider.Lease lease;
        try {
            lease = channels.acquire();
        } catch (IllegalStateException ex) {
            metrics.rejected("upstream_unavailable");
            return unavailable();
        }

        SseEmitter emitter = new SseEmitter(streamDeadline.toMillis() + EMITTER_TIMEOUT_MARGIN_MS);
        StreamBridge bridge = new StreamBridge(requestId, emitter, registry, lease, streamQueueCapacity, metrics, timeouts);
        ActiveStreamRegistry.Admission admission = registry.admit(bridge);
        if (admission != ActiveStreamRegistry.Admission.ADMITTED) {
            lease.close();
            metrics.rejected(reason(admission));
            return unavailable();
        }
        try {
            TokenStreamServiceGrpc.newStub(lease.channel())
                    .withDeadlineAfter(streamDeadline.toMillis(), TimeUnit.MILLISECONDS)
                    .generateTokens(params.toRequest(requestId), bridge);
        } catch (RuntimeException ex) {
            bridge.onError(Status.INTERNAL.withDescription("failed to start upstream call").withCause(ex).asRuntimeException());
        }
        return ResponseEntity.ok()
                .header(REQUEST_ID_HEADER, requestId)
                .body(emitter);
    }

    private static String reason(ActiveStreamRegistry.Admission admission) {
        return switch (admission) {
            case REJECTED_LIMIT -> "limit";
            case REJECTED_QUEUE_TIMEOUT -> "queue_timeout";
            case REJECTED_SHUTDOWN -> "shutdown";
            case REJECTED_INTERRUPTED -> "interrupted";
            case ADMITTED -> throw new IllegalArgumentException("not a rejection");
        };
    }

    private static ResponseEntity<SseEmitter> unavailable() {
        return ResponseEntity.status(HttpStatus.SERVICE_UNAVAILABLE)
                .header("Retry-After", "1")
                .build();
    }
}
