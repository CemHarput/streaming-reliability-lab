package io.github.cemharput.streamlab.loadgen.client;

import java.io.IOException;
import java.net.ConnectException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpConnectTimeoutException;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.net.http.HttpTimeoutException;
import java.time.Duration;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.LongConsumer;

/**
 * Runs one {@code GET} against the gateway's SSE endpoint and classifies the result. The calling thread only
 * waits (with a bounded, progress-based timeout); all parsing happens in the {@link StreamSubscriber}.
 */
public final class StreamClient {

    private static final int DETAIL_LIMIT = 512;

    private enum End { EOF, ERROR, TIMEOUT, INTERRUPTED }

    private final HttpClient http;
    private final URI target;
    private final long idleNanos;
    private final LongConsumer interChunkSink;

    /**
     * @param interChunkSink receives every inter-chunk interval (nanoseconds); called from HttpClient threads
     *                       and must be thread safe
     */
    public StreamClient(HttpClient http, URI target, Duration idleTimeout, LongConsumer interChunkSink) {
        this.http = http;
        this.target = target;
        this.idleNanos = idleTimeout.toNanos();
        this.interChunkSink = interChunkSink;
    }

    /**
     * Executes one request. Never throws; if the calling thread is interrupted, the request is cancelled and the
     * interrupt flag is set again.
     *
     * @param runStartNanos {@code System.nanoTime} of the run start; offsets in the result are relative to it
     */
    public StreamResult execute(StreamParams params, long runStartNanos) {
        long start = System.nanoTime();
        HttpRequest request = HttpRequest.newBuilder(URI.create(target + "?" + params.toQuery()))
                .GET()
                .header("Accept", "text/event-stream")
                .build();
        AtomicReference<StreamSubscriber> holder = new AtomicReference<>();
        CompletableFuture<HttpResponse<Void>> response = http.sendAsync(request, info -> {
            StreamSubscriber s = new StreamSubscriber(info, start, interChunkSink);
            holder.set(s);
            return s;
        });

        End end;
        Throwable failure = null;
        // The subscriber as it was when the wait ended: a response whose headers arrive only after the timeout
        // decision is still a "headers" timeout, not a body-phase one.
        StreamSubscriber[] seen = new StreamSubscriber[1];
        try {
            end = await(response, holder, start, seen);
        } catch (ExecutionException ex) {
            end = End.ERROR;
            failure = ex.getCause() == null ? ex : ex.getCause();
        } catch (CancellationException ex) {
            end = End.ERROR;
            failure = ex;
        } catch (InterruptedException ex) {
            end = End.INTERRUPTED;
            Thread.currentThread().interrupt();
        }
        StreamSubscriber decided;
        if (end == End.TIMEOUT) {
            decided = seen[0];
        } else {
            decided = holder.get();
        }
        if (end == End.TIMEOUT || end == End.INTERRUPTED) {
            // Cancel whatever exists now, even a subscriber created after the decision.
            StreamSubscriber current = holder.get();
            if (current != null) {
                current.cancel();
            }
            response.cancel(true);
            if (end == End.INTERRUPTED) {
                decided = current;
            }
        }
        long endNanos = System.nanoTime();
        return classify(decided == null ? null : decided.snapshot(), end, failure, start, endNanos, runStartNanos);
    }

    /** Waits until the body ends, nothing happened for the idle timeout, or the thread is interrupted. */
    private End await(CompletableFuture<HttpResponse<Void>> response, AtomicReference<StreamSubscriber> holder,
            long start, StreamSubscriber[] seen) throws InterruptedException, ExecutionException {
        while (true) {
            StreamSubscriber s = holder.get();
            long lastProgress = s == null ? start : s.lastProgressNanos();
            long remaining = idleNanos - (System.nanoTime() - lastProgress);
            if (remaining <= 0) {
                seen[0] = s;
                return End.TIMEOUT;
            }
            try {
                response.get(remaining, TimeUnit.NANOSECONDS);
                return End.EOF;
            } catch (TimeoutException ex) {
                // progress may have happened meanwhile: loop and recompute
            }
        }
    }

    private StreamResult classify(StreamSubscriber.Snapshot snap, End end, Throwable failure, long start,
            long endNanos, long runStart) {
        long startOffset = start - runStart;
        long endOffset = endNanos - runStart;
        long elapsed = endNanos - start;
        boolean bodyPhase = snap != null;

        if (!bodyPhase) {
            return preHeaders(end, failure, startOffset, endOffset, elapsed);
        }

        boolean hasTerminal = !"none".equals(snap.terminal());
        String bodyEnd = switch (end) {
            case EOF -> snap.parserLimitHit() || snap.discardLimitHit() ? "limit" : "eof";
            case ERROR -> "reset";
            case TIMEOUT -> "timeout";
            case INTERRUPTED -> "interrupted";
        };
        Outcome outcome;
        String timeoutKind = null;
        String errorClass = null;
        String errorDetail = null;
        String errorPhase = null;
        long total = elapsed;
        int violations = snap.violations();

        if (snap.status() == 503) {
            outcome = Outcome.REJECTED;
            total = headersOrFirstBody(snap);
        } else if (snap.status() != 200) {
            outcome = Outcome.HTTP_ERROR;
            total = headersOrFirstBody(snap);
        } else if (!snap.sse()) {
            outcome = Outcome.PROTOCOL_ERROR;
            errorDetail = "unexpected content type";
            total = headersOrFirstBody(snap);
        } else if (hasTerminal) {
            outcome = "complete".equals(snap.terminal()) ? Outcome.COMPLETED : Outcome.ERROR_EVENT;
            total = snap.terminalAt();
            if (snap.parserLimitHit()) {
                violations++;
            }
        } else if (snap.parserLimitHit()) {
            outcome = Outcome.PROTOCOL_ERROR;
            errorDetail = "SSE parser limit exceeded (line or event data)";
        } else {
            switch (end) {
                case EOF -> outcome = Outcome.NO_TERMINAL;
                case TIMEOUT -> {
                    outcome = Outcome.TIMEOUT;
                    timeoutKind = "idle";
                }
                case INTERRUPTED -> outcome = Outcome.INTERRUPTED;
                default -> {
                    outcome = Outcome.RESET;
                    errorPhase = "body";
                }
            }
        }
        boolean answeredWithoutTransportError = outcome == Outcome.COMPLETED || outcome == Outcome.ERROR_EVENT
                || outcome == Outcome.REJECTED || outcome == Outcome.HTTP_ERROR
                || outcome == Outcome.PROTOCOL_ERROR;
        // After a terminal event (or a rejection) a later body failure is only visible in bodyEnd, so that
        // "errorClass != null" never marks a request that got its answer.
        if (end == End.ERROR && failure != null && !answeredWithoutTransportError) {
            errorClass = failure.getClass().getName();
            errorDetail = truncate(failure.getMessage());
            if (errorPhase == null) {
                errorPhase = "body";
            }
            if (outcome == Outcome.RESET && !(failure instanceof IOException)) {
                outcome = Outcome.CLIENT_ERROR;
            }
        }
        if (outcome == Outcome.RESET || outcome == Outcome.TIMEOUT || outcome == Outcome.NO_TERMINAL
                || outcome == Outcome.INTERRUPTED || outcome == Outcome.CLIENT_ERROR) {
            // no terminal event: the outcome moment is the end of the wait
            total = elapsed;
        }
        return new StreamResult(outcome, startOffset, endOffset, snap.status(), snap.retryAfter(), snap.requestId(),
                snap.headersAt(), snap.firstBodyByteAt(), snap.firstChunkAt(), total, snap.chunks(), snap.lastSeq(),
                snap.interChunkMin(), snap.interChunkMax(), snap.terminal(), snap.errorCode(),
                truncate(snap.errorMessage()), snap.completeChunks(), violations, timeoutKind, errorPhase, errorClass,
                errorDetail, bodyEnd);
    }

    private static StreamResult preHeaders(End end, Throwable failure, long startOffset, long endOffset,
            long elapsed) {
        Outcome outcome;
        String timeoutKind = null;
        String errorClass = null;
        String errorDetail = null;
        String errorPhase = "before_headers";
        switch (end) {
            case TIMEOUT -> {
                outcome = Outcome.TIMEOUT;
                timeoutKind = "headers";
            }
            case INTERRUPTED -> {
                outcome = Outcome.INTERRUPTED;
                errorPhase = null;
            }
            case EOF -> {
                // cannot normally happen: the response future completes only after the body subscriber was created
                outcome = Outcome.CLIENT_ERROR;
                errorClass = "internal";
                errorDetail = "response completed without a body subscriber";
            }
            default -> {
                errorClass = failure == null ? null : failure.getClass().getName();
                errorDetail = failure == null ? null : truncate(failure.getMessage());
                if (failure instanceof HttpConnectTimeoutException) {
                    outcome = Outcome.TIMEOUT;
                    timeoutKind = "connect";
                } else if (failure instanceof HttpTimeoutException) {
                    outcome = Outcome.TIMEOUT;
                    timeoutKind = "headers";
                } else if (failure instanceof ConnectException) {
                    outcome = Outcome.CONNECT_FAILURE;
                } else if (failure instanceof IOException) {
                    outcome = Outcome.RESET;
                } else {
                    outcome = Outcome.CLIENT_ERROR;
                }
            }
        }
        return new StreamResult(outcome, startOffset, endOffset, null, null, null, null, null, null, elapsed, 0,
                null, null, null, "none", null, null, null, 0, timeoutKind, errorPhase, errorClass, errorDetail,
                null);
    }

    /** The moment of a response without a stream: the headers, or the first body byte when that came later. */
    private static long headersOrFirstBody(StreamSubscriber.Snapshot snap) {
        Long firstBody = snap.firstBodyByteAt();
        return firstBody == null ? snap.headersAt() : Math.max(snap.headersAt(), firstBody);
    }

    private static String truncate(String s) {
        if (s == null || s.length() <= DETAIL_LIMIT) {
            return s;
        }
        return s.substring(0, DETAIL_LIMIT);
    }
}
