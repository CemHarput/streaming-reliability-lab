package io.github.cemharput.streamlab.loadgen.client;

/**
 * Raw measurements of one request. All durations are nanoseconds from the request start
 * ({@code System.nanoTime}); the two offsets are relative to the run start. Absent values are null.
 *
 * @param endOffsetNanos the moment the record closed, including the wait for the body to end after a terminal event
 * @param totalNanos     from the request start to the moment the outcome was decided
 * @param terminal       {@code complete}, {@code error} or {@code none}
 * @param bodyEnd        how the body ended: {@code eof}, {@code reset}, {@code timeout}, {@code interrupted} or
 *                       {@code limit} (parser or discard limit hit); null when no body was read
 */
public record StreamResult(
        Outcome outcome,
        long startOffsetNanos,
        long endOffsetNanos,
        Integer status,
        String retryAfter,
        String requestId,
        Long ttfbNanos,
        Long firstBodyByteNanos,
        Long ttfcNanos,
        long totalNanos,
        long chunks,
        Long lastSeq,
        Long interChunkMinNanos,
        Long interChunkMaxNanos,
        String terminal,
        String errorCode,
        String errorMessage,
        Long completeChunks,
        int protocolViolations,
        String timeoutKind,
        String errorPhase,
        String errorClass,
        String errorDetail,
        String bodyEnd) {
}
