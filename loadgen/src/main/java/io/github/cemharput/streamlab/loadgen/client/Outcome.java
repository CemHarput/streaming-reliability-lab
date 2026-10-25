package io.github.cemharput.streamlab.loadgen.client;

/** Exactly one of these is recorded per request. */
public enum Outcome {
    /** HTTP 200 and a {@code complete} event was received. */
    COMPLETED,
    /** HTTP 200 and an {@code error} event was received. */
    ERROR_EVENT,
    /** HTTP 503 (the gateway rejected the request). */
    REJECTED,
    /** Any other non-200 status. */
    HTTP_ERROR,
    /** HTTP 200 with a content type other than text/event-stream, or an SSE parser limit was exceeded. */
    PROTOCOL_ERROR,
    /** The body ended cleanly (EOF) without a terminal event. */
    NO_TERMINAL,
    /** I/O error after the response headers and before a terminal event (reset, truncated chunked body). */
    RESET,
    /** No progress within the idle timeout, or the HttpClient connect timeout; see {@code timeoutKind}. */
    TIMEOUT,
    /** The connection could not be established. */
    CONNECT_FAILURE,
    /** The run was stopped (drain timeout or signal) while the request was open. */
    INTERRUPTED,
    /** Anything unexpected; {@code errorClass} is set. */
    CLIENT_ERROR
}
