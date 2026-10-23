package io.github.cemharput.streamlab.gateway.stream;

/** JSON payloads of the SSE events. Event names: chunk, complete, error. */
final class SseEvents {

    static final String CHUNK = "chunk";
    static final String COMPLETE = "complete";
    static final String ERROR = "error";

    record ChunkEvent(String requestId, int seq, String text, boolean last) {
    }

    record CompleteEvent(String requestId, int chunks) {
    }

    record ErrorEvent(String requestId, String code, String message) {
    }

    private SseEvents() {
    }
}
