package io.github.cemharput.streamlab.loadgen.sse;

/** One dispatched server-sent event. {@code type} is {@code "message"} when no {@code event} field was sent. */
public record SseEvent(String type, String id, String data) {
}
