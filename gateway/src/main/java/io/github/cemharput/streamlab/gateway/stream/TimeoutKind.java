package io.github.cemharput.streamlab.gateway.stream;

import io.github.cemharput.streamlab.gateway.stream.StreamBridge.StreamOutcome;

/** The two upstream silence limits; each maps to a terminal outcome, a metric tag and the client-visible message. */
enum TimeoutKind {

    FIRST_CHUNK("first_chunk", StreamOutcome.FIRST_CHUNK_TIMEOUT, "first chunk timeout"),
    IDLE("idle", StreamOutcome.IDLE_TIMEOUT, "idle timeout between chunks");

    private final String tag;
    private final StreamOutcome outcome;
    private final String message;

    TimeoutKind(String tag, StreamOutcome outcome, String message) {
        this.tag = tag;
        this.outcome = outcome;
        this.message = message;
    }

    String tag() {
        return tag;
    }

    StreamOutcome outcome() {
        return outcome;
    }

    String message() {
        return message;
    }
}
