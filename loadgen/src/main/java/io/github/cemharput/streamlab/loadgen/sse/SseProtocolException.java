package io.github.cemharput.streamlab.loadgen.sse;

/** The stream broke a parser limit (a line longer than the maximum, or event data longer than the event limit). */
public class SseProtocolException extends RuntimeException {

    public SseProtocolException(String message) {
        super(message);
    }
}
