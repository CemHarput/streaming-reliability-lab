package io.github.cemharput.streamlab.loadgen.sse;

@FunctionalInterface
public interface SseListener {

    /** @param nanos the {@code nanos} value passed to the {@code feed} call that completed the event */
    void onEvent(SseEvent event, long nanos);
}
