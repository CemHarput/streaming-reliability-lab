package io.github.cemharput.streamlab.bench;

import java.time.Duration;
import java.util.List;

/** What the runner needs from a running gateway (actuator over HTTP). */
public interface GatewayApi extends AutoCloseable {

    /** Releases the HTTP client; the runner calls it at the end of every run. */
    @Override
    default void close() {
    }

    /** Result of the connection mode check. */
    record ModeCheck(boolean ok, List<String> observedTags, Double value, String message) {
    }

    /** Result of waiting for the gateway to drain. */
    record IdleResult(boolean idle, long waitedMillis, String message) {
    }

    /** True when {@code /actuator/health/readiness} answers 200 with status UP. */
    boolean readiness();

    /** Compares the {@code mode} tag values of {@code connection_mode} with the expected tag; value must be 1. */
    ModeCheck verifyConnectionMode(String expectedTag);

    /** One request per snapshot path; failures are recorded in the entry, never thrown. */
    List<Snapshot.Entry> snapshotEntries();

    /** Waits until {@code active_streams} and both {@code queue_depth} series are zero, at most {@code timeout}. */
    IdleResult awaitIdle(Duration timeout) throws InterruptedException;
}
