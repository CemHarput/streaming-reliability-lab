package io.github.cemharput.streamlab.bench;

import java.util.Arrays;
import java.util.Optional;

/** Gateway connection mode: the {@code lab.connection-mode} property value and the {@code mode} metric tag. */
public enum ConnectionMode {
    SHARED("shared", "shared"),
    PER_REQUEST("per-request", "per_request");

    private final String property;
    private final String metricTag;

    ConnectionMode(String property, String metricTag) {
        this.property = property;
        this.metricTag = metricTag;
    }

    /** Value of {@code lab.connection-mode}; also used in CLI values and run directory names. */
    public String property() {
        return property;
    }

    /** Value of the {@code mode} tag of the gateway's {@code connection_mode} metric. */
    public String metricTag() {
        return metricTag;
    }

    public static Optional<ConnectionMode> fromProperty(String text) {
        return Arrays.stream(values()).filter(m -> m.property.equals(text)).findFirst();
    }
}
