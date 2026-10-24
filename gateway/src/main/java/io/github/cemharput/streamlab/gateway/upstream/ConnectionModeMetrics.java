package io.github.cemharput.streamlab.gateway.upstream;

import io.github.cemharput.streamlab.gateway.config.GatewayProperties;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import org.springframework.stereotype.Component;

/** Exposes the configured connection mode as {@code connection_mode{mode}} = 1 (one low-cardinality tag). */
@Component
public class ConnectionModeMetrics {

    public ConnectionModeMetrics(MeterRegistry registry, GatewayProperties properties) {
        Gauge.builder("connection_mode", () -> 1)
                .description("Configured upstream connection mode (1 for the active mode)")
                .tag("mode", properties.connectionMode().tag())
                .register(registry);
    }
}
