package io.github.cemharput.streamlab.gateway;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.github.cemharput.streamlab.gateway.config.GatewayProperties;
import io.github.cemharput.streamlab.gateway.config.GatewayProperties.ConnectionMode;
import io.github.cemharput.streamlab.testsupport.InProcessSimulatorConfig;
import java.time.Duration;
import org.junit.jupiter.api.Test;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.context.ConfigurableApplicationContext;

class ConnectionModeConfigTest {

    private SpringApplicationBuilder builder() {
        return new SpringApplicationBuilder(GatewayApplication.class, InProcessSimulatorConfig.class)
                .properties("server.port=0");
    }

    @Test
    void configuredValueIsShared() {
        try (ConfigurableApplicationContext context = builder().run()) {
            assertThat(context.getBean(GatewayProperties.class).connectionMode()).isEqualTo(ConnectionMode.SHARED);
        }
    }

    @Test
    void codeDefaultsApplyWithoutApplicationYaml() {
        // No application.yaml: only the @DefaultValue annotations can supply these.
        try (ConfigurableApplicationContext context = builder().run("--spring.config.name=nonexistent")) {
            GatewayProperties properties = context.getBean(GatewayProperties.class);
            assertThat(properties.connectionMode()).isEqualTo(ConnectionMode.SHARED);
            assertThat(properties.upstream().target()).isEqualTo("localhost:50051");
            assertThat(properties.upstream().streamDeadline()).isEqualTo(Duration.ofSeconds(60));
            assertThat(properties.shutdown().drainTimeout()).isEqualTo(Duration.ofSeconds(10));
            assertThat(properties.shutdown().channelTerminationTimeout()).isEqualTo(Duration.ofSeconds(5));
        }
    }

    @Test
    void perRequestModeFailsAtStartup() {
        assertThatThrownBy(() -> builder().run("--lab.connection-mode=per-request").close())
                .hasStackTraceContaining("lab.connection-mode");
    }
}
