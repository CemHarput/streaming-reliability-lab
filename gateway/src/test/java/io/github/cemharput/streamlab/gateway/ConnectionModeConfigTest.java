package io.github.cemharput.streamlab.gateway;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.github.cemharput.streamlab.gateway.config.GatewayProperties;
import io.github.cemharput.streamlab.gateway.config.GatewayProperties.ConnectionMode;
import io.github.cemharput.streamlab.gateway.config.GatewayProperties.OverloadPolicy;
import io.github.cemharput.streamlab.testsupport.InProcessSimulatorConfig;
import java.time.Duration;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
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
            assertThat(properties.limits().maxActiveStreams()).isEqualTo(100);
            assertThat(properties.limits().overloadPolicy()).isEqualTo(OverloadPolicy.REJECT);
            assertThat(properties.limits().queueWait()).isEqualTo(Duration.ofSeconds(5));
            assertThat(properties.limits().maxWaiting()).isEqualTo(100);
            assertThat(properties.limits().streamQueueCapacity()).isEqualTo(16);
        }
    }

    @Test
    void waitPolicyIsBoundFromRelaxedNames() {
        try (ConfigurableApplicationContext context = builder().run("--lab.limits.overload-policy=wait",
                "--lab.limits.max-active-streams=7", "--lab.limits.stream-queue-capacity=4096")) {
            GatewayProperties.Limits limits = context.getBean(GatewayProperties.class).limits();
            assertThat(limits.overloadPolicy()).isEqualTo(OverloadPolicy.WAIT);
            assertThat(limits.maxActiveStreams()).isEqualTo(7);
            assertThat(limits.streamQueueCapacity()).isEqualTo(4096);
        }
    }

    @ParameterizedTest
    @ValueSource(strings = {
            "--lab.limits.max-active-streams=0",
            "--lab.limits.max-waiting=0",
            "--lab.limits.stream-queue-capacity=0",
            "--lab.limits.stream-queue-capacity=5000",
            "--lab.limits.queue-wait=0s",
            "--lab.limits.queue-wait=-1s",
            "--lab.limits.overload-policy=drop"})
    void invalidLimitsFailAtStartup(String argument) {
        String property = argument.substring(2, argument.indexOf('='));
        assertThatThrownBy(() -> builder().run(argument).close())
                .hasStackTraceContaining(property);
    }

    @Test
    void perRequestModeFailsAtStartup() {
        assertThatThrownBy(() -> builder().run("--lab.connection-mode=per-request").close())
                .hasStackTraceContaining("lab.connection-mode");
    }
}
