package io.github.cemharput.streamlab.gateway;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.github.cemharput.streamlab.gateway.config.GatewayProperties;
import io.github.cemharput.streamlab.gateway.config.GatewayProperties.ConnectionMode;
import io.github.cemharput.streamlab.gateway.config.GatewayProperties.OverloadPolicy;
import io.github.cemharput.streamlab.gateway.upstream.ChannelProvider;
import io.github.cemharput.streamlab.gateway.upstream.PerRequestChannelProvider;
import io.github.cemharput.streamlab.gateway.upstream.SharedChannelProvider;
import io.github.cemharput.streamlab.testsupport.InProcessSimulatorConfig;
import java.time.Duration;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.context.ConfigurableApplicationContext;

@Timeout(value = 60, threadMode = Timeout.ThreadMode.SEPARATE_THREAD)
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
            assertThat(properties.timeouts().firstChunk()).isEqualTo(Duration.ofSeconds(30));
            assertThat(properties.timeouts().idleBetweenChunks()).isEqualTo(Duration.ofSeconds(30));
            assertThat(properties.retry().maxAttempts()).isEqualTo(3);
            assertThat(properties.retry().initialBackoff()).isEqualTo(Duration.ofMillis(100));
            assertThat(properties.retry().maxBackoff()).isEqualTo(Duration.ofSeconds(2));
        }
    }

    @Test
    void retryIsBoundFromRelaxedNames() {
        try (ConfigurableApplicationContext context = builder().run("--lab.retry.max-attempts=5",
                "--lab.retry.initialBackoff=50ms", "--lab.retry.max-backoff=1500ms")) {
            GatewayProperties.Retry retry = context.getBean(GatewayProperties.class).retry();
            assertThat(retry.maxAttempts()).isEqualTo(5);
            assertThat(retry.initialBackoff()).isEqualTo(Duration.ofMillis(50));
            assertThat(retry.maxBackoff()).isEqualTo(Duration.ofMillis(1500));
        }
    }

    @Test
    void zeroAttemptsDisablesRetryAndIsValid() {
        try (ConfigurableApplicationContext context = builder().run("--lab.retry.max-attempts=0")) {
            assertThat(context.getBean(GatewayProperties.class).retry().maxAttempts()).isZero();
        }
    }

    @ParameterizedTest
    @ValueSource(strings = {
            "--lab.retry.max-attempts=-1",
            "--lab.retry.max-attempts=11",
            "--lab.retry.initial-backoff=0s",
            "--lab.retry.max-backoff=-1s",
            "--lab.retry.initial-backoff=3s",
            "--lab.retry.max-backoff=60s"})
    void invalidRetryFailsAtStartup(String argument) {
        String property = argument.substring(2, argument.indexOf('='));
        assertThatThrownBy(() -> builder().run(argument).close())
                .hasStackTraceContaining(property);
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
    void timeoutsAreBoundFromRelaxedNames() {
        try (ConfigurableApplicationContext context = builder().run("--lab.timeouts.first-chunk=2s",
                "--lab.timeouts.idleBetweenChunks=1500ms")) {
            GatewayProperties.Timeouts timeouts = context.getBean(GatewayProperties.class).timeouts();
            assertThat(timeouts.firstChunk()).isEqualTo(Duration.ofSeconds(2));
            assertThat(timeouts.idleBetweenChunks()).isEqualTo(Duration.ofMillis(1500));
        }
    }

    @ParameterizedTest
    @ValueSource(strings = {
            "--lab.timeouts.first-chunk=0s",
            "--lab.timeouts.first-chunk=-1s",
            "--lab.timeouts.idle-between-chunks=0s",
            "--lab.timeouts.first-chunk=60s",
            "--lab.timeouts.idle-between-chunks=61s"})
    void invalidTimeoutsFailAtStartup(String argument) {
        String property = argument.substring(2, argument.indexOf('='));
        assertThatThrownBy(() -> builder().run(argument).close())
                .hasStackTraceContaining(property);
    }

    @Test
    void timeoutNotShorterThanDeadlineFailsAtStartup() {
        // The default 30s timeouts no longer fit under a 10s call deadline.
        assertThatThrownBy(() -> builder().run("--lab.upstream.stream-deadline=10s").close())
                .hasStackTraceContaining("lab.timeouts.first-chunk")
                .hasStackTraceContaining("lab.upstream.stream-deadline");
    }

    @ParameterizedTest
    @ValueSource(strings = {"per-request", "per_request", "PER_REQUEST"})
    void perRequestModeIsBoundFromRelaxedNames(String spelling) {
        try (ConfigurableApplicationContext context = builder().run("--lab.connection-mode=" + spelling)) {
            assertThat(context.getBean(GatewayProperties.class).connectionMode()).isEqualTo(ConnectionMode.PER_REQUEST);
            assertThat(context.getBeansOfType(ChannelProvider.class)).hasSize(1);
            assertThat(context.getBean(ChannelProvider.class)).isInstanceOf(PerRequestChannelProvider.class);
        }
    }

    @Test
    void sharedModeHasTheSharedProvider() {
        try (ConfigurableApplicationContext context = builder().run()) {
            assertThat(context.getBeansOfType(ChannelProvider.class)).hasSize(1);
            assertThat(context.getBean(ChannelProvider.class)).isInstanceOf(SharedChannelProvider.class);
        }
    }

    @Test
    void invalidConnectionModeFailsAtStartup() {
        assertThatThrownBy(() -> builder().run("--lab.connection-mode=pooled").close())
                .hasStackTraceContaining("lab.connection-mode");
    }

    @Test
    void emptyConnectionModeFailsAtStartup() {
        // The properties binding alone would treat an empty value as unset and run the shared mode silently.
        assertThatThrownBy(() -> builder().run("--lab.connection-mode=").close())
                .hasStackTraceContaining("lab.connection-mode must be shared or per-request, but it is empty");
        assertThatThrownBy(() -> builder().run("--lab.connection-mode=   ").close())
                .hasStackTraceContaining("lab.connection-mode must be shared or per-request, but it is empty");
        // The environment resolves relaxed spellings of the name, so they are rejected the same way.
        assertThatThrownBy(() -> builder().run("--lab.connectionMode=").close())
                .hasStackTraceContaining("lab.connection-mode must be shared or per-request, but it is empty");
    }
}
