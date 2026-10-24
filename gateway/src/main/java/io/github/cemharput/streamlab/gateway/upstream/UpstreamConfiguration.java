package io.github.cemharput.streamlab.gateway.upstream;

import io.github.cemharput.streamlab.gateway.config.GatewayProperties;
import io.github.cemharput.streamlab.gateway.config.GatewayProperties.ConnectionMode;
import io.grpc.netty.shaded.io.grpc.netty.NettyChannelBuilder;
import org.springframework.boot.context.properties.bind.Binder;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.ConditionContext;
import org.springframework.context.annotation.Conditional;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.type.AnnotatedTypeMetadata;

@Configuration(proxyBeanMethods = false)
@EnableConfigurationProperties(GatewayProperties.class)
public class UpstreamConfiguration {

    @Bean
    ManagedChannelFactory nettyChannelFactory() {
        return target -> NettyChannelBuilder.forTarget(target).usePlaintext().build();
    }

    @Bean
    @Conditional(SharedMode.class)
    SharedChannelProvider sharedChannelProvider(ManagedChannelFactory factory, GatewayProperties properties) {
        return new SharedChannelProvider(factory, properties);
    }

    @Bean
    @Conditional(PerRequestMode.class)
    PerRequestChannelProvider perRequestChannelProvider(ManagedChannelFactory factory, GatewayProperties properties) {
        return new PerRequestChannelProvider(factory, properties);
    }

    @Bean
    UpstreamChannelHealthIndicator upstreamChannelHealthIndicator(ChannelProvider provider) {
        return new UpstreamChannelHealthIndicator(provider);
    }

    /**
     * Reads {@code lab.connection-mode} with the binder, so the spelling rules (relaxed enum names) are the ones
     * {@link GatewayProperties} uses; the default is shared. An invalid value fails here or in the properties binding.
     * A blank value fails here too: the properties binding would treat it as unset and silently run the shared mode.
     */
    private static ConnectionMode configuredMode(ConditionContext context) {
        String raw = context.getEnvironment().getProperty("lab.connection-mode");
        if (raw != null && raw.isBlank()) {
            throw new IllegalStateException("lab.connection-mode must be shared or per-request, but it is empty");
        }
        return Binder.get(context.getEnvironment())
                .bind("lab.connection-mode", ConnectionMode.class)
                .orElse(ConnectionMode.SHARED);
    }

    static final class SharedMode implements org.springframework.context.annotation.Condition {

        @Override
        public boolean matches(ConditionContext context, AnnotatedTypeMetadata metadata) {
            return configuredMode(context) == ConnectionMode.SHARED;
        }
    }

    static final class PerRequestMode implements org.springframework.context.annotation.Condition {

        @Override
        public boolean matches(ConditionContext context, AnnotatedTypeMetadata metadata) {
            return configuredMode(context) == ConnectionMode.PER_REQUEST;
        }
    }
}
