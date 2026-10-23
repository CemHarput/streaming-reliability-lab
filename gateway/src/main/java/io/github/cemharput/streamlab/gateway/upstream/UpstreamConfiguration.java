package io.github.cemharput.streamlab.gateway.upstream;

import io.github.cemharput.streamlab.gateway.config.GatewayProperties;
import io.grpc.netty.shaded.io.grpc.netty.NettyChannelBuilder;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration(proxyBeanMethods = false)
@EnableConfigurationProperties(GatewayProperties.class)
public class UpstreamConfiguration {

    @Bean
    ManagedChannelFactory nettyChannelFactory() {
        return target -> NettyChannelBuilder.forTarget(target).usePlaintext().build();
    }

    @Bean
    SharedChannelProvider sharedChannelProvider(ManagedChannelFactory factory, GatewayProperties properties) {
        return new SharedChannelProvider(factory, properties);
    }

    @Bean
    UpstreamChannelHealthIndicator upstreamChannelHealthIndicator(ChannelProvider provider) {
        return new UpstreamChannelHealthIndicator(provider);
    }
}
