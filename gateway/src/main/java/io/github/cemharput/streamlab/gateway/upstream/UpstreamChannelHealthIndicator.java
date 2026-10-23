package io.github.cemharput.streamlab.gateway.upstream;

import io.grpc.ConnectivityState;
import org.springframework.boot.health.contributor.Health;
import org.springframework.boot.health.contributor.HealthIndicator;

public class UpstreamChannelHealthIndicator implements HealthIndicator {

    private final ChannelProvider provider;

    public UpstreamChannelHealthIndicator(ChannelProvider provider) {
        this.provider = provider;
    }

    @Override
    public Health health() {
        ConnectivityState state = provider.state(true);
        Health.Builder builder = state == ConnectivityState.READY ? Health.up() : Health.down();
        return builder.withDetail("state", state.name()).build();
    }
}
