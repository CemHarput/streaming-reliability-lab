package io.github.cemharput.streamlab.testsupport;

import io.github.cemharput.streamlab.gateway.upstream.ManagedChannelFactory;
import io.github.cemharput.streamlab.simulator.RequestLimits;
import io.github.cemharput.streamlab.simulator.SimulatorConfig;
import io.github.cemharput.streamlab.simulator.SimulatorServer;
import io.grpc.inprocess.InProcessChannelBuilder;
import io.grpc.inprocess.InProcessServerBuilder;
import java.io.IOException;
import java.time.Duration;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Primary;

/**
 * Runs the real simulator in-process and points the gateway channel at it.
 * Lives outside the gateway component-scan root on purpose.
 */
@Configuration(proxyBeanMethods = false)
public class InProcessSimulatorConfig {

    private final String name = InProcessServerBuilder.generateName();

    @Bean(destroyMethod = "stop")
    SimulatorServer simulatorServer() throws IOException {
        return SimulatorServer.start(
                InProcessServerBuilder.forName(name),
                new SimulatorConfig(0, Duration.ofSeconds(2), 2, RequestLimits.DEFAULT));
    }

    @Bean
    @Primary
    ManagedChannelFactory inProcessChannelFactory(SimulatorServer simulator) {
        return target -> InProcessChannelBuilder.forName(name).build();
    }
}
