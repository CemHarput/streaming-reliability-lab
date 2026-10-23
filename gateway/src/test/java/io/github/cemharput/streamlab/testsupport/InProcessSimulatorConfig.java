package io.github.cemharput.streamlab.testsupport;

import io.github.cemharput.streamlab.gateway.upstream.ManagedChannelFactory;
import io.github.cemharput.streamlab.simulator.RequestLimits;
import io.github.cemharput.streamlab.simulator.SimulatorConfig;
import io.github.cemharput.streamlab.simulator.SimulatorServer;
import io.github.cemharput.streamlab.tokenstream.v1.GetStatsRequest;
import io.github.cemharput.streamlab.tokenstream.v1.SimulatorAdminServiceGrpc;
import io.github.cemharput.streamlab.tokenstream.v1.SimulatorStats;
import io.grpc.ManagedChannel;
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

    /** The in-process server name, so tests can open their own channel to the simulator (e.g. for its stats). */
    public String serverName() {
        return name;
    }

    /** The simulator's own counters, read over a fresh in-process channel (proves what reached the upstream). */
    public SimulatorStats stats() {
        ManagedChannel channel = InProcessChannelBuilder.forName(name).build();
        try {
            return SimulatorAdminServiceGrpc.newBlockingStub(channel).getStats(GetStatsRequest.getDefaultInstance());
        } finally {
            channel.shutdownNow();
        }
    }

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
