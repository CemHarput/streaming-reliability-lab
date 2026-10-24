package io.github.cemharput.streamlab.testsupport;

import io.github.cemharput.streamlab.gateway.upstream.ManagedChannelFactory;
import io.github.cemharput.streamlab.simulator.RequestLimits;
import io.github.cemharput.streamlab.simulator.SimulatorConfig;
import io.github.cemharput.streamlab.simulator.SimulatorServer;
import io.github.cemharput.streamlab.tokenstream.v1.GetStatsRequest;
import io.github.cemharput.streamlab.tokenstream.v1.SimulatorAdminServiceGrpc;
import io.github.cemharput.streamlab.tokenstream.v1.SimulatorStats;
import io.github.cemharput.streamlab.tokenstream.v1.GenerateRequest;
import io.github.cemharput.streamlab.tokenstream.v1.StreamProfile;
import io.github.cemharput.streamlab.tokenstream.v1.TokenChunk;
import io.github.cemharput.streamlab.tokenstream.v1.TokenStreamServiceGrpc;
import io.grpc.ManagedChannel;
import io.grpc.inprocess.InProcessChannelBuilder;
import io.grpc.inprocess.InProcessServerBuilder;
import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.TimeUnit;
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
    private final FaultInjectingInterceptor faults = new FaultInjectingInterceptor();
    private final CountingChannelFactory channelFactory = new CountingChannelFactory(
            target -> InProcessChannelBuilder.forName(name).intercept(faults).build());

    /** Test-side fault plan for the gateway channel; the simulator itself is untouched. */
    public FaultInjectingInterceptor faults() {
        return faults;
    }

    /** Counts the channels the gateway creates and shuts down (the factory wraps the in-process channel builder). */
    public CountingChannelFactory channelFactory() {
        return channelFactory;
    }

    /**
     * The texts an uninterrupted call returns for this request id and seed, read over a fresh channel without
     * the interceptor (no delay between chunks): the reference a retried stream must equal byte for byte.
     */
    public List<String> referenceTexts(String requestId, long seed, int chunks) {
        ManagedChannel channel = InProcessChannelBuilder.forName(name).build();
        try {
            GenerateRequest request = GenerateRequest.newBuilder()
                    .setRequestId(requestId)
                    .setSeed(seed)
                    .setProfile(StreamProfile.newBuilder().setChunkCount(chunks).setChunkIntervalMs(0))
                    .build();
            Iterator<TokenChunk> it = TokenStreamServiceGrpc.newBlockingStub(channel)
                    .withDeadlineAfter(30, TimeUnit.SECONDS)
                    .generateTokens(request);
            List<String> texts = new ArrayList<>();
            while (it.hasNext()) {
                texts.add(it.next().getText());
            }
            return texts;
        } finally {
            channel.shutdownNow();
        }
    }

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
        return channelFactory;
    }
}
