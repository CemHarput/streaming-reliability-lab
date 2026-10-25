package io.github.cemharput.streamlab.bench;

import io.github.cemharput.streamlab.tokenstream.v1.GetStatsRequest;
import io.github.cemharput.streamlab.tokenstream.v1.SimulatorAdminServiceGrpc;
import io.github.cemharput.streamlab.tokenstream.v1.SimulatorStats;
import io.grpc.Grpc;
import io.grpc.InsecureChannelCredentials;
import io.grpc.ManagedChannel;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/** Reads {@code SimulatorAdminService.GetStats}. One short-lived channel per call; deadline 2 s. */
public final class SimulatorStatsClient implements SimulatorApi {

    private final String target;

    public SimulatorStatsClient(int port) {
        this.target = "127.0.0.1:" + port;
    }

    @Override
    public StatsResult stats() {
        ManagedChannel channel = Grpc.newChannelBuilder(target, InsecureChannelCredentials.create()).build();
        try {
            SimulatorStats s = SimulatorAdminServiceGrpc.newBlockingStub(channel)
                    .withDeadlineAfter(2, TimeUnit.SECONDS).getStats(GetStatsRequest.getDefaultInstance());
            Map<String, Long> m = new LinkedHashMap<>();
            m.put("active_generations", s.getActiveGenerations());
            m.put("started_total", s.getStartedTotal());
            m.put("completed_total", s.getCompletedTotal());
            m.put("cancelled_total", s.getCancelledTotal());
            m.put("faulted_total", s.getFaultedTotal());
            m.put("rejected_total", s.getRejectedTotal());
            m.put("chunks_emitted_total", s.getChunksEmittedTotal());
            m.put("flow_control_pauses_total", s.getFlowControlPausesTotal());
            return new StatsResult(m, null);
        } catch (RuntimeException ex) {
            return StatsResult.unavailable(ex.getClass().getSimpleName() + ": " + ex.getMessage());
        } finally {
            close(channel);
        }
    }

    private static void close(ManagedChannel channel) {
        channel.shutdownNow();
        try {
            if (!channel.awaitTermination(5, TimeUnit.SECONDS)) {
                channel.shutdownNow();
            }
        } catch (InterruptedException ex) {
            Thread.currentThread().interrupt();
        }
    }
}
