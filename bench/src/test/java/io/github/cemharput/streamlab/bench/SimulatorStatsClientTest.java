package io.github.cemharput.streamlab.bench;

import static org.assertj.core.api.Assertions.assertThat;

import io.github.cemharput.streamlab.tokenstream.v1.GetStatsRequest;
import io.github.cemharput.streamlab.tokenstream.v1.SimulatorAdminServiceGrpc;
import io.github.cemharput.streamlab.tokenstream.v1.SimulatorStats;
import io.grpc.Grpc;
import io.grpc.InsecureServerCredentials;
import io.grpc.Server;
import io.grpc.stub.StreamObserver;
import java.net.ServerSocket;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.Timeout.ThreadMode;

@Timeout(value = 60, unit = TimeUnit.SECONDS, threadMode = ThreadMode.SEPARATE_THREAD)
class SimulatorStatsClientTest {

    @Test
    void readsAllEightFieldsFromGetStats() throws Exception {
        Server server = Grpc.newServerBuilderForPort(0, InsecureServerCredentials.create())
                .addService(new SimulatorAdminServiceGrpc.SimulatorAdminServiceImplBase() {
                    @Override
                    public void getStats(GetStatsRequest request, StreamObserver<SimulatorStats> observer) {
                        observer.onNext(SimulatorStats.newBuilder().setActiveGenerations(1).setStartedTotal(2)
                                .setCompletedTotal(3).setCancelledTotal(4).setFaultedTotal(5).setRejectedTotal(6)
                                .setChunksEmittedTotal(7).setFlowControlPausesTotal(8).build());
                        observer.onCompleted();
                    }
                }).build().start();
        try {
            SimulatorApi.StatsResult r = new SimulatorStatsClient(server.getPort()).stats();
            assertThat(r.available()).isTrue();
            assertThat(r.values().keySet()).containsExactlyElementsOf(SimulatorApi.FIELDS);
            assertThat(r.values().values()).containsExactly(1L, 2L, 3L, 4L, 5L, 6L, 7L, 8L);
        } finally {
            server.shutdownNow();
            server.awaitTermination(10, TimeUnit.SECONDS);
        }
    }

    @Test
    void unavailableWhenNothingListens() throws Exception {
        int port;
        try (ServerSocket s = new ServerSocket(0)) {
            port = s.getLocalPort();
        }
        SimulatorApi.StatsResult r = new SimulatorStatsClient(port).stats();
        assertThat(r.available()).isFalse();
        assertThat(r.error()).isNotBlank();
    }
}
