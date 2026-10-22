package io.github.cemharput.streamlab.simulator;

import io.github.cemharput.streamlab.tokenstream.v1.GetStatsRequest;
import io.github.cemharput.streamlab.tokenstream.v1.SimulatorAdminServiceGrpc;
import io.github.cemharput.streamlab.tokenstream.v1.SimulatorStats;
import io.grpc.stub.StreamObserver;

final class SimulatorAdminServiceImpl extends SimulatorAdminServiceGrpc.SimulatorAdminServiceImplBase {

    private final SimulatorMetrics metrics;

    SimulatorAdminServiceImpl(SimulatorMetrics metrics) {
        this.metrics = metrics;
    }

    @Override
    public void getStats(GetStatsRequest request, StreamObserver<SimulatorStats> responseObserver) {
        responseObserver.onNext(metrics.snapshot());
        responseObserver.onCompleted();
    }
}
