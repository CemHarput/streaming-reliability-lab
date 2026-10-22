package io.github.cemharput.streamlab.simulator;

import io.github.cemharput.streamlab.tokenstream.v1.GenerateRequest;
import io.github.cemharput.streamlab.tokenstream.v1.TokenChunk;
import io.github.cemharput.streamlab.tokenstream.v1.TokenStreamServiceGrpc;
import io.grpc.Status;
import io.grpc.stub.ServerCallStreamObserver;
import io.grpc.stub.StreamObserver;
import java.util.Optional;
import java.util.concurrent.ScheduledExecutorService;

final class TokenStreamServiceImpl extends TokenStreamServiceGrpc.TokenStreamServiceImplBase {

    private final ScheduledExecutorService scheduler;
    private final SimulatorMetrics metrics;
    private final RequestLimits limits;

    TokenStreamServiceImpl(ScheduledExecutorService scheduler, SimulatorMetrics metrics, RequestLimits limits) {
        this.scheduler = scheduler;
        this.metrics = metrics;
        this.limits = limits;
    }

    @Override
    public void generateTokens(GenerateRequest request, StreamObserver<TokenChunk> responseObserver) {
        Optional<String> violation = limits.violation(request);
        if (violation.isPresent()) {
            metrics.requestRejected();
            responseObserver.onError(Status.INVALID_ARGUMENT.withDescription(violation.get()).asRuntimeException());
            return;
        }
        var observer = (ServerCallStreamObserver<TokenChunk>) responseObserver;
        new Generation(request, observer, scheduler, metrics).start();
    }
}
