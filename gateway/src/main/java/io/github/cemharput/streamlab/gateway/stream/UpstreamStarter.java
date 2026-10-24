package io.github.cemharput.streamlab.gateway.stream;

import io.github.cemharput.streamlab.tokenstream.v1.GenerateRequest;
import io.github.cemharput.streamlab.tokenstream.v1.TokenChunk;
import io.grpc.stub.ClientResponseObserver;
import java.time.Duration;

/**
 * Starts one upstream {@code GenerateTokens} call. The bridge calls it for the first call and for
 * every retry; tests replace it with a fake.
 */
@FunctionalInterface
interface UpstreamStarter {

    /** Starts the call with the given gRPC deadline; {@code observer.beforeStart} runs on the calling thread. */
    void start(GenerateRequest request, Duration deadline, ClientResponseObserver<GenerateRequest, TokenChunk> observer);
}
