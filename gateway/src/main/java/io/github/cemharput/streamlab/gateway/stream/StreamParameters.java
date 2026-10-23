package io.github.cemharput.streamlab.gateway.stream;

import io.github.cemharput.streamlab.tokenstream.v1.FaultSpec;
import io.github.cemharput.streamlab.tokenstream.v1.GenerateRequest;
import io.github.cemharput.streamlab.tokenstream.v1.StreamProfile;

/**
 * Query parameters of GET /stream. Range validation is left to the simulator, except
 * {@code faultAtSeq}, which the controller rejects when negative because the proto
 * field is unsigned and a wrapped value would silently never fire.
 */
public record StreamParameters(
        int chunks,
        int firstDelayMs,
        int intervalMs,
        FaultParam fault,
        int faultAtSeq,
        long seed) {

    public GenerateRequest toRequest(String requestId) {
        return GenerateRequest.newBuilder()
                .setRequestId(requestId)
                .setSeed(seed)
                .setProfile(StreamProfile.newBuilder()
                        .setChunkCount(chunks)
                        .setFirstChunkDelayMs(firstDelayMs)
                        .setChunkIntervalMs(intervalMs))
                .setFault(FaultSpec.newBuilder()
                        .setType(fault.protoType())
                        .setAtSeq(faultAtSeq))
                .build();
    }
}
