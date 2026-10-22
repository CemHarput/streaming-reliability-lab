package io.github.cemharput.streamlab.simulator;

import io.github.cemharput.streamlab.tokenstream.v1.FaultType;
import io.github.cemharput.streamlab.tokenstream.v1.GenerateRequest;
import io.github.cemharput.streamlab.tokenstream.v1.StreamProfile;
import java.util.Optional;

/**
 * Upper bounds on a single request. A synthetic producer must still refuse
 * inputs that would make one call unbounded in time or memory.
 */
public record RequestLimits(
        int maxRequestIdLength,
        int maxChunkCount,
        int maxDelayMs,
        int maxChunkSizeBytes
) {

    public static final RequestLimits DEFAULT = new RequestLimits(128, 1_000_000, 60_000, 64 * 1024);

    /** Returns a human-readable reason when the request is invalid. */
    Optional<String> violation(GenerateRequest request) {
        String requestId = request.getRequestId();
        if (requestId.isBlank()) {
            return Optional.of("request_id is required");
        }
        if (requestId.length() > maxRequestIdLength) {
            return Optional.of("request_id longer than " + maxRequestIdLength);
        }
        StreamProfile profile = request.getProfile();
        if (profile.getChunkCount() == 0 || Integer.compareUnsigned(profile.getChunkCount(), maxChunkCount) > 0) {
            return Optional.of("profile.chunk_count must be in [1, " + maxChunkCount + "]");
        }
        if (Integer.compareUnsigned(profile.getFirstChunkDelayMs(), maxDelayMs) > 0
                || Integer.compareUnsigned(profile.getChunkIntervalMs(), maxDelayMs) > 0) {
            return Optional.of("delays must be <= " + maxDelayMs + " ms");
        }
        if (Integer.compareUnsigned(profile.getChunkSizeBytes(), maxChunkSizeBytes) > 0) {
            return Optional.of("profile.chunk_size_bytes must be <= " + maxChunkSizeBytes);
        }
        if (Integer.compareUnsigned(request.getResumeFromSeq(), profile.getChunkCount()) >= 0) {
            return Optional.of("resume_from_seq must be < chunk_count");
        }
        if (request.getFault().getType() == FaultType.UNRECOGNIZED) {
            return Optional.of("unknown fault type");
        }
        return Optional.empty();
    }
}
