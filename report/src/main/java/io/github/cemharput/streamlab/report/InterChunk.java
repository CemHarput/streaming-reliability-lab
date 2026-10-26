package io.github.cemharput.streamlab.report;

import java.nio.ByteBuffer;
import java.util.Base64;
import java.util.Map;
import org.HdrHistogram.Histogram;

/**
 * The inter-chunk gap distribution of a run: the HdrHistogram that the load generator wrote into the JSONL footer
 * (compressed, Base64, nanoseconds). The values are the library's own results: three significant digits, so about
 * 0.1 % resolution, bucket-equivalent upper values.
 */
public final class InterChunk {

    /** Decoded values in nanoseconds; {@code count} is the number of gaps (all streams of the run). */
    public record Result(long count, Long p50, Long p90, Long p99, Long max) {
    }

    private InterChunk() {
    }

    /**
     * Decodes the histogram of a footer. Returns null when the footer lacks the fields, the unit is not
     * {@code ns}, the text cannot be decoded, or the histogram total differs from {@code interChunkCount}.
     */
    public static Result decode(Map<String, Object> footer) {
        if (footer == null) {
            return null;
        }
        String encoded = Jv.str(footer.get("interChunkHistogram"));
        Long count = Jv.lng(footer.get("interChunkCount"));
        if (encoded == null || count == null || !"ns".equals(footer.get("interChunkHistogramUnit"))) {
            return null;
        }
        try {
            byte[] bytes = Base64.getDecoder().decode(encoded);
            Histogram h = Histogram.decodeFromCompressedByteBuffer(ByteBuffer.wrap(bytes), 0);
            if (h.getTotalCount() != count) {
                return null;
            }
            if (count == 0) {
                return new Result(0, null, null, null, null);
            }
            return new Result(count, h.getValueAtPercentile(50), h.getValueAtPercentile(90),
                    h.getValueAtPercentile(99), h.getMaxValue());
        } catch (Exception ex) {
            return null;
        }
    }
}
