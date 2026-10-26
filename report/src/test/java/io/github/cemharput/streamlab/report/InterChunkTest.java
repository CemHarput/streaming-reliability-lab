package io.github.cemharput.streamlab.report;

import static org.assertj.core.api.Assertions.assertThat;

import java.nio.ByteBuffer;
import java.util.Base64;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import org.HdrHistogram.ConcurrentHistogram;
import org.HdrHistogram.Histogram;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.Timeout.ThreadMode;

@Timeout(value = 30, unit = TimeUnit.SECONDS, threadMode = ThreadMode.SEPARATE_THREAD)
class InterChunkTest {

    /** The same encoding as loadgen's Summary: ConcurrentHistogram(3), compressed, Base64. */
    static String encode(long... values) {
        ConcurrentHistogram h = new ConcurrentHistogram(3);
        for (long v : values) {
            h.recordValue(v);
        }
        ByteBuffer buf = ByteBuffer.allocate(h.getNeededByteBufferCapacity());
        h.encodeIntoCompressedByteBuffer(buf);
        buf.flip();
        byte[] bytes = new byte[buf.remaining()];
        buf.get(bytes);
        return Base64.getEncoder().encodeToString(bytes);
    }

    static Map<String, Object> footer(long count, String unit, String encoded) {
        Map<String, Object> f = new LinkedHashMap<>();
        f.put("type", "footer");
        f.put("interChunkCount", count);
        f.put("interChunkHistogramUnit", unit);
        f.put("interChunkHistogram", encoded);
        return f;
    }

    @Test
    void oneToTenNanosecondsGiveTheHandComputedValues() {
        // 1..10 ns: p50 is the 5th value, p90 the 9th, p99 the 10th; the maximum is 10. The sub-bucket resolution
        // at these magnitudes is exact, so the library and the nearest-rank rule agree.
        InterChunk.Result r = InterChunk.decode(footer(10, "ns", encode(1, 2, 3, 4, 5, 6, 7, 8, 9, 10)));
        assertThat(r.count()).isEqualTo(10);
        assertThat(r.p50()).isEqualTo(5);
        assertThat(r.p90()).isEqualTo(9);
        assertThat(r.p99()).isEqualTo(10);
        assertThat(r.max()).isEqualTo(10);
    }

    @Test
    void aConcurrentHistogramEncodingIsReadByThePlainDecoder() throws Exception {
        String encoded = encode(40_000_000L, 41_000_000L);
        Histogram h = Histogram.decodeFromCompressedByteBuffer(ByteBuffer.wrap(Base64.getDecoder().decode(encoded)), 0);
        assertThat(h.getTotalCount()).isEqualTo(2);
        InterChunk.Result r = InterChunk.decode(footer(2, "ns", encoded));
        assertThat(r.p50()).isBetween(39_900_000L, 40_100_000L); // three significant digits
        assertThat(r.max()).isBetween(40_900_000L, 41_100_000L);
    }

    @Test
    void anEmptyHistogramWithCountZeroIsAvailableButHasNoPercentiles() {
        InterChunk.Result r = InterChunk.decode(footer(0, "ns", encode()));
        assertThat(r.count()).isZero();
        assertThat(r.p50()).isNull();
    }

    @Test
    void brokenOrInconsistentFootersAreNotAvailable() {
        assertThat(InterChunk.decode(null)).isNull();
        assertThat(InterChunk.decode(footer(10, "ns", "!!!not base64!!!"))).isNull();
        assertThat(InterChunk.decode(footer(10, "ns", Base64.getEncoder().encodeToString("garbage".getBytes())))).isNull();
        assertThat(InterChunk.decode(footer(3, "ns", encode(1, 2)))).isNull(); // count differs from the histogram
        assertThat(InterChunk.decode(footer(2, "us", encode(1, 2)))).isNull(); // unit
        Map<String, Object> noHistogram = footer(2, "ns", null);
        noHistogram.remove("interChunkHistogram");
        assertThat(InterChunk.decode(noHistogram)).isNull();
    }
}
