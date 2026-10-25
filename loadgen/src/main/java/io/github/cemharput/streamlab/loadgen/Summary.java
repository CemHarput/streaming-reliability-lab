package io.github.cemharput.streamlab.loadgen;

import io.github.cemharput.streamlab.loadgen.client.Outcome;
import io.github.cemharput.streamlab.loadgen.client.StreamResult;
import io.github.cemharput.streamlab.loadgen.json.JsonLine;
import java.nio.ByteBuffer;
import java.util.Base64;
import java.util.EnumMap;
import java.util.Map;
import org.HdrHistogram.ConcurrentHistogram;
import org.HdrHistogram.Histogram;

/**
 * Convenience aggregation of one run: counts per outcome and percentiles. The raw JSONL is the result; this is
 * only a quick look. Outcome counts and the ttfc/total histograms are touched by the writer thread only; the
 * inter-chunk histogram is fed from HttpClient threads and is therefore concurrent.
 */
public final class Summary {

    public static final String LABEL = "CONVENIENCE SUMMARY - not a benchmark result";

    private static final double[] PERCENTILES = {50, 90, 99, 99.9};
    private static final String[] PERCENTILE_NAMES = {"P50", "P90", "P99", "P999"};

    private final Map<Outcome, Long> counts = new EnumMap<>(Outcome.class);
    private final Histogram ttfc = new Histogram(3);
    private final Histogram total = new Histogram(3);
    private final ConcurrentHistogram interChunk = new ConcurrentHistogram(3);

    /** Thread safe; used as the inter-chunk sink of the client. */
    public void recordInterChunk(long nanos) {
        interChunk.recordValue(Math.max(0, nanos));
    }

    /** Writer thread only. */
    void record(StreamResult r) {
        counts.merge(r.outcome(), 1L, Long::sum);
        if (r.outcome() == Outcome.COMPLETED) {
            if (r.ttfcNanos() != null) {
                ttfc.recordValue(Math.max(0, r.ttfcNanos()));
            }
            total.recordValue(Math.max(0, r.totalNanos()));
        }
    }

    long count(Outcome outcome) {
        return counts.getOrDefault(outcome, 0L);
    }

    long interChunkCount() {
        return interChunk.getTotalCount();
    }

    /** Compressed HdrHistogram, Base64, nanoseconds. */
    String encodedInterChunk() {
        ByteBuffer buf = ByteBuffer.allocate(interChunk.getNeededByteBufferCapacity());
        interChunk.encodeIntoCompressedByteBuffer(buf);
        buf.flip();
        byte[] bytes = new byte[buf.remaining()];
        buf.get(bytes);
        return Base64.getEncoder().encodeToString(bytes);
    }

    /** Run facts that belong to the summary but are not aggregated here. */
    public record RunFacts(String target, int concurrency, long elapsedNanos, String stopReason, long requestsStarted,
            long recordsWritten) {
    }

    public String toText(RunFacts f) {
        StringBuilder sb = new StringBuilder();
        sb.append("=== ").append(LABEL).append(" ===\n");
        sb.append("Closed model; latency is measured from the actual send, coordinated omission is NOT corrected.\n");
        sb.append("The raw JSONL file is the result. Percentiles are over COMPLETED requests only (inter-chunk: ")
                .append("all streams).\n");
        sb.append(String.format("target=%s concurrency=%d elapsed=%.3fs stopReason=%s started=%d recorded=%d%n",
                f.target(), f.concurrency(), f.elapsedNanos() / 1e9, f.stopReason(), f.requestsStarted(),
                f.recordsWritten()));
        sb.append("outcomes:");
        for (Outcome o : Outcome.values()) {
            sb.append(' ').append(o).append('=').append(count(o));
        }
        sb.append('\n');
        appendHistogram(sb, "ttfc", ttfc);
        appendHistogram(sb, "total", total);
        appendHistogram(sb, "interChunk", interChunk);
        return sb.toString();
    }

    public String toJson(RunFacts f) {
        JsonLine j = new JsonLine()
                .field("type", "summary")
                .field("schemaVersion", 1L)
                .field("label", LABEL)
                .field("target", f.target())
                .field("concurrency", f.concurrency())
                .field("elapsedNanos", f.elapsedNanos())
                .field("stopReason", f.stopReason())
                .field("requestsStarted", f.requestsStarted())
                .field("recordsWritten", f.recordsWritten());
        for (Outcome o : Outcome.values()) {
            j.field("outcome_" + o, count(o));
        }
        jsonHistogram(j, "ttfc", ttfc);
        jsonHistogram(j, "total", total);
        jsonHistogram(j, "interChunk", interChunk);
        return j.toString();
    }

    private static void appendHistogram(StringBuilder sb, String name, Histogram h) {
        sb.append(String.format("%-10s (us) count=%d", name, h.getTotalCount()));
        if (h.getTotalCount() > 0) {
            for (int i = 0; i < PERCENTILES.length; i++) {
                sb.append(' ').append(PERCENTILE_NAMES[i].toLowerCase()).append('=')
                        .append(h.getValueAtPercentile(PERCENTILES[i]) / 1000);
            }
            sb.append(" max=").append(h.getMaxValue() / 1000);
        }
        sb.append('\n');
    }

    private static void jsonHistogram(JsonLine j, String name, Histogram h) {
        j.field(name + "Count", h.getTotalCount());
        for (int i = 0; i < PERCENTILES.length; i++) {
            String field = name + PERCENTILE_NAMES[i] + "Us";
            if (h.getTotalCount() > 0) {
                j.field(field, Long.valueOf(h.getValueAtPercentile(PERCENTILES[i]) / 1000));
            } else {
                j.nullField(field);
            }
        }
        if (h.getTotalCount() > 0) {
            j.field(name + "MaxUs", Long.valueOf(h.getMaxValue() / 1000));
        } else {
            j.nullField(name + "MaxUs");
        }
    }
}
