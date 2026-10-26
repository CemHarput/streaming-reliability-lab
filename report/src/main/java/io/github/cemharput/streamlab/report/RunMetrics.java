package io.github.cemharput.streamlab.report;

import io.github.cemharput.streamlab.bench.GatewayProbe;
import io.github.cemharput.streamlab.report.Checks.Finding;
import io.github.cemharput.streamlab.report.Percentiles.Dist;
import io.github.cemharput.streamlab.report.ResultSetReader.RunData;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * The metrics of one measured phase, with the definitions of the report (section "Metric definitions"):
 * outcome counts and shares over the request lines; completed streams per second over the window
 * {@code [rampUp, duration)}; exact nearest-rank percentiles of COMPLETED records; the footer inter-chunk
 * histogram; snapshot deltas (post minus pre, same JVM). Every value is exact integer or BigDecimal arithmetic.
 */
public record RunMetrics(
        long requestLines,
        Map<String, Long> outcomes,
        long completed,
        long rejected,
        Dist ttfb,
        Dist ttfc,
        Dist total,
        InterChunk.Result interChunk,
        BigDecimal completedPerSecond,
        Long completedInWindow,
        Long windowMs,
        Long footerElapsedNanos,
        long generatorDelayCount,
        Long generatorDelayP99,
        Long generatorDelayMax,
        BigDecimal rejectedShare,
        Snapshots snapshots,
        List<Finding> findings) {

    /** The outcome names of loadgen's {@code Outcome} enum, in its order (a test pins this list to the enum). */
    public static final List<String> KNOWN_OUTCOMES = List.of("COMPLETED", "ERROR_EVENT", "REJECTED", "HTTP_ERROR",
            "PROTOCOL_ERROR", "NO_TERMINAL", "RESET", "TIMEOUT", "CONNECT_FAILURE", "INTERRUPTED", "CLIENT_ERROR");

    /** A gauge before and after the measured phase. */
    public record Gauge(Double pre, Double post) {
    }

    /**
     * Snapshot based values. Counter deltas are null per entry when a side is missing; a counter that decreased is
     * kept as the negative delta (never clipped).
     */
    public record Snapshots(boolean available, Map<String, BigDecimal> gatewayDeltas,
            Map<String, BigDecimal> simulatorDeltas, Map<String, Gauge> gatewayGauges, Gauge simulatorActive) {
    }

    private static final Map<String, String> GATEWAY_COUNTERS = new LinkedHashMap<>();
    private static final Map<String, String> GATEWAY_GAUGES = new LinkedHashMap<>();
    public static final List<String> SIMULATOR_COUNTERS = List.of("started_total", "completed_total",
            "cancelled_total", "faulted_total", "rejected_total", "chunks_emitted_total",
            "flow_control_pauses_total");

    static {
        GATEWAY_COUNTERS.put("rejected_total", GatewayProbe.REJECTED_TOTAL);
        for (String r : GatewayProbe.REJECTED_REASONS) {
            GATEWAY_COUNTERS.put("rejected_total{reason=" + r + "}", GatewayProbe.rejectedPath(r));
        }
        for (String k : GatewayProbe.TIMEOUT_KINDS) {
            GATEWAY_COUNTERS.put("timeouts_total{kind=" + k + "}", GatewayProbe.timeoutsPath(k));
        }
        for (String r : GatewayProbe.RETRY_REASONS) {
            GATEWAY_COUNTERS.put("retries_total{reason=" + r + "}", GatewayProbe.retriesPath(r));
        }
        GATEWAY_COUNTERS.put("retry_exhausted_total", GatewayProbe.RETRY_EXHAUSTED);
        GATEWAY_GAUGES.put("active_streams", GatewayProbe.ACTIVE_STREAMS);
        GATEWAY_GAUGES.put("queue_depth{queue=chunks}", GatewayProbe.QUEUE_CHUNKS);
        GATEWAY_GAUGES.put("queue_depth{queue=admission}", GatewayProbe.QUEUE_ADMISSION);
        GATEWAY_GAUGES.put("jvm.memory.used{area=heap}", "/actuator/metrics/jvm.memory.used?tag=area:heap");
        GATEWAY_GAUGES.put("jvm.threads.live", "/actuator/metrics/jvm.threads.live");
        GATEWAY_GAUGES.put("process.cpu.usage", "/actuator/metrics/process.cpu.usage");
        GATEWAY_GAUGES.put("system.cpu.usage", "/actuator/metrics/system.cpu.usage");
    }

    public static List<String> gatewayCounterLabels() {
        return List.copyOf(GATEWAY_COUNTERS.keySet());
    }

    public static List<String> gatewayGaugeLabels() {
        return List.copyOf(GATEWAY_GAUGES.keySet());
    }

    /** Outcome names in report order: the known ones first (loadgen order), then unknown ones alphabetically. */
    public static List<String> orderOutcomes(Iterable<String> names) {
        java.util.TreeSet<String> seen = new java.util.TreeSet<>();
        names.forEach(seen::add);
        List<String> ordered = new ArrayList<>();
        for (String k : KNOWN_OUTCOMES) {
            if (seen.remove(k)) {
                ordered.add(k);
            }
        }
        ordered.addAll(seen);
        return ordered;
    }

    /** Null when the run has no analysable measured file. */
    public static RunMetrics compute(RunData data, ManifestView manifest) {
        JsonlScanner.Result r = data.measured();
        if (r == null || r.schemaUnsupported()) {
            return null;
        }
        List<Finding> findings = new ArrayList<>();
        Map<String, Long> ordered = new LinkedHashMap<>();
        for (String name : orderOutcomes(r.outcomes().keySet())) {
            ordered.put(name, r.outcomes().get(name));
        }
        long rejected = r.outcomes().getOrDefault("REJECTED", 0L);

        BigDecimal rate = null;
        Long windowMs = null;
        if (r.completedInWindow() != null) {
            windowMs = r.windowEndMs() - r.windowStartMs();
            rate = BigDecimal.valueOf(r.completedInWindow()).multiply(BigDecimal.valueOf(1000))
                    .divide(BigDecimal.valueOf(windowMs), 30, RoundingMode.HALF_EVEN);
        }
        Long elapsed = r.footer() == null ? null : Jv.lng(r.footer().get("elapsedNanos"));

        InterChunk.Result inter = InterChunk.decode(r.footer());
        if (inter == null) {
            findings.add(new Finding(Checks.INTER_CHUNK_UNAVAILABLE,
                    "the footer inter-chunk histogram is missing, undecodable or its count differs from interChunkCount"));
        }

        headerChecks(data, r, manifest, findings);
        countChecks(data, r, findings);

        Snapshots snaps = snapshots(data, findings);
        if (snaps.available() && snaps.gatewayDeltas().get("rejected_total") != null
                && snaps.gatewayDeltas().get("rejected_total").compareTo(BigDecimal.valueOf(rejected)) != 0) {
            findings.add(new Finding(Checks.REJECTED_METRIC_MISMATCH, "gateway rejected_total delta "
                    + Fmt.num(snaps.gatewayDeltas().get("rejected_total")) + " differs from " + rejected
                    + " REJECTED request lines"));
        }

        return new RunMetrics(r.requestLines(), Collections.unmodifiableMap(ordered), r.completed(), rejected,
                r.ttfb(), r.ttfc(), r.total(), inter, rate, r.completedInWindow(), windowMs, elapsed,
                r.generatorDelayCount(), r.generatorDelayP99(), r.generatorDelayMax(),
                Fmt.ratioPercent(rejected, r.requestLines()), snaps, List.copyOf(findings));
    }

    private static void headerChecks(RunData data, JsonlScanner.Result r, ManifestView manifest,
            List<Finding> findings) {
        if (r.header() == null) {
            return;
        }
        List<String> diffs = new ArrayList<>();
        Long hc = Jv.lng(r.header().get("concurrency"));
        if (hc != null && hc != data.run().concurrency()) {
            diffs.add("concurrency");
        }
        Long hd = Jv.lng(r.header().get("durationMs"));
        Long pd = manifest.planLong("durationMs");
        if (hd != null && pd != null && !hd.equals(pd)) {
            diffs.add("durationMs");
        }
        Long hr = Jv.lng(r.header().get("rampUpMs"));
        Long pr = manifest.planLong("rampUpMs");
        if (hr != null && pr != null && !hr.equals(pr)) {
            diffs.add("rampUpMs");
        }
        if (!diffs.isEmpty()) {
            findings.add(new Finding(Checks.HEADER_PARAMETERS_MISMATCH,
                    "the JSONL header differs from the manifest in: " + String.join(", ", diffs)));
        }
    }

    private static void countChecks(RunData data, JsonlScanner.Result r, List<Finding> findings) {
        List<String> diffs = new ArrayList<>();
        Long footerCount = r.footer() == null ? null : Jv.lng(r.footer().get("recordsWritten"));
        if (footerCount != null && footerCount != r.requestLines()) {
            diffs.add("footer recordsWritten " + footerCount + " vs " + r.requestLines() + " request lines");
        }
        ResultSetReader.SummaryData s = data.summary();
        if (s != null) {
            if (s.recordsWritten() != null && s.recordsWritten() != r.requestLines()) {
                diffs.add("summary recordsWritten " + s.recordsWritten() + " vs " + r.requestLines()
                        + " request lines");
            }
            compareOutcomes("summary", s.outcomes(), r.outcomes(), diffs);
        }
        Map<String, Long> manifestOutcomes = data.run().phase("measured").outcomes();
        if (!manifestOutcomes.isEmpty()) {
            compareOutcomes("manifest", manifestOutcomes, r.outcomes(), diffs);
        }
        if (!diffs.isEmpty()) {
            List<String> shown = diffs.size() > 5 ? diffs.subList(0, 5) : diffs;
            findings.add(new Finding(Checks.COUNT_MISMATCH, diffs.size() + " count difference(s): "
                    + String.join("; ", shown)));
        }
    }

    private static void compareOutcomes(String label, Map<String, Long> other, Map<String, Long> lines,
            List<String> diffs) {
        java.util.Set<String> names = new java.util.TreeSet<>(other.keySet());
        names.addAll(lines.keySet());
        for (String name : names) {
            long a = other.getOrDefault(name, 0L);
            long b = lines.getOrDefault(name, 0L);
            if (a != b) {
                diffs.add(label + " outcome " + name + " " + a + " vs " + b + " request lines");
            }
        }
    }

    private static Snapshots snapshots(RunData data, List<Finding> findings) {
        SnapshotReader.Data pre = data.pre();
        SnapshotReader.Data post = data.post();
        Map<String, BigDecimal> gw = new LinkedHashMap<>();
        Map<String, BigDecimal> sim = new LinkedHashMap<>();
        Map<String, Gauge> gauges = new LinkedHashMap<>();
        if (pre == null || post == null) {
            return new Snapshots(false, gw, sim, gauges, null);
        }
        List<String> decreased = new ArrayList<>();
        GATEWAY_COUNTERS.forEach((label, path) -> {
            BigDecimal d = delta(pre.gateway().get(path), post.gateway().get(path));
            gw.put(label, d);
            if (d != null && d.signum() < 0) {
                decreased.add("gateway " + label);
            }
        });
        GATEWAY_GAUGES.forEach((label, path) -> gauges.put(label,
                new Gauge(pre.gateway().get(path), post.gateway().get(path))));
        for (String name : SIMULATOR_COUNTERS) {
            BigDecimal d = pre.simulator() == null || post.simulator() == null ? null
                    : delta(pre.simulator().get(name) == null ? null : pre.simulator().get(name).doubleValue(),
                            post.simulator().get(name) == null ? null : post.simulator().get(name).doubleValue());
            sim.put(name, d);
            if (d != null && d.signum() < 0) {
                decreased.add("simulator " + name);
            }
        }
        Gauge simActive = null;
        if (pre.simulator() != null && post.simulator() != null) {
            Long a = pre.simulator().get("active_generations");
            Long b = post.simulator().get("active_generations");
            simActive = new Gauge(a == null ? null : a.doubleValue(), b == null ? null : b.doubleValue());
        }
        if (!decreased.isEmpty()) {
            findings.add(new Finding(Checks.COUNTER_DECREASED, "counter(s) decreased between the snapshots: "
                    + String.join(", ", decreased)));
        }
        return new Snapshots(true, gw, sim, gauges, simActive);
    }

    private static BigDecimal delta(Double pre, Double post) {
        if (pre == null || post == null) {
            return null;
        }
        return BigDecimal.valueOf(post).subtract(BigDecimal.valueOf(pre));
    }
}
