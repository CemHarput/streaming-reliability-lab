package io.github.cemharput.streamlab.report;

import io.github.cemharput.streamlab.bench.json.Json;
import io.github.cemharput.streamlab.report.CellComparison.Cell;
import io.github.cemharput.streamlab.report.CellComparison.Entry;
import io.github.cemharput.streamlab.report.CellComparison.ModeStats;
import io.github.cemharput.streamlab.report.Percentiles.Dist;
import io.github.cemharput.streamlab.report.RunAssessment.Analysed;
import io.github.cemharput.streamlab.report.RunAssessment.Analysis;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * {@code analysis.json}. Counters and nanosecond values are numbers (Long); every decimal is written as a string,
 * because {@code Json.write} would truncate a BigDecimal to a long.
 */
public final class AnalysisJson {

    private AnalysisJson() {
    }

    public static String write(Analysis analysis, List<Cell> cells) {
        Map<String, Object> root = new LinkedHashMap<>();
        root.put("analysisVersion", (long) ReportMain.ANALYSIS_VERSION);
        root.put("runId", analysis.manifest().runId());
        root.put("setStatus", analysis.manifest().status());
        root.put("preset", analysis.manifest().preset());
        root.put("modeOrder", new ArrayList<Object>(CellComparison.modeOrder(analysis)));
        Map<String, Object> counts = new LinkedHashMap<>();
        counts.put("runs", (long) analysis.runs().size());
        counts.put("usable", analysis.count(RunAssessment.USABLE));
        counts.put("caution", analysis.count(RunAssessment.CAUTION));
        counts.put("excluded", analysis.count(RunAssessment.EXCLUDED));
        root.put("counts", counts);
        root.put("unknownFlags", new ArrayList<Object>(analysis.unknownFlags()));
        root.put("setReasons", reasons(analysis.setReasons()));
        List<Object> runs = new ArrayList<>();
        for (Analysed a : analysis.runs()) {
            runs.add(run(a));
        }
        root.put("runs", runs);
        List<Object> cellList = new ArrayList<>();
        for (Cell c : cells) {
            cellList.add(cell(c));
        }
        root.put("cells", cellList);
        return Json.write(root);
    }

    private static List<Object> reasons(List<RunAssessment.Reason> reasons) {
        List<Object> out = new ArrayList<>();
        for (RunAssessment.Reason r : reasons) {
            Map<String, Object> m = new LinkedHashMap<>();
            m.put("source", r.source());
            m.put("code", r.code());
            m.put("class", r.severity().label());
            out.add(m);
        }
        return out;
    }

    private static Map<String, Object> run(Analysed a) {
        ManifestView.RunView run = a.run();
        Map<String, Object> m = new LinkedHashMap<>();
        m.put("index", run.index());
        m.put("id", run.id());
        m.put("mode", run.mode());
        m.put("concurrency", (long) run.concurrency());
        m.put("repetition", run.repetition());
        m.put("orderLabel", run.orderLabel());
        m.put("status", run.status());
        m.put("class", a.assessment().classification());
        m.put("reasons", reasons(a.assessment().reasons()));
        RunMetrics x = a.metrics();
        if (x == null) {
            m.put("metrics", null);
            return m;
        }
        Map<String, Object> metrics = new LinkedHashMap<>();
        metrics.put("requestLines", x.requestLines());
        metrics.put("completed", x.completed());
        metrics.put("rejected", x.rejected());
        Map<String, Object> outcomes = new LinkedHashMap<>();
        x.outcomes().forEach(outcomes::put);
        metrics.put("outcomes", outcomes);
        metrics.put("rejectedSharePercent", str(x.rejectedShare()));
        metrics.put("completedPerSecond", str(x.completedPerSecond()));
        metrics.put("completedInWindow", x.completedInWindow());
        metrics.put("windowMs", x.windowMs());
        metrics.put("footerElapsedNanos", x.footerElapsedNanos());
        metrics.put("ttfbNanos", dist(x.ttfb()));
        metrics.put("ttfcNanos", dist(x.ttfc()));
        metrics.put("totalNanos", dist(x.total()));
        if (x.interChunk() == null) {
            metrics.put("interChunkNanos", null);
        } else {
            Map<String, Object> ic = new LinkedHashMap<>();
            ic.put("count", x.interChunk().count());
            ic.put("p50", x.interChunk().p50());
            ic.put("p90", x.interChunk().p90());
            ic.put("p99", x.interChunk().p99());
            ic.put("max", x.interChunk().max());
            metrics.put("interChunkNanos", ic);
        }
        Map<String, Object> gen = new LinkedHashMap<>();
        gen.put("count", x.generatorDelayCount());
        gen.put("p99", x.generatorDelayP99());
        gen.put("max", x.generatorDelayMax());
        metrics.put("generatorDelayNanos", gen);
        RunMetrics.Snapshots s = x.snapshots();
        if (!s.available()) {
            metrics.put("snapshots", null);
        } else {
            Map<String, Object> snap = new LinkedHashMap<>();
            Map<String, Object> gw = new LinkedHashMap<>();
            s.gatewayDeltas().forEach((k, v) -> gw.put(k, str(v)));
            snap.put("gatewayCounterDeltas", gw);
            Map<String, Object> sim = new LinkedHashMap<>();
            s.simulatorDeltas().forEach((k, v) -> sim.put(k, str(v)));
            snap.put("simulatorCounterDeltas", sim);
            Map<String, Object> gauges = new LinkedHashMap<>();
            s.gatewayGauges().forEach((k, g) -> gauges.put(k, gauge(g)));
            snap.put("gatewayGauges", gauges);
            snap.put("simulatorActiveGenerations", s.simulatorActive() == null ? null : gauge(s.simulatorActive()));
            metrics.put("snapshots", snap);
        }
        m.put("metrics", metrics);
        return m;
    }

    private static Map<String, Object> gauge(RunMetrics.Gauge g) {
        Map<String, Object> m = new LinkedHashMap<>();
        m.put("pre", g.pre() == null ? null : Fmt.num(BigDecimal.valueOf(g.pre())));
        m.put("post", g.post() == null ? null : Fmt.num(BigDecimal.valueOf(g.post())));
        return m;
    }

    private static Map<String, Object> dist(Dist d) {
        Map<String, Object> m = new LinkedHashMap<>();
        m.put("count", d.count());
        m.put("missing", d.missing());
        m.put("p50", d.p50());
        m.put("p90", d.p90());
        m.put("p99", d.p99());
        m.put("max", d.max());
        return m;
    }

    private static Map<String, Object> cell(Cell c) {
        Map<String, Object> m = new LinkedHashMap<>();
        m.put("concurrency", (long) c.concurrency());
        m.put("metric", c.metric().code());
        m.put("statement", c.decision().statement().name());
        m.put("withoutCautionStatement", c.withoutCaution() == null ? null : c.withoutCaution().name());
        m.put("differenceOfMedians", c.decision().difference() == null ? null
                : c.metric().exact(c.decision().difference()));
        m.put("largestWithinModeRange", c.decision().spread() == null ? null : c.metric().exact(c.decision().spread()));
        Map<String, Object> modes = new LinkedHashMap<>();
        for (String mode : c.modes()) {
            ModeStats s = c.stats().get(mode);
            Map<String, Object> mm = new LinkedHashMap<>();
            mm.put("n", (long) s.n());
            mm.put("median", s.median() == null ? null : c.metric().exact(s.median()));
            mm.put("min", s.min() == null ? null : c.metric().exact(s.min()));
            mm.put("max", s.max() == null ? null : c.metric().exact(s.max()));
            List<Object> es = new ArrayList<>();
            for (Entry e : c.entries().get(mode)) {
                Map<String, Object> em = new LinkedHashMap<>();
                em.put("index", e.index());
                em.put("repetition", e.repetition());
                em.put("position", e.position());
                em.put("caution", e.caution());
                em.put("value", c.metric().exact(e.value()));
                es.add(em);
            }
            mm.put("runs", es);
            modes.put(mode, mm);
        }
        m.put("modes", modes);
        return m;
    }

    private static String str(BigDecimal v) {
        return v == null ? null : v.stripTrailingZeros().toPlainString();
    }
}
