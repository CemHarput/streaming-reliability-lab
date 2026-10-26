package io.github.cemharput.streamlab.report.testsupport;

import io.github.cemharput.streamlab.bench.ConnectionMode;
import io.github.cemharput.streamlab.bench.GatewayProbe;
import io.github.cemharput.streamlab.bench.Manifest;
import io.github.cemharput.streamlab.bench.PlanBuilder;
import io.github.cemharput.streamlab.bench.json.Json;
import io.github.cemharput.streamlab.report.RunMetrics;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Base64;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.HdrHistogram.ConcurrentHistogram;

/**
 * Builds a synthetic result set (manifest, run directories, JSONL, summaries, snapshots) in the shape the bench
 * runner writes. All values are artificial. The user name in paths is always {@code fixtureuser}.
 */
public final class ResultSetBuilder {

    public static final String USER = "fixtureuser";
    public static final long MS = 1_000_000L;

    /** One request line. Offsets and durations in nanoseconds; null ttfb/ttfc are written as null. */
    public record Req(String outcome, long scheduledOffset, long startOffset, long total, Long ttfb, Long ttfc) {

        public static Req completed(long startOffsetMs, long ttfbMs, long ttfcMs, long totalMs) {
            return new Req("COMPLETED", startOffsetMs * MS, startOffsetMs * MS, totalMs * MS, ttfbMs * MS,
                    ttfcMs * MS);
        }

        public static Req rejected(long startOffsetMs) {
            return new Req("REJECTED", startOffsetMs * MS, startOffsetMs * MS, 1 * MS, 1 * MS, null);
        }

        public static Req of(String outcome, long startOffsetMs, long totalMs) {
            return new Req(outcome, startOffsetMs * MS, startOffsetMs * MS, totalMs * MS, null, null);
        }
    }

    /** What one run contains. All fields have defaults derived from the run's mode and repetition. */
    public static final class RunSpec {
        final PlanBuilder.PlannedRun planned;
        public String status = "completed";
        public List<String> runFlags = new ArrayList<>();
        public List<String> measuredFlags = new ArrayList<>();
        public List<String> warmupFlags = new ArrayList<>();
        public List<Req> requests;
        public long[] interChunkNanos;
        public Map<String, Object> footerOverrides = new LinkedHashMap<>();
        public Map<String, Object> headerOverrides = new LinkedHashMap<>();
        public Map<String, Object> summaryOverrides = new LinkedHashMap<>();
        public Map<String, Object> manifestRunOverrides = new LinkedHashMap<>();
        public Map<String, Double> preGateway = new LinkedHashMap<>();
        public Map<String, Double> postGateway = new LinkedHashMap<>();
        public Map<String, Long> preSimulator;
        public Map<String, Long> postSimulator;
        public boolean simulatorUnavailable;
        public List<String> extraJsonlLines = new ArrayList<>();
        public boolean reverseRequests;
        public boolean writeJsonl = true;
        public boolean writeFooter = true;
        public boolean writeHeader = true;
        public boolean writeSummary = true;
        public boolean writeSnapshots = true;
        public String stopMethod;
        public Integer stopExitCode;
        public List<Double> cpu = new ArrayList<>(List.of(0.05, 0.07, 0.06));
        public String dir;

        RunSpec(PlanBuilder.PlannedRun planned) {
            this.planned = planned;
        }

        public int index() {
            return planned.index();
        }

        public RunSpec status(String s) {
            this.status = s;
            return this;
        }

        public RunSpec measuredFlag(String f) {
            measuredFlags.add(f);
            return this;
        }

        public RunSpec runFlag(String f) {
            runFlags.add(f);
            return this;
        }

        public RunSpec warmupFlag(String f) {
            warmupFlags.add(f);
            return this;
        }

        public RunSpec requests(List<Req> r) {
            this.requests = new ArrayList<>(r);
            return this;
        }

        /** Replaces the default requests by completed ones with the given ttfc and total values in milliseconds. */
        public RunSpec completedMs(long[] ttfcMs, long[] totalMs) {
            List<Req> r = new ArrayList<>();
            for (int i = 0; i < ttfcMs.length; i++) {
                long start = 1500 + i * 500L;
                r.add(Req.completed(start, 1, ttfcMs[i], totalMs[i]));
            }
            this.requests = r;
            return this;
        }
    }

    private final Path root;
    private final List<RunSpec> runs = new ArrayList<>();
    private List<Integer> concurrency = List.of(4);
    private int repetitions = 2;
    private List<ConnectionMode> modeOrder = List.of(ConnectionMode.SHARED, ConnectionMode.PER_REQUEST);
    private long durationMs = 10_000;
    private long rampUpMs = 1_000;
    private String runId = "20260101T000000Z-abc1234-fixture";
    private String setStatus = "completed";
    private String preset = "default";
    private boolean linux;
    private boolean crlf;
    private List<String> setFlags = new ArrayList<>();
    private boolean built;
    private Long schemaVersion;
    private Long protocolVersion;
    private String tool;
    private final Map<String, Object> rootOverrides = new LinkedHashMap<>();
    private final List<String> removeRootKeys = new ArrayList<>();
    private boolean omitPlan;

    private ResultSetBuilder(Path root) {
        this.root = root;
    }

    /** A builder writing into {@code root} (created if needed); two repetitions, one concurrency of 4. */
    public static ResultSetBuilder at(Path root) {
        return new ResultSetBuilder(root);
    }

    public ResultSetBuilder concurrency(Integer... c) {
        this.concurrency = List.of(c);
        return this;
    }

    public ResultSetBuilder repetitions(int r) {
        this.repetitions = r;
        return this;
    }

    public ResultSetBuilder modeOrder(ConnectionMode a, ConnectionMode b) {
        this.modeOrder = List.of(a, b);
        return this;
    }

    public ResultSetBuilder runId(String id) {
        this.runId = id;
        return this;
    }

    public ResultSetBuilder setStatus(String s) {
        this.setStatus = s;
        return this;
    }

    public ResultSetBuilder preset(String p) {
        this.preset = p;
        return this;
    }

    public ResultSetBuilder linux() {
        this.linux = true;
        return this;
    }

    public ResultSetBuilder crlf() {
        this.crlf = true;
        return this;
    }

    public ResultSetBuilder setFlag(String f) {
        setFlags.add(f);
        return this;
    }

    public ResultSetBuilder schemaVersion(long v) {
        this.schemaVersion = v;
        return this;
    }

    public ResultSetBuilder protocolVersion(long v) {
        this.protocolVersion = v;
        return this;
    }

    public ResultSetBuilder tool(String t) {
        this.tool = t;
        return this;
    }

    public ResultSetBuilder rootKey(String key, Object value) {
        rootOverrides.put(key, value);
        return this;
    }

    public ResultSetBuilder removeRootKey(String key) {
        removeRootKeys.add(key);
        return this;
    }

    public ResultSetBuilder omitPlan() {
        this.omitPlan = true;
        return this;
    }

    public Path root() {
        return root;
    }

    /** The spec of run {@code index} (1 based); available after {@link #plan()} or the first call. */
    public RunSpec run(int index) {
        plan();
        return runs.get(index - 1);
    }

    public List<RunSpec> runs() {
        plan();
        return runs;
    }

    /** Fixes the plan (call after concurrency/repetitions/modeOrder) and creates the run specs. */
    public ResultSetBuilder plan() {
        if (runs.isEmpty()) {
            PlanBuilder.RunPlan plan = PlanBuilder.build(concurrency, repetitions, modeOrder);
            plan.runs().forEach(r -> runs.add(new RunSpec(r)));
        }
        return this;
    }

    // ------------------------------------------------------------------ writing

    public Path build() throws IOException {
        plan();
        if (built) {
            throw new IllegalStateException("built twice");
        }
        built = true;
        Files.createDirectories(root);
        Map<String, Object> m = Manifest.newRoot(runId, "0.1.0-SNAPSHOT", "2026-01-01T00:00:00Z");
        if (schemaVersion != null) {
            m.put("schemaVersion", schemaVersion);
        }
        if (protocolVersion != null) {
            m.put("protocolVersion", protocolVersion);
        }
        if (tool != null) {
            m.put("tool", tool);
        }
        m.put("status", setStatus);
        m.put("finishedAt", "2026-01-01T01:00:00Z");
        m.put("preset", preset);
        m.put("commandLine", home() + "/bench --operator-note secret");
        m.put("operatorNote", "note written by " + USER + " in " + home() + "/notes");
        m.put("operatorChecklist", checklist());
        m.put("environment", environment());
        m.put("artifacts", artifacts());
        m.put("parameters", parameters());
        List<Object> runList = new ArrayList<>();
        for (RunSpec spec : runs) {
            runList.add(manifestRun(spec));
        }
        m.put("runs", runList);
        m.put("flags", new ArrayList<>(setFlags));
        if (setStatus.equals("failed")) {
            m.put("harnessFailure", "failed in " + home() + "/somewhere");
        }
        m.putAll(rootOverrides);
        removeRootKeys.forEach(m::remove);
        write(root.resolve("manifest.json"), Json.write(m));
        for (RunSpec spec : runs) {
            writeRunFiles(spec);
        }
        return root;
    }

    private String home() {
        return linux ? "/home/" + USER + "/streamlab" : "C:\\Users\\" + USER + "\\streamlab";
    }

    private void write(Path file, String text) throws IOException {
        String content = crlf ? text.replace("\r\n", "\n").replace("\n", "\r\n") : text;
        Files.writeString(file, content, StandardCharsets.UTF_8);
    }

    private Map<String, Object> checklist() {
        Map<String, Object> c = new LinkedHashMap<>();
        c.put("source", "flag");
        Map<String, Object> answers = new LinkedHashMap<>();
        answers.put("otherAppsClosed", "yes");
        answers.put("onAcPower", "yes");
        answers.put("samePowerPlan", "yes");
        c.put("answers", answers);
        c.put("note", "fixture");
        return c;
    }

    private Map<String, Object> environment() {
        Map<String, Object> e = new LinkedHashMap<>();
        e.put("os", linux ? "Linux 6.1.0-18-amd64 amd64" : "Windows 11 10.0.26200 amd64");
        Map<String, Object> cpu = new LinkedHashMap<>();
        cpu.put("model", "Fixture CPU 9000");
        cpu.put("modelSource", linux ? "/proc/cpuinfo model name" : "reg query ProcessorNameString");
        cpu.put("logicalProcessors", 8L);
        cpu.put("physicalCores", 4L);
        e.put("cpu", cpu);
        Map<String, Object> mem = new LinkedHashMap<>();
        mem.put("totalBytes", 17_179_869_184L);
        mem.put("freeBytesAtStart", 8_589_934_592L);
        e.put("memory", mem);
        Map<String, Object> java = new LinkedHashMap<>();
        java.put("version", "26");
        java.put("vendor", "Fixture Vendor");
        java.put("vmName", "Fixture VM");
        java.put("vmVersion", "26+1");
        java.put("home", linux ? "/usr/lib/jvm/" + USER + "-jdk" : "C:\\Users\\" + USER + "\\jdk");
        e.put("java", java);
        Map<String, Object> git = new LinkedHashMap<>();
        git.put("commit", "abc1234abc1234abc1234abc1234abc1234abcd");
        git.put("shortCommit", "abc1234");
        git.put("dirty", false);
        git.put("commitTime", "2025-12-31T12:00:00Z");
        git.put("statusLines", new ArrayList<Object>());
        e.put("git", git);
        e.put("powerPlan", linux ? null : "Power Scheme GUID: 381b4222-f694-41f0-9685-ff5bb260df2e  (Balanced)");
        Map<String, Object> tw = new LinkedHashMap<>();
        tw.put("total", 3L);
        tw.put("gatewayPort", 1L);
        tw.put("simulatorPort", 0L);
        e.put("timeWaitAtStart", tw);
        e.put("removedEnvironmentVariables", new ArrayList<Object>());
        return e;
    }

    private Map<String, Object> artifact(String name, long size) {
        Map<String, Object> a = new LinkedHashMap<>();
        a.put("path", linux ? "/home/" + USER + "/streamlab/x/target/" + name
                : "C:\\Users\\" + USER + "\\streamlab\\x\\target\\" + name);
        a.put("sizeBytes", size);
        a.put("sha256", "0123456789abcdef".repeat(4));
        a.put("lastModified", "2026-01-01T00:00:00Z");
        return a;
    }

    private Map<String, Object> artifacts() {
        Map<String, Object> a = new LinkedHashMap<>();
        a.put("gatewayJar", artifact("gateway-0.1.0-SNAPSHOT.jar", 1000));
        a.put("loadgenJar", artifact("loadgen-0.1.0-SNAPSHOT.jar", 500));
        a.put("loadgenLib", new ArrayList<Object>(List.of(artifact("HdrHistogram-2.2.2.jar", 200))));
        a.put("simulatorClasspath", new ArrayList<Object>(List.of(artifact("simulator-0.1.0-SNAPSHOT.jar", 300))));
        a.put("benchJar", artifact("bench-0.1.0-SNAPSHOT.jar", 400));
        return a;
    }

    private Map<String, Object> parameters() {
        Map<String, Object> p = new LinkedHashMap<>();
        Map<String, Object> gateway = new LinkedHashMap<>();
        Map<String, Object> props = new LinkedHashMap<>();
        props.put("server.port", "8080");
        props.put("lab.limits.max-active-streams", "100");
        props.put("lab.connection-mode", "<per run: shared | per-request>");
        gateway.put("properties", props);
        p.put("gateway", gateway);
        Map<String, Object> jvm = new LinkedHashMap<>();
        jvm.put("gateway", new ArrayList<Object>(List.of("-Xms1g", "-Xmx1g", "-XX:+UseG1GC")));
        jvm.put("simulator", new ArrayList<Object>(List.of("-Xms256m", "-Xmx256m")));
        jvm.put("loadgen", new ArrayList<Object>(List.of("-Xms256m", "-Xmx256m")));
        p.put("jvm", jvm);
        Map<String, Object> loadgen = new LinkedHashMap<>();
        loadgen.put("target", "http://127.0.0.1:8080/stream");
        loadgen.put("fixedArguments", new ArrayList<Object>(List.of("--chunks", "10", "--interval-ms", "50")));
        p.put("loadgen", loadgen);
        if (!omitPlan) {
            Map<String, Object> plan = new LinkedHashMap<>();
            plan.put("concurrency", new ArrayList<Object>(concurrency.stream().map(Integer::longValue).toList()));
            plan.put("repetitions", (long) repetitions);
            plan.put("durationMs", durationMs);
            plan.put("warmupMs", 2000L);
            plan.put("rampUpMs", rampUpMs);
            plan.put("cooldownMs", 1000L);
            plan.put("modeOrder", new ArrayList<Object>(modeOrder.stream().map(ConnectionMode::property).toList()));
            plan.put("alternation", "ABBA");
            plan.put("seed", 42L);
            plan.put("cpuBusyThreshold", 0.25);
            plan.put("cpuWindowMs", 1500L);
            plan.put("logLimitBytes", 16_777_216L);
            p.put("plan", plan);
        }
        return p;
    }

    private Map<String, Object> manifestRun(RunSpec s) {
        Map<String, Object> r = Manifest.newRun(s.planned);
        if (s.dir != null) {
            r.put("dir", s.dir);
        }
        r.put("status", s.status);
        r.put("startedAt", "2026-01-01T00:10:00Z");
        r.put("endedAt", "2026-01-01T00:11:00Z");
        if (s.status.equals("failed")) {
            Map<String, Object> f = new LinkedHashMap<>();
            f.put("phase", "gateway-ready");
            f.put("message", "gateway did not start in " + home() + "/logs");
            r.put("failure", f);
        }
        if (s.status.equals("planned") || s.status.equals("not_run")) {
            r.putAll(s.manifestRunOverrides);
            return r;
        }
        Map<String, Object> procs = new LinkedHashMap<>();
        for (String name : List.of("gateway", "simulator")) {
            procs.put(name, process(name, s));
        }
        r.put("processes", procs);
        Map<String, Object> tw = new LinkedHashMap<>();
        tw.put("total", 2L);
        tw.put("gatewayPort", 0L);
        tw.put("simulatorPort", 0L);
        r.put("timeWaitBefore", tw);
        r.put("cpuLoadBefore", new ArrayList<Object>(s.cpu));
        r.put("warmup", phase(s, "warmup", s.warmupFlags));
        r.put("measured", phase(s, "measured", s.measuredFlags));
        Map<String, Object> snaps = new LinkedHashMap<>();
        for (String n : List.of("ready", "pre", "post")) {
            Map<String, Object> ref = new LinkedHashMap<>();
            ref.put("file", "snapshot-" + n + ".json");
            ref.put("capturedAt", "2026-01-01T00:10:30Z");
            snaps.put(n, ref);
        }
        r.put("snapshots", snaps);
        r.put("portsFreed", true);
        r.put("flags", new ArrayList<>(s.runFlags));
        r.putAll(s.manifestRunOverrides);
        return r;
    }

    private Map<String, Object> process(String name, RunSpec s) {
        Map<String, Object> p = new LinkedHashMap<>();
        p.put("pid", 1234L);
        p.put("command", new ArrayList<Object>(List.of(home() + "/jdk/bin/java", "-jar", home() + "/" + name + ".jar")));
        p.put("envOverrides", new LinkedHashMap<String, Object>());
        p.put("startedAt", "2026-01-01T00:10:01Z");
        Map<String, Object> stop = new LinkedHashMap<>();
        stop.put("method", s.stopMethod != null ? s.stopMethod : "destroy");
        stop.put("exitCode", s.stopExitCode != null ? s.stopExitCode.longValue() : (linux ? 143L : 1L));
        stop.put("durationMs", 120L);
        p.put("stop", stop);
        Map<String, Object> log = new LinkedHashMap<>();
        log.put("file", name + ".log");
        log.put("bytesWritten", 1000L);
        log.put("bytesDropped", 0L);
        log.put("bytesLostAfterWriteError", 0L);
        log.put("writeError", null);
        p.put("log", log);
        return p;
    }

    private Map<String, Object> phase(RunSpec s, String phase, List<String> flags) {
        Map<String, Object> ph = new LinkedHashMap<>();
        ph.put("warmup", phase.equals("warmup"));
        Map<String, Object> lg = new LinkedHashMap<>();
        lg.put("pid", 999L);
        lg.put("command", new ArrayList<Object>(List.of(home() + "/jdk/bin/java", "-jar", home() + "/loadgen.jar")));
        lg.put("exitCode", 0L);
        lg.put("durationMs", 3000L);
        Map<String, Object> files = new LinkedHashMap<>();
        files.put("jsonl", phase + ".jsonl");
        files.put("summary", phase + "-summary.json");
        files.put("log", phase + "-loadgen.log");
        lg.put("files", files);
        ph.put("loadgen", lg);
        List<Req> reqs = requestsOf(s);
        Map<String, Object> outcomes = new LinkedHashMap<>();
        if (phase.equals("measured")) {
            for (String o : RunMetrics.KNOWN_OUTCOMES) {
                outcomes.put(o, reqs.stream().filter(q -> q.outcome().equals(o)).count());
            }
        }
        ph.put("outcomes", outcomes.isEmpty() ? null : outcomes);
        Map<String, Object> footer = new LinkedHashMap<>();
        footer.put("stopReason", "duration");
        footer.put("requestsStarted", (long) reqs.size());
        footer.put("recordsWritten", (long) reqs.size());
        footer.put("interrupted", 0L);
        ph.put("footer", footer);
        ph.put("flags", new ArrayList<>(flags));
        return ph;
    }

    // ------------------------------------------------------------------ run files

    List<Req> requestsOf(RunSpec s) {
        if (s.requests != null) {
            return s.requests;
        }
        long base = s.planned.mode() == ConnectionMode.SHARED ? 10 : 20;
        base += s.planned.repetition();
        List<Req> r = new ArrayList<>();
        for (int i = 1; i <= 10; i++) {
            r.add(Req.completed(1000 + i * 500L, 1, base + i, base + 100 + i));
        }
        return r;
    }

    private long[] interChunkOf(RunSpec s) {
        if (s.interChunkNanos != null) {
            return s.interChunkNanos;
        }
        long[] v = new long[10];
        for (int i = 0; i < v.length; i++) {
            v[i] = (40 + i) * MS;
        }
        return v;
    }

    private void writeRunFiles(RunSpec s) throws IOException {
        if (s.status.equals("planned") || s.status.equals("not_run")) {
            return;
        }
        Path dir = root.resolve("runs").resolve(s.planned.id());
        Files.createDirectories(dir);
        List<Req> reqs = requestsOf(s);
        if (s.writeJsonl) {
            write(dir.resolve("measured.jsonl"), jsonl(s, reqs));
        }
        write(dir.resolve("warmup.jsonl"), jsonl(s, List.of()));
        write(dir.resolve("warmup-summary.json"), summary(s, List.of()) + "\n");
        if (s.writeSummary) {
            write(dir.resolve("measured-summary.json"), summary(s, reqs) + "\n");
        }
        if (s.writeSnapshots) {
            write(dir.resolve("snapshot-ready.json"), Json.write(snapshot(s, s.preGateway, s.preSimulator, true)));
            write(dir.resolve("snapshot-pre.json"), Json.write(snapshot(s, s.preGateway, s.preSimulator, true)));
            Map<String, Double> post = new LinkedHashMap<>();
            post.put(GatewayProbe.REJECTED_TOTAL,
                    (double) reqs.stream().filter(q -> q.outcome().equals("REJECTED")).count());
            post.putAll(s.postGateway);
            Map<String, Long> postSim = s.postSimulator;
            if (postSim == null && !s.simulatorUnavailable) {
                postSim = new LinkedHashMap<>();
                postSim.put("active_generations", 0L);
                postSim.put("started_total", (long) reqs.size());
                postSim.put("completed_total", reqs.stream().filter(q -> q.outcome().equals("COMPLETED")).count());
                postSim.put("cancelled_total", 0L);
                postSim.put("faulted_total", 0L);
                postSim.put("rejected_total", 0L);
                postSim.put("chunks_emitted_total", reqs.size() * 10L);
                postSim.put("flow_control_pauses_total", 0L);
            }
            write(dir.resolve("snapshot-post.json"), Json.write(snapshot(s, post, postSim, false)));
        }
        write(dir.resolve("gateway.log"), "log\n");
        write(dir.resolve("simulator.log"), "log\n");
        write(dir.resolve("gateway-gc.log"), "gc\n");
        write(dir.resolve("simulator-gc.log"), "gc\n");
        write(dir.resolve("measured-loadgen.log"), "log\n");
        write(dir.resolve("warmup-loadgen.log"), "log\n");
    }

    private String jsonl(RunSpec s, List<Req> reqs) {
        StringBuilder sb = new StringBuilder();
        Map<String, Object> header = new LinkedHashMap<>();
        header.put("type", "header");
        header.put("schemaVersion", 1L);
        header.put("tool", "streamlab-loadgen");
        header.put("toolVersion", "0.1.0-SNAPSHOT");
        header.put("startedAt", "2026-01-01T00:10:20Z");
        header.put("target", "http://127.0.0.1:8080/stream");
        header.put("loadModel", "closed");
        header.put("concurrency", (long) s.planned.concurrency());
        header.put("durationMs", durationMs);
        header.put("requests", null);
        header.put("rampUpMs", rampUpMs);
        header.put("timeUnit", "ns");
        header.put("commandLine", home() + "/loadgen");
        header.putAll(s.headerOverrides);
        if (s.writeHeader) {
            sb.append(Json.writeCompact(header)).append('\n');
        }
        long i = 0;
        List<Req> ordered = new ArrayList<>(reqs);
        if (s.reverseRequests) {
            java.util.Collections.reverse(ordered);
        }
        for (Req q : ordered) {
            sb.append(Json.writeCompact(requestMap(i++, q))).append('\n');
        }
        for (String extra : s.extraJsonlLines) {
            sb.append(extra).append('\n');
        }
        if (s.writeFooter) {
            long[] ic = interChunkOf(s);
            ConcurrentHistogram h = new ConcurrentHistogram(3);
            for (long v : ic) {
                h.recordValue(v);
            }
            ByteBuffer buf = ByteBuffer.allocate(h.getNeededByteBufferCapacity());
            h.encodeIntoCompressedByteBuffer(buf);
            buf.flip();
            byte[] bytes = new byte[buf.remaining()];
            buf.get(bytes);
            Map<String, Object> footer = new LinkedHashMap<>();
            footer.put("type", "footer");
            footer.put("endedAt", "2026-01-01T00:10:30Z");
            footer.put("elapsedNanos", durationMs * MS + 5 * MS);
            footer.put("stopReason", "duration");
            footer.put("requestsStarted", (long) reqs.size());
            footer.put("recordsWritten", (long) reqs.size());
            footer.put("interrupted", 0L);
            footer.put("interChunkCount", (long) ic.length);
            footer.put("interChunkHistogramUnit", "ns");
            footer.put("interChunkHistogram", Base64.getEncoder().encodeToString(bytes));
            footer.putAll(s.footerOverrides);
            sb.append(Json.writeCompact(footer)).append('\n');
        }
        return sb.toString();
    }

    /** A request line map in the shape of loadgen's RecordWriter (subset of fields). */
    public static Map<String, Object> requestMap(long index, Req q) {
        Map<String, Object> m = new LinkedHashMap<>();
        m.put("type", "request");
        m.put("index", index);
        m.put("worker", 0L);
        m.put("scheduledOffsetNanos", q.scheduledOffset());
        m.put("startOffsetNanos", q.startOffset());
        m.put("endOffsetNanos", q.startOffset() + q.total());
        m.put("outcome", q.outcome());
        m.put("status", q.outcome().equals("REJECTED") ? 503L : 200L);
        m.put("ttfbNanos", q.ttfb());
        m.put("firstBodyByteNanos", q.ttfc());
        m.put("ttfcNanos", q.ttfc());
        m.put("totalNanos", q.total());
        m.put("chunks", 10L);
        return m;
    }

    private String summary(RunSpec s, List<Req> reqs) {
        Map<String, Object> m = new LinkedHashMap<>();
        m.put("type", "summary");
        m.put("schemaVersion", 1L);
        m.put("label", "CONVENIENCE SUMMARY - not a benchmark result");
        m.put("target", "http://127.0.0.1:8080/stream");
        m.put("concurrency", (long) s.planned.concurrency());
        m.put("elapsedNanos", durationMs * MS);
        m.put("stopReason", "duration");
        m.put("requestsStarted", (long) reqs.size());
        m.put("recordsWritten", (long) reqs.size());
        for (String o : RunMetrics.KNOWN_OUTCOMES) {
            m.put("outcome_" + o, reqs.stream().filter(q -> q.outcome().equals(o)).count());
        }
        m.putAll(s.summaryOverrides);
        return Json.writeCompact(m);
    }

    private Map<String, Object> snapshot(RunSpec s, Map<String, Double> gateway, Map<String, Long> simulator,
            boolean pre) {
        Map<String, Object> m = new LinkedHashMap<>();
        m.put("capturedAt", "2026-01-01T00:10:30Z");
        List<Object> entries = new ArrayList<>();
        for (String path : GatewayProbe.snapshotPaths()) {
            Map<String, Object> e = new LinkedHashMap<>();
            e.put("path", path);
            e.put("status", 200L);
            if (path.equals(GatewayProbe.READINESS)) {
                e.put("value", "UP");
            } else {
                e.put("value", gateway.getOrDefault(path, 0.0));
            }
            e.put("raw", "{}");
            entries.add(e);
        }
        m.put("gateway", entries);
        Map<String, Object> sim = new LinkedHashMap<>();
        if (s.simulatorUnavailable) {
            sim.put("unavailable", "UNAVAILABLE: fixture");
        } else if (simulator == null) {
            for (String k : List.of("active_generations", "started_total", "completed_total", "cancelled_total",
                    "faulted_total", "rejected_total", "chunks_emitted_total", "flow_control_pauses_total")) {
                sim.put(k, 0L);
            }
        } else {
            sim.putAll(simulator);
        }
        m.put("simulator", sim);
        return m;
    }
}
