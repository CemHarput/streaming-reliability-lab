package io.github.cemharput.streamlab.report;

import io.github.cemharput.streamlab.bench.ConnectionMode;
import io.github.cemharput.streamlab.bench.Manifest;
import io.github.cemharput.streamlab.bench.PlanBuilder;
import io.github.cemharput.streamlab.report.Checks.Finding;
import io.github.cemharput.streamlab.report.FlagPolicy.Source;
import io.github.cemharput.streamlab.report.ResultSetReader.ResultSet;
import io.github.cemharput.streamlab.report.ResultSetReader.RunData;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

/**
 * The class of one run: {@code excluded} when at least one reason is an exclude, {@code caution} when at least one
 * is a caution, otherwise {@code usable}. The reasons are sorted by class, then by code.
 */
public record RunAssessment(String classification, List<Reason> reasons) {

    public static final String USABLE = "usable";
    public static final String CAUTION = "caution";
    public static final String EXCLUDED = "excluded";

    private static final Set<String> KNOWN_STOP_METHODS = Set.of("already_exited", "destroy", "forced", "survived");

    /** {@code source} is SET, RUN, MEASURED, WARMUP, STATUS or CHECK. */
    public record Reason(String source, String code, Severity severity, String detail) {

        public String label() {
            return source + ":" + code;
        }
    }

    /** One run with everything derived from it. */
    public record Analysed(RunData data, RunMetrics metrics, RunAssessment assessment) {

        public ManifestView.RunView run() {
            return data.run();
        }

        public boolean included() {
            return !EXCLUDED.equals(assessment.classification());
        }
    }

    /** The whole set: runs, set level reasons (banners) and the names of unknown flags. */
    public record Analysis(ManifestView manifest, List<Analysed> runs, List<Reason> setReasons,
            List<String> unknownFlags, Integer expectedRunCount, boolean smoke) {

        public long count(String classification) {
            return runs.stream().filter(r -> r.assessment().classification().equals(classification)).count();
        }
    }

    public static Analysis analyse(ResultSet set) {
        ManifestView manifest = set.manifest();
        Set<String> unknown = new TreeSet<>();
        List<Reason> setReasons = new ArrayList<>();
        boolean smoke = "smoke".equals(manifest.preset());
        for (String flag : manifest.flags()) {
            FlagPolicy.Classification c = FlagPolicy.classify(flag, Source.SET);
            if (!c.known()) {
                unknown.add(flag);
            }
            setReasons.add(new Reason("SET", flag, c.severity(), c.rationale()));
            if (flag.equals(io.github.cemharput.streamlab.bench.Flags.SMOKE_PRESET)) {
                smoke = true;
            }
        }
        if (smoke && setReasons.stream().noneMatch(r -> r.code().equals(
                io.github.cemharput.streamlab.bench.Flags.SMOKE_PRESET))) {
            setReasons.add(new Reason("SET", io.github.cemharput.streamlab.bench.Flags.SMOKE_PRESET, Severity.EXCLUDE,
                    "preset smoke"));
        }
        List<String> declaredModes = manifest.modeOrder();
        if (!declaredModes.isEmpty() && !CellComparison.validModeOrder(declaredModes)) {
            setReasons.add(new Reason("SET", Checks.PLAN_MISMATCH, Severity.CAUTION,
                    "plan.modeOrder is not two different known connection modes; the order of first appearance is used"));
        }
        sort(setReasons);
        List<Reason> setExcludes = setReasons.stream().filter(r -> r.severity() == Severity.EXCLUDE).toList();

        Map<String, String> planProblems = new HashMap<>();
        Integer expectedCount = planMismatches(manifest, planProblems);

        List<Analysed> runs = new ArrayList<>();
        for (RunData data : set.runs()) {
            RunMetrics metrics = RunMetrics.compute(data, manifest);
            List<Reason> reasons = new ArrayList<>(setExcludes);
            ManifestView.RunView run = data.run();
            if (!Manifest.STATUS_COMPLETED.equals(run.status())) {
                reasons.add(new Reason("STATUS", "STATUS_" + run.status().toUpperCase(Locale.ROOT).replaceAll("[^A-Z0-9]+", "_"),
                        Severity.EXCLUDE, "status " + sanitize(run.status())));
            }
            flagReasons(run.flags(), Source.RUN, reasons, unknown);
            flagReasons(run.phase("measured").flags(), Source.MEASURED, reasons, unknown);
            flagReasons(run.phase("warmup").flags(), Source.WARMUP, reasons, unknown);
            for (Finding f : data.findings()) {
                reasons.add(check(f));
            }
            if (metrics != null) {
                for (Finding f : metrics.findings()) {
                    reasons.add(check(f));
                }
            }
            if (ConnectionMode.fromProperty(run.mode()).isEmpty()) {
                reasons.add(check(new Finding(Checks.MODE_UNKNOWN, "the mode is not a known connection mode")));
            }
            if (planProblems.containsKey(run.id())) {
                reasons.add(check(new Finding(Checks.PLAN_MISMATCH, planProblems.get(run.id()))));
            }
            for (String proc : List.of("gateway", "simulator")) {
                ManifestView.ProcessView p = run.process(proc);
                if (p != null && p.stopMethod() != null && !KNOWN_STOP_METHODS.contains(p.stopMethod())) {
                    reasons.add(check(new Finding(Checks.STOP_METHOD_UNKNOWN, proc
                            + " stop method is not one of the methods this tool knows")));
                }
            }
            if (!run.missingFields().isEmpty()) {
                reasons.add(check(new Finding(Checks.FIELD_MISSING,
                        run.missingFields().size() + " optional field(s) not recorded")));
            }
            runs.add(new Analysed(data, metrics, assess(reasons)));
        }
        return new Analysis(manifest, List.copyOf(runs), List.copyOf(setReasons), List.copyOf(unknown), expectedCount,
                smoke);
    }

    /** Builds the assessment from reasons (deduplicated by source and code, sorted by class then code). */
    public static RunAssessment assess(List<Reason> reasons) {
        List<Reason> unique = new ArrayList<>();
        Set<String> seen = new HashSet<>();
        for (Reason r : reasons) {
            if (seen.add(r.source() + "|" + r.code())) {
                unique.add(r);
            }
        }
        sort(unique);
        String classification = USABLE;
        if (unique.stream().anyMatch(r -> r.severity() == Severity.EXCLUDE)) {
            classification = EXCLUDED;
        } else if (unique.stream().anyMatch(r -> r.severity() == Severity.CAUTION)) {
            classification = CAUTION;
        }
        return new RunAssessment(classification, List.copyOf(unique));
    }

    private static void sort(List<Reason> reasons) {
        reasons.sort(Comparator.comparing((Reason r) -> r.severity().ordinal()).thenComparing(Reason::code)
                .thenComparing(Reason::source));
    }

    private static Reason check(Finding f) {
        return new Reason("CHECK", f.code(), Checks.severity(f.code()), f.detail());
    }

    private static void flagReasons(List<String> flags, Source source, List<Reason> out, Set<String> unknown) {
        for (String flag : flags) {
            FlagPolicy.Classification c = FlagPolicy.classify(flag, source);
            if (!c.known()) {
                unknown.add(flag);
            }
            out.add(new Reason(source.name(), flag, c.severity(), c.rationale()));
        }
    }

    private static String sanitize(String s) {
        return s.length() > 40 ? s.substring(0, 40) : s;
    }

    /**
     * The run id of the plan, formatted here without the default locale: the runner builds it with
     * {@code String.format}, which would write non-Latin digits under some locales.
     */
    static String expectedId(PlanBuilder.PlannedRun e) {
        String index = Integer.toString(e.index());
        return "0".repeat(Math.max(0, 3 - index.length())) + index + "-c" + e.concurrency() + "-"
                + e.mode().property() + "-r" + e.repetition();
    }

    /**
     * Rebuilds the plan from the recorded plan parameters and fills {@code problems} (run id to text) for runs that
     * differ. Returns the expected number of runs, or null when the plan parameters are not all recorded.
     */
    static Integer planMismatches(ManifestView manifest, Map<String, String> problems) {
        List<Integer> concurrency = manifest.planConcurrency();
        Long repetitions = manifest.planLong("repetitions");
        List<String> modeNames = manifest.modeOrder();
        if (concurrency == null || repetitions == null || modeNames.size() != 2 || repetitions < 1
                || repetitions > 1000 || concurrency.size() > 1000) {
            return null;
        }
        List<ConnectionMode> modes = new ArrayList<>();
        for (String name : modeNames) {
            ConnectionMode m = ConnectionMode.fromProperty(name).orElse(null);
            if (m == null) {
                return null;
            }
            modes.add(m);
        }
        PlanBuilder.RunPlan plan = PlanBuilder.build(concurrency, repetitions.intValue(), modes);
        Map<Long, PlanBuilder.PlannedRun> expected = new HashMap<>();
        plan.runs().forEach(r -> expected.put((long) r.index(), r));
        for (ManifestView.RunView run : manifest.runs()) {
            PlanBuilder.PlannedRun e = expected.get(run.index());
            if (e == null) {
                problems.put(run.id(), "the run is not part of the plan rebuilt from the recorded parameters");
            } else if (!expectedId(e).equals(run.id()) || e.concurrency() != run.concurrency()
                    || !e.mode().property().equals(run.mode()) || e.repetition() != run.repetition()
                    || !e.orderLabel().equals(run.orderLabel())) {
                problems.put(run.id(), "the run differs from the plan rebuilt from the recorded parameters");
            }
        }
        return plan.runs().size();
    }
}
