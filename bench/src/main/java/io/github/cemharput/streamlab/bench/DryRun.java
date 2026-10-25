package io.github.cemharput.streamlab.bench;

import io.github.cemharput.streamlab.bench.PlanBuilder.PlannedRun;
import io.github.cemharput.streamlab.bench.PlanBuilder.RunPlan;
import io.github.cemharput.streamlab.bench.ProcessSpecs.ProcessSpec;
import java.io.PrintStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

/** Prints the resolved plan and the exact commands. Starts nothing and writes nothing. */
final class DryRun {

    private DryRun() {
    }

    /** The resolved artifacts with placeholders for missing ones, so command lines can still be shown. */
    static Artifacts.Resolved forDisplay(Artifacts.Resolved a) {
        Path gateway = a.gatewayJar() != null ? a.gatewayJar() : Path.of("MISSING-gateway-jar");
        Path loadgen = a.loadgenJar() != null ? a.loadgenJar() : Path.of("MISSING-loadgen-jar");
        List<Path> cp = a.simulatorClasspath().isEmpty() ? List.of(Path.of("MISSING-simulator-classpath"))
                : a.simulatorClasspath();
        return new Artifacts.Resolved(gateway, loadgen, a.loadgenLib(), cp, a.benchJar(), a.problems(),
                a.problemKinds());
    }

    static void print(PrintStream out, BenchOptions o, Artifacts.Resolved resolved, ProcessSpecs specs, RunPlan plan,
            String runIdPreview, Set<String> removedEnv) {
        out.println("streamlab-bench dry run (protocol version " + Manifest.PROTOCOL_VERSION + ")");
        out.println("Nothing is started and nothing is written. This output makes no performance claim.");
        out.println();
        out.println("Preset: " + o.preset().cliName());
        out.println("Parameters:");
        out.println("  concurrency      " + o.concurrency());
        out.println("  repetitions      " + o.repetitions() + " per mode and concurrency");
        out.println("  mode order       " + o.modeOrder().stream().map(ConnectionMode::property).toList()
                + " (A,B; " + PlanBuilder.ALTERNATION + " alternation)");
        out.println("  duration         " + BenchOptionsParser.format(o.duration()));
        out.println("  warm-up          " + BenchOptionsParser.format(o.warmup()));
        out.println("  ramp-up          " + BenchOptionsParser.format(o.rampUp()));
        out.println("  cooldown         " + BenchOptionsParser.format(o.cooldown()));
        out.println("  cpu window       " + BenchOptionsParser.format(o.cpuWindow()));
        out.println("  gateway port     " + o.gatewayPort());
        out.println("  simulator port   " + o.simulatorPort());
        out.println("  checklist        " + o.checklist().cliName());
        out.println();
        out.println("Pinned settings (identical in both modes except lab.connection-mode):");
        out.println("  host                 " + ProcessSpecs.HOST);
        out.println("  gateway JVM          " + String.join(" ", specs.gatewayJvmOptions()));
        specs.gatewayProperties(o.modeOrder().get(0)).forEach((k, v) -> out.println("  gateway --" + k + "=" + v));
        out.println("  simulator JVM        " + String.join(" ", specs.simulatorJvmOptions()));
        specs.simulatorEnv().forEach((k, v) -> out.println("  simulator env        " + k + "=" + v));
        out.println("  loadgen JVM          " + String.join(" ", specs.loadgenJvmOptions("<phase>")));
        out.println("  loadgen fixed args   " + String.join(" ", specs.loadgenFixedArguments()));
        out.println("  log limit per stream " + ProcessSpecs.LOG_LIMIT_BYTES + " bytes");
        out.println("  environment removed from children (names only): " + (removedEnv.isEmpty() ? "none" : removedEnv));
        out.println();
        out.println("Artifacts:");
        artifact(out, "gateway jar", resolved.gatewayJar());
        artifact(out, "loadgen jar", resolved.loadgenJar());
        out.println("  loadgen lib          " + resolved.loadgenLib().size() + " jar(s)");
        out.println("  simulator classpath  " + resolved.simulatorClasspath().size() + " jar(s)");
        for (Path p : resolved.simulatorClasspath()) {
            out.println("    " + p);
        }
        for (int i = 0; i < resolved.problems().size(); i++) {
            out.println("  " + resolved.problemKinds().get(i) + ": " + resolved.problems().get(i));
        }
        out.println();
        out.println("Result directory: " + o.outputDir().resolve(runIdPreview) + " (id preview; the time is taken at the start)");
        out.println("Planned runs: " + plan.runs().size());
        Duration nominal = plan.nominalDuration(o);
        out.println("Nominal total duration: " + nominal.toMinutes() + " min " + (nominal.toSecondsPart()) + " s"
                + " (warm-up + measured + cooldown per run; start-up, drain and stop times come on top)");
        out.println();
        Path dir = o.outputDir().resolve(runIdPreview).resolve(ResultLayout.RUNS_DIR);
        for (PlannedRun run : plan.runs()) {
            out.println("Run " + run.index() + " " + run.id() + " (" + run.mode().property() + ", c="
                    + run.concurrency() + ", repetition " + run.repetition() + ", order " + run.orderLabel() + ")");
            out.println("  directory: " + dir.resolve(run.id()));
            command(out, "simulator", specs.simulator(dir.resolve(run.id())));
            command(out, "gateway", specs.gateway(dir.resolve(run.id()), run.mode()));
            command(out, "warm-up loadgen", specs.loadgen(dir.resolve(run.id()), "warmup", run.concurrency(),
                    o.warmup()));
            command(out, "measured loadgen", specs.loadgen(dir.resolve(run.id()), "measured", run.concurrency(),
                    o.duration()));
        }
    }

    private static void artifact(PrintStream out, String label, Path path) {
        if (path == null) {
            out.println("  " + label + ": MISSING");
        } else {
            out.println("  " + label + ": " + path + (Files.isRegularFile(path) ? " [found]" : " [MISSING]"));
        }
    }

    private static void command(PrintStream out, String label, ProcessSpec spec) {
        out.println("  " + label + ":");
        if (!spec.envOverrides().isEmpty()) {
            out.println("    env: " + spec.envOverrides().entrySet().stream()
                    .map((Map.Entry<String, String> e) -> e.getKey() + "=" + e.getValue())
                    .collect(Collectors.joining(" ")));
        }
        out.println("    cwd: " + spec.workDir());
        out.println("    " + spec.command().stream().map(DryRun::quote).collect(Collectors.joining(" ")));
    }

    private static String quote(String s) {
        return s.contains(" ") ? "\"" + s + "\"" : s;
    }
}
