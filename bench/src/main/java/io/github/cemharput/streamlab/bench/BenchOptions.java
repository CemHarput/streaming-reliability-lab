package io.github.cemharput.streamlab.bench;

import java.nio.file.Path;
import java.time.Duration;
import java.util.List;

/** Resolved command line. {@code help} instances carry no other data. */
public record BenchOptions(
        boolean help,
        Preset preset,
        int repetitions,
        Duration duration,
        Duration warmup,
        Duration rampUp,
        List<Integer> concurrency,
        List<ConnectionMode> modeOrder,
        Duration cooldown,
        Path outputDir,
        Path repoRoot,
        Path gatewayJar,
        Path loadgenJar,
        List<Path> simulatorClasspath,
        int gatewayPort,
        int simulatorPort,
        ChecklistMode checklist,
        String operatorNote,
        boolean dryRun,
        String commandLine) {

    /** How the operator checklist is answered. */
    public enum ChecklistMode {
        ASK, YES, SKIP;

        public String cliName() {
            return name().toLowerCase(java.util.Locale.ROOT);
        }
    }

    public Duration cpuWindow() {
        return preset.cpuWindow();
    }

    public boolean smoke() {
        return preset == Preset.SMOKE;
    }

    static BenchOptions helpRequest() {
        return new BenchOptions(true, null, 0, null, null, null, null, null, null, null, null, null, null, null, 0, 0,
                null, null, false, "");
    }
}
