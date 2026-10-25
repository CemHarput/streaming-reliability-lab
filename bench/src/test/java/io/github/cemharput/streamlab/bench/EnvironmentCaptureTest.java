package io.github.cemharput.streamlab.bench;

import static org.assertj.core.api.Assertions.assertThat;

import io.github.cemharput.streamlab.bench.CommandRunner.CommandResult;
import io.github.cemharput.streamlab.bench.EnvironmentCapture.Captured;
import io.github.cemharput.streamlab.bench.EnvironmentCapture.Platform;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.Timeout.ThreadMode;
import org.junit.jupiter.api.io.TempDir;

@Timeout(value = 30, unit = TimeUnit.SECONDS, threadMode = ThreadMode.SEPARATE_THREAD)
class EnvironmentCaptureTest {

    @TempDir
    Path root;

    private final List<List<String>> seen = new ArrayList<>();

    private CommandRunner runner(Function<String, CommandResult> byJoinedCommand) {
        return (command, timeout) -> {
            seen.add(command);
            return byJoinedCommand.apply(String.join(" ", command));
        };
    }

    private static CommandResult ok(String output) {
        return new CommandResult(true, 0, output);
    }

    private BenchOptions options() throws Exception {
        Files.writeString(root.resolve("pom.xml"), "<project/>");
        Files.createDirectories(root.resolve("gateway"));
        Files.createDirectories(root.resolve("loadgen"));
        return BenchOptionsParser.parse(new String[0], root);
    }

    @Test
    void gitFactsAreCollectedFromReadOnlyCommands() {
        EnvironmentCapture capture = new EnvironmentCapture(runner(cmd -> {
            if (cmd.contains("rev-parse")) {
                return ok("0123456789abcdef0123456789abcdef01234567\n");
            }
            if (cmd.contains("status")) {
                return ok(" M a.txt\n?? b.txt\n");
            }
            if (cmd.contains("log")) {
                return ok("2026-03-04T05:06:07+02:00\n");
            }
            return new CommandResult(false, 1, ""); // check-ignore: not ignored
        }), new Platform(false, Map.of(), List::of));

        GitInfo git = capture.git(root, root.resolve("bench/results"));

        assertThat(git.available()).isTrue();
        assertThat(git.shortCommit()).isEqualTo("0123456");
        assertThat(git.dirty()).isTrue();
        assertThat(git.statusLines()).containsExactly(" M a.txt", "?? b.txt");
        assertThat(git.commitTime()).isEqualTo(java.time.Instant.parse("2026-03-04T03:06:07Z"));
        assertThat(git.ignoreKnown()).isTrue();
        assertThat(git.ignoredOutput()).isFalse();
        assertThat(seen).allMatch(c -> c.get(0).equals("git") && !c.contains("add") && !c.contains("commit"));
    }

    @Test
    void gitFailureMeansUnavailable() {
        EnvironmentCapture capture = new EnvironmentCapture(runner(cmd -> CommandResult.failed("no git")),
                new Platform(false, Map.of(), List::of));
        assertThat(capture.git(root, root)).isEqualTo(GitInfo.UNAVAILABLE);
    }

    @Test
    void windowsFactsComeFromTheDocumentedCommands() throws Exception {
        EnvironmentCapture capture = new EnvironmentCapture(runner(cmd -> {
            if (cmd.startsWith("reg query")) {
                return ok("\nHKEY_LOCAL_MACHINE\\HARDWARE\\DESCRIPTION\\System\\CentralProcessor\\0\n"
                        + "    ProcessorNameString    REG_SZ    Test CPU 9000\n");
            }
            if (cmd.startsWith("powershell")) {
                return ok("8\r\n");
            }
            if (cmd.startsWith("powercfg")) {
                return ok("Power Scheme GUID: abc  (Balanced)\n");
            }
            if (cmd.startsWith("netstat")) {
                return ok("  TCP    127.0.0.1:8080    127.0.0.1:50000   TIME_WAIT\n"
                        + "  TCP    127.0.0.1:50001   127.0.0.1:8080    TIME_WAIT\n"
                        + "  TCP    127.0.0.1:50051   127.0.0.1:50002   TIME_WAIT\n"
                        + "  TCP    127.0.0.1:1       127.0.0.1:2       ESTABLISHED\n");
            }
            return CommandResult.failed("unexpected " + cmd);
        }), new Platform(true, Map.of("PROCESSOR_IDENTIFIER", "fallback"), List::of));

        Captured c = capture.environment(options(), GitInfo.UNAVAILABLE, Set.of("LAB_X"));

        @SuppressWarnings("unchecked")
        Map<String, Object> cpu = (Map<String, Object>) c.map().get("cpu");
        assertThat(cpu.get("model")).isEqualTo("Test CPU 9000");
        assertThat(cpu.get("modelSource")).isEqualTo("reg query ProcessorNameString");
        assertThat(cpu.get("physicalCores")).isEqualTo(8);
        assertThat((Integer) cpu.get("logicalProcessors")).isPositive();
        assertThat((String) c.map().get("powerPlan")).contains("Balanced");
        @SuppressWarnings("unchecked")
        Map<String, Object> tw = (Map<String, Object>) c.map().get("timeWaitAtStart");
        assertThat(tw).containsEntry("total", 3L).containsEntry("gatewayPort", 2L).containsEntry("simulatorPort", 1L);
        assertThat(c.map().get("removedEnvironmentVariables")).isEqualTo(List.of("LAB_X"));
        assertThat(c.flags()).isEmpty();
        assertThat(c.map().keySet()).containsExactly("os", "cpu", "memory", "java", "git", "powerPlan",
                "timeWaitAtStart", "removedEnvironmentVariables");
    }

    @Test
    void failingCommandsGiveNullValuesAndFlags() throws Exception {
        EnvironmentCapture capture = new EnvironmentCapture(runner(cmd -> CommandResult.failed("denied")),
                new Platform(true, Map.of("PROCESSOR_IDENTIFIER", "Fallback Family 6"), List::of));

        Captured c = capture.environment(options(), GitInfo.UNAVAILABLE, Set.of());

        @SuppressWarnings("unchecked")
        Map<String, Object> cpu = (Map<String, Object>) c.map().get("cpu");
        assertThat(cpu.get("model")).isEqualTo("Fallback Family 6");
        assertThat(cpu.get("modelSource")).isEqualTo("env PROCESSOR_IDENTIFIER");
        assertThat(cpu.get("physicalCores")).isNull();
        assertThat(c.map().get("powerPlan")).isNull();
        assertThat(c.map().get("timeWaitAtStart")).isNull();
        assertThat(c.flags()).containsExactly(Flags.TIME_WAIT_UNAVAILABLE);
    }

    @Test
    void localisedNetstatWithoutAnyRecognisedStateIsUnavailableNotZero() {
        EnvironmentCapture capture = new EnvironmentCapture(runner(cmd -> ok(
                "  TCP    127.0.0.1:8080    127.0.0.1:50000   ZAMAN_BEKLE\n  TCP    0.0.0.0:80  0.0.0.0:0  DINLEME\n")),
                new Platform(true, Map.of(), List::of));
        assertThat(capture.timeWait(List.of(8080, 50051))).isNull();
        assertThat(EnvironmentCapture.countTimeWait("  TCP 127.0.0.1:1 127.0.0.1:2 ESTABLISHED\n", List.of(8080, 50051)))
                .containsEntry("total", 0L);
    }

    @Test
    void linuxCpuComesFromCpuInfo() throws Exception {
        List<String> cpuInfo = List.of("processor\t: 0", "model name\t: Linux CPU", "physical id\t: 0", "core id\t\t: 0",
                "", "processor\t: 1", "model name\t: Linux CPU", "physical id\t: 0", "core id\t\t: 0", "",
                "processor\t: 2", "model name\t: Linux CPU", "physical id\t: 0", "core id\t\t: 1");
        EnvironmentCapture capture = new EnvironmentCapture(runner(cmd -> cmd.startsWith("ss")
                ? ok("State Recv-Q\nTIME-WAIT 0 0 127.0.0.1:8080 127.0.0.1:1\n") : CommandResult.failed("x")),
                new Platform(false, Map.of(), () -> cpuInfo));
        Captured c = capture.environment(options(), GitInfo.UNAVAILABLE, Set.of());
        @SuppressWarnings("unchecked")
        Map<String, Object> cpu = (Map<String, Object>) c.map().get("cpu");
        assertThat(cpu.get("model")).isEqualTo("Linux CPU");
        assertThat(cpu.get("modelSource")).isEqualTo("/proc/cpuinfo model name");
        assertThat(cpu.get("physicalCores")).isEqualTo(2);
        assertThat(c.map().get("powerPlan")).isNull();
        assertThat(c.flags()).isEmpty();
        assertThat(c.map().get("timeWaitAtStart")).isNotNull();
    }
}
