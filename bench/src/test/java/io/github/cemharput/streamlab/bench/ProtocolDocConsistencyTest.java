package io.github.cemharput.streamlab.bench;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.Timeout.ThreadMode;
import org.junit.jupiter.api.io.TempDir;

/** The protocol document and the code must say the same thing. */
@Timeout(value = 30, unit = TimeUnit.SECONDS, threadMode = ThreadMode.SEPARATE_THREAD)
class ProtocolDocConsistencyTest {

    @TempDir
    Path tmp;

    private String doc;
    private ProcessSpecs specs;

    @BeforeEach
    void load() throws Exception {
        Path file = Path.of("..", "docs", "benchmark-protocol.md");
        if (!Files.exists(file)) {
            file = Path.of("docs", "benchmark-protocol.md"); // run from the repository root
        }
        doc = Files.readString(file, StandardCharsets.UTF_8);
        Files.writeString(tmp.resolve("pom.xml"), "<project/>");
        Files.createDirectories(tmp.resolve("gateway"));
        Files.createDirectories(tmp.resolve("loadgen"));
        BenchOptions o = BenchOptionsParser.parse(new String[0], tmp);
        Artifacts.Resolved art = new Artifacts.Resolved(Path.of("g.jar"), Path.of("l.jar"), List.of(),
                List.of(Path.of("s.jar")), null, List.of());
        specs = new ProcessSpecs("java", Map.of(), o, art, 1);
    }

    @Test
    void firstLineStatesTheProtocolVersionOfTheCode() throws IOException {
        assertThat(doc.lines().findFirst()).contains("Protocol version: " + Manifest.PROTOCOL_VERSION);
        assertThat(doc).contains("`schemaVersion` is " + Manifest.SCHEMA_VERSION);
        assertThat(doc).contains("`protocolVersion` is " + Manifest.PROTOCOL_VERSION);
    }

    @Test
    void everyManifestKeyIsDocumented() {
        for (String key : Manifest.TOP_LEVEL_KEYS) {
            assertThat(doc).as("top-level key " + key).contains("`" + key + "`");
        }
        for (String key : Manifest.RUN_KEYS) {
            assertThat(doc).as("run key " + key).contains("`" + key + "`");
        }
    }

    @Test
    void everyFlagOfTheDictionaryIsDocumented() {
        for (String flag : Flags.DOCUMENTED) {
            assertThat(doc).as("flag " + flag).contains("`" + flag + "`");
        }
        assertThat(doc).contains(String.valueOf(Flags.CPU_BUSY_THRESHOLD));
    }

    @Test
    void pinnedGatewaySettingsMatchTheCode() {
        specs.gatewayProperties(ConnectionMode.SHARED).forEach((k, v) -> {
            if (!k.equals("lab.connection-mode")) {
                assertThat(doc).as(k).contains("--" + k + "=" + v);
            }
        });
        assertThat(doc).contains("--lab.connection-mode=<shared|per-request>");
        assertThat(doc).contains(String.join(" ", specs.gatewayJvmOptions()));
    }

    @Test
    void pinnedSimulatorAndLoadgenSettingsMatchTheCode() {
        specs.simulatorEnv().forEach((k, v) -> assertThat(doc).as(k).contains(k + "="));
        assertThat(doc).contains("SIMULATOR_PORT=50051").contains("SIMULATOR_SHUTDOWN_GRACE_SECONDS=10");
        assertThat(doc).contains(String.join(" ", specs.simulatorJvmOptions()));
        assertThat(doc).contains(String.join(" ", specs.loadgenJvmOptions("<phase>")));
        assertThat(doc).contains(String.join(" ", specs.loadgenFixedArguments().subList(0, 12)));
        assertThat(doc).contains("--connect-timeout 5s --idle-timeout 90s --drain-timeout 30s --reject-pause 100ms");
        assertThat(doc).contains("--seed " + ProcessSpecs.SEED);
    }

    @Test
    void presetsAndPlanSettingsAreDocumented() {
        assertThat(doc).contains("`" + PlanBuilder.ALTERNATION + "`").contains("ABBA ABBA")
                .contains("`--preset smoke`");
        assertThat(doc).contains(Preset.DEFAULT.concurrency().stream().map(String::valueOf)
                .reduce((a, b) -> a + "," + b).orElseThrow());
        assertThat(Preset.DEFAULT.duration()).isEqualTo(Duration.ofSeconds(60));
        assertThat(doc).contains("measured duration 60 s, warm-up 20 s").contains("30 s cooldown");
        assertThat(doc).contains("16 MiB");
    }

    @Test
    void sanitisedEnvironmentPrefixesAreDocumented() {
        for (String prefix : List.of("SPRING_", "LAB_", "MANAGEMENT_", "SERVER_", "SIMULATOR_", "LOGGING_",
                "JAVA_TOOL_OPTIONS", "JDK_JAVA_OPTIONS", "_JAVA_OPTIONS")) {
            assertThat(doc).as(prefix).contains("`" + prefix + "`");
        }
        assertThat(ProcessSpecs.removedEnvironment(Map.of("LOGGING_LEVEL_ROOT", "x"))).containsExactly("LOGGING_LEVEL_ROOT");
    }

    @Test
    void everyRecordedTimeoutIsDocumented() {
        for (String key : BenchRunner.Timeouts.defaults().toMap().keySet()) {
            assertThat(doc).as("timeout " + key).contains("`" + key + "`");
        }
        assertThat(BenchRunner.Timeouts.defaults().hookKill().toSeconds()).isEqualTo(10);
        assertThat(doc).contains("best effort within that wait");
    }

    @Test
    void exitCodeExceptionsAreDocumentedAndEchoedInTheUsageText() {
        String flat = doc.replace("\n", " ");
        assertThat(flat).contains("cannot be created").contains("writing the manifest keeps failing during the run")
                .contains("keeps its last written status");
        assertThat(BenchOptionsParser.USAGE.replace("\n", " ")).contains("cannot be created")
                .contains("manifest cannot be written");
    }

    @Test
    void loadgenGracefulWaitCoversTheDrainTimeout() {
        assertThat(BenchRunner.Timeouts.defaults().loadgenGraceful())
                .isGreaterThan(ProcessSpecs.LOADGEN_DRAIN_TIMEOUT);
        assertThat(doc).contains("40 s after `destroy()`");
    }

    @Test
    void stopMethodsAndStatusesAreDocumented() {
        for (String s : List.of("already_exited", "destroy", "forced", "survived", "planned", "running", "completed",
                "failed", "interrupted", "not_run")) {
            assertThat(doc).as(s).contains("`" + s + "`");
        }
    }
}
