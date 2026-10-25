package io.github.cemharput.streamlab.bench;

import static org.assertj.core.api.Assertions.assertThat;

import io.github.cemharput.streamlab.bench.ProcessSpecs.ProcessSpec;
import java.io.File;
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

@Timeout(value = 30, unit = TimeUnit.SECONDS, threadMode = ThreadMode.SEPARATE_THREAD)
class ProcessSpecsTest {

    @TempDir
    Path root;

    private ProcessSpecs specs;
    private Path runDir;

    @BeforeEach
    void setUp() throws Exception {
        Files.writeString(root.resolve("pom.xml"), "<project/>");
        Files.createDirectories(root.resolve("gateway"));
        Files.createDirectories(root.resolve("loadgen"));
        BenchOptions o = BenchOptionsParser.parse(new String[] {"--preset", "smoke"}, root);
        Path gateway = Path.of("g", "gateway.jar").toAbsolutePath();
        Path loadgen = Path.of("l", "loadgen.jar").toAbsolutePath();
        List<Path> cp = List.of(Path.of("c", "a.jar").toAbsolutePath(), Path.of("c", "b.jar").toAbsolutePath());
        Artifacts.Resolved art = new Artifacts.Resolved(gateway, loadgen, List.of(), cp, null, List.of());
        specs = new ProcessSpecs("JAVA", Map.of("PATH", "x", "spring_profiles_active", "dev", "JAVA_TOOL_OPTIONS", "-Xfoo",
                "LAB_X", "1", "SIMULATOR_PORT", "1"), o, art, 6);
        runDir = root.resolve("run");
    }

    @Test
    void gatewayCommandIsPinned() {
        ProcessSpec spec = specs.gateway(runDir, ConnectionMode.PER_REQUEST);
        Path jar = Path.of("g", "gateway.jar").toAbsolutePath();
        assertThat(spec.command()).containsExactly("JAVA", "-Xms1g", "-Xmx1g", "-XX:+UseG1GC",
                "-Xlog:gc:file=gateway-gc.log:uptime,time,level,tags:filecount=2,filesize=8M", "-jar", jar.toString(),
                "--server.port=8080", "--spring.threads.virtual.enabled=true",
                "--server.tomcat.max-connections=8192", "--server.tomcat.accept-count=100",
                "--lab.connection-mode=per-request", "--lab.upstream.target=127.0.0.1:50051",
                "--lab.upstream.stream-deadline=60s", "--lab.limits.max-active-streams=100",
                "--lab.limits.overload-policy=reject", "--lab.limits.queue-wait=5s", "--lab.limits.max-waiting=100",
                "--lab.limits.stream-queue-capacity=16", "--lab.timeouts.first-chunk=30s",
                "--lab.timeouts.idle-between-chunks=30s", "--lab.retry.max-attempts=3",
                "--lab.retry.initial-backoff=100ms", "--lab.retry.max-backoff=2s",
                "--lab.shutdown.drain-timeout=10s", "--lab.shutdown.channel-termination-timeout=5s");
        assertThat(spec.workDir()).isEqualTo(runDir);
        assertThat(spec.logFile()).isEqualTo(runDir.resolve("gateway.log"));
    }

    @Test
    void onlyTheConnectionModeDiffersBetweenGatewayRuns() {
        List<String> shared = specs.gateway(runDir, ConnectionMode.SHARED).command();
        List<String> perRequest = specs.gateway(runDir, ConnectionMode.PER_REQUEST).command();
        assertThat(shared).hasSameSizeAs(perRequest);
        for (int i = 0; i < shared.size(); i++) {
            if (!shared.get(i).equals(perRequest.get(i))) {
                assertThat(shared.get(i)).isEqualTo("--lab.connection-mode=shared");
                assertThat(perRequest.get(i)).isEqualTo("--lab.connection-mode=per-request");
            }
        }
    }

    @Test
    void simulatorCommandUsesTheExplicitSortedClasspathAndPinnedEnvironment() {
        ProcessSpec spec = specs.simulator(runDir);
        String cp = Path.of("c", "a.jar").toAbsolutePath() + File.pathSeparator + Path.of("c", "b.jar").toAbsolutePath();
        assertThat(spec.command()).containsExactly("JAVA", "-Xms512m", "-Xmx512m", "-XX:+UseG1GC",
                "-Xlog:gc:file=simulator-gc.log:uptime,time,level,tags:filecount=2,filesize=8M", "-cp", cp,
                "io.github.cemharput.streamlab.simulator.SimulatorMain");
        assertThat(spec.envOverrides()).containsEntry("SIMULATOR_PORT", "50051")
                .containsEntry("SIMULATOR_SHUTDOWN_GRACE_SECONDS", "10")
                .containsEntry("SIMULATOR_SCHEDULER_THREADS", "6");
    }

    @Test
    void loadgenCommandPerPhase() {
        ProcessSpec spec = specs.loadgen(runDir, "measured", 4, Duration.ofSeconds(3));
        Path jar = Path.of("l", "loadgen.jar").toAbsolutePath();
        assertThat(spec.command()).containsExactly("JAVA", "-Xms1g", "-Xmx1g", "-XX:+UseG1GC",
                "-Xlog:gc:file=measured-loadgen-gc.log:uptime,time,level,tags:filecount=2,filesize=8M", "-jar",
                jar.toString(), "--target", "http://127.0.0.1:8080/stream", "--concurrency", "4", "--duration", "3s",
                "--ramp-up", "0s", "--chunks", "40", "--interval-ms", "50", "--first-delay-ms", "0", "--fault",
                "NONE", "--fault-at-seq", "0", "--fault-rate", "1.0", "--seed", "42", "--connect-timeout", "5s",
                "--idle-timeout", "90s", "--drain-timeout", "30s", "--reject-pause", "100ms", "--output",
                "measured.jsonl", "--summary-output", "measured-summary.json");
        assertThat(specs.loadgen(runDir, "warmup", 4, Duration.ofSeconds(2)).command())
                .contains("warmup.jsonl", "warmup-summary.json");
    }

    @Test
    void environmentSanitisationRecordsNamesOnly() {
        ProcessSpec spec = specs.gateway(runDir, ConnectionMode.SHARED);
        assertThat(spec.envRemoved()).containsExactlyInAnyOrder("spring_profiles_active", "JAVA_TOOL_OPTIONS", "LAB_X",
                "SIMULATOR_PORT");
        assertThat(ProcessSpecs.removedEnvironment(Map.of("PATH", "x", "_JAVA_OPTIONS", "y", "JDK_JAVA_OPTIONS", "z",
                "MANAGEMENT_X", "1", "SERVER_PORT", "9", "HOME", "h", "Logging_level_root", "debug")))
                .containsExactlyInAnyOrder("_JAVA_OPTIONS", "JDK_JAVA_OPTIONS", "MANAGEMENT_X", "SERVER_PORT",
                        "Logging_level_root");
    }

    @Test
    void javaBinaryNameDependsOnThePlatform() {
        assertThat(ProcessSpecs.javaBinary("/jdk", true)).endsWith("java.exe");
        assertThat(ProcessSpecs.javaBinary("/jdk", false)).endsWith("java").doesNotEndWith(".exe");
    }
}
