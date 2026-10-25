package io.github.cemharput.streamlab.bench;

import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.stream.Collectors;

/**
 * Builds the pinned command lines of the protocol. Hosts are always {@code 127.0.0.1}. Every child runs in the
 * run directory, so the relative GC log and loadgen output paths land there (and the drive letter colon of
 * Windows paths never reaches the {@code -Xlog} syntax).
 */
public final class ProcessSpecs {

    public static final String HOST = "127.0.0.1";
    public static final String SIMULATOR_MAIN = "io.github.cemharput.streamlab.simulator.SimulatorMain";
    public static final int SEED = 42;
    public static final int MAX_ACTIVE_STREAMS = 100;
    /** The pinned loadgen {@code --drain-timeout}; a graceful loadgen stop must wait at least this long. */
    public static final Duration LOADGEN_DRAIN_TIMEOUT = Duration.ofSeconds(30);
    public static final long LOG_LIMIT_BYTES = 16L * 1024 * 1024;

    private static final Set<String> REMOVED_EXACT = Set.of("JAVA_TOOL_OPTIONS", "JDK_JAVA_OPTIONS", "_JAVA_OPTIONS");
    private static final List<String> REMOVED_PREFIXES = List.of("SPRING_", "LAB_", "MANAGEMENT_", "SERVER_",
            "SIMULATOR_", "LOGGING_");

    /** One child process to start. {@code envRemoved} holds names only. */
    public record ProcessSpec(String name, List<String> command, Map<String, String> envOverrides,
            Set<String> envRemoved, Path workDir, Path logFile) {
    }

    private final String javaBin;
    private final Map<String, String> parentEnv;
    private final BenchOptions options;
    private final Artifacts.Resolved artifacts;
    private final int schedulerThreads;

    public ProcessSpecs(String javaBin, Map<String, String> parentEnv, BenchOptions options,
            Artifacts.Resolved artifacts, int schedulerThreads) {
        this.javaBin = javaBin;
        this.parentEnv = parentEnv;
        this.options = options;
        this.artifacts = artifacts;
        this.schedulerThreads = schedulerThreads;
    }

    /** {@code <java.home>/bin/java[.exe]}. */
    public static String javaBinary(String javaHome, boolean windows) {
        return Path.of(javaHome, "bin", windows ? "java.exe" : "java").toString();
    }

    /** Names of the environment variables that must not reach the children (sorted). */
    public static Set<String> removedEnvironment(Map<String, String> env) {
        Set<String> removed = new TreeSet<>();
        for (String name : env.keySet()) {
            String upper = name.toUpperCase(Locale.ROOT);
            if (REMOVED_EXACT.contains(upper) || REMOVED_PREFIXES.stream().anyMatch(upper::startsWith)) {
                removed.add(name);
            }
        }
        return removed;
    }

    public Set<String> removedEnvironment() {
        return removedEnvironment(parentEnv);
    }

    private static String gcLog(String file) {
        return "-Xlog:gc:file=" + file + ":uptime,time,level,tags:filecount=2,filesize=8M";
    }

    public List<String> simulatorJvmOptions() {
        return List.of("-Xms512m", "-Xmx512m", "-XX:+UseG1GC", gcLog(ResultLayout.SIMULATOR_GC_LOG));
    }

    public List<String> gatewayJvmOptions() {
        return List.of("-Xms1g", "-Xmx1g", "-XX:+UseG1GC", gcLog(ResultLayout.GATEWAY_GC_LOG));
    }

    public List<String> loadgenJvmOptions(String phase) {
        return List.of("-Xms1g", "-Xmx1g", "-XX:+UseG1GC", gcLog(ResultLayout.loadgenGcLog(phase)));
    }

    public Map<String, String> simulatorEnv() {
        Map<String, String> env = new LinkedHashMap<>();
        env.put("SIMULATOR_PORT", String.valueOf(options.simulatorPort()));
        env.put("SIMULATOR_SHUTDOWN_GRACE_SECONDS", "10");
        env.put("SIMULATOR_SCHEDULER_THREADS", String.valueOf(schedulerThreads));
        return env;
    }

    /** The explicit gateway properties of the protocol; only {@code lab.connection-mode} differs between modes. */
    public Map<String, String> gatewayProperties(ConnectionMode mode) {
        Map<String, String> p = new LinkedHashMap<>();
        p.put("server.port", String.valueOf(options.gatewayPort()));
        p.put("spring.threads.virtual.enabled", "true");
        p.put("server.tomcat.max-connections", "8192");
        p.put("server.tomcat.accept-count", "100");
        p.put("lab.connection-mode", mode.property());
        p.put("lab.upstream.target", HOST + ":" + options.simulatorPort());
        p.put("lab.upstream.stream-deadline", "60s");
        p.put("lab.limits.max-active-streams", String.valueOf(MAX_ACTIVE_STREAMS));
        p.put("lab.limits.overload-policy", "reject");
        p.put("lab.limits.queue-wait", "5s");
        p.put("lab.limits.max-waiting", "100");
        p.put("lab.limits.stream-queue-capacity", "16");
        p.put("lab.timeouts.first-chunk", "30s");
        p.put("lab.timeouts.idle-between-chunks", "30s");
        p.put("lab.retry.max-attempts", "3");
        p.put("lab.retry.initial-backoff", "100ms");
        p.put("lab.retry.max-backoff", "2s");
        p.put("lab.shutdown.drain-timeout", "10s");
        p.put("lab.shutdown.channel-termination-timeout", "5s");
        return p;
    }

    /** Loadgen arguments that do not depend on the phase (phase specific ones are added by {@link #loadgen}). */
    public List<String> loadgenFixedArguments() {
        return List.of("--chunks", "40", "--interval-ms", "50", "--first-delay-ms", "0", "--fault", "NONE",
                "--fault-at-seq", "0", "--fault-rate", "1.0", "--seed", String.valueOf(SEED),
                "--connect-timeout", "5s", "--idle-timeout", "90s",
                "--drain-timeout", BenchOptionsParser.format(LOADGEN_DRAIN_TIMEOUT),
                "--reject-pause", "100ms");
    }

    public String loadgenTarget() {
        return "http://" + HOST + ":" + options.gatewayPort() + "/stream";
    }

    public ProcessSpec simulator(Path runDir) {
        List<String> cmd = new ArrayList<>();
        cmd.add(javaBin);
        cmd.addAll(simulatorJvmOptions());
        cmd.add("-cp");
        cmd.add(artifacts.simulatorClasspath().stream().map(p -> p.toAbsolutePath().toString())
                .collect(Collectors.joining(java.io.File.pathSeparator)));
        cmd.add(SIMULATOR_MAIN);
        return new ProcessSpec("simulator", List.copyOf(cmd), simulatorEnv(), removedEnvironment(), runDir,
                runDir.resolve(ResultLayout.SIMULATOR_LOG));
    }

    public ProcessSpec gateway(Path runDir, ConnectionMode mode) {
        List<String> cmd = new ArrayList<>();
        cmd.add(javaBin);
        cmd.addAll(gatewayJvmOptions());
        cmd.add("-jar");
        cmd.add(artifacts.gatewayJar().toAbsolutePath().toString());
        gatewayProperties(mode).forEach((k, v) -> cmd.add("--" + k + "=" + v));
        return new ProcessSpec("gateway", List.copyOf(cmd), Map.of(), removedEnvironment(), runDir,
                runDir.resolve(ResultLayout.GATEWAY_LOG));
    }

    /** Phase is {@code warmup} or {@code measured}; both use the concurrency of the cell. */
    public ProcessSpec loadgen(Path runDir, String phase, int concurrency, Duration duration) {
        List<String> cmd = new ArrayList<>();
        cmd.add(javaBin);
        cmd.addAll(loadgenJvmOptions(phase));
        cmd.add("-jar");
        cmd.add(artifacts.loadgenJar().toAbsolutePath().toString());
        cmd.addAll(List.of("--target", loadgenTarget(), "--concurrency", String.valueOf(concurrency),
                "--duration", BenchOptionsParser.format(duration), "--ramp-up",
                BenchOptionsParser.format(options.rampUp())));
        cmd.addAll(loadgenFixedArguments());
        cmd.addAll(List.of("--output", ResultLayout.loadgenJsonl(phase), "--summary-output",
                ResultLayout.loadgenSummary(phase)));
        return new ProcessSpec(phase + "-loadgen", List.copyOf(cmd), Map.of(), removedEnvironment(), runDir,
                runDir.resolve(ResultLayout.loadgenLog(phase)));
    }
}
