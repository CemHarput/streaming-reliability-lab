package io.github.cemharput.streamlab.bench;

import io.github.cemharput.streamlab.bench.CommandRunner.CommandResult;
import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Supplier;

/**
 * Records facts about the machine and the repository. Every external command may fail; the value is then null
 * (and a flag is set where the dictionary has one). Nothing here is fatal.
 */
public final class EnvironmentCapture {

    /** What differs between Windows and the rest, injectable for tests. */
    public record Platform(boolean windows, Map<String, String> env, Supplier<List<String>> cpuInfoLines) {

        public static Platform current() {
            boolean windows = System.getProperty("os.name", "").toLowerCase(java.util.Locale.ROOT).contains("win");
            return new Platform(windows, System.getenv(), EnvironmentCapture::readProcCpuInfo);
        }
    }

    /** The {@code environment} manifest object and the flags it implies. */
    public record Captured(Map<String, Object> map, List<String> flags) {
    }

    private static final java.util.regex.Pattern KNOWN_STATES = java.util.regex.Pattern.compile(
            "ESTABLISHED|LISTENING|LISTEN|TIME[_-]WAIT|CLOSE[_-]WAIT|FIN[_-]WAIT|SYN[_-]SENT|SYN[_-]RECV|LAST[_-]ACK|CLOSING|ESTAB");
    private static final Duration TIMEOUT = SystemCommandRunner.DEFAULT_TIMEOUT;

    private final CommandRunner runner;
    private final Platform platform;

    public EnvironmentCapture(CommandRunner runner, Platform platform) {
        this.runner = runner;
        this.platform = platform;
    }

    /** git facts; {@link GitInfo#UNAVAILABLE} when {@code rev-parse HEAD} fails. */
    public GitInfo git(Path repoRoot, Path outputDir) {
        String root = repoRoot.toString();
        CommandResult head = runner.run(List.of("git", "-C", root, "rev-parse", "HEAD"), TIMEOUT);
        String commit = head.ok() ? head.output().trim() : "";
        if (commit.length() < 7) {
            return GitInfo.UNAVAILABLE;
        }
        CommandResult status = runner.run(List.of("git", "-C", root, "status", "--porcelain"), TIMEOUT);
        List<String> lines = new ArrayList<>();
        if (status.ok()) {
            for (String line : status.output().split("\\R")) {
                if (!line.isBlank() && lines.size() < 200) {
                    lines.add(line);
                }
            }
        }
        boolean dirty = !status.ok() || !lines.isEmpty();
        Instant commitTime = null;
        CommandResult log = runner.run(List.of("git", "-C", root, "log", "-1", "--format=%cI"), TIMEOUT);
        if (log.ok()) {
            try {
                commitTime = java.time.OffsetDateTime.parse(log.output().trim()).toInstant();
            } catch (java.time.format.DateTimeParseException ex) {
                commitTime = null;
            }
        }
        // A directory pattern such as "bench/results/" only matches below the directory: probe a child path.
        CommandResult ignore = runner.run(List.of("git", "-C", root, "check-ignore", "-q",
                outputDir.resolve(".bench-ignore-probe").toString()), TIMEOUT);
        boolean ignoreKnown = ignore.exitCode() == 0 || ignore.exitCode() == 1;
        return new GitInfo(commit, dirty, commitTime, List.copyOf(lines), ignore.exitCode() == 0, ignoreKnown);
    }

    public Captured environment(BenchOptions options, GitInfo git, Set<String> removedEnv) {
        List<String> flags = new ArrayList<>();
        Map<String, Object> env = new LinkedHashMap<>();
        env.put("os", System.getProperty("os.name") + " " + System.getProperty("os.version") + " "
                + System.getProperty("os.arch"));
        env.put("cpu", cpu());
        env.put("memory", memory());
        env.put("java", java());
        env.put("git", git.toMap());
        env.put("powerPlan", powerPlan());
        Map<String, Object> timeWait = timeWait(List.of(options.gatewayPort(), options.simulatorPort()));
        env.put("timeWaitAtStart", timeWait);
        if (timeWait == null) {
            flags.add(Flags.TIME_WAIT_UNAVAILABLE);
        }
        env.put("removedEnvironmentVariables", new ArrayList<>(removedEnv));
        return new Captured(env, flags);
    }

    Map<String, Object> cpu() {
        Map<String, Object> cpu = new LinkedHashMap<>();
        String model = null;
        String source = "unavailable";
        Integer physical = null;
        if (platform.windows()) {
            CommandResult r = runner.run(List.of("reg", "query",
                    "HKLM\\HARDWARE\\DESCRIPTION\\System\\CentralProcessor\\0", "/v", "ProcessorNameString"), TIMEOUT);
            if (r.ok()) {
                for (String line : r.output().split("\\R")) {
                    int i = line.indexOf("REG_SZ");
                    if (line.contains("ProcessorNameString") && i >= 0) {
                        model = line.substring(i + "REG_SZ".length()).trim();
                        source = "reg query ProcessorNameString";
                    }
                }
            }
            CommandResult cores = runner.run(List.of("powershell", "-NoProfile", "-Command",
                    "(Get-CimInstance Win32_Processor | Measure-Object -Property NumberOfCores -Sum).Sum"), TIMEOUT);
            if (cores.ok()) {
                physical = parseInt(cores.output().trim());
            }
        } else {
            List<String> lines = platform.cpuInfoLines().get();
            Set<String> cores = new HashSet<>();
            String physicalId = "0";
            for (String line : lines) {
                String[] kv = line.split(":", 2);
                if (kv.length < 2) {
                    continue;
                }
                String key = kv[0].trim();
                String value = kv[1].trim();
                if (key.equals("model name") && model == null) {
                    model = value;
                    source = "/proc/cpuinfo model name";
                } else if (key.equals("physical id")) {
                    physicalId = value;
                } else if (key.equals("core id")) {
                    cores.add(physicalId + "/" + value);
                }
            }
            if (!cores.isEmpty()) {
                physical = cores.size();
            }
        }
        if (model == null) {
            String id = platform.env().get("PROCESSOR_IDENTIFIER");
            if (id != null && !id.isBlank()) {
                model = id.trim();
                source = "env PROCESSOR_IDENTIFIER";
            }
        }
        cpu.put("model", model);
        cpu.put("modelSource", source);
        cpu.put("logicalProcessors", Runtime.getRuntime().availableProcessors());
        cpu.put("physicalCores", physical);
        return cpu;
    }

    private static Integer parseInt(String text) {
        try {
            return Integer.valueOf(text);
        } catch (NumberFormatException ex) {
            return null;
        }
    }

    static List<String> readProcCpuInfo() {
        try {
            return Files.readAllLines(Path.of("/proc/cpuinfo"));
        } catch (IOException | RuntimeException ex) {
            return List.of();
        }
    }

    Map<String, Object> memory() {
        Map<String, Object> m = new LinkedHashMap<>();
        Long total = null;
        Long free = null;
        if (ManagementFactory.getOperatingSystemMXBean() instanceof com.sun.management.OperatingSystemMXBean os) {
            total = os.getTotalMemorySize();
            free = os.getFreeMemorySize();
        }
        m.put("totalBytes", total);
        m.put("freeBytesAtStart", free);
        return m;
    }

    static Map<String, Object> java() {
        Map<String, Object> m = new LinkedHashMap<>();
        m.put("version", System.getProperty("java.version"));
        m.put("vendor", System.getProperty("java.vendor"));
        m.put("vmName", System.getProperty("java.vm.name"));
        m.put("vmVersion", System.getProperty("java.vm.version"));
        m.put("home", System.getProperty("java.home"));
        return m;
    }

    String powerPlan() {
        if (!platform.windows()) {
            return null;
        }
        CommandResult r = runner.run(List.of("powercfg", "/getactivescheme"), TIMEOUT);
        return r.ok() ? r.output().trim() : null;
    }

    /** TIME_WAIT sockets in total and touching the two ports; null when the command is unavailable. */
    public Map<String, Object> timeWait(List<Integer> ports) {
        CommandResult r = platform.windows() ? runner.run(List.of("netstat", "-an", "-p", "tcp"), TIMEOUT)
                : runner.run(List.of("ss", "-tan"), TIMEOUT);
        if (!r.ok()) {
            return null;
        }
        return countTimeWait(r.output(), ports);
    }

    static Map<String, Object> countTimeWait(String output, List<Integer> ports) {
        if (!KNOWN_STATES.matcher(output).find()) {
            return null; // no recognisable TCP state at all (e.g. localised output): a count of 0 would be a lie
        }
        long total = 0;
        long[] perPort = new long[ports.size()];
        for (String line : output.split("\\R")) {
            if (!line.contains("TIME_WAIT") && !line.contains("TIME-WAIT")) {
                continue;
            }
            total++;
            String[] tokens = line.trim().split("\\s+");
            for (int i = 0; i < ports.size(); i++) {
                String suffix = ":" + ports.get(i);
                for (String token : tokens) {
                    if (token.endsWith(suffix)) {
                        perPort[i]++;
                        break;
                    }
                }
            }
        }
        Map<String, Object> m = new LinkedHashMap<>();
        m.put("total", total);
        m.put("gatewayPort", perPort.length > 0 ? perPort[0] : 0L);
        m.put("simulatorPort", perPort.length > 1 ? perPort[1] : 0L);
        return m;
    }
}
