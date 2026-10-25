package io.github.cemharput.streamlab.bench;

import io.github.cemharput.streamlab.bench.BenchOptions.ChecklistMode;
import java.io.File;
import java.nio.file.Files;
import java.nio.file.InvalidPathException;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/** Hand-written parser for {@code --name value} and {@code --name=value}. Fails fast with a {@link UsageException}. */
public final class BenchOptionsParser {

    private static final Set<String> FLAGS = Set.of("dry-run", "help");
    private static final Set<String> VALUE_OPTIONS = Set.of("preset", "repetitions", "duration", "warmup", "ramp-up",
            "concurrency", "mode-order", "cooldown", "output-dir", "repo-root", "gateway-jar", "loadgen-jar",
            "simulator-classpath", "gateway-port", "simulator-port", "checklist", "operator-note");
    private static final Pattern DURATION = Pattern.compile("(\\d{1,9})(ms|s|m)");
    private static final Duration MIN_RUN = Duration.ofSeconds(1);
    private static final Duration MAX_RUN = Duration.ofHours(2);
    private static final Duration MAX_COOLDOWN = Duration.ofMinutes(10);
    private static final int MAX_CONCURRENCY_VALUES = 10;
    private static final int MAX_NOTE = 500;

    public static final String USAGE = """
            Usage: java -jar bench.jar [options]

            Benchmark protocol runner (protocol version 1, docs/benchmark-protocol.md). For every cell
            (concurrency x connection mode x repetition) it starts a fresh simulator and gateway JVM, runs a
            warm-up and a measured loadgen run and writes raw files plus manifest.json to
            <output-dir>/<run-id>/. It analyses nothing and makes no performance claim.
            Build first: ./mvnw -B package -DskipTests

              --preset default|smoke   parameter preset (default: default)
              --repetitions N          measured runs per mode and concurrency, 1..20 (default 4; smoke 2)
              --duration D             measured run length, 1s..2h (default 60s; smoke 3s)
              --warmup D               warm-up length, 1s..2h (default 20s; smoke 2s)
              --ramp-up D              loadgen ramp-up, shorter than --duration and --warmup (default 5s; smoke 0s)
              --concurrency LIST       comma separated, 1..10000, at most 10, no duplicates (default 50,100,150; smoke 4)
              --mode-order A,B         shared,per-request in the order A,B (default shared,per-request)
              --cooldown D             pause between runs, 0..10m (default 30s; smoke 1s)
              --output-dir PATH        result root (default <repo-root>/bench/results; created when missing)
              --repo-root PATH         repository root (default: current directory)
              --gateway-jar PATH       gateway jar (default: the single gateway/target/gateway-*.jar)
              --loadgen-jar PATH       loadgen jar, lib/ next to it (default: the single loadgen/target/loadgen-*.jar)
              --simulator-classpath P  simulator classpath, entries separated by the path separator
                                       (default: all jars in bench/target/lib, sorted by name)
              --gateway-port N         gateway port (default 8080)
              --simulator-port N       simulator port (default 50051)
              --checklist ask|yes|skip operator checklist (default ask; ask needs a terminal)
              --operator-note TEXT     free text stored in the manifest, at most 500 characters
              --dry-run                print the plan and the exact commands; start nothing, write nothing
              -h, --help               print this text and exit 0

            Durations are <integer>ms, <integer>s or <integer>m. Exit codes: 0 all planned runs completed (flags do
            not change it), 1 harness or run failure after the result directory exists (or the result directory cannot be
            created, or the manifest cannot be written), 2 invalid arguments or a failed precondition (nothing was
            started).
            """;

    private BenchOptionsParser() {
    }

    public static BenchOptions parse(String[] args, Path cwd) throws UsageException {
        Map<String, String> values = new HashMap<>();
        boolean dryRun = false;
        boolean help = false;
        for (int i = 0; i < args.length; i++) {
            String arg = args[i];
            if (arg.equals("-h")) {
                help = true;
                continue;
            }
            if (!arg.startsWith("--") || arg.length() <= 2) {
                throw new UsageException("unexpected argument: " + arg);
            }
            String name = arg.substring(2);
            String value = null;
            int eq = name.indexOf('=');
            if (eq >= 0) {
                value = name.substring(eq + 1);
                name = name.substring(0, eq);
            }
            if (FLAGS.contains(name)) {
                if (value != null) {
                    throw new UsageException("--" + name + " takes no value");
                }
                if (name.equals("help")) {
                    help = true;
                } else {
                    if (dryRun) {
                        throw new UsageException("duplicate option --dry-run");
                    }
                    dryRun = true;
                }
                continue;
            }
            if (!VALUE_OPTIONS.contains(name)) {
                throw new UsageException("unknown option --" + name);
            }
            if (value == null) {
                if (i + 1 >= args.length || args[i + 1].startsWith("--")) {
                    throw new UsageException("missing value for --" + name);
                }
                value = args[++i];
            }
            if (values.put(name, value) != null) {
                throw new UsageException("duplicate option --" + name);
            }
        }
        if (help) {
            return BenchOptions.helpRequest();
        }

        Preset preset = Preset.DEFAULT;
        if (values.containsKey("preset")) {
            preset = Preset.fromCliName(values.get("preset"));
            if (preset == null) {
                throw new UsageException("--preset must be default or smoke: " + values.get("preset"));
            }
        }
        int repetitions = (int) integer(values, "repetitions", preset.repetitions(), 1, 20);
        Duration duration = bounded("duration", values, preset.duration(), MIN_RUN, MAX_RUN);
        Duration warmup = bounded("warmup", values, preset.warmup(), MIN_RUN, MAX_RUN);
        Duration rampUp = bounded("ramp-up", values, preset.rampUp(), Duration.ZERO, MAX_RUN);
        if (rampUp.compareTo(duration) >= 0) {
            throw new UsageException("--ramp-up must be shorter than --duration");
        }
        if (rampUp.compareTo(warmup) >= 0) {
            throw new UsageException("--ramp-up must be shorter than --warmup");
        }
        Duration cooldown = bounded("cooldown", values, preset.cooldown(), Duration.ZERO, MAX_COOLDOWN);
        List<Integer> concurrency = concurrency(values.get("concurrency"), preset);
        List<ConnectionMode> modeOrder = modeOrder(values.get("mode-order"));

        int gatewayPort = (int) integer(values, "gateway-port", 8080, 1, 65535);
        int simulatorPort = (int) integer(values, "simulator-port", 50051, 1, 65535);
        if (gatewayPort == simulatorPort) {
            throw new UsageException("--gateway-port and --simulator-port must differ");
        }
        ChecklistMode checklist = ChecklistMode.ASK;
        if (values.containsKey("checklist")) {
            String text = values.get("checklist");
            checklist = switch (text) {
                case "ask" -> ChecklistMode.ASK;
                case "yes" -> ChecklistMode.YES;
                case "skip" -> ChecklistMode.SKIP;
                default -> throw new UsageException("--checklist must be ask, yes or skip: " + text);
            };
        }
        String note = values.get("operator-note");
        if (note != null && note.length() > MAX_NOTE) {
            throw new UsageException("--operator-note must not exceed " + MAX_NOTE + " characters");
        }

        Path repoRoot = repoRoot(values.get("repo-root"), cwd);
        Path outputDir = values.containsKey("output-dir")
                ? resolve("output-dir", values.get("output-dir"), cwd)
                : repoRoot.resolve("bench").resolve("results");
        if (Files.exists(outputDir) && !Files.isDirectory(outputDir)) {
            throw new UsageException("--output-dir is not a directory: " + outputDir);
        }
        Path gatewayJar = values.containsKey("gateway-jar") ? resolve("gateway-jar", values.get("gateway-jar"), cwd) : null;
        Path loadgenJar = values.containsKey("loadgen-jar") ? resolve("loadgen-jar", values.get("loadgen-jar"), cwd) : null;
        List<Path> classpath = null;
        if (values.containsKey("simulator-classpath")) {
            classpath = new ArrayList<>();
            for (String entry : values.get("simulator-classpath").split(Pattern.quote(File.pathSeparator))) {
                if (!entry.isBlank()) {
                    classpath.add(resolve("simulator-classpath", entry, cwd));
                }
            }
            if (classpath.isEmpty()) {
                throw new UsageException("--simulator-classpath has no entries");
            }
        }
        return new BenchOptions(false, preset, repetitions, duration, warmup, rampUp, concurrency, modeOrder, cooldown,
                outputDir, repoRoot, gatewayJar, loadgenJar, classpath == null ? null : List.copyOf(classpath),
                gatewayPort, simulatorPort, checklist, note, dryRun, String.join(" ", args));
    }

    private static Path repoRoot(String text, Path cwd) throws UsageException {
        Path root = text == null ? cwd.toAbsolutePath().normalize() : resolve("repo-root", text, cwd);
        if (!Files.isRegularFile(root.resolve("pom.xml")) || !Files.isDirectory(root.resolve("gateway"))
                || !Files.isDirectory(root.resolve("loadgen"))) {
            throw new UsageException("not the repository root (pom.xml, gateway/ and loadgen/ expected): " + root
                    + " (use --repo-root)");
        }
        return root;
    }

    private static Path resolve(String option, String text, Path cwd) throws UsageException {
        try {
            return cwd.toAbsolutePath().resolve(Path.of(text)).normalize();
        } catch (InvalidPathException ex) {
            throw new UsageException("--" + option + " is not a valid path: " + text);
        }
    }

    private static List<Integer> concurrency(String text, Preset preset) throws UsageException {
        if (text == null) {
            return preset.concurrency();
        }
        List<Integer> result = new ArrayList<>();
        Set<Integer> seen = new HashSet<>();
        for (String part : text.split(",", -1)) {
            int value;
            try {
                value = Integer.parseInt(part.trim());
            } catch (NumberFormatException ex) {
                throw new UsageException("--concurrency must be a comma separated list of integers: " + text);
            }
            if (value < 1 || value > 10_000) {
                throw new UsageException("--concurrency values must be between 1 and 10000: " + text);
            }
            if (!seen.add(value)) {
                throw new UsageException("--concurrency has a duplicate value: " + value);
            }
            result.add(value);
        }
        if (result.size() > MAX_CONCURRENCY_VALUES) {
            throw new UsageException("--concurrency accepts at most " + MAX_CONCURRENCY_VALUES + " values");
        }
        return List.copyOf(result);
    }

    private static List<ConnectionMode> modeOrder(String text) throws UsageException {
        if (text == null) {
            return List.of(ConnectionMode.SHARED, ConnectionMode.PER_REQUEST);
        }
        String[] parts = text.split(",", -1);
        if (parts.length != 2) {
            throw new UsageException("--mode-order needs exactly two modes, e.g. shared,per-request: " + text);
        }
        List<ConnectionMode> result = new ArrayList<>();
        for (String part : parts) {
            ConnectionMode mode = ConnectionMode.fromProperty(part.trim()).orElseThrow(
                    () -> new UsageException("--mode-order: unknown mode '" + part + "' (shared or per-request)"));
            result.add(mode);
        }
        if (result.get(0) == result.get(1)) {
            throw new UsageException("--mode-order must name each mode exactly once: " + text);
        }
        return List.copyOf(result);
    }

    private static long integer(Map<String, String> values, String name, long defaultValue, long min, long max)
            throws UsageException {
        String text = values.get(name);
        if (text == null) {
            return defaultValue;
        }
        long v;
        try {
            v = Long.parseLong(text);
        } catch (NumberFormatException ex) {
            throw new UsageException("--" + name + " must be an integer: " + text);
        }
        if (v < min || v > max) {
            throw new UsageException("--" + name + " must be between " + min + " and " + max + ": " + text);
        }
        return v;
    }

    private static Duration bounded(String name, Map<String, String> values, Duration defaultValue, Duration min,
            Duration max) throws UsageException {
        String text = values.get(name);
        if (text == null) {
            return defaultValue;
        }
        Matcher m = DURATION.matcher(text);
        if (!m.matches()) {
            throw new UsageException("--" + name + " must look like 500ms, 30s or 2m: " + text);
        }
        long n = Long.parseLong(m.group(1));
        Duration d = switch (m.group(2)) {
            case "ms" -> Duration.ofMillis(n);
            case "s" -> Duration.ofSeconds(n);
            default -> Duration.ofMinutes(n);
        };
        if (d.compareTo(min) < 0 || d.compareTo(max) > 0) {
            throw new UsageException("--" + name + " must be between " + format(min) + " and " + format(max) + ": "
                    + text);
        }
        return d;
    }

    /** {@code 60s}, {@code 1500ms}, {@code 0s}: the notation the loadgen CLI accepts. */
    public static String format(Duration d) {
        long ms = d.toMillis();
        return ms % 1000 == 0 ? (ms / 1000) + "s" : ms + "ms";
    }
}
