package io.github.cemharput.streamlab.loadgen;

import io.github.cemharput.streamlab.loadgen.Options.IntRange;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.file.Files;
import java.nio.file.InvalidPathException;
import java.nio.file.Path;
import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/** Hand-written parser for {@code --name value} and {@code --name=value}. Fails fast with a {@link UsageException}. */
public final class OptionsParser {

    private static final Set<String> FLAGS = Set.of("overwrite", "help");
    private static final Set<String> VALUE_OPTIONS = Set.of("target", "concurrency", "duration", "requests",
            "ramp-up", "chunks", "interval-ms", "first-delay-ms", "fault", "fault-at-seq", "fault-rate", "seed",
            "output", "summary-output", "connect-timeout", "idle-timeout", "drain-timeout", "reject-pause");
    private static final List<String> FAULTS = List.of("NONE", "UNAVAILABLE", "INTERNAL", "HANG");
    private static final Duration MAX_DURATION = Duration.ofDays(7);
    private static final Pattern DURATION = Pattern.compile("(\\d{1,9})(ms|s|m)");
    private static final Pattern RANGE = Pattern.compile("(\\d{1,9})(?:-(\\d{1,9}))?");

    public static final String USAGE = """
            Usage: java -jar loadgen.jar --target URL (--duration D | --requests M) --output PATH [options]

            Closed-model SSE load generator: --concurrency workers each open one stream after the other.
            Writes one JSONL record per request plus a header and a footer record.

              --target URL            gateway stream endpoint without query, e.g. http://localhost:8080/stream
              --concurrency N         number of workers, 1..10000 (default 1)
              --duration D            run time, e.g. 30s, 2m, 500ms (exactly one of --duration/--requests)
              --requests M            total number of requests, 1..100000000
              --ramp-up D             linear worker start-up time (default 0s; shorter than --duration)
              --chunks N|A-B          chunks per stream, 1..1000000 (default 10)
              --interval-ms N|A-B     delay between chunks, 0..60000 (default 50)
              --first-delay-ms N|A-B  delay before the first chunk, 0..60000 (default 0)
              --fault KIND            NONE, UNAVAILABLE, INTERNAL or HANG (default NONE)
              --fault-at-seq N|A-B    chunk seq at which the fault fires, >= 0 (default 0)
              --fault-rate P          share of requests that carry the fault, 0..1 (default 1.0)
              --seed L                seed of the request parameter sequence (default 0)
              --output PATH           JSONL result file (required; the directory must exist)
              --summary-output PATH   optional JSON file with the convenience summary
              --overwrite             allow replacing existing output files
              --connect-timeout D     connect timeout (default 5s)
              --idle-timeout D        no-progress timeout per request (default 90s)
              --drain-timeout D       wait for open streams after the run ends (default 30s)
              --reject-pause D        pause of a worker after an HTTP 503 (default 100ms)
              -h, --help              print this text and exit 0

            Durations are <integer>ms, <integer>s or <integer>m. Exit codes: 0 run completed (whatever the
            outcomes), 1 I/O or unexpected error, 2 invalid arguments.
            """;

    private OptionsParser() {
    }

    public static Options parse(String[] args) throws UsageException {
        Map<String, String> values = new HashMap<>();
        boolean overwrite = false;
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
                    if (overwrite) {
                        throw new UsageException("duplicate option --overwrite");
                    }
                    overwrite = true;
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
            return help();
        }

        URI target = target(required(values, "target"));
        int concurrency = (int) integer(values, "concurrency", 1, 1, 10_000);
        Duration duration = values.containsKey("duration") ? duration("duration", values.get("duration")) : null;
        Long requests = values.containsKey("requests") ? integer(values, "requests", 0, 1, 100_000_000) : null;
        if (duration != null && requests != null) {
            throw new UsageException("--duration and --requests cannot be used together");
        }
        if (duration == null && requests == null) {
            throw new UsageException("one of --duration or --requests is required");
        }
        if (duration != null && duration.isZero()) {
            throw new UsageException("--duration must be greater than 0");
        }
        Duration rampUp = values.containsKey("ramp-up") ? duration("ramp-up", values.get("ramp-up")) : Duration.ZERO;
        if (duration != null && rampUp.compareTo(duration) >= 0) {
            throw new UsageException("--ramp-up must be shorter than --duration");
        }
        IntRange chunks = range(values, "chunks", IntRange.of(10), 1, 1_000_000);
        IntRange interval = range(values, "interval-ms", IntRange.of(50), 0, 60_000);
        IntRange firstDelay = range(values, "first-delay-ms", IntRange.of(0), 0, 60_000);
        String fault = values.getOrDefault("fault", "NONE");
        if (!FAULTS.contains(fault)) {
            throw new UsageException("--fault must be one of " + String.join(", ", FAULTS) + ": " + fault);
        }
        IntRange faultAtSeq = range(values, "fault-at-seq", IntRange.of(0), 0, Integer.MAX_VALUE);
        double faultRate = faultRate(values.getOrDefault("fault-rate", "1.0"));
        long seed = seed(values.getOrDefault("seed", "0"));
        Path output = path(values, "output", true, overwrite);
        Path summary = path(values, "summary-output", false, overwrite);
        if (summary != null && summary.toAbsolutePath().normalize().equals(output.toAbsolutePath().normalize())) {
            throw new UsageException("--output and --summary-output must be different files");
        }
        Duration connect = positive("connect-timeout", values.getOrDefault("connect-timeout", "5s"));
        Duration idle = positive("idle-timeout", values.getOrDefault("idle-timeout", "90s"));
        Duration drain = duration("drain-timeout", values.getOrDefault("drain-timeout", "30s"));
        Duration rejectPause = duration("reject-pause", values.getOrDefault("reject-pause", "100ms"));

        return new Options(false, target, concurrency, duration, requests, rampUp, chunks, interval, firstDelay, fault,
                faultAtSeq, faultRate, seed, output, summary, overwrite, connect, idle, drain, rejectPause);
    }

    private static Options help() {
        return new Options(true, null, 0, null, null, null, null, null, null, null, null, 0, 0, null, null, false,
                null, null, null, null);
    }

    private static String required(Map<String, String> values, String name) throws UsageException {
        String v = values.get(name);
        if (v == null) {
            throw new UsageException("missing required option --" + name);
        }
        return v;
    }

    private static URI target(String text) throws UsageException {
        URI uri;
        try {
            uri = new URI(text);
        } catch (URISyntaxException ex) {
            throw new UsageException("--target is not a valid URL: " + text);
        }
        String scheme = uri.getScheme();
        if (scheme == null || !(scheme.equals("http") || scheme.equals("https"))) {
            throw new UsageException("--target must be an http or https URL: " + text);
        }
        if (uri.getHost() == null) {
            throw new UsageException("--target has no host: " + text);
        }
        if (uri.getRawQuery() != null || uri.getRawFragment() != null) {
            throw new UsageException("--target must not contain a query or fragment (the parameters are generated): "
                    + text);
        }
        return uri;
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

    private static Duration duration(String name, String text) throws UsageException {
        Matcher m = DURATION.matcher(text);
        if (!m.matches()) {
            throw new UsageException("--" + name + " must look like 500ms, 30s or 2m: " + text);
        }
        long n = Long.parseLong(m.group(1));
        Duration result = switch (m.group(2)) {
            case "ms" -> Duration.ofMillis(n);
            case "s" -> Duration.ofSeconds(n);
            default -> Duration.ofMinutes(n);
        };
        if (result.compareTo(MAX_DURATION) > 0) {
            throw new UsageException("--" + name + " must not exceed 7d: " + text);
        }
        return result;
    }

    private static Duration positive(String name, String text) throws UsageException {
        Duration d = duration(name, text);
        if (d.isZero()) {
            throw new UsageException("--" + name + " must be greater than 0");
        }
        return d;
    }

    private static IntRange range(Map<String, String> values, String name, IntRange defaultValue, int min, int max)
            throws UsageException {
        String text = values.get(name);
        if (text == null) {
            return defaultValue;
        }
        Matcher m = RANGE.matcher(text);
        if (!m.matches()) {
            throw new UsageException("--" + name + " must be N or MIN-MAX: " + text);
        }
        long lo = Long.parseLong(m.group(1));
        long hi = m.group(2) == null ? lo : Long.parseLong(m.group(2));
        if (lo > hi) {
            throw new UsageException("--" + name + " range is reversed: " + text);
        }
        if (lo < min || hi > max) {
            throw new UsageException("--" + name + " must be within " + min + ".." + max + ": " + text);
        }
        return new IntRange((int) lo, (int) hi);
    }

    private static double faultRate(String text) throws UsageException {
        double v;
        try {
            v = Double.parseDouble(text);
        } catch (NumberFormatException ex) {
            throw new UsageException("--fault-rate must be a number between 0 and 1: " + text);
        }
        if (Double.isNaN(v) || v < 0 || v > 1) {
            throw new UsageException("--fault-rate must be between 0 and 1: " + text);
        }
        return v;
    }

    private static long seed(String text) throws UsageException {
        try {
            return Long.parseLong(text);
        } catch (NumberFormatException ex) {
            throw new UsageException("--seed must be a 64-bit integer: " + text);
        }
    }

    private static Path path(Map<String, String> values, String name, boolean required, boolean overwrite)
            throws UsageException {
        String text = required ? required(values, name) : values.get(name);
        if (text == null) {
            return null;
        }
        Path path;
        try {
            path = Path.of(text);
        } catch (InvalidPathException ex) {
            throw new UsageException("--" + name + " is not a valid path: " + text);
        }
        Path parent = path.toAbsolutePath().getParent();
        if (parent == null || !Files.isDirectory(parent)) {
            throw new UsageException("--" + name + ": directory does not exist: " + parent);
        }
        if (Files.isDirectory(path)) {
            throw new UsageException("--" + name + " is a directory: " + text);
        }
        if (Files.exists(path) && !overwrite) {
            throw new UsageException("--" + name + ": file exists (use --overwrite to replace it): " + text);
        }
        return path;
    }
}
