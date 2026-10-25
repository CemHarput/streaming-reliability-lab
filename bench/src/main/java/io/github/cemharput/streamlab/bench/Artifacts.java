package io.github.cemharput.streamlab.bench;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HexFormat;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/** Locates the jars the runner starts. The runner never builds anything. */
public final class Artifacts {

    public static final String BUILD_HINT = "run './mvnw -B package -DskipTests' first";

    private Artifacts() {
    }

    /** What is wrong with an artifact; the dry run labels each problem with its kind. */
    public enum Kind { MISSING, AMBIGUOUS, STALE }

    /** Problem messages with their kinds, in parallel lists. */
    private static final class Found {
        final List<String> messages = new ArrayList<>();
        final List<Kind> kinds = new ArrayList<>();

        void add(Kind kind, String message) {
            kinds.add(kind);
            messages.add(message);
        }
    }

    /**
     * Paths of the artefacts. Missing or ambiguous ones are null (or empty) and described in {@code problems}.
     */
    public record Resolved(Path gatewayJar, Path loadgenJar, List<Path> loadgenLib, List<Path> simulatorClasspath,
            Path benchJar, List<String> problems, List<Kind> problemKinds) {

        public Resolved {
            if (problems.size() != problemKinds.size()) {
                throw new IllegalArgumentException("problems and problemKinds differ in size: " + problems.size()
                        + " vs " + problemKinds.size());
            }
            loadgenLib = List.copyOf(loadgenLib);
            simulatorClasspath = List.copyOf(simulatorClasspath);
            problems = List.copyOf(problems);
            problemKinds = List.copyOf(problemKinds);
        }

        /** Problems of unknown kind are treated as missing. */
        public Resolved(Path gatewayJar, Path loadgenJar, List<Path> loadgenLib, List<Path> simulatorClasspath,
                Path benchJar, List<String> problems) {
            this(gatewayJar, loadgenJar, loadgenLib, simulatorClasspath, benchJar, problems,
                    problems.stream().map(p -> Kind.MISSING).toList());
        }

        public boolean complete() {
            return problems.isEmpty();
        }
    }

    /** Path, size, SHA-256 and modification time of one file. */
    public record ArtifactInfo(Path path, long sizeBytes, String sha256, Instant lastModified) {

        public static ArtifactInfo of(Path path) throws IOException {
            return new ArtifactInfo(path.toAbsolutePath().normalize(), Files.size(path), Artifacts.sha256(path),
                    Files.getLastModifiedTime(path).toInstant());
        }

        public Map<String, Object> toMap() {
            Map<String, Object> m = new LinkedHashMap<>();
            m.put("path", path.toString());
            m.put("sizeBytes", sizeBytes);
            m.put("sha256", sha256);
            m.put("lastModified", lastModified.toString());
            return m;
        }
    }

    public static Resolved resolve(BenchOptions o) {
        Found problems = new Found();
        Path root = o.repoRoot();

        Path gateway = o.gatewayJar() != null ? explicit("gateway jar", o.gatewayJar(), problems)
                : single("gateway jar", root.resolve("gateway").resolve("target"), "gateway-", problems);
        Path loadgen = o.loadgenJar() != null ? explicit("loadgen jar", o.loadgenJar(), problems)
                : single("loadgen jar", root.resolve("loadgen").resolve("target"), "loadgen-", problems);

        List<Path> lib = List.of();
        if (loadgen != null) {
            Path libDir = loadgen.toAbsolutePath().getParent().resolve("lib");
            lib = jars(libDir);
            if (lib.isEmpty()) {
                problems.add(Kind.MISSING, "loadgen lib directory missing or without jars: " + libDir + " ("
                        + BUILD_HINT + ")");
            }
            duplicates(lib, libDir).forEach(d -> problems.add(Kind.STALE, d));
        }

        List<Path> classpath;
        if (o.simulatorClasspath() != null) {
            classpath = new ArrayList<>();
            for (Path entry : o.simulatorClasspath()) {
                if (Files.exists(entry)) {
                    classpath.add(entry);
                } else {
                    problems.add(Kind.MISSING, "simulator classpath entry not found: " + entry);
                }
            }
        } else {
            Path libDir = root.resolve("bench").resolve("target").resolve("lib");
            classpath = jars(libDir);
            boolean hasSimulator = classpath.stream().anyMatch(p -> p.getFileName().toString().startsWith("simulator-"));
            if (!hasSimulator) {
                problems.add(Kind.MISSING, "simulator jar not found in " + libDir + " (" + BUILD_HINT + ")");
            }
            duplicates(classpath, libDir).forEach(d -> problems.add(Kind.STALE, d));
        }

        Path benchJar = null;
        List<Path> benchJars = matching(root.resolve("bench").resolve("target"), "bench-");
        if (benchJars.size() == 1) {
            benchJar = benchJars.get(0);
        }
        return new Resolved(gateway, loadgen, List.copyOf(lib), List.copyOf(classpath), benchJar,
                List.copyOf(problems.messages), List.copyOf(problems.kinds));
    }

    /**
     * {@code <id>-<version>[-<classifier>].jar}; the version may end in -SNAPSHOT. This is deliberately conservative:
     * a name that does not match (no {@code -<digit>...} version part) is never reported, so an unusual naming scheme
     * can only hide a stale jar, never produce a false precondition failure. Versions that start with a letter are
     * therefore not checked.
     */
    private static final java.util.regex.Pattern JAR_NAME = java.util.regex.Pattern.compile(
            "^(.*?)-(\\d[0-9A-Za-z.]*(?:-SNAPSHOT)?)(?:-(.+))?\\.jar$");

    /**
     * Two jars with the same artifact id and classifier but different versions mean a stale lib directory. The same
     * version with different classifiers (for example {@code -tests}) is fine.
     */
    static List<String> duplicates(List<Path> jars, Path dir) {
        java.util.Map<String, List<String>> byKey = new java.util.TreeMap<>();
        for (Path jar : jars) {
            String name = jar.getFileName().toString();
            java.util.regex.Matcher m = JAR_NAME.matcher(name);
            if (m.matches()) {
                String key = m.group(1) + (m.group(3) == null ? "" : " [" + m.group(3) + "]");
                byKey.computeIfAbsent(key, k -> new ArrayList<>()).add(name);
            }
        }
        List<String> problems = new ArrayList<>();
        byKey.forEach((key, names) -> {
            if (names.size() > 1) {
                problems.add("stale jars in " + dir + ": " + names + " (same artifact, different versions; run"
                        + " './mvnw -B clean package -DskipTests')");
            }
        });
        return problems;
    }

    private static Path explicit(String what, Path path, Found problems) {
        if (!Files.isRegularFile(path)) {
            problems.add(Kind.MISSING, what + " not found: " + path);
            return null;
        }
        return path;
    }

    private static Path single(String what, Path dir, String prefix, Found problems) {
        List<Path> found = matching(dir, prefix);
        if (found.isEmpty()) {
            problems.add(Kind.MISSING, what + " not found in " + dir + " (" + BUILD_HINT + ")");
            return null;
        }
        if (found.size() > 1) {
            problems.add(Kind.AMBIGUOUS, what + " is ambiguous in " + dir + ": " + found.stream().map(p -> p.getFileName().toString())
                    .toList() + " (use the explicit option)");
            return null;
        }
        return found.get(0).toAbsolutePath().normalize();
    }

    /** Runnable jars {@code <prefix>*.jar}, without sources, javadoc and test jars, sorted by name. */
    static List<Path> matching(Path dir, String prefix) {
        List<Path> result = new ArrayList<>();
        if (!Files.isDirectory(dir)) {
            return result;
        }
        try (DirectoryStream<Path> stream = Files.newDirectoryStream(dir, prefix + "*.jar")) {
            for (Path p : stream) {
                String name = p.getFileName().toString();
                if (Files.isRegularFile(p) && !name.endsWith("-sources.jar") && !name.endsWith("-javadoc.jar")
                        && !name.endsWith("-tests.jar")) {
                    result.add(p);
                }
            }
        } catch (IOException ex) {
            return result;
        }
        result.sort(Comparator.comparing(p -> p.getFileName().toString()));
        return result;
    }

    /** All jars of a directory, sorted by file name, absolute. */
    static List<Path> jars(Path dir) {
        List<Path> result = new ArrayList<>();
        for (Path p : matching(dir, "")) {
            result.add(p.toAbsolutePath().normalize());
        }
        return result;
    }

    static String sha256(Path file) throws IOException {
        try (InputStream in = Files.newInputStream(file)) {
            MessageDigest digest = MessageDigest.getInstance("SHA-256");
            byte[] buffer = new byte[64 * 1024];
            int n;
            while ((n = in.read(buffer)) > 0) {
                digest.update(buffer, 0, n);
            }
            return HexFormat.of().formatHex(digest.digest());
        } catch (NoSuchAlgorithmException ex) {
            throw new IllegalStateException(ex);
        }
    }
}
