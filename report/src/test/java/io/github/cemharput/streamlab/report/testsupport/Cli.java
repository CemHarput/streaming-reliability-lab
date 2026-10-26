package io.github.cemharput.streamlab.report.testsupport;

import io.github.cemharput.streamlab.bench.json.Json;
import io.github.cemharput.streamlab.report.ReportMain;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.MessageDigest;
import java.util.ArrayList;
import java.util.HexFormat;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.stream.Stream;

/** Runs the tool in-process and captures its output; also hashes directory trees. */
public final class Cli {

    public record Result(int exit, String out, String err) {

        /** The last line of standard output parsed as JSON. */
        public Map<String, Object> summary() {
            String[] lines = out.stripTrailing().split("\n");
            return Json.parseObject(lines[lines.length - 1]);
        }
    }

    private Cli() {
    }

    public static Result run(Path cwd, String... args) {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        ByteArrayOutputStream err = new ByteArrayOutputStream();
        int exit = ReportMain.run(args, new PrintStream(out, true, StandardCharsets.UTF_8),
                new PrintStream(err, true, StandardCharsets.UTF_8), cwd);
        return new Result(exit, out.toString(StandardCharsets.UTF_8), err.toString(StandardCharsets.UTF_8));
    }

    /** Relative file name to sha256 for every regular file below {@code root}. */
    public static Map<String, String> hashes(Path root) throws IOException {
        Map<String, String> m = new TreeMap<>();
        try (Stream<Path> s = Files.walk(root)) {
            for (Path p : (Iterable<Path>) s.filter(Files::isRegularFile)::iterator) {
                m.put(root.relativize(p).toString().replace('\\', '/'), sha256(p));
            }
        }
        return m;
    }

    /** Relative file name to "size/mtime" for every regular file and directory below {@code root}. */
    public static Map<String, String> stats(Path root) throws IOException {
        Map<String, String> m = new TreeMap<>();
        try (Stream<Path> s = Files.walk(root)) {
            for (Path p : (Iterable<Path>) s::iterator) {
                m.put(root.relativize(p).toString().replace('\\', '/'),
                        (Files.isRegularFile(p) ? Files.size(p) : -1) + "/" + Files.getLastModifiedTime(p).toMillis());
            }
        }
        return m;
    }

    public static String sha256(Path p) throws IOException {
        try {
            return HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256").digest(Files.readAllBytes(p)));
        } catch (java.security.NoSuchAlgorithmException ex) {
            throw new IllegalStateException(ex);
        }
    }

    public static List<String> lines(String text) {
        return new ArrayList<>(List.of(text.split("\n", -1)));
    }
}
