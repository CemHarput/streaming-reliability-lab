package io.github.cemharput.streamlab.report;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Writes the fixed list of output files. The names do not depend on the data and all of them are written every time,
 * so no stale file can remain. All files are first written to unique {@code .report-*.tmp} files next to their
 * targets and only then moved into place; on an error only those temp files (and a directory this run created and
 * that is still empty) are deleted. Nothing else is ever deleted or truncated.
 */
public final class OutputWriter {

    public static final String REPORT = "benchmark-report.md";
    public static final String DATA_DIR = "benchmark";

    /** The data files below {@code benchmark/}, in a fixed order. */
    public static final List<String> DATA_FILES = List.of("benchmark/runs.csv", "benchmark/cells.csv",
            "benchmark/analysis.json", "benchmark/chart-completed-per-second.svg", "benchmark/chart-ttfc-p50.svg",
            "benchmark/chart-ttfc-p99.svg", "benchmark/chart-total-p99.svg");

    public static final List<String> ALL_FILES = concat();

    private OutputWriter() {
    }

    private static List<String> concat() {
        List<String> all = new ArrayList<>();
        all.add(REPORT);
        all.addAll(DATA_FILES);
        return List.copyOf(all);
    }

    /** The names of {@code names} that already exist below {@code out}. */
    public static List<String> existing(Path out, List<String> names) {
        List<String> found = new ArrayList<>();
        for (String name : names) {
            if (Files.exists(out.resolve(name))) {
                found.add(name);
            }
        }
        return found;
    }

    /** The write failed; {@code replaced} are the final files that were already replaced when it happened. */
    public static final class WriteException extends IOException {
        private final List<String> replaced;

        WriteException(String message, List<String> replaced, Throwable cause) {
            super(message + (replaced.isEmpty() ? " (no final file was replaced)"
                    : " (already replaced: " + String.join(", ", replaced) + ")"), cause);
            this.replaced = List.copyOf(replaced);
        }

        public List<String> replaced() {
            return replaced;
        }
    }

    /**
     * Writes every entry (name relative to {@code out}, content): first all directories and all temporary files
     * (unique names created by the JDK next to the target, so a user's file is never reused), then the moves. On an
     * error only the temporary files created here are deleted; files that were already replaced are named in the
     * exception. Files this method did not create are never truncated or deleted.
     */
    public static void write(Path out, Map<String, String> contents) throws IOException {
        Map<String, Path> temps = new java.util.LinkedHashMap<>();
        List<String> replaced = new ArrayList<>();
        List<Path> createdDirs = new ArrayList<>();
        try {
            for (Map.Entry<String, String> e : contents.entrySet()) {
                Path target = out.resolve(e.getKey());
                Path parent = target.getParent();
                if (!Files.exists(parent)) {
                    Files.createDirectories(parent);
                    createdDirs.add(parent);
                }
                Path tmp = createTemp(parent);
                temps.put(e.getKey(), tmp);
                Files.write(tmp, e.getValue().getBytes(StandardCharsets.UTF_8));
            }
            for (Map.Entry<String, Path> e : temps.entrySet()) {
                Files.move(e.getValue(), out.resolve(e.getKey()), StandardCopyOption.REPLACE_EXISTING);
                replaced.add(e.getKey());
            }
        } catch (IOException | RuntimeException ex) {
            for (Path tmp : temps.values()) {
                try {
                    Files.deleteIfExists(tmp);
                } catch (IOException ignored) {
                    // best effort: only files created by this method are touched
                }
            }
            if (replaced.isEmpty()) {
                for (Path dir : createdDirs.reversed()) {
                    try {
                        Files.deleteIfExists(dir); // only succeeds when empty
                    } catch (IOException ignored) {
                        // not empty or not removable: leave it
                    }
                }
            }
            throw new WriteException("writing the output failed: " + ex.getClass().getSimpleName(), replaced, ex);
        }
    }

    /** A unique temp file; on POSIX file systems with the permissions of a normal file (644, subject to umask). */
    private static Path createTemp(Path parent) throws IOException {
        if (parent.getFileSystem().supportedFileAttributeViews().contains("posix")) {
            return Files.createTempFile(parent, ".report-", ".tmp", java.nio.file.attribute.PosixFilePermissions
                    .asFileAttribute(java.nio.file.attribute.PosixFilePermissions.fromString("rw-r--r--")));
        }
        return Files.createTempFile(parent, ".report-", ".tmp");
    }
}
