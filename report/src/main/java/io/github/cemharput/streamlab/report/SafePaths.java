package io.github.cemharput.streamlab.report;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.Path;
import java.util.Optional;
import java.util.regex.Pattern;

/**
 * Resolves manifest paths below the result set root. A directory name from the manifest is accepted only in the
 * exact form {@code runs/<id>} (split at both {@code /} and the backslash); file names never come from the manifest.
 * A path whose real location leaves the root (a symbolic link) is refused.
 */
public final class SafePaths {

    public static final Pattern RUN_ID = Pattern.compile("^\\d{3}-c\\d+-(shared|per-request)-r\\d+$");

    private SafePaths() {
    }

    /** True when {@code dir} is {@code runs/<id>} and {@code id} has the run id shape. */
    public static boolean validRunDir(String dir, String id) {
        if (dir == null || id == null || !RUN_ID.matcher(id).matches()) {
            return false;
        }
        String[] parts = dir.split("[/\\\\]", -1);
        return parts.length == 2 && parts[0].equals("runs") && parts[1].equals(id);
    }

    /**
     * The real path of {@code runs/<id>} below {@code realRoot}, or empty when it is missing, not a directory or
     * outside the root.
     */
    public static Optional<Path> runDir(Path realRoot, String id) {
        return inside(realRoot, realRoot.resolve("runs").resolve(id), true);
    }

    /** The real path of a regular file named {@code name} directly in {@code realDir}, or empty. */
    public static Optional<Path> file(Path realRoot, Path realDir, String name) {
        if (name.contains("/") || name.contains("\\") || name.equals("..") || name.equals(".")) {
            return Optional.empty();
        }
        return inside(realRoot, realDir.resolve(name), false);
    }

    private static Optional<Path> inside(Path realRoot, Path candidate, boolean directory) {
        try {
            if (!Files.exists(candidate, LinkOption.NOFOLLOW_LINKS)) {
                return Optional.empty();
            }
            Path real = candidate.toRealPath();
            if (!real.startsWith(realRoot)) {
                return Optional.empty();
            }
            boolean ok = directory ? Files.isDirectory(real) : Files.isRegularFile(real);
            return ok ? Optional.of(real) : Optional.empty();
        } catch (IOException | RuntimeException ex) {
            return Optional.empty();
        }
    }
}
