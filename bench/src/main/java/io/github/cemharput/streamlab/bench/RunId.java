package io.github.cemharput.streamlab.bench;

import java.io.IOException;
import java.nio.file.FileAlreadyExistsException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;

/** Run-set id and the atomic creation of its directory. Existing directories and files are never touched. */
public final class RunId {

    private static final DateTimeFormatter STAMP = DateTimeFormatter.ofPattern("yyyyMMdd'T'HHmmss'Z'")
            .withZone(ZoneOffset.UTC);
    private static final int MAX_SUFFIX = 99;

    private RunId() {
    }

    /** {@code yyyyMMdd'T'HHmmss'Z'-<shortCommit|nogit>[-dirty][-smoke]}. */
    public static String format(Instant now, GitInfo git, boolean smoke) {
        StringBuilder sb = new StringBuilder(STAMP.format(now)).append('-');
        sb.append(git.available() ? git.shortCommit() : "nogit");
        if (git.available() && git.dirty()) {
            sb.append("-dirty");
        }
        if (smoke) {
            sb.append("-smoke");
        }
        return sb.toString();
    }

    /**
     * Creates {@code outputDir/<id>} with {@link Files#createDirectory}, which fails when it exists. Then
     * {@code <id>-2} to {@code <id>-99} are tried.
     *
     * @throws IOException when no free name was found or the directory cannot be created
     */
    public static Path createUniqueDirectory(Path outputDir, Instant now, GitInfo git, boolean smoke)
            throws IOException {
        Files.createDirectories(outputDir);
        String base = format(now, git, smoke);
        for (int attempt = 1; attempt <= MAX_SUFFIX; attempt++) {
            Path candidate = outputDir.resolve(attempt == 1 ? base : base + "-" + attempt);
            try {
                return Files.createDirectory(candidate);
            } catch (FileAlreadyExistsException ex) {
                // taken: try the next suffix
            }
        }
        throw new IOException("no free result directory name for " + base + " in " + outputDir);
    }
}
