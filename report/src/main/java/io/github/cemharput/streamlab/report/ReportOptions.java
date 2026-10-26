package io.github.cemharput.streamlab.report;

import java.nio.file.Path;

/** Validated command line. {@code results} and {@code out} are absolute, normalised paths (or null). */
public record ReportOptions(boolean help, Path results, Path out, boolean check, boolean overwrite,
        boolean placeholder, boolean allowSmoke) {
}
