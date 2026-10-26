package io.github.cemharput.streamlab.report;

/** Invalid command line (exit code 2). */
public final class UsageException extends Exception {

    public UsageException(String message) {
        super(message);
    }
}
