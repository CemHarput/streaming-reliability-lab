package io.github.cemharput.streamlab.loadgen;

/** Invalid command line. The message is a single line meant for the user; the process exits with code 2. */
public final class UsageException extends Exception {

    public UsageException(String message) {
        super(message);
    }
}
