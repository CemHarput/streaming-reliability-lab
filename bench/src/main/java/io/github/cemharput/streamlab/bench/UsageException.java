package io.github.cemharput.streamlab.bench;

/** Invalid arguments or a failed precondition; the runner maps it to exit code 2. */
public final class UsageException extends Exception {

    public UsageException(String message) {
        super(message);
    }
}
