package io.github.cemharput.streamlab.report;

/** The result set cannot be analysed at all: unsupported version, unreadable or malformed manifest (exit code 2). */
public final class InputException extends Exception {

    public InputException(String message) {
        super(message);
    }
}
