package io.github.cemharput.streamlab.bench;

import java.time.Duration;
import java.util.List;

/** A started child process. Implemented over the JDK and by fakes in tests. */
public interface ChildProcess {

    /** A process (the child itself or one of its descendants) that can be killed. */
    interface Killable {
        long pid();

        boolean isAlive();

        void destroyForcibly();
    }

    long pid();

    boolean isAlive();

    /** Polite stop: SIGTERM on POSIX; on Windows the JDK terminates the process, same as {@link #destroyForcibly}. */
    void destroy();

    void destroyForcibly();

    /** Waits at most {@code timeout}; returns true when the process has exited. */
    boolean waitFor(Duration timeout) throws InterruptedException;

    /** Exit code, or null while the process is alive. */
    Integer exitValue();

    /**
     * Waits (at most {@code bound}) until everything the child wrote has been copied to its log, so the log counters
     * are final. A no-op for children without a log pump.
     */
    default void awaitLogDrained(Duration bound) throws InterruptedException {
    }

    /** Snapshot of all descendants that are alive now. */
    List<Killable> descendants();

    /** Bytes of the combined stdout/stderr that were written to the log file. */
    long logBytesWritten();

    /** Bytes that were read but not written because the log size limit was reached (a write error is not counted). */
    long logBytesDropped();

    /** Bytes discarded after a log write or open error (0 when none). */
    default long logBytesLostAfterWriteError() {
        return 0;
    }

    /** Text of the first error while writing the log file (the log is then incomplete), or null. */
    default String logWriteError() {
        return null;
    }
}
