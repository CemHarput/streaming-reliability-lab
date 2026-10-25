package io.github.cemharput.streamlab.bench;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;

/**
 * Copies a child's combined output to a file, at most {@code maxBytes}. The rest is still read and discarded so
 * the child never blocks on a full pipe; the number of discarded bytes is kept. A note is appended at the end of
 * a truncated log. Runs on a platform daemon thread.
 */
public final class BoundedLogPump implements AutoCloseable {

    private final InputStream in;
    private final Path file;
    private final long maxBytes;
    private final Opener opener;
    private final Thread thread;
    private volatile long written;
    private volatile long dropped;
    private volatile long lostAfterError;
    private volatile IOException failure;

    /** Opens the log file; replaceable in tests to simulate a failing disk. */
    interface Opener {
        OutputStream open(Path file) throws IOException;
    }

    public BoundedLogPump(InputStream in, Path file, long maxBytes, String threadName) {
        this(in, file, maxBytes, threadName, Files::newOutputStream);
    }

    BoundedLogPump(InputStream in, Path file, long maxBytes, String threadName, Opener opener) {
        this.opener = opener;
        this.in = in;
        this.file = file;
        this.maxBytes = maxBytes;
        this.thread = new Thread(this::pump, threadName);
        this.thread.setDaemon(true);
    }

    public BoundedLogPump start() {
        thread.start();
        return this;
    }

    private void pump() {
        byte[] buffer = new byte[8192];
        OutputStream out = null;
        try {
            out = opener.open(file);
        } catch (IOException ex) {
            failure = first(ex); // keep reading below so the child never blocks on a full pipe
        }
        long w = 0;
        long d = 0;
        long lost = 0;
        try {
            int n;
            while ((n = in.read(buffer)) >= 0) {
                if (out == null) { // after a write or open error: keep draining, count separately
                    lost += n;
                    lostAfterError = lost;
                    continue;
                }
                int toWrite = (int) Math.min(n, Math.max(0, maxBytes - w));
                if (toWrite > 0) {
                    try {
                        out.write(buffer, 0, toWrite);
                        w += toWrite;
                        written = w;
                    } catch (IOException ex) { // e.g. disk full: record it, stop writing, keep draining
                        failure = first(ex);
                        closeQuietly(out);
                        out = null;
                        lost += n;
                        lostAfterError = lost;
                        continue;
                    }
                }
                if (toWrite < n) { // the size limit, not an error
                    d += n - toWrite;
                    dropped = d;
                }
            }
        } catch (IOException ex) {
            // the stream was closed under us: keep what was written
        }
        if (out != null) {
            try {
                if (d > 0) {
                    out.write(("%n[bench: log truncated, %d bytes dropped]%n".formatted(d))
                            .getBytes(StandardCharsets.UTF_8));
                }
            } catch (IOException ex) {
                failure = first(ex);
            } finally {
                closeQuietly(out);
            }
        }
    }

    private IOException first(IOException ex) {
        return failure == null ? ex : failure;
    }

    private static void closeQuietly(OutputStream out) {
        try {
            out.close();
        } catch (IOException ex) {
            // nothing more to do
        }
    }

    /** Waits until the stream ended (the child and everything holding its pipe is gone) or the time is up. */
    public boolean await(Duration timeout) throws InterruptedException {
        thread.join(timeout.toMillis());
        return !thread.isAlive();
    }

    public long bytesWritten() {
        return written;
    }

    /** Bytes discarded because the size limit was reached (this is what "truncated" means). */
    public long bytesDropped() {
        return dropped;
    }

    /** Bytes discarded after a write or open error; not counted in {@link #bytesDropped()}. */
    public long bytesLostAfterWriteError() {
        return lostAfterError;
    }

    public IOException failure() {
        return failure;
    }

    /** Text of the first write error (the log is then incomplete), or null. */
    public String writeError() {
        IOException f = failure;
        return f == null ? null : f.getClass().getSimpleName() + ": " + f.getMessage();
    }

    @Override
    public void close() {
        try {
            in.close();
        } catch (IOException ex) {
            // ignore
        }
    }
}
