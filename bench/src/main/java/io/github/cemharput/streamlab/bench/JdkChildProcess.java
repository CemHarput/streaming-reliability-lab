package io.github.cemharput.streamlab.bench;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.TimeUnit;

/** {@link ChildProcess} over {@link Process}; its output goes through a {@link BoundedLogPump}. */
public final class JdkChildProcess implements ChildProcess {

    private final Process process;
    private final BoundedLogPump pump;

    JdkChildProcess(Process process, BoundedLogPump pump) {
        this.process = process;
        this.pump = pump;
    }

    @Override
    public long pid() {
        return process.pid();
    }

    @Override
    public boolean isAlive() {
        return process.isAlive();
    }

    @Override
    public void destroy() {
        process.destroy();
    }

    @Override
    public void destroyForcibly() {
        process.destroyForcibly();
    }

    @Override
    public boolean waitFor(Duration timeout) throws InterruptedException {
        boolean exited = process.waitFor(timeout.toMillis(), TimeUnit.MILLISECONDS);
        if (exited) {
            pump.await(Duration.ofSeconds(2));
        }
        return exited;
    }

    @Override
    public void awaitLogDrained(Duration bound) throws InterruptedException {
        pump.await(bound);
    }

    @Override
    public Integer exitValue() {
        return process.isAlive() ? null : process.exitValue();
    }

    @Override
    public List<Killable> descendants() {
        return process.descendants().<Killable>map(JdkChildProcess::killable).toList();
    }

    private static Killable killable(ProcessHandle handle) {
        return new Killable() {
            @Override
            public long pid() {
                return handle.pid();
            }

            @Override
            public boolean isAlive() {
                return handle.isAlive();
            }

            @Override
            public void destroyForcibly() {
                handle.destroyForcibly();
            }
        };
    }

    @Override
    public long logBytesWritten() {
        return pump.bytesWritten();
    }

    @Override
    public long logBytesDropped() {
        return pump.bytesDropped();
    }

    @Override
    public long logBytesLostAfterWriteError() {
        return pump.bytesLostAfterWriteError();
    }

    @Override
    public String logWriteError() {
        return pump.writeError();
    }
}
