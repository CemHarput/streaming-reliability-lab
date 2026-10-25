package io.github.cemharput.streamlab.bench.testsupport;

import io.github.cemharput.streamlab.bench.ChildProcess;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/** Scriptable child: whether {@code destroy} / {@code destroyForcibly} end it, and which descendants it has. */
public final class FakeChildProcess implements ChildProcess {

    /** A descendant that dies on {@code destroyForcibly}. */
    public static final class FakeKillable implements Killable {
        private final long pid;
        private volatile boolean alive = true;

        public FakeKillable(long pid) {
            this.pid = pid;
        }

        @Override
        public long pid() {
            return pid;
        }

        @Override
        public boolean isAlive() {
            return alive;
        }

        @Override
        public void destroyForcibly() {
            alive = false;
        }
    }

    private final long pid;
    private final List<String> calls = Collections.synchronizedList(new ArrayList<>());
    private final List<FakeKillable> descendants = new ArrayList<>();
    private volatile boolean alive;
    private volatile Integer exit;
    private volatile boolean exitsOnDestroy = true;
    private volatile boolean exitsOnForce = true;
    private volatile long logDropped;
    private volatile String logWriteError;
    private volatile long logLost;
    private volatile boolean failing;
    private volatile boolean logVisibleAfterDrain;
    private volatile boolean drained = true;
    private volatile Runnable afterWait = () -> { };
    private volatile boolean blockingWait;
    private volatile Runnable onWaitFor = () -> { };
    private final java.util.concurrent.CountDownLatch ended = new java.util.concurrent.CountDownLatch(1);
    private final List<Duration> waits = Collections.synchronizedList(new ArrayList<>());

    public FakeChildProcess(long pid, boolean alive) {
        this.pid = pid;
        this.alive = alive;
        this.exit = alive ? null : 0;
    }

    public FakeChildProcess exitsOnDestroy(boolean value) {
        this.exitsOnDestroy = value;
        return this;
    }

    public FakeChildProcess exitsOnForce(boolean value) {
        this.exitsOnForce = value;
        return this;
    }

    public FakeChildProcess exited(int code) {
        this.alive = false;
        this.exit = code;
        return this;
    }

    public FakeChildProcess withDescendant(FakeKillable k) {
        descendants.add(k);
        return this;
    }

    /** {@code waitFor} blocks (really, at most 20 s) until the child is ended by destroy or destroyForcibly. */
    public FakeChildProcess blockingWait() {
        this.blockingWait = true;
        return this;
    }

    /** Runs after {@code waitFor} returned (still on the waiting thread). */
    public FakeChildProcess afterWait(Runnable action) {
        this.afterWait = action;
        return this;
    }

    public FakeChildProcess logLostAfterWriteError(long bytes) {
        this.logLost = bytes;
        return this;
    }

    /** The log counters read 0 until {@code awaitLogDrained} was called, like a pump that is still copying. */
    public FakeChildProcess logVisibleAfterDrain() {
        this.logVisibleAfterDrain = true;
        this.drained = false;
        return this;
    }

    @Override
    public void awaitLogDrained(Duration bound) {
        calls.add("awaitLogDrained");
        drained = true;
    }

    /** {@code descendants()} and {@code destroyForcibly()} throw, like a process handle that went wrong. */
    public FakeChildProcess failing() {
        this.failing = true;
        return this;
    }

    public FakeChildProcess onWaitFor(Runnable action) {
        this.onWaitFor = action;
        return this;
    }

    public FakeChildProcess logWriteError(String error) {
        this.logWriteError = error;
        return this;
    }

    /** The timeouts passed to {@code waitFor}, in call order. */
    public List<Duration> waits() {
        synchronized (waits) {
            return List.copyOf(waits);
        }
    }

    public FakeChildProcess logDropped(long bytes) {
        this.logDropped = bytes;
        return this;
    }

    public List<String> calls() {
        synchronized (calls) {
            return List.copyOf(calls);
        }
    }

    @Override
    public long pid() {
        return pid;
    }

    @Override
    public boolean isAlive() {
        return alive;
    }

    @Override
    public void destroy() {
        calls.add("destroy");
        if (exitsOnDestroy) {
            alive = false;
            exit = 143;
            ended.countDown();
        }
    }

    @Override
    public void destroyForcibly() {
        calls.add("destroyForcibly");
        if (failing) {
            throw new IllegalStateException("destroyForcibly failed");
        }
        if (exitsOnForce) {
            alive = false;
            exit = 137;
            ended.countDown();
        }
    }

    @Override
    public boolean waitFor(Duration timeout) throws InterruptedException {
        calls.add("waitFor");
        waits.add(timeout);
        onWaitFor.run();
        if (blockingWait && alive) {
            ended.await(Math.min(timeout.toMillis(), 20_000), java.util.concurrent.TimeUnit.MILLISECONDS);
        }
        afterWait.run();
        return !alive;
    }

    @Override
    public Integer exitValue() {
        return alive ? null : exit;
    }

    @Override
    public List<Killable> descendants() {
        if (failing) {
            throw new IllegalStateException("descendants failed");
        }
        return new ArrayList<>(descendants);
    }

    @Override
    public long logBytesWritten() {
        return logVisibleAfterDrain && !drained ? 0 : 10;
    }

    @Override
    public long logBytesDropped() {
        return logDropped;
    }

    @Override
    public long logBytesLostAfterWriteError() {
        return logLost;
    }

    @Override
    public String logWriteError() {
        return logWriteError;
    }
}
