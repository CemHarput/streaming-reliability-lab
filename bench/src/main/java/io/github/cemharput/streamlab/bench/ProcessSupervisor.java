package io.github.cemharput.streamlab.bench;

import io.github.cemharput.streamlab.bench.ChildProcess.Killable;
import io.github.cemharput.streamlab.bench.ProcessSpecs.ProcessSpec;
import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;

/**
 * Owns every child the runner starts. {@link #stop} is the bounded stop sequence; {@link #killAll} is what the
 * shutdown hook uses and also closes the supervisor so that no new child can start afterwards.
 *
 * <p>Stop sequence: snapshot the descendants, {@code destroy()}, wait for the graceful time, then
 * {@code destroyForcibly()}, then force-kill the snapshot plus the current descendants, then wait for the forced
 * time. On Windows {@code destroy()} and {@code destroyForcibly()} are both {@code TerminateProcess}, so no
 * JVM shutdown hook of the child runs there.
 */
public final class ProcessSupervisor {

    /** How a child ended: {@code already_exited}, {@code destroy} (exited after destroy()), {@code forced} or {@code survived}. */
    public record StopResult(String method, Integer exitCode, long durationMs) {
    }

    private static final Duration POLL = Duration.ofMillis(50);
    private static final Duration LOG_DRAIN = Duration.ofSeconds(2);

    private final ProcessLauncher launcher;
    private final Ticker ticker;
    private final Object lock = new Object();
    private final List<ChildProcess> children = new ArrayList<>();
    private boolean closed;

    public ProcessSupervisor(ProcessLauncher launcher, Ticker ticker) {
        this.launcher = launcher;
        this.ticker = ticker;
    }

    /** @throws IllegalStateException after {@link #killAll} */
    public ChildProcess start(ProcessSpec spec) throws IOException {
        synchronized (lock) {
            if (closed) {
                throw new IllegalStateException("supervisor is closed: no new process may start");
            }
            ChildProcess child = launcher.launch(spec);
            children.add(child);
            return child;
        }
    }

    public boolean isClosed() {
        synchronized (lock) {
            return closed;
        }
    }

    /** Children that were started and not stopped yet. */
    public List<ChildProcess> registered() {
        synchronized (lock) {
            return List.copyOf(children);
        }
    }

    public StopResult stop(ChildProcess child, Duration graceful, Duration forced) throws InterruptedException {
        long start = ticker.nanoTime();
        try {
            if (!child.isAlive()) {
                killAndAwait(List.of(), child, forced);
                child.awaitLogDrained(LOG_DRAIN); // a child that died on its own may still have unread output
                return new StopResult("already_exited", child.exitValue(), millisSince(start));
            }
            List<Killable> snapshot = new ArrayList<>(child.descendants());
            child.destroy();
            boolean exited = child.waitFor(graceful);
            String method = "destroy";
            if (!exited) {
                child.destroyForcibly();
                method = "forced";
            }
            List<Killable> all = new ArrayList<>(snapshot);
            all.addAll(child.descendants());
            for (Killable k : all) {
                if (k.isAlive()) {
                    k.destroyForcibly();
                }
            }
            if (!exited) {
                exited = child.waitFor(forced);
            }
            awaitDead(all, forced);
            child.awaitLogDrained(LOG_DRAIN);
            if (!exited) {
                method = "survived";
            }
            return new StopResult(method, child.exitValue(), millisSince(start));
        } finally {
            synchronized (lock) {
                children.remove(child);
            }
        }
    }

    private void killAndAwait(List<Killable> snapshot, ChildProcess child, Duration bound) throws InterruptedException {
        List<Killable> all = new ArrayList<>(snapshot);
        all.addAll(child.descendants());
        for (Killable k : all) {
            if (k.isAlive()) {
                k.destroyForcibly();
            }
        }
        awaitDead(all, bound);
    }

    /** Closes the supervisor and force-kills every registered child and its descendants, within {@code bound}. */
    public void killAll(Duration bound) {
        killAll(bound, () -> { });
    }

    /**
     * Like {@link #killAll(Duration)}; {@code afterKill} runs after the force-kill was issued and before the
     * (bounded) wait, so state such as the manifest can be updated without waiting for the processes first.
     */
    public void killAll(Duration bound, Runnable afterKill) {
        List<ChildProcess> toKill;
        synchronized (lock) {
            closed = true;
            toKill = new ArrayList<>(children);
            children.clear();
        }
        long deadline = ticker.nanoTime() + bound.toNanos();
        List<Killable> all = new ArrayList<>();
        for (ChildProcess child : toKill) { // one misbehaving child must not stop the others from being killed
            try {
                all.addAll(child.descendants());
            } catch (RuntimeException ex) {
                // snapshot failed: the child itself is still killed below
            }
            try {
                child.destroyForcibly();
            } catch (RuntimeException ex) {
                // keep going
            }
        }
        for (Killable k : all) {
            try {
                if (k.isAlive()) {
                    k.destroyForcibly();
                }
            } catch (RuntimeException ex) {
                // keep going
            }
        }
        try {
            afterKill.run();
        } catch (RuntimeException ex) {
            // the callback must not prevent the wait below
        }
        try {
            for (ChildProcess child : toKill) {
                long remaining = Math.max(1, deadline - ticker.nanoTime());
                try {
                    child.waitFor(Duration.ofNanos(remaining));
                } catch (RuntimeException ex) {
                    // keep going
                }
            }
            awaitDead(all, Duration.ofNanos(Math.max(1, deadline - ticker.nanoTime())));
        } catch (InterruptedException ex) {
            Thread.currentThread().interrupt();
        }
    }

    private void awaitDead(List<Killable> victims, Duration bound) throws InterruptedException {
        long deadline = ticker.nanoTime() + bound.toNanos();
        while (victims.stream().anyMatch(Killable::isAlive) && ticker.nanoTime() < deadline) {
            ticker.sleep(POLL);
        }
    }

    private long millisSince(long start) {
        return (ticker.nanoTime() - start) / 1_000_000;
    }
}
