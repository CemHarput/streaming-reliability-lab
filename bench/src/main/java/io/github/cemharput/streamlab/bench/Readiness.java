package io.github.cemharput.streamlab.bench;

import java.time.Duration;
import java.util.function.BooleanSupplier;

/** The one bounded wait loop used for every "wait until ready" step. */
public final class Readiness {

    public enum Result { READY, TIMEOUT, PROCESS_DIED }

    /** {@code waitedMillis} is measured with the ticker. */
    public record Outcome(Result result, long waitedMillis) {
    }

    private Readiness() {
    }

    /**
     * Polls {@code probe} every {@code interval}. The process is checked before every probe, so a dead process
     * ends the wait at once; the wait never exceeds {@code timeout} plus one probe.
     */
    public static Outcome await(BooleanSupplier probe, Duration timeout, Duration interval, BooleanSupplier alive,
            Ticker ticker) throws InterruptedException {
        long start = ticker.nanoTime();
        long deadline = start + timeout.toNanos();
        while (true) {
            if (!alive.getAsBoolean()) {
                return new Outcome(Result.PROCESS_DIED, millisSince(ticker, start));
            }
            boolean ready;
            try {
                ready = probe.getAsBoolean();
            } catch (RuntimeException ex) {
                ready = false;
            }
            if (ready) {
                return new Outcome(Result.READY, millisSince(ticker, start));
            }
            if (ticker.nanoTime() >= deadline) {
                return new Outcome(Result.TIMEOUT, millisSince(ticker, start));
            }
            ticker.sleep(interval);
        }
    }

    private static long millisSince(Ticker ticker, long start) {
        return (ticker.nanoTime() - start) / 1_000_000;
    }
}
