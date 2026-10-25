package io.github.cemharput.streamlab.bench.testsupport;

import io.github.cemharput.streamlab.bench.Ticker;
import java.time.Duration;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/** Virtual time: {@code sleep} advances the clock instantly, so polling loops finish at once. */
public final class FakeTicker implements Ticker {

    private final AtomicLong now = new AtomicLong(1_000_000_000L);
    private final AtomicInteger sleeps = new AtomicInteger();

    @Override
    public long nanoTime() {
        return now.get();
    }

    @Override
    public void sleep(Duration duration) {
        sleeps.incrementAndGet();
        now.addAndGet(duration.toNanos());
    }

    public int sleepCount() {
        return sleeps.get();
    }
}
