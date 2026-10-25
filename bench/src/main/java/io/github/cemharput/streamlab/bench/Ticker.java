package io.github.cemharput.streamlab.bench;

import java.time.Duration;

/** Time source and delay of the polling loops. Tests inject a fake that advances virtual time. */
public interface Ticker {

    long nanoTime();

    void sleep(Duration duration) throws InterruptedException;

    Ticker SYSTEM = new Ticker() {
        @Override
        public long nanoTime() {
            return System.nanoTime();
        }

        @Override
        public void sleep(Duration duration) throws InterruptedException {
            if (!duration.isNegative() && !duration.isZero()) {
                Thread.sleep(duration);
            }
        }
    };
}
