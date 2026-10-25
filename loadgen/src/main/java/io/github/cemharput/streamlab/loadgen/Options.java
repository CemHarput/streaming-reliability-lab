package io.github.cemharput.streamlab.loadgen;

import java.net.URI;
import java.nio.file.Path;
import java.time.Duration;

/**
 * Validated command line. Exactly one of {@code duration} and {@code requests} is set (unless {@code help}).
 */
public record Options(
        boolean help,
        URI target,
        int concurrency,
        Duration duration,
        Long requests,
        Duration rampUp,
        IntRange chunks,
        IntRange intervalMs,
        IntRange firstDelayMs,
        String fault,
        IntRange faultAtSeq,
        double faultRate,
        long seed,
        Path output,
        Path summaryOutput,
        boolean overwrite,
        Duration connectTimeout,
        Duration idleTimeout,
        Duration drainTimeout,
        Duration rejectPause) {

    /** Closed integer interval {@code [min, max]}; written as {@code N} or {@code MIN-MAX}. */
    public record IntRange(int min, int max) {

        public IntRange {
            if (min > max) {
                throw new IllegalArgumentException("min > max");
            }
        }

        public static IntRange of(int value) {
            return new IntRange(value, value);
        }

        @Override
        public String toString() {
            return min == max ? Integer.toString(min) : min + "-" + max;
        }
    }
}
