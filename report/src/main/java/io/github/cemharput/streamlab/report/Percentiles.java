package io.github.cemharput.streamlab.report;

import java.util.Arrays;

/** Exact nearest-rank percentiles: for sorted {@code x[1..n]}, {@code p = x[ceil(p/100 * n)]}, max is {@code x[n]}. */
public final class Percentiles {

    /** A distribution summary; the percentile fields are null when {@code count} is zero. */
    public record Dist(long count, long missing, Long p50, Long p90, Long p99, Long max) {

        public static Dist empty(long missing) {
            return new Dist(0, missing, null, null, null, null);
        }
    }

    private Percentiles() {
    }

    /** Rank (1-based) of percentile {@code p} (1..100) among {@code n} values. */
    public static int rank(int p, int n) {
        long r = ((long) p * n + 99) / 100;
        return (int) Math.max(1, Math.min(n, r));
    }

    /** Nearest-rank percentile of the first {@code n} values, which must already be sorted; n must be at least 1. */
    public static long nearestRank(long[] sorted, int n, int p) {
        return sorted[rank(p, n) - 1];
    }

    /** Sorts the first {@code n} values in place and summarises them. */
    public static Dist of(long[] values, int n, long missing) {
        if (n <= 0) {
            return Dist.empty(missing);
        }
        Arrays.sort(values, 0, n);
        return new Dist(n, missing, nearestRank(values, n, 50), nearestRank(values, n, 90),
                nearestRank(values, n, 99), values[n - 1]);
    }
}
