package io.github.cemharput.streamlab.loadgen;

import io.github.cemharput.streamlab.loadgen.Options.IntRange;
import io.github.cemharput.streamlab.loadgen.client.StreamParams;

/**
 * Request parameters as a pure function of {@code (seed, index)}. Uses SplitMix64 with
 * {@code state = seed + index * 8 * 0x9E3779B97F4A7C15}; the draw order is fixed: chunks, intervalMs, firstDelayMs,
 * faultApplies, faultAtSeq, contentSeed. All six values are always drawn, so changing one option never shifts the
 * others. Range draws use {@code floorMod}; the modulo bias is negligible for the small ranges in use.
 *
 * <p>A drawn {@code faultAtSeq} that is not below the drawn {@code chunks} never fires: the request then streams
 * to completion although a fault was requested.
 */
public final class RequestVariation {

    private static final long GAMMA = 0x9E3779B97F4A7C15L;
    /** Draw slots reserved per index (6 are used); keeps the sequences of neighbouring indexes disjoint. */
    private static final long SLOTS_PER_INDEX = 8;

    private final IntRange chunks;
    private final IntRange intervalMs;
    private final IntRange firstDelayMs;
    private final String fault;
    private final double faultRate;
    private final IntRange faultAtSeq;
    private final long seed;

    public RequestVariation(IntRange chunks, IntRange intervalMs, IntRange firstDelayMs, String fault,
            double faultRate, IntRange faultAtSeq, long seed) {
        this.chunks = chunks;
        this.intervalMs = intervalMs;
        this.firstDelayMs = firstDelayMs;
        this.fault = fault;
        this.faultRate = faultRate;
        this.faultAtSeq = faultAtSeq;
        this.seed = seed;
    }

    public static RequestVariation from(Options o) {
        return new RequestVariation(o.chunks(), o.intervalMs(), o.firstDelayMs(), o.fault(), o.faultRate(),
                o.faultAtSeq(), o.seed());
    }

    public StreamParams forIndex(long index) {
        long[] state = {seed + index * SLOTS_PER_INDEX * GAMMA};
        int c = draw(state, chunks);
        int interval = draw(state, intervalMs);
        int firstDelay = draw(state, firstDelayMs);
        double u = (next(state) >>> 11) * 0x1.0p-53;
        int at = draw(state, faultAtSeq);
        long contentSeed = next(state);
        boolean applies = !"NONE".equals(fault) && u < faultRate;
        return new StreamParams(c, interval, firstDelay, applies ? fault : "NONE", applies ? at : 0, contentSeed);
    }

    private static int draw(long[] state, IntRange range) {
        long span = (long) range.max() - range.min() + 1;
        return (int) (range.min() + Math.floorMod(next(state), span));
    }

    private static long next(long[] state) {
        long z = state[0] += GAMMA;
        z = (z ^ (z >>> 30)) * 0xBF58476D1CE4E5B9L;
        z = (z ^ (z >>> 27)) * 0x94D049BB133111EBL;
        return z ^ (z >>> 31);
    }
}
