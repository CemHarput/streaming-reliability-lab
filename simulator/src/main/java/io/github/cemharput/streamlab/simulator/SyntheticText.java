package io.github.cemharput.streamlab.simulator;

import java.util.SplittableRandom;

/**
 * Deterministic filler text. A chunk depends only on (seed, requestId, seq),
 * so a stream resumed from seq N is byte-identical to the original suffix and
 * tests can detect gaps, duplicates and reordering by content alone.
 */
final class SyntheticText {

    private static final String[] WORDS = {
            "alpha", "bravo", "charlie", "delta", "echo", "foxtrot", "golf", "hotel",
            "india", "juliet", "kilo", "lima", "mike", "november", "oscar", "papa",
            "quebec", "romeo", "sierra", "tango", "uniform", "victor", "whiskey", "xray",
            "yankee", "zulu"
    };

    private SyntheticText() {
    }

    static String chunk(long seed, String requestId, int seq, int sizeBytes) {
        SplittableRandom random = new SplittableRandom(mix(seed, requestId, seq));
        String first = WORDS[random.nextInt(WORDS.length)];
        if (sizeBytes <= first.length()) {
            return first;
        }
        StringBuilder text = new StringBuilder(sizeBytes + 8).append(first);
        while (text.length() < sizeBytes) {
            text.append(' ').append(WORDS[random.nextInt(WORDS.length)]);
        }
        return text.toString();
    }

    private static long mix(long seed, String requestId, int seq) {
        long h = seed;
        h = h * 0x9E3779B97F4A7C15L + requestId.hashCode();
        h = h * 0x9E3779B97F4A7C15L + Integer.toUnsignedLong(seq);
        return h;
    }
}
