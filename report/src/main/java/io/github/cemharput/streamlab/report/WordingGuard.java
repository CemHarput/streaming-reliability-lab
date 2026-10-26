package io.github.cemharput.streamlab.report;

import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Scans generated text for phrases that would turn a description into a claim. The only exemption is the block
 * between {@link #BEGIN} and {@link #END}: the verbatim limits of the protocol. The block must appear exactly once
 * (when required) and be balanced.
 */
public final class WordingGuard {

    public static final String BEGIN = "<!-- quoted-limits:begin -->";
    public static final String END = "<!-- quoted-limits:end -->";

    /** The forbidden phrases, as listed in the plan; matching is word bounded and case insensitive. */
    public static final List<String> FORBIDDEN = List.of("faster", "slower", "quicker", "better", "worse", "best",
            "worst", "outperform*", "win", "wins", "winner", "beat", "beats", "superior", "inferior", "cheaper",
            "more efficient", "less efficient", "significant*", "p-value", "p value", "confidence interval",
            "statistically", "prove", "proves", "proven", "recommend*", "should use", "clearly", "dramatic*");

    private static final Pattern PATTERN = build();

    private WordingGuard() {
    }

    private static Pattern build() {
        StringBuilder sb = new StringBuilder("(?<![\\p{L}\\p{N}_])(?:");
        for (int i = 0; i < FORBIDDEN.size(); i++) {
            String phrase = FORBIDDEN.get(i);
            boolean prefix = phrase.endsWith("*");
            String core = prefix ? phrase.substring(0, phrase.length() - 1) : phrase;
            if (i > 0) {
                sb.append('|');
            }
            sb.append(Pattern.quote(core).replace(" ", "\\E\\s+\\Q"));
            if (prefix) {
                sb.append("[\\p{L}]*");
            }
        }
        sb.append(")(?![\\p{L}\\p{N}_])");
        return Pattern.compile(sb.toString(), Pattern.CASE_INSENSITIVE | Pattern.UNICODE_CASE);
    }

    /**
     * Returns the violations (phrase and line number, without the surrounding text); an empty list means clean.
     * With {@code requireBlock} the quoted block must exist exactly once; without it no marker may appear.
     */
    public static List<String> scan(String text, boolean requireBlock) {
        List<String> violations = new ArrayList<>();
        String masked = text;
        int begin = count(text, BEGIN);
        int end = count(text, END);
        if (requireBlock) {
            if (begin != 1 || end != 1) {
                violations.add("the quoted-limits block must appear exactly once and be balanced");
                return violations;
            }
            int b = text.indexOf(BEGIN);
            int e = text.indexOf(END);
            if (e < b) {
                violations.add("the quoted-limits block is not balanced");
                return violations;
            }
            masked = blank(text, b, e + END.length());
        } else if (begin != 0 || end != 0) {
            violations.add("a quoted-limits marker appears where no block is allowed");
            return violations;
        }
        Matcher m = PATTERN.matcher(masked);
        while (m.find()) {
            int line = 1;
            for (int i = 0; i < m.start(); i++) {
                if (masked.charAt(i) == '\n') {
                    line++;
                }
            }
            violations.add("forbidden phrase '" + m.group().toLowerCase(java.util.Locale.ROOT) + "' at line " + line);
        }
        return violations;
    }

    private static int count(String text, String marker) {
        int n = 0;
        int from = 0;
        while ((from = text.indexOf(marker, from)) >= 0) {
            n++;
            from += marker.length();
        }
        return n;
    }

    /** Replaces everything except line breaks with spaces so that line numbers stay right. */
    private static String blank(String text, int from, int to) {
        StringBuilder sb = new StringBuilder(text.length());
        for (int i = 0; i < text.length(); i++) {
            char c = text.charAt(i);
            sb.append(i >= from && i < to && c != '\n' ? ' ' : c);
        }
        return sb.toString();
    }
}
