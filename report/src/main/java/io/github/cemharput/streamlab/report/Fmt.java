package io.github.cemharput.streamlab.report;

import java.math.BigDecimal;
import java.math.RoundingMode;

/**
 * Number formatting. Everything is exact integer or {@link BigDecimal} arithmetic with HALF_EVEN rounding and
 * {@code toPlainString()}; neither the default locale nor {@code String.format} is involved.
 */
public final class Fmt {

    public static final String NA = "n/a";
    public static final String NOT_RECORDED = "not recorded";

    private static final BigDecimal THOUSAND = BigDecimal.valueOf(1000);
    private static final BigDecimal HUNDRED = BigDecimal.valueOf(100);
    private static final BigDecimal GIB = BigDecimal.valueOf(1024L * 1024 * 1024);

    private Fmt() {
    }

    /** Nanoseconds as milliseconds with three decimals (nanoseconds are first rounded to microseconds). */
    public static String ms(long nanos) {
        return ms(BigDecimal.valueOf(nanos));
    }

    /** Nanoseconds (possibly fractional, e.g. a median of two values) as milliseconds with three decimals. */
    public static String ms(BigDecimal nanos) {
        BigDecimal micros = nanos.divide(THOUSAND, 0, RoundingMode.HALF_EVEN);
        return micros.divide(THOUSAND, 3, RoundingMode.HALF_EVEN).toPlainString();
    }

    public static String msOrNa(Long nanos) {
        return nanos == null ? NA : ms(nanos);
    }

    public static String dec(BigDecimal value, int scale) {
        return value.setScale(scale, RoundingMode.HALF_EVEN).toPlainString();
    }

    public static String decOrNa(BigDecimal value, int scale) {
        return value == null ? NA : dec(value, scale);
    }

    /** {@code part / whole} in percent with two decimals; n/a when {@code whole} is zero. */
    public static String percent(long part, long whole) {
        BigDecimal v = ratioPercent(part, whole);
        return v == null ? NA : dec(v, 2);
    }

    /** {@code part / whole * 100} at thirty decimals (the exact comparison precision; display rounds), or null for a zero {@code whole}. */
    public static BigDecimal ratioPercent(long part, long whole) {
        if (whole <= 0) {
            return null;
        }
        return BigDecimal.valueOf(part).multiply(HUNDRED).divide(BigDecimal.valueOf(whole), 30, RoundingMode.HALF_EVEN);
    }

    /** Bytes as GiB with one decimal. */
    public static String gib(long bytes) {
        return BigDecimal.valueOf(bytes).divide(GIB, 1, RoundingMode.HALF_EVEN).toPlainString();
    }

    /** A counter value without a trailing fraction. */
    public static String num(BigDecimal value) {
        if (value == null) {
            return NA;
        }
        BigDecimal v = value.signum() == 0 ? BigDecimal.ZERO : value.stripTrailingZeros();
        return v.toPlainString();
    }

    public static String numOrNa(Long value) {
        return value == null ? NA : Long.toString(value);
    }

    /** A double sample (CPU load) with three decimals. */
    public static String cpu(double value) {
        return dec(BigDecimal.valueOf(value), 3);
    }

    public static String orNotRecorded(Object value) {
        return value == null ? NOT_RECORDED : String.valueOf(value);
    }

    /** One line of plain text: control characters become a question mark (for standard output). */
    public static String line(String text) {
        StringBuilder sb = new StringBuilder(text.length());
        for (int i = 0; i < text.length(); i++) {
            char c = text.charAt(i);
            sb.append(c < 0x20 || c == 0x7f ? '?' : c);
        }
        return sb.toString();
    }

    private static final char OPEN = (char) 0xE000;
    private static final char CLOSE = (char) 0xE001;

    /**
     * Escapes a value for a markdown table cell; apply it once, where the cell is rendered. Every backtick in the
     * input becomes an apostrophe, so untrusted text can never open a code span of its own; the only code spans in a
     * cell are those made by {@link #code} (marked with private-use characters, turned into backticks here). Inside
     * such a span only the pipe is escaped (a backslash becomes a slash); outside, the backslash (first), the pipe and
     * the angle brackets are. Line breaks become spaces.
     */
    public static String cell(String text) {
        String t = text.replace((char) 13, (char) 32).replace((char) 10, (char) 32).replace('`', '\'');
        StringBuilder out = new StringBuilder(t.length() + 8);
        int i = 0;
        while (i < t.length()) {
            int open = t.indexOf(OPEN, i);
            int close = open < 0 ? -1 : t.indexOf(CLOSE, open + 1);
            if (open < 0 || close < 0) {
                out.append(plain(stray(t.substring(i))));
                break;
            }
            out.append(plain(stray(t.substring(i, open))));
            out.append('`').append(stray(t.substring(open + 1, close)).replace('\\', '/')).append('`');
            i = close + 1;
        }
        return out.toString().replace("|", "\\|");
    }

    private static String stray(String s) {
        return s.replace(OPEN, '?').replace(CLOSE, '?');
    }

    private static String plain(String s) {
        return s.replace("\\", "\\\\").replace("<", "&lt;").replace(">", "&gt;");
    }

    /** A name as a code span inside a table cell (finished by {@link #cell}); control characters become a question mark. */
    public static String code(String text) {
        return OPEN + stray(line(text)) + CLOSE;
    }

    /** A name as a code span in a line of text outside a table (a backtick inside it becomes a quote). */
    public static String codeInline(String text) {
        return "`" + line(text).replace("`", "'") + "`";
    }
}
