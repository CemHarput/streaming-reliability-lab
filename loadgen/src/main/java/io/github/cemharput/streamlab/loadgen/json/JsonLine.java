package io.github.cemharput.streamlab.loadgen.json;

/**
 * Builds one flat JSON object on a single line, with fields in insertion order. Only what the JSONL
 * records need: strings, longs, booleans, finite doubles and null. Escaping follows RFC 8259.
 */
public final class JsonLine {

    private static final char[] HEX = "0123456789abcdef".toCharArray();

    private final StringBuilder sb = new StringBuilder(256).append('{');
    private boolean first = true;

    public JsonLine field(String name, String value) {
        key(name);
        if (value == null) {
            sb.append("null");
        } else {
            quote(sb, value);
        }
        return this;
    }

    public JsonLine field(String name, long value) {
        key(name);
        sb.append(value);
        return this;
    }

    public JsonLine field(String name, Long value) {
        key(name);
        sb.append(value == null ? "null" : value.toString());
        return this;
    }

    public JsonLine field(String name, boolean value) {
        key(name);
        sb.append(value);
        return this;
    }

    public JsonLine field(String name, double value) {
        if (Double.isNaN(value) || Double.isInfinite(value)) {
            throw new IllegalArgumentException("JSON cannot represent " + value + " (field " + name + ")");
        }
        key(name);
        sb.append(value);
        return this;
    }

    public JsonLine nullField(String name) {
        key(name);
        sb.append("null");
        return this;
    }

    /** The finished object, without a trailing newline. */
    @Override
    public String toString() {
        return sb + "}";
    }

    private void key(String name) {
        if (!first) {
            sb.append(',');
        }
        first = false;
        quote(sb, name);
        sb.append(':');
    }

    private static void quote(StringBuilder out, String s) {
        out.append('"');
        for (int i = 0; i < s.length(); i++) {
            char c = s.charAt(i);
            switch (c) {
                case '"' -> out.append("\\\"");
                case '\\' -> out.append("\\\\");
                case '\b' -> out.append("\\b");
                case '\f' -> out.append("\\f");
                case '\n' -> out.append("\\n");
                case '\r' -> out.append("\\r");
                case '\t' -> out.append("\\t");
                default -> {
                    if (c < 0x20) {
                        out.append("\\u00").append(HEX[c >> 4]).append(HEX[c & 0xF]);
                    } else {
                        out.append(c);
                    }
                }
            }
        }
        out.append('"');
    }
}
