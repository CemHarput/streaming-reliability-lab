package io.github.cemharput.streamlab.loadgen.json;

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Parses one flat JSON object (no nested objects or arrays) into a map. Values are String, Long, Double,
 * Boolean or null. Used for the gateway's SSE {@code data} payloads and by tests reading the JSONL output.
 */
public final class FlatJson {

    private final String s;
    private int pos;

    private FlatJson(String s) {
        this.s = s;
    }

    /** @throws IllegalArgumentException if the input is not a flat JSON object */
    public static Map<String, Object> parse(String json) {
        if (json == null) {
            throw new IllegalArgumentException("null input");
        }
        FlatJson p = new FlatJson(json);
        Map<String, Object> result = p.object();
        p.skipWs();
        if (p.pos != json.length()) {
            throw p.error("trailing content");
        }
        return result;
    }

    private Map<String, Object> object() {
        skipWs();
        expect('{');
        Map<String, Object> map = new LinkedHashMap<>();
        skipWs();
        if (peek() == '}') {
            pos++;
            return map;
        }
        while (true) {
            skipWs();
            String key = string();
            skipWs();
            expect(':');
            skipWs();
            if (map.containsKey(key)) {
                throw error("duplicate key " + key);
            }
            map.put(key, value());
            skipWs();
            char c = next();
            if (c == '}') {
                return map;
            }
            if (c != ',') {
                pos--;
                throw error("expected ',' or '}'");
            }
        }
    }

    private Object value() {
        char c = peek();
        if (c == '"') {
            return string();
        }
        if (c == '{' || c == '[') {
            throw error("nested values are not supported");
        }
        if (s.startsWith("true", pos)) {
            pos += 4;
            return Boolean.TRUE;
        }
        if (s.startsWith("false", pos)) {
            pos += 5;
            return Boolean.FALSE;
        }
        if (s.startsWith("null", pos)) {
            pos += 4;
            return null;
        }
        return number();
    }

    private Object number() {
        int start = pos;
        boolean fractional = false;
        while (pos < s.length()) {
            char c = s.charAt(pos);
            if (c == '.' || c == 'e' || c == 'E') {
                fractional = true;
            } else if (!(c == '-' || c == '+' || (c >= '0' && c <= '9'))) {
                break;
            }
            pos++;
        }
        String text = s.substring(start, pos);
        if (text.isEmpty()) {
            throw error("unexpected character");
        }
        try {
            if (!fractional) {
                return Long.parseLong(text);
            }
            return Double.parseDouble(text);
        } catch (NumberFormatException ex) {
            pos = start;
            throw error("bad number");
        }
    }

    private String string() {
        expect('"');
        StringBuilder out = new StringBuilder();
        while (true) {
            if (pos >= s.length()) {
                throw error("unterminated string");
            }
            char c = s.charAt(pos++);
            if (c == '"') {
                return out.toString();
            }
            if (c < 0x20) {
                pos--;
                throw error("unescaped control character");
            }
            if (c != '\\') {
                out.append(c);
                continue;
            }
            if (pos >= s.length()) {
                throw error("unterminated escape");
            }
            char e = s.charAt(pos++);
            switch (e) {
                case '"', '\\', '/' -> out.append(e);
                case 'b' -> out.append('\b');
                case 'f' -> out.append('\f');
                case 'n' -> out.append('\n');
                case 'r' -> out.append('\r');
                case 't' -> out.append('\t');
                case 'u' -> {
                    if (pos + 4 > s.length()) {
                        throw error("short \\u escape");
                    }
                    try {
                        out.append((char) Integer.parseInt(s.substring(pos, pos + 4), 16));
                    } catch (NumberFormatException ex) {
                        throw error("bad \\u escape");
                    }
                    pos += 4;
                }
                default -> {
                    pos--;
                    throw error("bad escape");
                }
            }
        }
    }

    private void skipWs() {
        while (pos < s.length()) {
            char c = s.charAt(pos);
            if (c == ' ' || c == '\t' || c == '\n' || c == '\r') {
                pos++;
            } else {
                return;
            }
        }
    }

    private char peek() {
        if (pos >= s.length()) {
            throw error("unexpected end");
        }
        return s.charAt(pos);
    }

    private char next() {
        char c = peek();
        pos++;
        return c;
    }

    private void expect(char c) {
        if (peek() != c) {
            throw error("expected '" + c + "'");
        }
        pos++;
    }

    private IllegalArgumentException error(String message) {
        return new IllegalArgumentException("invalid JSON at " + pos + ": " + message);
    }
}
