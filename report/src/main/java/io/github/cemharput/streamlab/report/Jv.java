package io.github.cemharput.streamlab.report;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/** Null-tolerant, typed access to values parsed by {@code bench.json.Json} (Map, List, String, Long, Double, Boolean). */
final class Jv {

    private Jv() {
    }

    @SuppressWarnings("unchecked")
    static Map<String, Object> map(Object o) {
        return o instanceof Map<?, ?> m ? (Map<String, Object>) m : null;
    }

    static List<Object> list(Object o) {
        @SuppressWarnings("unchecked")
        List<Object> l = o instanceof List<?> x ? (List<Object>) x : null;
        return l;
    }

    static String str(Object o) {
        return o instanceof String s ? s : null;
    }

    /** A whole number: a Long, or a Double without a fraction that fits a long. */
    static Long lng(Object o) {
        if (o instanceof Long l) {
            return l;
        }
        if (o instanceof Double d && !d.isNaN() && !d.isInfinite() && d == Math.rint(d)
                && Math.abs(d) < 9.0e18) {
            return d.longValue();
        }
        return null;
    }

    static Double dbl(Object o) {
        return o instanceof Number n ? Double.valueOf(n.doubleValue()) : null;
    }

    static Boolean bool(Object o) {
        return o instanceof Boolean b ? b : null;
    }

    /** Follows {@code keys} through nested maps; null when any step is missing or not a map. */
    static Object get(Object root, String... keys) {
        Object cur = root;
        for (String key : keys) {
            Map<String, Object> m = map(cur);
            if (m == null) {
                return null;
            }
            cur = m.get(key);
        }
        return cur;
    }

    /** The strings of a list; non-string elements are skipped. */
    static List<String> strings(Object o) {
        List<Object> l = list(o);
        List<String> out = new ArrayList<>();
        if (l != null) {
            for (Object e : l) {
                if (e instanceof String s) {
                    out.add(s);
                }
            }
        }
        return out;
    }
}
