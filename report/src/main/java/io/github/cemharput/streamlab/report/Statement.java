package io.github.cemharput.streamlab.report;

import java.util.Map;

/**
 * The closed set of sentence templates for a comparison. Placeholders carry only numbers, mode names, the metric
 * name and the concurrency. There is no ranking, no probability and no general claim.
 */
public enum Statement {
    HIGHER("In this result set, at concurrency {c}, the median {metric} of {m1} ({v1}) was higher than that of {m2}"
            + " ({v2}); the ranges did not overlap and the difference between the medians ({d}) was larger than the"
            + " largest within-mode range ({s}). With n={n1} and n={n2} repetitions this describes this set only.",
            "the ranges do not overlap and the difference between the medians is larger than the largest"
                    + " within-mode range"),
    CANNOT_SEPARATE_OVERLAP("In this result set, at concurrency {c}, the median {metric} was {v1} for {m1} and {v2}"
            + " for {m2}; the ranges overlapped; this set cannot separate the two modes on this metric. With n={n1} and n={n2} repetitions this describes this set only.",
            "the ranges overlap and the set cannot separate the two modes"),
    CANNOT_SEPARATE_SPREAD("In this result set, at concurrency {c}, the median {metric} was {v1} for {m1} and {v2}"
            + " for {m2}; the ranges did not overlap, but the difference between the medians ({d})"
            + " was not larger than the largest within-mode range ({s}); this set cannot separate the two modes on"
            + " this metric. With n={n1} and n={n2} repetitions this describes this set only.",
            "the difference between the medians is not larger than the largest within-mode range and the set"
                    + " cannot separate the two modes"),
    EQUAL("In this result set, at concurrency {c}, the median {metric} of {m1} ({v1}) and of {m2} ({v2}) were equal;"
            + " this set cannot separate the two modes on this metric. With n={n1} and n={n2} repetitions this"
            + " describes this set only.", "the medians are equal"),
    INSUFFICIENT("In this result set, at concurrency {c}, there were fewer than two usable repetitions for {modes}"
            + " for the {metric}; no comparison is stated.", "fewer than two usable repetitions for one mode"),
    DEPENDS_ON_CAUTION("This statement depends on the runs marked * (caution). Without them the outcome for this"
            + " metric would be: {alt}.", "");

    private final String template;
    private final String shortLabel;

    Statement(String template, String shortLabel) {
        this.template = template;
        this.shortLabel = shortLabel;
    }

    public String template() {
        return template;
    }

    /** A clause that describes the outcome; used by {@link #DEPENDS_ON_CAUTION}. */
    public String shortLabel() {
        return shortLabel;
    }

    /**
     * Fills the placeholders in one pass; values are never scanned again, so braces or dollar signs inside a value
     * are harmless. A placeholder of the template without a value is a programming error.
     */
    public String render(Map<String, String> values) {
        StringBuilder out = new StringBuilder();
        int i = 0;
        while (i < template.length()) {
            char c = template.charAt(i);
            if (c == '{') {
                int end = template.indexOf('}', i);
                String key = end < 0 ? null : template.substring(i + 1, end);
                if (key == null || !values.containsKey(key)) {
                    throw new IllegalArgumentException("unfilled placeholder in " + name());
                }
                out.append(values.get(key));
                i = end + 1;
            } else {
                out.append(c);
                i++;
            }
        }
        return out.toString();
    }
}
