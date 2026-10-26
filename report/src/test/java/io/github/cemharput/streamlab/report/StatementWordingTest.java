package io.github.cemharput.streamlab.report;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.Map;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.Timeout.ThreadMode;

@Timeout(value = 30, unit = TimeUnit.SECONDS, threadMode = ThreadMode.SEPARATE_THREAD)
class StatementWordingTest {

    private static final Map<String, String> VALUES = Map.ofEntries(Map.entry("c", "50"),
            Map.entry("metric", "time to first chunk p50"), Map.entry("m1", "shared"), Map.entry("v1", "1.000 ms"),
            Map.entry("m2", "per-request"), Map.entry("v2", "2.000 ms"), Map.entry("d", "1.000 ms"),
            Map.entry("s", "0.500 ms"), Map.entry("n1", "4"), Map.entry("n2", "4"), Map.entry("modes", "shared (n=1)"), Map.entry("alt", "the medians are equal"));

    @Test
    void everyTemplateRendersWithOnlyTheClosedPlaceholders() {
        for (Statement s : Statement.values()) {
            String text = s.render(VALUES);
            assertThat(text).as(s.name()).doesNotContain("{").doesNotContain("}").isNotBlank();
        }
    }

    @Test
    void everyTemplateAndEveryShortLabelPassesTheWordingGuard() {
        for (Statement s : Statement.values()) {
            assertThat(WordingGuard.scan(s.template(), false)).as(s.name()).isEmpty();
            assertThat(WordingGuard.scan(s.render(VALUES), false)).as(s.name() + " rendered").isEmpty();
            assertThat(WordingGuard.scan(s.shortLabel(), false)).as(s.name() + " label").isEmpty();
        }
    }

    @Test
    void theTemplatesStateOnlyThisResultSetAndNeverGeneralise() {
        for (Statement s : new Statement[] {Statement.HIGHER, Statement.CANNOT_SEPARATE_OVERLAP,
            Statement.CANNOT_SEPARATE_SPREAD, Statement.EQUAL}) {
            assertThat(s.template()).startsWith("In this result set, at concurrency {c}, the median {metric}");
            assertThat(s.template()).contains("With n={n1} and n={n2} repetitions this describes this set only.");
        }
        assertThat(Statement.INSUFFICIENT.template()).startsWith("In this result set, at concurrency {c},")
                .contains("no comparison is stated");
    }

    @Test
    void theHigherTemplateIsTheWordingOfThePlan() {
        assertThat(Statement.HIGHER.render(VALUES)).isEqualTo("In this result set, at concurrency 50, the median"
                + " time to first chunk p50 of shared (1.000 ms) was higher than that of per-request (2.000 ms); the"
                + " ranges did not overlap and the difference between the medians (1.000 ms) was larger than the"
                + " largest within-mode range (0.500 ms). With n=4 and n=4 repetitions this describes this set only.");
    }

    @Test
    void placeholdersCarryNumbersModeNamesTheMetricAndTheConcurrencyOnly() {
        for (Statement s : Statement.values()) {
            java.util.regex.Matcher m = java.util.regex.Pattern.compile("\\{([a-z0-9]+)}").matcher(s.template());
            while (m.find()) {
                assertThat(VALUES).as(s.name()).containsKey(m.group(1));
            }
        }
    }

    @Test
    void bracesAndDollarSignsInValuesAreHarmless() {
        Map<String, String> v = new java.util.HashMap<>(VALUES);
        v.put("m1", "{x}$1");
        v.put("m2", "${y}");
        String text = Statement.HIGHER.render(v);
        assertThat(text).contains("of {x}$1 (1.000 ms)").contains("of ${y} (2.000 ms)");
    }

    @Test
    void anUnfilledPlaceholderIsAnError() {
        assertThatThrownBy(() -> Statement.HIGHER.render(Map.of("c", "1"))).isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void thereAreNoPValuesAndNoRankingWords() {
        for (Statement s : Statement.values()) {
            assertThat(s.template().toLowerCase(java.util.Locale.ROOT)).doesNotContain("p-value", "rank", "confidence");
        }
    }
}
