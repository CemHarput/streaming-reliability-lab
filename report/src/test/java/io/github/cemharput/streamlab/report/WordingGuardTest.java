package io.github.cemharput.streamlab.report;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.Timeout.ThreadMode;

@Timeout(value = 30, unit = TimeUnit.SECONDS, threadMode = ThreadMode.SEPARATE_THREAD)
class WordingGuardTest {

    private static String withBlock(String outside, String inside) {
        return outside + "\n" + WordingGuard.BEGIN + "\n" + inside + "\n" + WordingGuard.END + "\n";
    }

    @Test
    void everyForbiddenPhraseIsCaughtInASampleSentence() {
        for (String phrase : WordingGuard.FORBIDDEN) {
            String sample = phrase.endsWith("*") ? phrase.substring(0, phrase.length() - 1) + "ed" : phrase;
            assertThat(WordingGuard.scan("In this set the mode was " + sample + " here.", false))
                    .as(phrase).hasSize(1);
            assertThat(WordingGuard.scan(sample.toUpperCase(java.util.Locale.ROOT) + " is loud", false))
                    .as(phrase + " upper case").hasSize(1);
        }
    }

    @Test
    void prefixPhrasesCatchTheirInflections() {
        for (String text : List.of("outperforms", "outperformed", "significantly", "recommended", "recommendation",
                "dramatically", "dramatic")) {
            assertThat(WordingGuard.scan(text, false)).as(text).hasSize(1);
        }
    }

    @Test
    void wordBoundariesPreventFalseHits() {
        for (String ok : List.of("window", "winding", "beaten track", "improve", "approve", "bestowed", "wine",
                "worsened", "response", "unwinnable", "interrupt")) {
            assertThat(WordingGuard.scan(ok, false)).as(ok).isEmpty();
        }
    }

    @Test
    void multiWordPhrasesMatchAcrossWhitespace() {
        assertThat(WordingGuard.scan("it is more   efficient", false)).hasSize(1);
        assertThat(WordingGuard.scan("it is more\nefficient", false)).hasSize(1);
        assertThat(WordingGuard.scan("a confidence interval of", false)).hasSize(1);
        assertThat(WordingGuard.scan("p value", false)).hasSize(1);
        assertThat(WordingGuard.scan("p-value", false)).hasSize(1);
    }

    @Test
    void violationsReportThePhraseAndTheLineNumberButNotTheContext() {
        List<String> v = WordingGuard.scan("line one\nline two is better than secret-context\n", false);
        assertThat(v).containsExactly("forbidden phrase 'better' at line 2");
    }

    @Test
    void theQuotedBlockIsExemptAndNothingElseIs() {
        assertThat(WordingGuard.scan(withBlock("clean text", "this is better and faster"), true)).isEmpty();
        assertThat(WordingGuard.scan(withBlock("this is better", "inside"), true)).hasSize(1);
        assertThat(WordingGuard.scan(withBlock("clean text", "inside") + "\nafter the block: wins", true)).hasSize(1);
    }

    @Test
    void theBlockMustAppearExactlyOnceAndBeBalancedWhenRequired() {
        assertThat(WordingGuard.scan("no block", true)).isNotEmpty();
        String block = withBlock("a", "b");
        assertThat(WordingGuard.scan(block + block, true)).isNotEmpty();
        assertThat(WordingGuard.scan(WordingGuard.BEGIN + " only a begin", true)).isNotEmpty();
        assertThat(WordingGuard.scan(WordingGuard.END + "\n" + WordingGuard.BEGIN, true)).isNotEmpty();
    }

    @Test
    void whereNoBlockIsAllowedAMarkerIsAViolationAndNoExemptionApplies() {
        assertThat(WordingGuard.scan(withBlock("a", "b"), false)).isNotEmpty();
        assertThat(WordingGuard.scan("faster", false)).hasSize(1);
        assertThat(WordingGuard.scan("plain numbers 1,2,3", false)).isEmpty();
    }

    @Test
    void ourOwnProseAndTemplatesAreClean() {
        String prose = String.join("\n", ReportText.INTRO, ReportText.PLACEHOLDER_STATUS, ReportText.MEASURED_INTRO,
                ReportText.INVENTORY_INTRO, ReportText.METRICS_INTRO, ReportText.COMPARISON_INTRO,
                ReportText.DELTAS_INTRO, ReportText.DEFINITIONS, ReportText.LIMITS_PREFACE, ReportText.LIMITS_SUFFIX,
                ReportText.REGENERATE, ReportText.DATA_FILES);
        assertThat(WordingGuard.scan(prose, false)).isEmpty();
    }
}
