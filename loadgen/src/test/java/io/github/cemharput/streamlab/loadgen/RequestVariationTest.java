package io.github.cemharput.streamlab.loadgen;

import static org.assertj.core.api.Assertions.assertThat;

import io.github.cemharput.streamlab.loadgen.Options.IntRange;
import io.github.cemharput.streamlab.loadgen.client.StreamParams;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.Timeout.ThreadMode;

@Timeout(value = 60, unit = TimeUnit.SECONDS, threadMode = ThreadMode.SEPARATE_THREAD)
class RequestVariationTest {

    private static RequestVariation variation(long seed, String fault, double rate) {
        return new RequestVariation(new IntRange(5, 20), new IntRange(10, 50), new IntRange(0, 30), fault, rate,
                new IntRange(0, 25), seed);
    }

    /** Golden values were computed with an independent Python implementation of the documented algorithm. */
    @Test
    void goldenValuesArePinned() {
        RequestVariation v = variation(42, "UNAVAILABLE", 0.5);

        assertThat(v.forIndex(0)).isEqualTo(new StreamParams(10, 31, 23, "UNAVAILABLE", 20, -2430762948046562554L));
        assertThat(v.forIndex(1)).isEqualTo(new StreamParams(10, 21, 13, "UNAVAILABLE", 8, -8854191821003330121L));
        assertThat(v.forIndex(2)).isEqualTo(new StreamParams(10, 49, 4, "NONE", 0, 1347604182271487641L));
    }

    @Test
    void singleValueRangesAndNoFault() {
        RequestVariation v = new RequestVariation(IntRange.of(10), IntRange.of(50), IntRange.of(0), "NONE", 1.0,
                IntRange.of(0), 0);

        assertThat(v.forIndex(0)).isEqualTo(new StreamParams(10, 50, 0, "NONE", 0, 6038094601263162090L));
    }

    @Test
    void sameSeedGivesSameSequenceRegardlessOfCallOrder() {
        RequestVariation a = variation(7, "INTERNAL", 0.3);
        RequestVariation b = variation(7, "INTERNAL", 0.3);
        StreamParams a5 = a.forIndex(5);
        for (int i = 0; i < 20; i++) {
            assertThat(b.forIndex(i)).isEqualTo(a.forIndex(i));
        }
        assertThat(b.forIndex(5)).isEqualTo(a5);
    }

    @Test
    void differentSeedsGiveDifferentSequences() {
        RequestVariation a = variation(1, "NONE", 1);
        RequestVariation b = variation(2, "NONE", 1);
        int differing = 0;
        for (int i = 0; i < 50; i++) {
            if (!a.forIndex(i).equals(b.forIndex(i))) {
                differing++;
            }
        }
        assertThat(differing).isGreaterThan(45);
    }

    @Test
    void valuesStayInsideTheRanges() {
        RequestVariation v = variation(99, "HANG", 1.0);
        for (int i = 0; i < 2000; i++) {
            StreamParams p = v.forIndex(i);
            assertThat(p.chunks()).isBetween(5, 20);
            assertThat(p.intervalMs()).isBetween(10, 50);
            assertThat(p.firstDelayMs()).isBetween(0, 30);
            assertThat(p.faultAtSeq()).isBetween(0, 25);
        }
    }

    @Test
    void faultRateZeroNeverAppliesAndRateOneAlwaysDoes() {
        RequestVariation never = variation(3, "UNAVAILABLE", 0.0);
        RequestVariation always = variation(3, "UNAVAILABLE", 1.0);
        for (int i = 0; i < 500; i++) {
            assertThat(never.forIndex(i).fault()).isEqualTo("NONE");
            assertThat(never.forIndex(i).faultAtSeq()).isZero();
            assertThat(always.forIndex(i).fault()).isEqualTo("UNAVAILABLE");
        }
    }

    @Test
    void faultRateIsRoughlyHonoured() {
        RequestVariation v = variation(5, "INTERNAL", 0.25);
        int faulted = 0;
        for (int i = 0; i < 4000; i++) {
            if (!v.forIndex(i).fault().equals("NONE")) {
                faulted++;
            }
        }
        assertThat(faulted).isBetween(800, 1200);
    }

    @Test
    void changingTheFaultDoesNotShiftOtherParameters() {
        RequestVariation plain = variation(11, "NONE", 1.0);
        RequestVariation faulty = variation(11, "HANG", 1.0);
        for (int i = 0; i < 50; i++) {
            StreamParams p = plain.forIndex(i);
            StreamParams f = faulty.forIndex(i);
            assertThat(f.chunks()).isEqualTo(p.chunks());
            assertThat(f.intervalMs()).isEqualTo(p.intervalMs());
            assertThat(f.firstDelayMs()).isEqualTo(p.firstDelayMs());
            assertThat(f.seed()).isEqualTo(p.seed());
        }
    }

    @Test
    void queryContainsEveryParameterExplicitly() {
        assertThat(new StreamParams(3, 4, 5, "HANG", 2, -7).toQuery())
                .isEqualTo("chunks=3&firstDelayMs=5&intervalMs=4&fault=HANG&faultAtSeq=2&seed=-7");
    }
}
