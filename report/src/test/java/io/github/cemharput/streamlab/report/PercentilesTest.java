package io.github.cemharput.streamlab.report;

import static org.assertj.core.api.Assertions.assertThat;

import io.github.cemharput.streamlab.report.Percentiles.Dist;

import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.Timeout.ThreadMode;

@Timeout(value = 30, unit = TimeUnit.SECONDS, threadMode = ThreadMode.SEPARATE_THREAD)
class PercentilesTest {

    private static Dist dist(long... values) {
        return Percentiles.of(values.clone(), values.length, 0);
    }

    @Test
    void unsortedFiveValues() {
        // sorted 1,2,3,4,5: p50 rank ceil(2.5)=3, p90 rank ceil(4.5)=5, p99 rank ceil(4.95)=5
        Dist d = dist(5, 1, 4, 2, 3);
        assertThat(d.count()).isEqualTo(5);
        assertThat(d.p50()).isEqualTo(3);
        assertThat(d.p90()).isEqualTo(5);
        assertThat(d.p99()).isEqualTo(5);
        assertThat(d.max()).isEqualTo(5);
    }

    @Test
    void tiesAreHandledNaturally() {
        // sorted 1,1,1,2: p50 rank 2 -> 1, p90 rank ceil(3.6)=4 -> 2
        Dist d = dist(1, 1, 1, 2);
        assertThat(d.p50()).isEqualTo(1);
        assertThat(d.p90()).isEqualTo(2);
        assertThat(d.p99()).isEqualTo(2);
        assertThat(d.max()).isEqualTo(2);
    }

    @Test
    void oneToHundred() {
        long[] v = new long[100];
        for (int i = 0; i < 100; i++) {
            v[i] = 100 - i; // reversed on purpose
        }
        Dist d = Percentiles.of(v, 100, 0);
        assertThat(d.p50()).isEqualTo(50);
        assertThat(d.p90()).isEqualTo(90);
        assertThat(d.p99()).isEqualTo(99);
        assertThat(d.max()).isEqualTo(100);
    }

    @Test
    void singleValueIsEveryPercentile() {
        Dist d = dist(7);
        assertThat(d.p50()).isEqualTo(7);
        assertThat(d.p90()).isEqualTo(7);
        assertThat(d.p99()).isEqualTo(7);
        assertThat(d.max()).isEqualTo(7);
    }

    @Test
    void emptyIsNotAvailableAndKeepsTheMissingCount() {
        Dist d = Percentiles.of(new long[4], 0, 3);
        assertThat(d.count()).isZero();
        assertThat(d.missing()).isEqualTo(3);
        assertThat(d.p50()).isNull();
        assertThat(d.p99()).isNull();
        assertThat(d.max()).isNull();
    }

    @Test
    void onlyTheFirstNValuesAreUsed() {
        long[] v = {3, 1, 2, 999, 999};
        Dist d = Percentiles.of(v, 3, 0);
        assertThat(d.max()).isEqualTo(3);
        assertThat(d.p50()).isEqualTo(2);
    }

    @Test
    void rankNeverLeavesTheRange() {
        assertThat(Percentiles.rank(50, 1)).isEqualTo(1);
        assertThat(Percentiles.rank(99, 1)).isEqualTo(1);
        assertThat(Percentiles.rank(100, 7)).isEqualTo(7);
    }
}
