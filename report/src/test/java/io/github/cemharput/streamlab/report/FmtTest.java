package io.github.cemharput.streamlab.report;

import static org.assertj.core.api.Assertions.assertThat;

import java.math.BigDecimal;
import java.util.Locale;

import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.Timeout.ThreadMode;

@Timeout(value = 30, unit = TimeUnit.SECONDS, threadMode = ThreadMode.SEPARATE_THREAD)
class FmtTest {

    @Test
    void millisecondsRoundNanosToMicrosFirstAndUseThreeDecimals() {
        assertThat(Fmt.ms(5_000_000L)).isEqualTo("5.000");
        assertThat(Fmt.ms(1_234_567L)).isEqualTo("1.235"); // 1234.567 us -> 1235 us
        assertThat(Fmt.ms(1_500L)).isEqualTo("0.002"); // 1.5 us half-even -> 2 us
        assertThat(Fmt.ms(2_500L)).isEqualTo("0.002"); // 2.5 us half-even -> 2 us
        assertThat(Fmt.ms(0L)).isEqualTo("0.000");
        assertThat(Fmt.ms(new BigDecimal("1500500"))).isEqualTo("1.500"); // 1500.5 us half-even -> 1500 us
    }

    @Test
    void percentUsesTwoDecimalsAndIsNotAvailableForAnEmptyWhole() {
        assertThat(Fmt.percent(1, 3)).isEqualTo("33.33");
        assertThat(Fmt.percent(2, 3)).isEqualTo("66.67");
        assertThat(Fmt.percent(0, 0)).isEqualTo("n/a");
    }

    @Test
    void gibUsesOneDecimal() {
        assertThat(Fmt.gib(17_179_869_184L)).isEqualTo("16.0");
        assertThat(Fmt.gib(1_610_612_736L)).isEqualTo("1.5");
    }

    @Test
    void numbersDoNotDependOnTheDefaultLocale() {
        Locale old = Locale.getDefault();
        try {
            Locale.setDefault(Locale.forLanguageTag("tr-TR"));
            assertThat(Fmt.dec(new BigDecimal("1234.5"), 2)).isEqualTo("1234.50");
            assertThat(Fmt.cpu(0.0625)).isEqualTo("0.062");
            Locale.setDefault(Locale.GERMANY);
            assertThat(Fmt.dec(new BigDecimal("1234.5"), 2)).isEqualTo("1234.50");
            assertThat(Fmt.ms(1_234_567L)).isEqualTo("1.235");
        } finally {
            Locale.setDefault(old);
        }
    }

    @Test
    void counterValuesDropTrailingZerosAndNeverUseExponents() {
        assertThat(Fmt.num(new BigDecimal("10.0"))).isEqualTo("10");
        assertThat(Fmt.num(new BigDecimal("1E+3"))).isEqualTo("1000");
        assertThat(Fmt.num(BigDecimal.ZERO)).isEqualTo("0");
        assertThat(Fmt.num(new BigDecimal("0.000"))).isEqualTo("0");
        assertThat(Fmt.num(null)).isEqualTo("n/a");
    }

    @Test
    void tableCellsEscapeThePipe() {
        assertThat(Fmt.cell("a|b")).isEqualTo("a\\|b");
    }
}
