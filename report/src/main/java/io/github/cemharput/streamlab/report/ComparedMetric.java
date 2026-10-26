package io.github.cemharput.streamlab.report;

import java.math.BigDecimal;
import java.util.function.Function;

/** The fixed set of metrics that are compared between the two modes at one concurrency. */
public enum ComparedMetric {
    COMPLETED_PER_SECOND("completed-per-second", "completed streams per second", Unit.RATE,
            m -> m.completedPerSecond()),
    TTFC_P50("ttfc-p50", "time to first chunk p50", Unit.MILLIS, m -> nanos(m.ttfc().p50())),
    TTFC_P99("ttfc-p99", "time to first chunk p99", Unit.MILLIS, m -> nanos(m.ttfc().p99())),
    TOTAL_P50("total-p50", "total stream duration p50", Unit.MILLIS, m -> nanos(m.total().p50())),
    TOTAL_P99("total-p99", "total stream duration p99", Unit.MILLIS, m -> nanos(m.total().p99())),
    INTER_CHUNK_P99("inter-chunk-p99", "inter-chunk gap p99", Unit.MILLIS,
            m -> m.interChunk() == null ? null : nanos(m.interChunk().p99())),
    REJECTED_SHARE("rejected-share", "share of REJECTED requests", Unit.PERCENT, m -> m.rejectedShare());

    /** How values are shown. */
    public enum Unit {
        RATE("per s"), MILLIS("ms"), PERCENT("%");

        private final String label;

        Unit(String label) {
            this.label = label;
        }

        public String label() {
            return label;
        }
    }

    private final String code;
    private final String title;
    private final Unit unit;
    private final Function<RunMetrics, BigDecimal> extractor;

    ComparedMetric(String code, String title, Unit unit, Function<RunMetrics, BigDecimal> extractor) {
        this.code = code;
        this.title = title;
        this.unit = unit;
        this.extractor = extractor;
    }

    private static BigDecimal nanos(Long v) {
        return v == null ? null : BigDecimal.valueOf(v);
    }

    /** Stable name used in CSV and JSON. */
    public String code() {
        return code;
    }

    /** Name used in sentences. */
    public String title() {
        return title;
    }

    public Unit unit() {
        return unit;
    }

    /** The exact value of this run (nanoseconds, per second, or percent at thirty decimals); null when n/a. */
    public BigDecimal value(RunMetrics metrics) {
        return metrics == null ? null : extractor.apply(metrics);
    }

    /** The usual number of decimals of this metric in the tables (milliseconds: three, otherwise two). */
    public int baseScale() {
        return unit == Unit.MILLIS ? 3 : 2;
    }

    /** {@code value} rounded half-even to {@code scale} decimals of the display unit (nanoseconds become milliseconds). */
    public BigDecimal round(BigDecimal value, int scale) {
        if (unit == Unit.MILLIS && scale == 3) {
            return new BigDecimal(Fmt.ms(value));
        }
        BigDecimal v = unit == Unit.MILLIS ? value.movePointLeft(6) : value;
        return v.setScale(scale, java.math.RoundingMode.HALF_EVEN);
    }

    /** Rounded for display without a unit, at the usual number of decimals. */
    public String bare(BigDecimal value) {
        return bare(value, baseScale());
    }

    public String bare(BigDecimal value, int scale) {
        return round(value, scale).toPlainString();
    }

    /** Rounded for display with a unit. */
    public String withUnit(BigDecimal value) {
        return withUnit(value, baseScale());
    }

    public String withUnit(BigDecimal value, int scale) {
        return bare(value, scale) + " " + unit.label();
    }

    /**
     * The smallest number of decimals (from the usual one, at most 30 more) at which what the sentence says stays
     * visible in the rounded values: the two medians differ when they differ exactly; for a statement that says the
     * ranges did not overlap (higher, cannot separate by spread) the rounded ranges do not touch; the rounded
     * difference of the medians is larger than the rounded largest range for "higher" and not larger for the spread
     * case.
     */
    public int scaleFor(CellComparison.ModeStats x, CellComparison.ModeStats y, Statement statement) {
        int base = baseScale();
        for (int scale = base; scale < base + 30; scale++) {
            if (shows(x, y, statement, scale)) {
                return scale;
            }
        }
        return base + 30;
    }

    private boolean shows(CellComparison.ModeStats x, CellComparison.ModeStats y, Statement statement, int scale) {
        if (x.median().compareTo(y.median()) != 0 && round(x.median(), scale).compareTo(round(y.median(), scale)) == 0) {
            return false;
        }
        if (statement != Statement.HIGHER && statement != Statement.CANNOT_SEPARATE_SPREAD) {
            return true;
        }
        BigDecimal lowestUpper = round(x.min(), scale).max(round(y.min(), scale));
        BigDecimal highestLower = round(x.max(), scale).min(round(y.max(), scale));
        if (lowestUpper.compareTo(highestLower) <= 0) {
            return false; // the rounded ranges touch or overlap, but the sentence says they did not
        }
        int c = displayDifference(x, y, scale).compareTo(displaySpread(x, y, scale));
        return statement == Statement.HIGHER ? c > 0 : c <= 0;
    }

    /** The difference of the two rounded medians (what a reader computes from the table). */
    public BigDecimal displayDifference(CellComparison.ModeStats x, CellComparison.ModeStats y, int scale) {
        return round(x.median(), scale).subtract(round(y.median(), scale)).abs();
    }

    /** The largest of the two rounded ranges (maximum minus minimum, as shown in the table). */
    public BigDecimal displaySpread(CellComparison.ModeStats x, CellComparison.ModeStats y, int scale) {
        BigDecimal rx = round(x.max(), scale).subtract(round(x.min(), scale));
        BigDecimal ry = round(y.max(), scale).subtract(round(y.min(), scale));
        return rx.max(ry);
    }

    /** An already rounded display value (see {@link #round}) with its unit. */
    public String format(BigDecimal displayed) {
        return displayed.toPlainString() + " " + unit.label();
    }

    /** The exact value as text for CSV and JSON (nanoseconds are written as they are). */
    public String exact(BigDecimal value) {
        return value == null ? "" : value.stripTrailingZeros().toPlainString();
    }
}
