package io.github.cemharput.streamlab.report;

import io.github.cemharput.streamlab.report.CellComparison.Cell;
import io.github.cemharput.streamlab.report.CellComparison.Entry;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.ArrayList;
import java.util.List;

/**
 * A dot plot, 640 x 360: concurrency groups on the x axis, one column per mode, one point per run (a circle for the
 * 1st and a square for the 2nd position of the ABBA pair), the median as a short horizontal line. With a handful of
 * repetitions a bar or an error bar would hide the individual runs; every point is drawn so that overlap is visible.
 * Integer coordinates, fixed order, no timestamps.
 */
public final class SvgDotPlot {

    private static final int WIDTH = 640;
    private static final int HEIGHT = 360;
    private static final int LEFT = 70;
    private static final int RIGHT = 20;
    private static final int TOP = 56;
    private static final int PLOT_H = 230;
    private static final int PLOT_W = WIDTH - LEFT - RIGHT;
    private static final int BOTTOM = TOP + PLOT_H;
    private static final String[] COLORS = {"#1f5fa8", "#b5541c"};

    private SvgDotPlot() {
    }

    /** {@code cells} are the cells of one metric, concurrency ascending. */
    public static String render(ComparedMetric metric, List<Cell> cells, List<String> modes) {
        StringBuilder sb = new StringBuilder();
        sb.append("<svg xmlns=\"http://www.w3.org/2000/svg\" width=\"").append(WIDTH).append("\" height=\"")
                .append(HEIGHT).append("\" viewBox=\"0 0 ").append(WIDTH).append(' ').append(HEIGHT)
                .append("\" font-family=\"sans-serif\" font-size=\"11\">\n");
        sb.append("<rect x=\"0\" y=\"0\" width=\"").append(WIDTH).append("\" height=\"").append(HEIGHT)
                .append("\" fill=\"#ffffff\"/>\n");
        text(sb, 10, 18, "start", 13, metric.title() + " (" + metric.unit().label()
                + "), one point per included run");
        text(sb, 10, 34, "start", 11, "Excluded runs are not drawn. Each point is one run; the line is the median"
                + " of the points in that column.");

        BigDecimal max = BigDecimal.ZERO;
        boolean any = false;
        for (Cell c : cells) {
            for (List<Entry> es : c.entries().values()) {
                for (Entry e : es) {
                    any = true;
                    max = max.max(e.value());
                }
            }
        }
        if (cells.isEmpty() || !any) {
            text(sb, WIDTH / 2, TOP + PLOT_H / 2, "middle", 12, "no included run has a value for this metric");
            sb.append("</svg>\n");
            return sb.toString();
        }
        BigDecimal top = niceMax(max);

        // y axis and grid
        for (int i = 0; i <= 4; i++) {
            BigDecimal v = top.multiply(BigDecimal.valueOf(i)).divide(BigDecimal.valueOf(4));
            int y = y(v, top);
            sb.append("<line x1=\"").append(LEFT).append("\" y1=\"").append(y).append("\" x2=\"")
                    .append(WIDTH - RIGHT).append("\" y2=\"").append(y).append("\" stroke=\"#d0d0d0\"/>\n");
            text(sb, LEFT - 6, y + 4, "end", 10, metric.bare(v));
        }
        sb.append("<line x1=\"").append(LEFT).append("\" y1=\"").append(TOP).append("\" x2=\"").append(LEFT)
                .append("\" y2=\"").append(BOTTOM).append("\" stroke=\"#000000\"/>\n");
        sb.append("<line x1=\"").append(LEFT).append("\" y1=\"").append(BOTTOM).append("\" x2=\"")
                .append(WIDTH - RIGHT).append("\" y2=\"").append(BOTTOM).append("\" stroke=\"#000000\"/>\n");
        text(sb, 10, TOP - 6, "start", 10, metric.unit().label());

        int groups = cells.size();
        int groupW = PLOT_W / groups;
        for (int g = 0; g < groups; g++) {
            Cell cell = cells.get(g);
            int groupLeft = LEFT + g * groupW;
            int colW = groupW / 2;
            for (int col = 0; col < 2; col++) {
                String mode = modes.get(col);
                int colLeft = groupLeft + col * colW;
                int center = colLeft + colW / 2;
                String color = COLORS[col];
                List<Entry> entries = cell.entries().get(mode);
                int n = entries.size();
                for (int k = 0; k < n; k++) {
                    Entry e = entries.get(k);
                    int x = center + ((2 * k - (n - 1)) * 10) / 2;
                    int y = y(e.value(), top);
                    if ("1st".equals(e.position())) {
                        sb.append("<circle cx=\"").append(x).append("\" cy=\"").append(y)
                                .append("\" r=\"4\" fill=\"").append(color).append("\"/>\n");
                    } else {
                        sb.append("<rect x=\"").append(x - 4).append("\" y=\"").append(y - 4)
                                .append("\" width=\"8\" height=\"8\" fill=\"").append(color).append("\"/>\n");
                    }
                }
                CellComparison.ModeStats stats = cell.stats().get(mode);
                if (stats.median() != null) {
                    int y = y(stats.median(), top);
                    sb.append("<line x1=\"").append(colLeft + colW / 6).append("\" y1=\"").append(y)
                            .append("\" x2=\"").append(colLeft + colW - colW / 6).append("\" y2=\"").append(y)
                            .append("\" stroke=\"").append(color).append("\" stroke-width=\"2\"/>\n");
                }
                text(sb, center, BOTTOM + 16, "middle", 10, mode);
            }
            text(sb, groupLeft + groupW / 2, BOTTOM + 34, "middle", 11, "concurrency " + cell.concurrency());
        }
        // legend
        int ly = HEIGHT - 12;
        sb.append("<circle cx=\"").append(LEFT + 6).append("\" cy=\"").append(ly - 4)
                .append("\" r=\"4\" fill=\"#555555\"/>\n");
        text(sb, LEFT + 16, ly, "start", 10, "1st run of its pair");
        sb.append("<rect x=\"").append(LEFT + 126).append("\" y=\"").append(ly - 8)
                .append("\" width=\"8\" height=\"8\" fill=\"#555555\"/>\n");
        text(sb, LEFT + 140, ly, "start", 10, "2nd run of its pair");
        text(sb, LEFT + 260, ly, "start", 10, "colours: " + modes.get(0) + " " + "(blue), " + modes.get(1) + " (orange)");
        sb.append("</svg>\n");
        return sb.toString();
    }

    private static int y(BigDecimal value, BigDecimal top) {
        BigDecimal frac = value.divide(top, 6, RoundingMode.HALF_EVEN);
        int h = frac.multiply(BigDecimal.valueOf(PLOT_H)).setScale(0, RoundingMode.HALF_EVEN).intValue();
        return BOTTOM - Math.max(0, Math.min(PLOT_H, h));
    }

    /** The smallest 1, 2, 5 or 10 times a power of ten that is not below {@code max}; 1 for zero. */
    static BigDecimal niceMax(BigDecimal max) {
        if (max.signum() <= 0) {
            return BigDecimal.ONE;
        }
        int exponent = max.precision() - max.scale() - 1;
        BigDecimal base = BigDecimal.ONE.scaleByPowerOfTen(exponent);
        List<BigDecimal> steps = new ArrayList<>(List.of(BigDecimal.ONE, BigDecimal.valueOf(2), BigDecimal.valueOf(5),
                BigDecimal.TEN));
        for (BigDecimal step : steps) {
            BigDecimal candidate = base.multiply(step);
            if (candidate.compareTo(max) >= 0) {
                return candidate;
            }
        }
        return base.multiply(BigDecimal.TEN);
    }

    private static void text(StringBuilder sb, int x, int y, String anchor, int size, String content) {
        sb.append("<text x=\"").append(x).append("\" y=\"").append(y).append("\" text-anchor=\"").append(anchor)
                .append("\" font-size=\"").append(size).append("\">").append(escape(content)).append("</text>\n");
    }

    private static String escape(String s) {
        return s.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;").replace("\"", "&quot;");
    }
}
