package io.github.cemharput.streamlab.bench;

import java.lang.management.ManagementFactory;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.function.DoubleSupplier;

/** Coarse system CPU load before a measurement: one priming read, then a sample every 500 ms over the window. */
public final class CpuLoadSampler {

    private static final Duration INTERVAL = Duration.ofMillis(500);

    private final DoubleSupplier load;
    private final Ticker ticker;

    public CpuLoadSampler(DoubleSupplier load, Ticker ticker) {
        this.load = load;
        this.ticker = ticker;
    }

    /** {@code getCpuLoad()} of the platform MXBean; negative (unavailable) when it is not the JDK one. */
    public static DoubleSupplier osLoad() {
        return () -> ManagementFactory.getOperatingSystemMXBean() instanceof com.sun.management.OperatingSystemMXBean os
                ? os.getCpuLoad() : -1.0;
    }

    /** Samples in [0,1]; unavailable samples (negative or NaN) are null. */
    public List<Double> sample(Duration window) throws InterruptedException {
        load.getAsDouble(); // priming read: the first value covers an undefined interval
        int count = (int) Math.max(1, window.toMillis() / INTERVAL.toMillis());
        List<Double> samples = new ArrayList<>(count);
        for (int i = 0; i < count; i++) {
            ticker.sleep(INTERVAL);
            double v = load.getAsDouble();
            samples.add(v < 0 || Double.isNaN(v) ? null : Double.valueOf(v));
        }
        return samples;
    }

    /** Flags for a sample series: unavailable when no sample is usable, busy when the mean exceeds the threshold. */
    public static List<String> flags(List<Double> samples) {
        List<Double> usable = samples.stream().filter(java.util.Objects::nonNull).toList();
        if (usable.isEmpty()) {
            return List.of(Flags.CPU_LOAD_UNAVAILABLE);
        }
        double mean = usable.stream().mapToDouble(Double::doubleValue).average().orElse(0);
        return mean > Flags.CPU_BUSY_THRESHOLD ? List.of(Flags.CPU_BUSY_BEFORE_RUN) : List.of();
    }
}
