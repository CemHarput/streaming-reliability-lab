package io.github.cemharput.streamlab.bench;

import java.time.Duration;
import java.util.List;

/** Parameter presets. Everything not listed here is pinned by the protocol and identical in both. */
public enum Preset {
    DEFAULT("default", List.of(50, 100, 150), 4, Duration.ofSeconds(60), Duration.ofSeconds(20),
            Duration.ofSeconds(5), Duration.ofSeconds(30), Duration.ofSeconds(3)),
    SMOKE("smoke", List.of(4), 2, Duration.ofSeconds(3), Duration.ofSeconds(2),
            Duration.ZERO, Duration.ofSeconds(1), Duration.ofSeconds(1));

    private final String cliName;
    private final List<Integer> concurrency;
    private final int repetitions;
    private final Duration duration;
    private final Duration warmup;
    private final Duration rampUp;
    private final Duration cooldown;
    private final Duration cpuWindow;

    Preset(String cliName, List<Integer> concurrency, int repetitions, Duration duration, Duration warmup,
            Duration rampUp, Duration cooldown, Duration cpuWindow) {
        this.cliName = cliName;
        this.concurrency = concurrency;
        this.repetitions = repetitions;
        this.duration = duration;
        this.warmup = warmup;
        this.rampUp = rampUp;
        this.cooldown = cooldown;
        this.cpuWindow = cpuWindow;
    }

    public String cliName() {
        return cliName;
    }

    public List<Integer> concurrency() {
        return concurrency;
    }

    public int repetitions() {
        return repetitions;
    }

    public Duration duration() {
        return duration;
    }

    public Duration warmup() {
        return warmup;
    }

    public Duration rampUp() {
        return rampUp;
    }

    public Duration cooldown() {
        return cooldown;
    }

    public Duration cpuWindow() {
        return cpuWindow;
    }

    /** The preset for a CLI value, or {@code null}. */
    public static Preset fromCliName(String text) {
        for (Preset p : values()) {
            if (p.cliName.equals(text)) {
                return p;
            }
        }
        return null;
    }
}
