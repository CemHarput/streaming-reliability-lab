package io.github.cemharput.streamlab.report;

/** How a flag or check affects a run. The order is the order of reasons: exclude first. */
public enum Severity {
    EXCLUDE("exclude"),
    CAUTION("caution"),
    INFO("info");

    private final String label;

    Severity(String label) {
        this.label = label;
    }

    public String label() {
        return label;
    }
}
