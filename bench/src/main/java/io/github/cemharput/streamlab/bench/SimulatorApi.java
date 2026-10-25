package io.github.cemharput.streamlab.bench;

import java.util.List;
import java.util.Map;

/** What the runner needs from a running simulator (gRPC GetStats). */
public interface SimulatorApi {

    /** The eight GetStats fields, or an error text. */
    record StatsResult(Map<String, Long> values, String error) {

        public boolean available() {
            return values != null;
        }

        public static StatsResult unavailable(String error) {
            return new StatsResult(null, error);
        }
    }

    List<String> FIELDS = List.of("active_generations", "started_total", "completed_total", "cancelled_total",
            "faulted_total", "rejected_total", "chunks_emitted_total", "flow_control_pauses_total");

    StatsResult stats();
}
