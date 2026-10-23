package io.github.cemharput.streamlab.testsupport;

import static org.assertj.core.api.Assertions.assertThat;

import io.github.cemharput.streamlab.testsupport.SseClient.SseEvent;
import java.util.List;

public final class SseAssertions {

    private SseAssertions() {
    }

    /** AC9: exactly one terminal event, and it is the last event. Returns it. */
    public static SseEvent assertSingleTerminal(List<SseEvent> events) {
        List<SseEvent> terminals = events.stream()
                .filter(e -> "complete".equals(e.name()) || "error".equals(e.name()))
                .toList();
        assertThat(terminals).as("terminal events in %s", events).hasSize(1);
        assertThat(events.get(events.size() - 1)).isSameAs(terminals.get(0));
        return terminals.get(0);
    }

    public static List<String> chunkIds(List<SseEvent> events) {
        return events.stream().filter(e -> "chunk".equals(e.name())).map(SseEvent::id).toList();
    }
}
