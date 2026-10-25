package io.github.cemharput.streamlab.bench;

import static org.assertj.core.api.Assertions.assertThat;

import io.github.cemharput.streamlab.bench.LoadgenOutputInspector.LoadgenFacts;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.Timeout.ThreadMode;
import org.junit.jupiter.api.io.TempDir;

@Timeout(value = 30, unit = TimeUnit.SECONDS, threadMode = ThreadMode.SEPARATE_THREAD)
class LoadgenOutputInspectorTest {

    static final String HEADER = "{\"type\":\"header\",\"schemaVersion\":1}";
    static final String RECORD = "{\"type\":\"record\",\"outcome\":\"COMPLETED\"}";
    static final String FOOTER = "{\"type\":\"footer\",\"stopReason\":\"duration\",\"requestsStarted\":10,"
            + "\"recordsWritten\":10,\"interrupted\":0,\"interChunkHistogram\":\"AAAA\"}";
    static final String SUMMARY = "{\"type\":\"summary\",\"schemaVersion\":1,\"outcome_COMPLETED\":9,"
            + "\"outcome_REJECTED\":1,\"outcome_TIMEOUT\":0,\"outcome_RESET\":2}";

    @TempDir
    Path dir;

    private LoadgenFacts inspect(String summary, String jsonl) throws IOException {
        Path s = dir.resolve("s.json");
        Path j = dir.resolve("j.jsonl");
        if (summary != null) {
            Files.writeString(s, summary);
        }
        if (jsonl != null) {
            Files.writeString(j, jsonl);
        }
        return LoadgenOutputInspector.inspect(s, j);
    }

    @Test
    void cleanOutputHasNoFlagsExceptGenericNonzeroOutcomes() throws IOException {
        LoadgenFacts f = inspect(SUMMARY, HEADER + "\n" + RECORD + "\n" + FOOTER + "\n");
        assertThat(f.summaryPresent()).isTrue();
        assertThat(f.outcome("COMPLETED")).isEqualTo(9);
        assertThat(f.outcome("MISSING")).isZero();
        assertThat(f.footer()).containsEntry("stopReason", "duration").containsEntry("requestsStarted", 10L)
                .doesNotContainKey("interChunkHistogram");
        assertThat(f.flags()).containsExactly(Flags.outcomeNonzero("RESET"));
    }

    @Test
    void completedAndRejectedNeverProduceOutcomeFlags() throws IOException {
        LoadgenFacts f = inspect("{\"type\":\"summary\",\"outcome_COMPLETED\":5,\"outcome_REJECTED\":5}",
                HEADER + "\n" + FOOTER);
        assertThat(f.flags()).isEmpty();
    }

    @Test
    void missingFilesAreFlaggedNotThrown() throws IOException {
        LoadgenFacts f = inspect(null, null);
        assertThat(f.summaryPresent()).isFalse();
        assertThat(f.footer()).isNull();
        assertThat(f.flags()).containsExactlyInAnyOrder(Flags.SUMMARY_MISSING, Flags.JSONL_HEADER_UNEXPECTED,
                Flags.JSONL_FOOTER_MISSING);
    }

    @Test
    void ioErrorLeavesNoFooter() throws IOException {
        LoadgenFacts f = inspect(SUMMARY, HEADER + "\n" + RECORD + "\n");
        assertThat(f.flags()).contains(Flags.JSONL_FOOTER_MISSING).doesNotContain(Flags.JSONL_HEADER_UNEXPECTED);
    }

    @Test
    void footerStopReasonAndInterruptedAreFlagged() throws IOException {
        String footer = "{\"type\":\"footer\",\"stopReason\":\"signal\",\"requestsStarted\":3,\"recordsWritten\":3,"
                + "\"interrupted\":2}";
        LoadgenFacts f = inspect(SUMMARY, HEADER + "\n" + footer + "\n");
        assertThat(f.flags()).contains(Flags.STOP_REASON_NOT_DURATION, Flags.FOOTER_INTERRUPTED_NONZERO);
    }

    @Test
    void largeFilesAreReadFromTheTailOnly() throws IOException {
        StringBuilder sb = new StringBuilder(HEADER).append('\n');
        for (int i = 0; i < 20_000; i++) {
            sb.append(RECORD).append('\n');
        }
        sb.append(FOOTER).append('\n');
        LoadgenFacts f = inspect(SUMMARY, sb.toString());
        assertThat(f.footer()).containsEntry("recordsWritten", 10L);
    }

    @Test
    void nothingIsModified() throws IOException {
        inspect(SUMMARY, HEADER + "\n" + FOOTER + "\n");
        assertThat(Files.readString(dir.resolve("j.jsonl"))).isEqualTo(HEADER + "\n" + FOOTER + "\n");
    }
}
