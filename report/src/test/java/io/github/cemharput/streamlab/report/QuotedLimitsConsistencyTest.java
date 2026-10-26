package io.github.cemharput.streamlab.report;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.Timeout.ThreadMode;

/** The quoted limits of the report are the "What it cannot answer" items of the protocol, character for character. */
@Timeout(value = 30, unit = TimeUnit.SECONDS, threadMode = ThreadMode.SEPARATE_THREAD)
class QuotedLimitsConsistencyTest {

    static Path doc(String name) {
        Path file = Path.of("..", "docs", name);
        if (!Files.exists(file)) {
            file = Path.of("docs", name); // run from the repository root
        }
        return file;
    }

    static String read(Path file) throws IOException {
        return Files.readString(file, StandardCharsets.UTF_8).replace("\r\n", "\n");
    }

    /** The lines of section 2 of the protocol, without the heading and without leading and trailing blank lines. */
    static String protocolItems() throws IOException {
        List<String> lines = read(doc("benchmark-protocol.md")).lines().toList();
        int start = lines.indexOf("## 2. What it cannot answer");
        assertThat(start).as("section 2 heading").isGreaterThanOrEqualTo(0);
        List<String> items = new ArrayList<>();
        for (int i = start + 1; i < lines.size() && !lines.get(i).startsWith("## "); i++) {
            items.add(lines.get(i));
        }
        while (!items.isEmpty() && items.get(0).isBlank()) {
            items.remove(0);
        }
        while (!items.isEmpty() && items.get(items.size() - 1).isBlank()) {
            items.remove(items.size() - 1);
        }
        return String.join("\n", items) + "\n";
    }

    @Test
    void theQuotedBlockEqualsTheProtocolItems() throws IOException {
        assertThat(ReportText.QUOTED_LIMITS).isEqualTo(protocolItems());
    }

    @Test
    void theBlockInTheGeneratedReportsEqualsTheProtocolItemsToo() throws IOException {
        for (String report : List.of(ReportRenderer.placeholder())) {
            int b = report.indexOf(WordingGuard.BEGIN);
            int e = report.indexOf(WordingGuard.END);
            assertThat(report.substring(b + WordingGuard.BEGIN.length() + 1, e)).isEqualTo(protocolItems());
        }
        assertThat(ReportText.limitsBlock()).startsWith(WordingGuard.BEGIN + "\n").endsWith(WordingGuard.END + "\n");
    }

    @Test
    void theProtocolSectionHasTheFiveItemsWeQuote() throws IOException {
        assertThat(protocolItems().lines().filter(l -> l.startsWith("- "))).hasSize(5);
    }

    @Test
    void theProtocolVersionIsUnchanged() throws IOException {
        assertThat(read(doc("benchmark-protocol.md")).lines().findFirst()).contains("Protocol version: 1");
    }
}
