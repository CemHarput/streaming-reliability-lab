package io.github.cemharput.streamlab.report;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.Timeout.ThreadMode;

@Timeout(value = 30, unit = TimeUnit.SECONDS, threadMode = ThreadMode.SEPARATE_THREAD)
class BoundedLineReaderTest {

    private static List<String> read(String text, int max) throws IOException {
        List<String> lines = new ArrayList<>();
        try (BoundedLineReader r = new BoundedLineReader(
                new ByteArrayInputStream(text.getBytes(StandardCharsets.UTF_8)), max)) {
            BoundedLineReader.Line l;
            while ((l = r.next()) != null) {
                lines.add(l.tooLong() ? "<too long #" + l.number() + ">" : l.text());
            }
        }
        return lines;
    }

    @Test
    void lfCrlfAndLoneCrAreLineEnds() throws IOException {
        assertThat(read("a\nb\r\nc\rd", 100)).containsExactly("a", "b", "c", "d");
    }

    @Test
    void aTrailingLineEndDoesNotAddAnEmptyLine() throws IOException {
        assertThat(read("a\nb\n", 100)).containsExactly("a", "b");
        assertThat(read("a\r\n", 100)).containsExactly("a");
        assertThat(read("", 100)).isEmpty();
    }

    @Test
    void emptyLinesAreKept() throws IOException {
        assertThat(read("a\n\nb", 100)).containsExactly("a", "", "b");
    }

    @Test
    void crlfSplitAcrossTheInternalBufferIsOneLineEnd() throws IOException {
        String first = "x".repeat(8191);
        assertThat(read(first + "\r\nnext", 100_000)).containsExactly(first, "next");
    }

    @Test
    void aLineOverTheLimitIsReportedAndSkipped() throws IOException {
        String big = "y".repeat(50);
        assertThat(read("ok\n" + big + "\nafter\n", 10)).containsExactly("ok", "<too long #2>", "after");
        assertThat(read("ok\n" + big, 10)).containsExactly("ok", "<too long #2>");
    }

    @Test
    void aLineExactlyAtTheLimitIsKept() throws IOException {
        assertThat(read("z".repeat(10) + "\nq", 10)).containsExactly("z".repeat(10), "q");
    }

    @Test
    void fiveMebibyteLineIsTooLongWithTheRealLimitAndLaterLinesAreRead() throws IOException {
        String text = "a\n" + "b".repeat(5 * 1024 * 1024) + "\nc\n";
        assertThat(read(text, BoundedLineReader.MAX_LINE_CHARS)).containsExactly("a", "<too long #2>", "c");
    }

    @Test
    void invalidUtf8IsReplacedNotAnError() throws IOException {
        byte[] bytes = {'a', (byte) 0xC3, (byte) 0x28, '\n', 'b'};
        List<String> lines = new ArrayList<>();
        try (BoundedLineReader r = new BoundedLineReader(new ByteArrayInputStream(bytes), 100)) {
            BoundedLineReader.Line l;
            while ((l = r.next()) != null) {
                lines.add(l.text());
            }
        }
        assertThat(lines).hasSize(2);
        assertThat(lines.get(1)).isEqualTo("b");
    }
}
