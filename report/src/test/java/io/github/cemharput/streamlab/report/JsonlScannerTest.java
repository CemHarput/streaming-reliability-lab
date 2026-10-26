package io.github.cemharput.streamlab.report;

import static org.assertj.core.api.Assertions.assertThat;

import io.github.cemharput.streamlab.bench.json.Json;
import io.github.cemharput.streamlab.report.testsupport.ResultSetBuilder;
import io.github.cemharput.streamlab.report.testsupport.ResultSetBuilder.Req;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.Timeout.ThreadMode;

@Timeout(value = 30, unit = TimeUnit.SECONDS, threadMode = ThreadMode.SEPARATE_THREAD)
class JsonlScannerTest {

    private static final long MS = ResultSetBuilder.MS;

    private static String header(long rampUpMs, long durationMs) {
        Map<String, Object> h = new LinkedHashMap<>();
        h.put("type", "header");
        h.put("schemaVersion", 1L);
        h.put("concurrency", 4L);
        h.put("durationMs", durationMs);
        h.put("rampUpMs", rampUpMs);
        return Json.writeCompact(h);
    }

    private static String footer() {
        return "{\"type\":\"footer\",\"recordsWritten\":0,\"elapsedNanos\":1}";
    }

    private static String line(Req r, long i) {
        return Json.writeCompact(ResultSetBuilder.requestMap(i, r));
    }

    private static JsonlScanner.Result scan(String... lines) throws IOException {
        String text = String.join("\n", lines) + "\n";
        try (BoundedLineReader r = new BoundedLineReader(
                new ByteArrayInputStream(text.getBytes(StandardCharsets.UTF_8)), 1_000_000)) {
            return JsonlScanner.scan(r);
        }
    }

    /** A completed request that ends (start + total) at exactly {@code endNanos}. */
    private static Req endingAt(long endNanos) {
        return new Req("COMPLETED", 0, 0, endNanos, 1 * MS, 2 * MS);
    }

    @Test
    void windowBoundsAreInclusiveAtTheStartAndExclusiveAtTheEnd() throws IOException {
        // window [1000 ms, 10000 ms)
        JsonlScanner.Result r = scan(header(1000, 10_000),
                line(endingAt(1000 * MS - 1), 0), // just before: out
                line(endingAt(1000 * MS), 1), // exactly at the start: in
                line(endingAt(5000 * MS), 2), // in
                line(endingAt(10_000 * MS - 1), 3), // in
                line(endingAt(10_000 * MS), 4), // exactly at the end: out
                line(endingAt(12_000 * MS), 5), // out
                footer());
        assertThat(r.completed()).isEqualTo(6);
        assertThat(r.completedInWindow()).isEqualTo(3);
        assertThat(r.windowStartMs()).isEqualTo(1000);
        assertThat(r.windowEndMs()).isEqualTo(10_000);
    }

    @Test
    void onlyCompletedRecordsCountInTheWindow() throws IOException {
        JsonlScanner.Result r = scan(header(0, 10_000),
                line(Req.of("REJECTED", 100, 1), 0), line(Req.of("TIMEOUT", 100, 50), 1),
                line(Req.completed(100, 1, 2, 50), 2), footer());
        assertThat(r.completedInWindow()).isEqualTo(1);
        assertThat(r.outcomes()).containsEntry("REJECTED", 1L).containsEntry("TIMEOUT", 1L).containsEntry("COMPLETED", 1L);
        assertThat(r.requestLines()).isEqualTo(3);
    }

    @Test
    void noWindowWhenDurationDoesNotExceedRampUp() throws IOException {
        assertThat(scan(header(5000, 5000), line(Req.completed(1, 1, 1, 1), 0), footer()).completedInWindow()).isNull();
        assertThat(scan(header(6000, 5000), footer()).completedInWindow()).isNull();
    }

    @Test
    void nullTtfbAndTtfcAreMissingNotZero() throws IOException {
        Req noTtfc = new Req("COMPLETED", 0, 0, 5 * MS, 1 * MS, null);
        Req noTtfb = new Req("COMPLETED", 0, 0, 7 * MS, null, 3 * MS);
        JsonlScanner.Result r = scan(header(0, 10_000), line(noTtfc, 0), line(noTtfb, 1),
                line(Req.completed(0, 2, 4, 9), 2), footer());
        assertThat(r.ttfc().count()).isEqualTo(2);
        assertThat(r.ttfc().missing()).isEqualTo(1);
        assertThat(r.ttfc().p50()).isEqualTo(3 * MS);
        assertThat(r.ttfb().count()).isEqualTo(2);
        assertThat(r.ttfb().missing()).isEqualTo(1);
        assertThat(r.total().count()).isEqualTo(3);
        assertThat(r.total().p50()).isEqualTo(7 * MS);
        assertThat(r.total().max()).isEqualTo(9 * MS);
    }

    @Test
    void malformedLinesAreCountedWithTheirNumbersAndSkipped() throws IOException {
        JsonlScanner.Result r = scan(header(0, 10_000),
                "not json at all", // 2
                line(Req.completed(0, 1, 2, 3), 0), // 3
                "{\"type\":\"request\",\"outcome\":\"COMPLETED\"}", // 4: required fields missing
                "{\"type\":\"mystery\"}", // 5: unknown type
                "{\"type\":\"request\",", // 6: cut off
                footer());
        assertThat(r.malformedLines()).isEqualTo(4);
        assertThat(r.firstMalformedLines()).containsExactly(2L, 4L, 5L, 6L);
        assertThat(r.requestLines()).isEqualTo(1);
        assertThat(r.structureProblems()).isEmpty();
    }

    @Test
    void onlyTheFirstFiveMalformedLineNumbersAreKept() throws IOException {
        List<String> lines = new ArrayList<>();
        lines.add(header(0, 10_000));
        for (int i = 0; i < 9; i++) {
            lines.add("garbage " + i);
        }
        lines.add(footer());
        JsonlScanner.Result r = scan(lines.toArray(new String[0]));
        assertThat(r.malformedLines()).isEqualTo(9);
        assertThat(r.firstMalformedLines()).containsExactly(2L, 3L, 4L, 5L, 6L);
    }

    @Test
    void structureProblemsAreReported() throws IOException {
        assertThat(scan(line(Req.completed(0, 1, 2, 3), 0), footer()).structureProblems()).contains("header missing");
        assertThat(scan(header(0, 10_000), line(Req.completed(0, 1, 2, 3), 0)).structureProblems())
                .containsExactly("footer missing");
        assertThat(scan(header(0, 10_000), footer(), footer()).structureProblems()).containsExactly("more than one footer");
        assertThat(scan(header(0, 10_000), footer(), line(Req.completed(0, 1, 2, 3), 0)).structureProblems())
                .contains("request line after the footer");
        assertThat(scan(header(0, 10_000), line(Req.completed(0, 1, 2, 3), 0), header(0, 10_000), footer())
                .structureProblems()).containsExactly("more than one header");
    }

    @Test
    void anUnsupportedHeaderSchemaStopsTheScan() throws IOException {
        String h = "{\"type\":\"header\",\"schemaVersion\":2}";
        JsonlScanner.Result r = scan(h, line(Req.completed(0, 1, 2, 3), 0), footer());
        assertThat(r.schemaUnsupported()).isTrue();
        assertThat(r.requestLines()).isZero();
        assertThat(r.structureProblems()).isEmpty();
    }

    @Test
    void generatorDelayIsTheStartMinusTheScheduledOffsetNeverNegative() throws IOException {
        Req late = new Req("COMPLETED", 100 * MS, 103 * MS, 5 * MS, 1 * MS, 2 * MS);
        Req early = new Req("COMPLETED", 200 * MS, 199 * MS, 5 * MS, 1 * MS, 2 * MS);
        JsonlScanner.Result r = scan(header(0, 10_000), line(late, 0), line(early, 1), footer());
        assertThat(r.generatorDelayCount()).isEqualTo(2);
        assertThat(r.generatorDelayMax()).isBetween(3 * MS, 3 * MS + 3 * MS / 1000);
    }

    @Test
    void blankLinesAreIgnoredAndCrlfIsAccepted() throws IOException {
        String text = header(0, 10_000) + "\r\n\r\n" + line(Req.completed(0, 1, 2, 3), 0) + "\r\n" + footer() + "\r\n";
        try (BoundedLineReader reader = new BoundedLineReader(
                new ByteArrayInputStream(text.getBytes(StandardCharsets.UTF_8)), 1_000_000)) {
            JsonlScanner.Result r = JsonlScanner.scan(reader);
            assertThat(r.requestLines()).isEqualTo(1);
            assertThat(r.malformedLines()).isZero();
        }
    }
}
