package io.github.cemharput.streamlab.loadgen;

import static org.assertj.core.api.Assertions.assertThat;

import io.github.cemharput.streamlab.loadgen.RecordWriter.RequestRecord;
import io.github.cemharput.streamlab.loadgen.client.Outcome;
import io.github.cemharput.streamlab.loadgen.client.StreamParams;
import io.github.cemharput.streamlab.loadgen.client.StreamResult;
import io.github.cemharput.streamlab.report.InterChunk;
import io.github.cemharput.streamlab.report.JsonlScanner;
import io.github.cemharput.streamlab.report.ResultSetReader;
import io.github.cemharput.streamlab.report.RunAssessment;
import io.github.cemharput.streamlab.report.RunMetrics;
import io.github.cemharput.streamlab.report.testsupport.ResultSetBuilder;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.Timeout.ThreadMode;
import org.junit.jupiter.api.io.TempDir;

/**
 * The analyser reads what the real load generator writes. The lines come from loadgen's own package-private
 * {@code RecordWriter} and {@code Summary}, so a change of the format breaks this test instead of a real analysis.
 * (The test lives in loadgen's package on purpose: same package, no reflection.)
 */
@Timeout(value = 60, unit = TimeUnit.SECONDS, threadMode = ThreadMode.SEPARATE_THREAD)
class ReportLoadgenFormatContractTest {

    private static final long MS = 1_000_000L;
    private static final StreamParams PARAMS = new StreamParams(10, 50, 0, "NONE", 0, 7L);

    @TempDir
    Path tmp;

    private static StreamResult result(Outcome outcome, long startMs, Long ttfbMs, Long ttfcMs, long totalMs,
            long chunks) {
        return new StreamResult(outcome, startMs * MS, (startMs + totalMs) * MS,
                outcome == Outcome.REJECTED ? Integer.valueOf(503) : Integer.valueOf(200), null, "req-" + startMs,
                ttfbMs == null ? null : ttfbMs * MS, ttfcMs == null ? null : ttfcMs * MS, ttfcMs == null ? null : ttfcMs * MS,
                totalMs * MS, chunks, chunks > 0 ? chunks - 1 : null, chunks > 1 ? 40 * MS : null,
                chunks > 1 ? 60 * MS : null, outcome == Outcome.COMPLETED ? "complete" : "none", null, null,
                outcome == Outcome.COMPLETED ? chunks : null, 0, null, null, null, null, "eof");
    }

    private static RequestRecord record(long index, StreamResult r) {
        return new RequestRecord(index, 0, PARAMS, r.startOffsetNanos(), r);
    }

    @Test
    void theRealWriterOutputIsReadWithTheExpectedNumbers() throws Exception {
        Options options = OptionsParser.parse(new String[] {"--target", "http://127.0.0.1:8080/stream",
            "--concurrency", "4", "--duration", "10s", "--ramp-up", "1s", "--output",
            tmp.resolve("unused.jsonl").toString()});
        Summary summary = new Summary();
        List<StreamResult> results = List.of(
                result(Outcome.COMPLETED, 500, 1L, 3L, 400, 10),   // ends at 900 ms: before the window
                result(Outcome.COMPLETED, 1500, 1L, 5L, 500, 10),  // ends at 2000 ms
                result(Outcome.COMPLETED, 2500, 2L, 7L, 500, 10),  // ends at 3000 ms
                result(Outcome.COMPLETED, 3500, 2L, 9L, 700, 10),  // ends at 4200 ms
                result(Outcome.REJECTED, 4000, 1L, null, 1, 0),
                result(Outcome.REJECTED, 4100, 1L, null, 1, 0),
                result(Outcome.TIMEOUT, 5000, null, null, 900, 3));
        String header = RecordWriter.headerLine(options, Instant.parse("2026-01-01T00:00:00Z"), "0.1.0-SNAPSHOT",
                "loadgen --fixture");
        StringBuilder jsonl = new StringBuilder(header).append('\n');
        long index = 0;
        for (StreamResult r : results) {
            summary.record(r);
            jsonl.append(RecordWriter.requestLine(record(index++, r))).append('\n');
        }
        long[] gaps = {40 * MS, 41 * MS, 42 * MS, 43 * MS, 44 * MS, 45 * MS, 46 * MS, 47 * MS, 48 * MS, 49 * MS};
        Arrays.stream(gaps).forEach(summary::recordInterChunk);
        jsonl.append(RecordWriter.footerLine(Instant.parse("2026-01-01T00:00:10Z"), 10_020 * MS, "duration",
                results.size(), results.size(), 0, summary)).append('\n');
        String summaryJson = summary.toJson(new Summary.RunFacts("http://127.0.0.1:8080/stream", 4, 10_020 * MS,
                "duration", results.size(), results.size())) + "\n";

        // a result set whose measured files are the real ones
        ResultSetBuilder b = ResultSetBuilder.at(tmp.resolve("set")).concurrency(4).repetitions(1).plan();
        b.run(1).writeJsonl = false;
        b.run(1).writeSummary = false;
        // the manifest and the snapshots of the fixture must describe the same seven requests
        b.run(1).requests(List.of(ResultSetBuilder.Req.completed(500, 1, 3, 400),
                ResultSetBuilder.Req.completed(1500, 1, 5, 500), ResultSetBuilder.Req.completed(2500, 2, 7, 500),
                ResultSetBuilder.Req.completed(3500, 2, 9, 700), ResultSetBuilder.Req.rejected(4000),
                ResultSetBuilder.Req.rejected(4100), ResultSetBuilder.Req.of("TIMEOUT", 5000, 900)));
        Path set = b.build();
        Path run = set.resolve("runs").resolve("001-c4-shared-r1");
        Files.writeString(run.resolve("measured.jsonl"), jsonl.toString(), StandardCharsets.UTF_8);
        Files.writeString(run.resolve("measured-summary.json"), summaryJson, StandardCharsets.UTF_8);

        // the scanner on the file itself
        JsonlScanner.Result scanned = JsonlScanner.scan(run.resolve("measured.jsonl"));
        assertThat(scanned.schemaUnsupported()).isFalse();
        assertThat(scanned.malformedLines()).isZero();
        assertThat(scanned.structureProblems()).isEmpty();
        assertThat(scanned.requestLines()).isEqualTo(7);
        assertThat(scanned.outcomes()).containsEntry("COMPLETED", 4L).containsEntry("REJECTED", 2L)
                .containsEntry("TIMEOUT", 1L);
        assertThat(scanned.windowStartMs()).isEqualTo(1000);
        assertThat(scanned.windowEndMs()).isEqualTo(10_000);
        assertThat(scanned.completedInWindow()).isEqualTo(3);
        // ttfc of the four COMPLETED requests: 3, 5, 7, 9 ms -> p50 is the 2nd value
        assertThat(scanned.ttfc().count()).isEqualTo(4);
        assertThat(scanned.ttfc().p50()).isEqualTo(5 * MS);
        assertThat(scanned.ttfc().max()).isEqualTo(9 * MS);
        assertThat(scanned.total().p99()).isEqualTo(700 * MS);

        // the whole analysis: every cross-check against the real summary and footer passes
        ResultSetReader.ResultSet read = ResultSetReader.read(set);
        RunAssessment.Analysis analysis = RunAssessment.analyse(read);
        RunMetrics metrics = analysis.runs().get(0).metrics();
        assertThat(metrics.findings()).isEmpty();
        assertThat(read.runs().get(0).findings()).isEmpty();
        assertThat(metrics.outcomes()).containsEntry("COMPLETED", 4L);
        assertThat(metrics.interChunk()).isNotNull();
        assertThat(metrics.interChunk().count()).isEqualTo(10);
        assertThat(metrics.interChunk().p50()).isBetween(44 * MS - 100_000, 44 * MS + 100_000);
        assertThat(read.runs().get(0).summary().outcomes()).containsEntry("COMPLETED", 4L)
                .containsEntry("REJECTED", 2L);
        InterChunk.Result decoded = InterChunk.decode(scanned.footer());
        assertThat(decoded.count()).isEqualTo(10);
    }

    @Test
    void theAnalysersOutcomeListEqualsLoadgensEnumInOrder() {
        assertThat(RunMetrics.KNOWN_OUTCOMES).isEqualTo(Arrays.stream(Outcome.values()).map(Enum::name).toList());
    }

    @Test
    void theHeaderCarriesTheFieldsTheAnalyserReads() throws Exception {
        Options options = OptionsParser.parse(new String[] {"--target", "http://127.0.0.1:8080/stream",
            "--concurrency", "4", "--duration", "10s", "--ramp-up", "1s", "--output",
            tmp.resolve("unused.jsonl").toString()});
        var header = io.github.cemharput.streamlab.bench.json.Json.parseObject(RecordWriter.headerLine(options,
                Instant.parse("2026-01-01T00:00:00Z"), "x", "cmd"));
        assertThat(header).containsEntry("type", "header").containsEntry("schemaVersion", 1L)
                .containsEntry("concurrency", 4L).containsEntry("durationMs", 10_000L)
                .containsEntry("rampUpMs", 1_000L).containsEntry("timeUnit", "ns");
    }

    @Test
    void theFooterAndSummaryCarryTheFieldsTheAnalyserReads() {
        Summary summary = new Summary();
        summary.recordInterChunk(5 * MS);
        var footer = io.github.cemharput.streamlab.bench.json.Json.parseObject(RecordWriter.footerLine(
                Instant.parse("2026-01-01T00:00:00Z"), 123L, "duration", 3, 3, 0, summary));
        assertThat(footer).containsKeys("type", "elapsedNanos", "stopReason", "requestsStarted", "recordsWritten",
                "interrupted", "interChunkCount", "interChunkHistogramUnit", "interChunkHistogram")
                .containsEntry("interChunkHistogramUnit", "ns").containsEntry("interChunkCount", 1L);
        var s = io.github.cemharput.streamlab.bench.json.Json.parseObject(summary.toJson(
                new Summary.RunFacts("t", 1, 1L, "duration", 0, 0)));
        assertThat(s).containsEntry("type", "summary").containsEntry("schemaVersion", 1L);
        for (Outcome o : Outcome.values()) {
            assertThat(s).containsKey("outcome_" + o);
        }
    }
}
