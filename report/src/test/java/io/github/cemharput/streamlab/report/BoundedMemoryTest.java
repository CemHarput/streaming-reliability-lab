package io.github.cemharput.streamlab.report;

import static org.assertj.core.api.Assertions.assertThat;

import io.github.cemharput.streamlab.bench.json.Json;
import io.github.cemharput.streamlab.report.testsupport.ResultSetBuilder;
import io.github.cemharput.streamlab.report.testsupport.ResultSetBuilder.Req;
import java.io.BufferedWriter;
import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.Timeout.ThreadMode;
import org.junit.jupiter.api.io.TempDir;

/**
 * Reads about 96 MB of JSONL with {@code --check} in a child JVM that has a 32 MB heap. The child is the tool itself
 * (the only process this module's tests start). The wait is bounded by {@code Process.waitFor(timeout)}.
 */
@Timeout(value = 170, unit = TimeUnit.SECONDS, threadMode = ThreadMode.SEPARATE_THREAD)
class BoundedMemoryTest {

    private static final long TARGET_BYTES = 96L * 1024 * 1024;

    @TempDir
    Path tmp;

    /** header line, then padded REJECTED lines until the target size, plus a few COMPLETED lines, then the footer. */
    private static long writeBigJsonl(Path file, String header, String footer) throws IOException {
        String padding = "p".repeat(900);
        long lines = 0;
        long bytes = 0;
        try (BufferedWriter w = Files.newBufferedWriter(file, StandardCharsets.UTF_8)) {
            w.write(header);
            w.write('\n');
            bytes += header.length() + 1;
            long i = 0;
            while (bytes < TARGET_BYTES) {
                Req q = i % 1000 == 0 ? Req.completed(1500 + i % 8000, 1, 2, 3) : Req.rejected(1000 + i % 8000);
                Map<String, Object> m = ResultSetBuilder.requestMap(i, q);
                m.put("errorDetail", padding);
                String line = Json.writeCompact(m);
                w.write(line);
                w.write('\n');
                bytes += line.length() + 1;
                i++;
            }
            lines = i;
            w.write(footer);
            w.write('\n');
        }
        return lines;
    }

    private static String classpathEntry(Class<?> c) throws Exception {
        return new File(c.getProtectionDomain().getCodeSource().getLocation().toURI()).getPath();
    }

    @Test
    void aChildJvmWithA32MegabyteHeapReadsAbout96MegabytesOfJsonl() throws Exception {
        ResultSetBuilder b = ResultSetBuilder.at(tmp.resolve("set")).concurrency(4).repetitions(1).plan();
        Path set = b.build();
        Path jsonl = set.resolve("runs").resolve("001-c4-shared-r1").resolve("measured.jsonl");
        List<String> original = Files.readAllLines(jsonl);
        long lines = writeBigJsonl(jsonl, original.get(0), original.get(original.size() - 1));
        assertThat(Files.size(jsonl)).isGreaterThanOrEqualTo(TARGET_BYTES);
        assertThat(lines).isGreaterThan(50_000);

        Set<String> cp = new LinkedHashSet<>();
        cp.add(classpathEntry(ReportMain.class));
        cp.add(classpathEntry(Json.class));
        cp.add(classpathEntry(org.HdrHistogram.Histogram.class));
        Path output = tmp.resolve("child-output.txt");
        String java = Path.of(System.getProperty("java.home"), "bin", "java").toString();
        List<String> command = new ArrayList<>(List.of(java, "-Xmx32m", "-cp", String.join(File.pathSeparator, cp),
                ReportMain.class.getName(), "--results", set.toString(), "--check"));
        Process p = new ProcessBuilder(command).redirectErrorStream(true).redirectOutput(output.toFile()).start();
        try {
            boolean done = p.waitFor(150, TimeUnit.SECONDS);
            assertThat(done).as("child finished in time").isTrue();
        } finally {
            if (p.isAlive()) {
                p.destroyForcibly();
                p.waitFor(10, TimeUnit.SECONDS);
            }
        }
        String text = Files.readString(output);
        assertThat(p.exitValue()).as(text).isZero();
        String[] outLines = text.stripTrailing().split("\\R");
        Map<String, Object> summary = Json.parseObject(outLines[outLines.length - 1]);
        assertThat(summary).containsEntry("type", "report-summary").containsEntry("runs", 2L);
        assertThat(text).contains("check only: nothing written");
        // the footer of the fixture says 10 records: the count check can only fire when every line was scanned
        assertThat(text).contains("excluded 001-c4-shared-r1: CHECK:CHECK_COUNT_MISMATCH");
    }

    @Test
    void aLineOverOneMebibyteIsMalformedAndSkippedWithoutDroppingTheRest() throws Exception {
        ResultSetBuilder b = ResultSetBuilder.at(tmp.resolve("set")).concurrency(4).repetitions(1).plan();
        Path set = b.build();
        Path jsonl = set.resolve("runs").resolve("001-c4-shared-r1").resolve("measured.jsonl");
        List<String> original = new ArrayList<>(Files.readAllLines(jsonl));
        Map<String, Object> huge = ResultSetBuilder.requestMap(99, Req.rejected(1000));
        huge.put("errorDetail", "h".repeat(5 * 1024 * 1024));
        original.add(original.size() - 1, Json.writeCompact(huge));
        Files.write(jsonl, original);
        ResultSetReader.ResultSet read = ResultSetReader.read(set);
        JsonlScanner.Result r = read.runs().get(0).measured();
        assertThat(r.malformedLines()).isEqualTo(1);
        assertThat(r.firstMalformedLines()).containsExactly((long) original.size() - 1);
        assertThat(r.requestLines()).isEqualTo(10);
        assertThat(read.runs().get(0).findings()).extracting(Checks.Finding::code)
                .containsExactly(Checks.JSONL_MALFORMED_LINE);
    }
}
