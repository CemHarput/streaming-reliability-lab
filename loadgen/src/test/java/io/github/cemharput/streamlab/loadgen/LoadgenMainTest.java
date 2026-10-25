package io.github.cemharput.streamlab.loadgen;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assumptions.assumeThat;
import static org.awaitility.Awaitility.await;

import io.github.cemharput.streamlab.loadgen.json.FlatJson;
import io.github.cemharput.streamlab.loadgen.testsupport.SseTestServer;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.net.ServerSocket;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.Timeout.ThreadMode;
import org.junit.jupiter.api.io.TempDir;

@Timeout(value = 90, unit = TimeUnit.SECONDS, threadMode = ThreadMode.SEPARATE_THREAD)
class LoadgenMainTest {

    @TempDir
    Path dir;

    private SseTestServer server;
    private final ByteArrayOutputStream outBytes = new ByteArrayOutputStream();
    private final ByteArrayOutputStream errBytes = new ByteArrayOutputStream();
    private PrintStream out;
    private PrintStream err;

    @BeforeEach
    void setUp() throws IOException {
        server = new SseTestServer().handler((ex, n) -> SseTestServer.fullStream(ex, 4));
        out = new PrintStream(outBytes, true, StandardCharsets.UTF_8);
        err = new PrintStream(errBytes, true, StandardCharsets.UTF_8);
    }

    @AfterEach
    void tearDown() {
        server.close();
    }

    private int run(String... args) {
        return LoadgenMain.run(args, out, err);
    }

    private String stdout() {
        return outBytes.toString(StandardCharsets.UTF_8);
    }

    private String stderr() {
        return errBytes.toString(StandardCharsets.UTF_8);
    }

    private long fileCount() throws IOException {
        try (Stream<Path> files = Files.list(dir)) {
            return files.count();
        }
    }

    @Test
    void endToEndRunWritesJsonlAndSummary() throws Exception {
        Path jsonl = dir.resolve("run.jsonl");
        Path summary = dir.resolve("summary.json");

        int exit = run("--target", server.uri().toString(), "--concurrency", "3", "--requests", "12", "--chunks", "4",
                "--interval-ms", "0", "--seed", "9", "--output", jsonl.toString(), "--summary-output",
                summary.toString());

        assertThat(exit).isZero();
        assertThat(stderr()).isEmpty();
        List<String> lines = Files.readAllLines(jsonl);
        assertThat(lines).hasSize(14);
        assertThat(FlatJson.parse(lines.get(0))).containsEntry("type", "header").containsEntry("seed", 9L)
                .containsEntry("target", server.uri().toString());
        assertThat(FlatJson.parse(lines.get(13))).containsEntry("type", "footer").containsEntry("recordsWritten", 12L);
        for (String line : lines.subList(1, 13)) {
            assertThat(FlatJson.parse(line)).containsEntry("type", "request").containsEntry("outcome", "COMPLETED");
        }
        Map<String, Object> s = FlatJson.parse(Files.readString(summary).trim());
        assertThat(s).containsEntry("type", "summary").containsEntry("outcome_COMPLETED", 12L)
                .containsEntry("outcome_REJECTED", 0L).containsEntry("recordsWritten", 12L)
                .containsEntry("ttfcCount", 12L).containsEntry("totalCount", 12L)
                .containsEntry("interChunkCount", 36L);
        assertThat((String) s.get("label")).contains("CONVENIENCE SUMMARY").contains("not a benchmark");
        assertThat(stdout()).contains("CONVENIENCE SUMMARY - not a benchmark result").contains("COMPLETED=12")
                .contains("coordinated omission is NOT corrected");
    }

    @Test
    void unreachableTargetStillCountsAsACompletedRun() throws Exception {
        int port;
        try (ServerSocket s = new ServerSocket(0)) {
            port = s.getLocalPort();
        }
        Path jsonl = dir.resolve("run.jsonl");

        int exit = run("--target", "http://127.0.0.1:" + port + "/stream", "--requests", "3", "--output",
                jsonl.toString());

        assertThat(exit).isZero();
        assertThat(stdout()).contains("CONNECT_FAILURE=3");
    }

    @Test
    void invalidArgumentsExitWithTwoAndCreateNoFile() throws Exception {
        int exit = run("--target", server.uri().toString(), "--requests", "5", "--duration", "5s", "--output",
                dir.resolve("run.jsonl").toString());

        assertThat(exit).isEqualTo(2);
        assertThat(stderr()).contains("cannot be used together").contains("--help");
        assertThat(stderr().strip().lines().count()).isEqualTo(2);
        assertThat(stdout()).isEmpty();
        assertThat(fileCount()).isZero();
        assertThat(server.requestCount()).isZero();
    }

    @Test
    void existingOutputFileIsAnErrorWithoutOverwrite() throws Exception {
        Path existing = Files.writeString(dir.resolve("run.jsonl"), "keep me");

        int exit = run("--target", server.uri().toString(), "--requests", "1", "--output", existing.toString());

        assertThat(exit).isEqualTo(2);
        assertThat(Files.readString(existing)).isEqualTo("keep me");
        assertThat(server.requestCount()).isZero();
    }

    @Test
    void overwriteReplacesTheFile() throws Exception {
        Path existing = Files.writeString(dir.resolve("run.jsonl"), "old");

        int exit = run("--target", server.uri().toString(), "--requests", "1", "--chunks", "4", "--output",
                existing.toString(), "--overwrite");

        assertThat(exit).isZero();
        assertThat(Files.readAllLines(existing)).hasSize(3);
    }

    @Test
    void helpExitsWithZeroAndPrintsUsage() {
        int exit = run("--help");

        assertThat(exit).isZero();
        assertThat(stdout()).contains("Usage:").contains("--reject-pause");
        assertThat(stderr()).isEmpty();
    }

    @Test
    void unwritableOutputIsAnIoErrorWithExitOne() throws Exception {
        Path readOnly = Files.writeString(dir.resolve("run.jsonl"), "x");
        assertThat(readOnly.toFile().setReadOnly()).isTrue();
        assumeThat(Files.isWritable(readOnly)).as("file system honours read-only (not running as root)").isFalse();
        try {
            int exit = run("--target", server.uri().toString(), "--requests", "1", "--output", readOnly.toString(),
                    "--overwrite");

            assertThat(exit).isEqualTo(1);
            assertThat(stderr()).contains("I/O error");
            assertThat(server.requestCount()).isZero();
        } finally {
            readOnly.toFile().setWritable(true);
        }
    }

    @Test
    void shutdownHookStopsTheRunAndKeepsTheJvmAliveUntilTheOutputIsWritten(@TempDir Path dir) throws Exception {
        Options options = OptionsParser.parse(new String[] {"--target", "http://127.0.0.1:1/stream", "--requests", "1",
            "--output", dir.resolve("o.jsonl").toString()});
        LoadRun run = new LoadRun(options, "test");
        CountDownLatch outputDone = new CountDownLatch(1);
        Thread hook = LoadgenMain.shutdownHook(run, outputDone, 60);

        hook.start();
        try {
            await().atMost(10, TimeUnit.SECONDS).until(run::isStopping);
            // The run itself is not running here, so "finished" alone must not release the hook.
            assertThat(hook.isAlive()).as("hook waits for the output").isTrue();
            outputDone.countDown();
            hook.join(10_000);
            assertThat(hook.isAlive()).isFalse();
        } finally {
            outputDone.countDown();
            hook.interrupt();
        }
    }
}
