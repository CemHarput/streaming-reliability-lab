package io.github.cemharput.streamlab.loadgen;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.github.cemharput.streamlab.loadgen.Options.IntRange;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.Timeout.ThreadMode;
import org.junit.jupiter.api.io.TempDir;

@Timeout(value = 60, unit = TimeUnit.SECONDS, threadMode = ThreadMode.SEPARATE_THREAD)
class OptionsParserTest {

    @TempDir
    Path dir;

    private String[] args(String... extra) {
        List<String> all = new ArrayList<>(List.of("--target", "http://localhost:8080/stream", "--requests", "5",
                "--output", dir.resolve("out.jsonl").toString()));
        all.addAll(Arrays.asList(extra));
        return all.toArray(String[]::new);
    }

    private void assertUsage(String message, String... args) {
        assertThatThrownBy(() -> OptionsParser.parse(args)).isInstanceOf(UsageException.class)
                .hasMessageContaining(message).satisfies(ex -> assertThat(ex.getMessage()).doesNotContain("\n"));
    }

    @Test
    void defaultsWithMinimalArguments() throws Exception {
        Options o = OptionsParser.parse(args());

        assertThat(o.help()).isFalse();
        assertThat(o.target().toString()).isEqualTo("http://localhost:8080/stream");
        assertThat(o.concurrency()).isEqualTo(1);
        assertThat(o.duration()).isNull();
        assertThat(o.requests()).isEqualTo(5L);
        assertThat(o.rampUp()).isEqualTo(Duration.ZERO);
        assertThat(o.chunks()).isEqualTo(IntRange.of(10));
        assertThat(o.intervalMs()).isEqualTo(IntRange.of(50));
        assertThat(o.firstDelayMs()).isEqualTo(IntRange.of(0));
        assertThat(o.fault()).isEqualTo("NONE");
        assertThat(o.faultAtSeq()).isEqualTo(IntRange.of(0));
        assertThat(o.faultRate()).isEqualTo(1.0);
        assertThat(o.seed()).isZero();
        assertThat(o.summaryOutput()).isNull();
        assertThat(o.overwrite()).isFalse();
        assertThat(o.connectTimeout()).isEqualTo(Duration.ofSeconds(5));
        assertThat(o.idleTimeout()).isEqualTo(Duration.ofSeconds(90));
        assertThat(o.drainTimeout()).isEqualTo(Duration.ofSeconds(30));
        assertThat(o.rejectPause()).isEqualTo(Duration.ofMillis(100));
    }

    @Test
    void allOptionsInSpaceAndEqualsForm() throws Exception {
        Path summary = dir.resolve("s.json");
        Options o = OptionsParser.parse(new String[] {"--target=https://h:9/stream", "--concurrency=10",
            "--duration=30s", "--ramp-up", "5s", "--chunks=5-20", "--interval-ms", "10-50", "--first-delay-ms=3",
            "--fault", "UNAVAILABLE", "--fault-at-seq=2-4", "--fault-rate=0.5", "--seed=-9",
            "--output", dir.resolve("o.jsonl").toString(), "--summary-output=" + summary, "--overwrite",
            "--connect-timeout=2s", "--idle-timeout=1m", "--drain-timeout=0ms", "--reject-pause=7ms"});

        assertThat(o.target().toString()).isEqualTo("https://h:9/stream");
        assertThat(o.concurrency()).isEqualTo(10);
        assertThat(o.duration()).isEqualTo(Duration.ofSeconds(30));
        assertThat(o.requests()).isNull();
        assertThat(o.rampUp()).isEqualTo(Duration.ofSeconds(5));
        assertThat(o.chunks()).isEqualTo(new IntRange(5, 20));
        assertThat(o.intervalMs()).isEqualTo(new IntRange(10, 50));
        assertThat(o.firstDelayMs()).isEqualTo(IntRange.of(3));
        assertThat(o.fault()).isEqualTo("UNAVAILABLE");
        assertThat(o.faultAtSeq()).isEqualTo(new IntRange(2, 4));
        assertThat(o.faultRate()).isEqualTo(0.5);
        assertThat(o.seed()).isEqualTo(-9);
        assertThat(o.summaryOutput()).isEqualTo(summary);
        assertThat(o.overwrite()).isTrue();
        assertThat(o.connectTimeout()).isEqualTo(Duration.ofSeconds(2));
        assertThat(o.idleTimeout()).isEqualTo(Duration.ofMinutes(1));
        assertThat(o.drainTimeout()).isZero();
        assertThat(o.rejectPause()).isEqualTo(Duration.ofMillis(7));
    }

    @Test
    void helpWinsOverEverythingElse() throws Exception {
        assertThat(OptionsParser.parse(new String[] {"--help"}).help()).isTrue();
        assertThat(OptionsParser.parse(new String[] {"-h"}).help()).isTrue();
        assertThat(OptionsParser.parse(new String[] {"--concurrency", "x", "--help"}).help()).isTrue();
        assertThat(OptionsParser.USAGE).contains("--target").contains("--duration").contains("--reject-pause");
    }

    @Test
    void missingRequiredOptions() {
        assertUsage("missing required option --target", "--requests", "5", "--output", dir.resolve("x").toString());
        assertUsage("missing required option --output", "--target", "http://h/stream", "--requests", "5");
        assertUsage("one of --duration or --requests", "--target", "http://h/stream", "--output",
                dir.resolve("x").toString());
        assertUsage("cannot be used together", args("--duration", "5s"));
    }

    @Test
    void malformedArguments() {
        assertUsage("unknown option --bogus", args("--bogus", "1"));
        assertUsage("unexpected argument: positional", args("positional"));
        assertUsage("unexpected argument: -x", args("-x"));
        assertUsage("missing value for --seed", args("--seed"));
        assertUsage("missing value for --seed", args("--seed", "--overwrite"));
        assertUsage("duplicate option --seed", args("--seed", "1", "--seed=2"));
        assertUsage("duplicate option --overwrite", args("--overwrite", "--overwrite"));
        assertUsage("takes no value", args("--overwrite=true"));
    }

    @Test
    void rejectsInvalidTargets() {
        for (String bad : List.of("ftp://h/stream", "localhost:8080/stream", "http:///stream", "http://h/stream?x=1",
                "http://h/stream#f", "http://h h/stream", "/stream")) {
            assertThatThrownBy(() -> OptionsParser.parse(new String[] {"--target", bad, "--requests", "1",
                "--output", dir.resolve("t.jsonl").toString()})).as(bad).isInstanceOf(UsageException.class);
        }
    }

    @Test
    void numericBounds() {
        assertUsage("--concurrency", args("--concurrency", "0"));
        assertUsage("--concurrency", args("--concurrency", "10001"));
        assertUsage("--concurrency", args("--concurrency", "abc"));
        assertUsage("--requests", args("--requests", "0"));
        assertUsage("--requests", new String[] {"--target", "http://h/s", "--requests", "100000001", "--output",
            dir.resolve("n").toString()});
        assertUsage("--chunks", args("--chunks", "0"));
        assertUsage("--chunks", args("--chunks", "1000001"));
        assertUsage("reversed", args("--chunks", "9-3"));
        assertUsage("--chunks", args("--chunks", "a-b"));
        assertUsage("--chunks", args("--chunks", "-3"));
        assertUsage("--interval-ms", args("--interval-ms", "60001"));
        assertUsage("--first-delay-ms", args("--first-delay-ms", "5-60001"));
        assertUsage("--fault-at-seq", args("--fault-at-seq", "x"));
        assertUsage("--fault-rate", args("--fault-rate", "1.5"));
        assertUsage("--fault-rate", args("--fault-rate", "-0.1"));
        assertUsage("--fault-rate", args("--fault-rate", "NaN"));
        assertUsage("--seed", args("--seed", "1.5"));
        assertUsage("--fault", args("--fault", "FOO"));
        assertUsage("--fault", args("--fault", "none"));
    }

    @Test
    void durationsAndRamp() {
        assertUsage("--ramp-up", args("--ramp-up", "5"));
        assertUsage("--ramp-up", args("--ramp-up", "-1s"));
        assertUsage("--connect-timeout must be greater than 0", args("--connect-timeout", "0s"));
        assertUsage("--idle-timeout must be greater than 0", args("--idle-timeout", "0ms"));
        assertUsage("must not exceed 7d", args("--drain-timeout", "999999999m"));
        assertUsage("--duration must be greater than 0", new String[] {"--target", "http://h/s", "--duration", "0s",
            "--output", dir.resolve("d").toString()});
        assertUsage("shorter than --duration", new String[] {"--target", "http://h/s", "--duration", "10s",
            "--ramp-up", "10s", "--output", dir.resolve("d").toString()});
    }

    @Test
    void rampUpIsOnlyBoundedByDurationNotByRequests() throws Exception {
        assertThat(OptionsParser.parse(args("--ramp-up", "2m")).rampUp()).isEqualTo(Duration.ofMinutes(2));
    }

    @Test
    void outputPathRules() throws Exception {
        Path existing = Files.writeString(dir.resolve("exists.jsonl"), "x");
        assertUsage("file exists", "--target", "http://h/s", "--requests", "1", "--output", existing.toString());
        assertThat(OptionsParser.parse(new String[] {"--target", "http://h/s", "--requests", "1", "--output",
            existing.toString(), "--overwrite"}).output()).isEqualTo(existing);
        assertUsage("directory does not exist", "--target", "http://h/s", "--requests", "1", "--output",
                dir.resolve("missing").resolve("o.jsonl").toString());
        assertUsage("is a directory", "--target", "http://h/s", "--requests", "1", "--output", dir.toString());
        assertUsage("--summary-output: file exists", args("--summary-output", existing.toString()));
        assertUsage("must be different", args("--summary-output", dir.resolve("out.jsonl").toString()));
        try (var files = Files.list(dir)) {
            assertThat(files.count()).as("parsing creates no files").isEqualTo(1);
        }
    }
}
