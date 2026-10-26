package io.github.cemharput.streamlab.report;

import static org.assertj.core.api.Assertions.assertThat;

import io.github.cemharput.streamlab.report.RunAssessment.Analysis;
import io.github.cemharput.streamlab.report.testsupport.Analyses;
import io.github.cemharput.streamlab.report.testsupport.Cli;
import io.github.cemharput.streamlab.report.testsupport.Cli.Result;
import io.github.cemharput.streamlab.report.testsupport.ResultSetBuilder;
import io.github.cemharput.streamlab.report.testsupport.Scans;
import io.github.cemharput.streamlab.report.testsupport.Scenarios;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.Timeout.ThreadMode;
import org.junit.jupiter.api.io.TempDir;

/** A result set from a Linux machine read on Windows: same metrics, different environment block. */
@Timeout(value = 60, unit = TimeUnit.SECONDS, threadMode = ThreadMode.SEPARATE_THREAD)
class LinuxFlavourTest {

    @TempDir
    Path tmp;

    private String generate(Path set, String outName, String... extra) throws Exception {
        Path out = Files.createDirectory(tmp.resolve(outName));
        List<String> args = new java.util.ArrayList<>(List.of("--results", set.toString(), "--out", out.toString()));
        args.addAll(List.of(extra));
        Result r = Cli.run(tmp, args.toArray(new String[0]));
        assertThat(r.exit()).as(r.err()).isZero();
        Scans.assertClean(out, r.out(), List.of(tmp));
        return outName;
    }

    @Test
    void theSameMetricsComeOutOfALinuxAndAWindowsSet() throws Exception {
        generate(Scenarios.mixed(tmp.resolve("w")).build(), "win");
        generate(Scenarios.mixed(tmp.resolve("l")).linux().build(), "lin");
        for (String data : List.of("runs.csv", "cells.csv", "analysis.json")) {
            String win = Files.readString(tmp.resolve("win").resolve("benchmark").resolve(data));
            String lin = Files.readString(tmp.resolve("lin").resolve("benchmark").resolve(data));
            assertThat(lin).as(data).isEqualTo(win);
        }
        for (String svg : List.of("completed-per-second", "ttfc-p50", "ttfc-p99", "total-p99")) {
            assertThat(Files.readString(tmp.resolve("lin").resolve("benchmark").resolve("chart-" + svg + ".svg")))
                    .isEqualTo(Files.readString(tmp.resolve("win").resolve("benchmark").resolve("chart-" + svg + ".svg")));
        }
    }

    @Test
    void aLinuxCrlfSetGivesTheSameDataAsALinuxLfSet() throws Exception {
        generate(Scenarios.mixed(tmp.resolve("a")).linux().build(), "lf");
        generate(Scenarios.mixed(tmp.resolve("b")).linux().crlf().build(), "crlf");
        for (String data : List.of("runs.csv", "cells.csv", "analysis.json")) {
            assertThat(Files.readString(tmp.resolve("crlf").resolve("benchmark").resolve(data))).as(data)
                    .isEqualTo(Files.readString(tmp.resolve("lf").resolve("benchmark").resolve(data)));
        }
        // only the raw sizes of the files that are listed but not analysed depend on the line ends
        assertThat(DeterminismTest.sizesMasked(tmp.resolve("crlf")))
                .isEqualTo(DeterminismTest.sizesMasked(tmp.resolve("lf")));
    }

    @Test
    void theLinuxEnvironmentIsShownAsRecordedAndMissingPartsAsNotRecorded() throws Exception {
        generate(Scenarios.clean(tmp).linux().build(), "o");
        String report = Files.readString(tmp.resolve("o").resolve("benchmark-report.md"));
        assertThat(report).contains("| Operating system | Linux 6.1.0-18-amd64 amd64 |")
                .contains("| CPU model | Fixture CPU 9000 (source: proc cpuinfo model name) |")
                .contains("| Power plan | not recorded (not captured on this OS) |");
        assertThat(report).contains("destroy (exit code 143)");
    }

    @Test
    void aWindowsPowerPlanShowsOnlyTheNameInParentheses() throws Exception {
        generate(Scenarios.clean(tmp).build(), "o");
        String report = Files.readString(tmp.resolve("o").resolve("benchmark-report.md"));
        assertThat(report).contains("| Power plan | Balanced |").doesNotContain("381b4222");
    }

    @Test
    void destroyWithExitCode143IsInformationAndAnUnknownStopMethodIsInformationToo() throws Exception {
        ResultSetBuilder b = ResultSetBuilder.at(tmp.resolve("s")).linux().concurrency(4).repetitions(2).plan();
        b.run(1).stopMethod = "graceful";
        b.run(2).stopMethod = "destroy";
        b.run(2).stopExitCode = 143;
        Analysis a = Analyses.of(b);
        assertThat(a.runs().get(0).assessment().reasons()).extracting(RunAssessment.Reason::code)
                .containsExactly(Checks.STOP_METHOD_UNKNOWN);
        assertThat(a.runs().get(0).assessment().classification()).isEqualTo("usable");
        assertThat(a.runs().get(1).assessment().reasons()).isEmpty();
    }

    @Test
    void missingCpuModelAndPhysicalCoresAreNotRecordedNotFailures() throws Exception {
        ResultSetBuilder b = ResultSetBuilder.at(tmp.resolve("arm")).linux().concurrency(4).repetitions(2).plan();
        Path root = b.build();
        String manifest = Files.readString(root.resolve("manifest.json"));
        Files.writeString(root.resolve("manifest.json"), manifest.replace("\"model\": \"Fixture CPU 9000\"",
                "\"model\": null").replace("\"physicalCores\": 4", "\"physicalCores\": null"));
        Path out = Files.createDirectory(tmp.resolve("o2"));
        Result r = Cli.run(tmp, "--results", root.toString(), "--out", out.toString());
        assertThat(r.exit()).isZero();
        String report = Files.readString(out.resolve("benchmark-report.md"));
        assertThat(report).contains("| CPU model | not recorded |").contains("| Physical cores | not recorded |");
        assertThat(report).contains("`environment.cpu.model`").contains("`environment.cpu.physicalCores`");
        assertThat(r.summary()).containsEntry("excluded", 0L);
    }
}
