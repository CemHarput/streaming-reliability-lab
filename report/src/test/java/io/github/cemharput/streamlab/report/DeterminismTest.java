package io.github.cemharput.streamlab.report;

import static org.assertj.core.api.Assertions.assertThat;

import io.github.cemharput.streamlab.report.testsupport.Cli;
import io.github.cemharput.streamlab.report.testsupport.Cli.Result;
import io.github.cemharput.streamlab.report.testsupport.ResultSetBuilder;
import io.github.cemharput.streamlab.report.testsupport.Scenarios;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Locale;
import java.util.Map;
import java.util.TimeZone;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.Timeout.ThreadMode;
import org.junit.jupiter.api.io.TempDir;

/** Same input, byte-identical output: across runs, locales, time zones, record order and line ends. */
@Timeout(value = 60, unit = TimeUnit.SECONDS, threadMode = ThreadMode.SEPARATE_THREAD)
class DeterminismTest {

    @TempDir
    Path tmp;

    private record Generated(Map<String, String> hashes, String stdout) {
    }

    private Generated generate(Path set, String outName) throws IOException {
        Path out = Files.createDirectory(tmp.resolve(outName));
        Result r = Cli.run(tmp, "--results", set.toString(), "--out", out.toString());
        assertThat(r.exit()).as(r.err()).isZero();
        return new Generated(Cli.hashes(out), r.out());
    }

    private static ResultSetBuilder mixedAt(Path base, String name) {
        ResultSetBuilder b = Scenarios.mixed(base.resolve(name));
        return b;
    }

    @Test
    void twoGenerationsOfTheSameSetAreByteIdentical() throws Exception {
        Path set = Scenarios.mixed(tmp).build();
        Generated a = generate(set, "out1");
        Generated b = generate(set, "out2");
        assertThat(a.hashes()).hasSize(8).isEqualTo(b.hashes());
        assertThat(a.stdout()).isEqualTo(b.stdout());
    }

    @Test
    void theDefaultLocaleAndTimeZoneDoNotChangeAnyByte() throws Exception {
        Path set = Scenarios.mixed(tmp).build();
        Locale oldLocale = Locale.getDefault();
        TimeZone oldZone = TimeZone.getDefault();
        try {
            Locale.setDefault(Locale.ROOT);
            TimeZone.setDefault(TimeZone.getTimeZone("UTC"));
            Generated base = generate(set, "base");
            int n = 0;
            for (String tag : new String[] {"tr-TR", "de-DE", "ar-SA", "th-TH-u-nu-thai"}) {
                for (String zone : new String[] {"UTC", "Asia/Tokyo", "America/Los_Angeles"}) {
                    Locale.setDefault(Locale.forLanguageTag(tag));
                    TimeZone.setDefault(TimeZone.getTimeZone(zone));
                    Generated g = generate(set, "g" + n++);
                    assertThat(g.hashes()).as(tag + " " + zone).isEqualTo(base.hashes());
                    assertThat(g.stdout()).as(tag + " " + zone).isEqualTo(base.stdout());
                }
            }
        } finally {
            Locale.setDefault(oldLocale);
            TimeZone.setDefault(oldZone);
        }
    }

    @Test
    void theOrderOfTheRecordsInTheJsonlDoesNotMatter() throws Exception {
        ResultSetBuilder plain = Scenarios.mixed(tmp.resolve("plain"));
        ResultSetBuilder shuffled = Scenarios.mixed(tmp.resolve("shuffled"));
        for (ResultSetBuilder.RunSpec spec : shuffled.runs()) {
            spec.reverseRequests = true;
        }
        Generated a = generate(plain.build(), "a");
        Generated b = generate(shuffled.build(), "b");
        assertThat(b.hashes()).isEqualTo(a.hashes());
    }

    @Test
    void lfAndCrlfInputsGiveTheSameOutput() throws Exception {
        Generated lf = generate(Scenarios.mixed(tmp.resolve("lf")).build(), "out-lf");
        Generated crlf = generate(Scenarios.mixed(tmp.resolve("crlf")).crlf().build(), "out-crlf");
        // The data files are byte-identical. The report lists the raw sizes of files that are listed but not
        // analysed (warm-up files, logs); those sizes differ with the line ends of the input, nothing else does.
        for (String name : OutputWriter.DATA_FILES) {
            assertThat(crlf.hashes().get(name)).as(name).isEqualTo(lf.hashes().get(name));
        }
        assertThat(sizesMasked(tmp.resolve("out-crlf"))).isEqualTo(sizesMasked(tmp.resolve("out-lf")));
        assertThat(crlf.stdout()).isEqualTo(lf.stdout());
    }

    static String sizesMasked(Path out) throws IOException {
        return Files.readString(out.resolve("benchmark-report.md")).replaceAll("\\(\\d+ bytes\\)", "(N bytes)");
    }

    @Test
    void nothingInTheOutputDependsOnTheClockOrTheTemporaryDirectory() throws Exception {
        Generated a = generate(Scenarios.mixed(tmp.resolve("one")).build(), "oa");
        Generated b = generate(Scenarios.mixed(tmp.resolve("two").resolve("deeper")).build(), "ob");
        assertThat(b.hashes()).isEqualTo(a.hashes());
    }

    @Test
    void theOutputOnlyCarriesTimestampsOfTheManifest() throws Exception {
        Path set = Scenarios.clean(tmp).build();
        Generated g = generate(set, "ts");
        String report = Files.readString(tmp.resolve("ts").resolve("benchmark-report.md"));
        java.util.regex.Matcher m = java.util.regex.Pattern.compile("\\d{4}-\\d{2}-\\d{2}T\\d{2}:\\d{2}:\\d{2}")
                .matcher(report);
        while (m.find()) {
            assertThat(m.group()).isIn("2026-01-01T00:00:00", "2026-01-01T01:00:00", "2025-12-31T12:00:00");
        }
        assertThat(g.hashes()).isNotEmpty();
    }
}
