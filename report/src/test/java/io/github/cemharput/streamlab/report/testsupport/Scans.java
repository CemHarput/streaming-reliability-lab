package io.github.cemharput.streamlab.report.testsupport;

import static org.assertj.core.api.Assertions.assertThat;

import io.github.cemharput.streamlab.report.OutputWriter;
import io.github.cemharput.streamlab.report.PrivacyGuard;
import io.github.cemharput.streamlab.report.WordingGuard;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

/** The assertions every generated output must pass: wording, privacy, line ends. */
public final class Scans {

    private Scans() {
    }

    /** Scans every file of {@code out} and the given standard output; {@code roots} are paths that must not occur. */
    public static void assertClean(Path out, String stdout, List<Path> roots) throws IOException {
        List<String> secrets = new java.util.ArrayList<>(List.of("fixtureuser", "C:\\Users", "/home/fixtureuser"));
        for (Path root : roots) {
            secrets.add(root.toString());
            secrets.add(root.toString().replace('\\', '/'));
            secrets.add(root.toAbsolutePath().toString());
        }
        secrets.add(System.getProperty("user.home"));
        for (String name : OutputWriter.ALL_FILES) {
            Path file = out.resolve(name);
            if (!Files.exists(file)) {
                continue;
            }
            String text = Files.readString(file, StandardCharsets.UTF_8);
            assertThat(WordingGuard.scan(text, name.equals(OutputWriter.REPORT))).as("wording " + name).isEmpty();
            assertThat(PrivacyGuard.scan(text, List.of())).as("privacy " + name).isEmpty();
            for (String secret : secrets) {
                assertThat(text).as(name + " must not contain " + secret.length() + " chars secret").doesNotContain(secret);
            }
            assertThat(text).as(name + " line ends").doesNotContain("\r");
        }
        assertThat(PrivacyGuard.scan(stdout, List.of())).as("privacy stdout").isEmpty();
        for (String secret : secrets) {
            assertThat(stdout).doesNotContain(secret);
        }
    }
}
