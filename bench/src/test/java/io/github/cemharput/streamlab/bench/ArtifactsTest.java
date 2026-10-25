package io.github.cemharput.streamlab.bench;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.Timeout.ThreadMode;
import org.junit.jupiter.api.io.TempDir;

@Timeout(value = 30, unit = TimeUnit.SECONDS, threadMode = ThreadMode.SEPARATE_THREAD)
class ArtifactsTest {

    @TempDir
    Path root;

    @BeforeEach
    void layout() throws IOException {
        Files.writeString(root.resolve("pom.xml"), "<project/>");
        Files.createDirectories(root.resolve("gateway/target"));
        Files.createDirectories(root.resolve("loadgen/target/lib"));
        Files.createDirectories(root.resolve("bench/target/lib"));
    }

    private BenchOptions options(String... args) throws UsageException {
        return BenchOptionsParser.parse(args, root);
    }

    private Path touch(String relative) throws IOException {
        Path p = root.resolve(relative);
        Files.createDirectories(p.getParent());
        return Files.writeString(p, relative);
    }

    private void completeBuild() throws IOException {
        touch("gateway/target/gateway-0.1.0-SNAPSHOT.jar");
        touch("gateway/target/gateway-0.1.0-SNAPSHOT.jar.original");
        touch("loadgen/target/loadgen-0.1.0-SNAPSHOT.jar");
        touch("loadgen/target/lib/HdrHistogram-2.2.2.jar");
        touch("bench/target/lib/zeta-1.jar");
        touch("bench/target/lib/simulator-0.1.0-SNAPSHOT.jar");
        touch("bench/target/lib/alpha-1.jar");
        touch("bench/target/bench-0.1.0-SNAPSHOT.jar");
    }

    @Test
    void autoDetectsTheSingleJarsAndSortsTheClasspathByName() throws Exception {
        completeBuild();
        Artifacts.Resolved r = Artifacts.resolve(options());
        assertThat(r.problems()).isEmpty();
        assertThat(r.complete()).isTrue();
        assertThat(r.gatewayJar().getFileName().toString()).isEqualTo("gateway-0.1.0-SNAPSHOT.jar");
        assertThat(r.loadgenJar().getFileName().toString()).isEqualTo("loadgen-0.1.0-SNAPSHOT.jar");
        assertThat(r.loadgenLib()).hasSize(1);
        assertThat(r.simulatorClasspath().stream().map(p -> p.getFileName().toString()))
                .containsExactly("alpha-1.jar", "simulator-0.1.0-SNAPSHOT.jar", "zeta-1.jar");
        assertThat(r.benchJar().getFileName().toString()).isEqualTo("bench-0.1.0-SNAPSHOT.jar");
    }

    @Test
    void missingBuildIsReportedWithAHint() throws Exception {
        Artifacts.Resolved r = Artifacts.resolve(options());
        assertThat(r.complete()).isFalse();
        assertThat(r.gatewayJar()).isNull();
        assertThat(String.join("|", r.problems())).contains("gateway jar not found").contains("loadgen jar not found")
                .contains("simulator jar not found").contains("./mvnw -B package -DskipTests");
    }

    @Test
    void ambiguousAutoDetectionIsAProblem() throws Exception {
        completeBuild();
        touch("gateway/target/gateway-0.2.0.jar");
        Artifacts.Resolved r = Artifacts.resolve(options());
        assertThat(r.gatewayJar()).isNull();
        assertThat(String.join("|", r.problems())).contains("ambiguous");
    }

    @Test
    void sourcesJarsAreIgnored() throws Exception {
        completeBuild();
        touch("gateway/target/gateway-0.1.0-SNAPSHOT-sources.jar");
        assertThat(Artifacts.resolve(options()).complete()).isTrue();
    }

    @Test
    void staleJarsOfTheSameArtifactInTheLibDirectoryFailThePrecondition() throws Exception {
        completeBuild();
        touch("bench/target/lib/alpha-2.jar");
        touch("loadgen/target/lib/HdrHistogram-2.2.1.jar");
        Artifacts.Resolved r = Artifacts.resolve(options());
        assertThat(r.complete()).isFalse();
        assertThat(String.join("|", r.problems())).contains("stale jars").contains("alpha-1.jar", "alpha-2.jar")
                .contains("HdrHistogram-2.2.1.jar").contains("clean package");
        // different artifacts that merely share a prefix, and classifiers of one version, are fine
        assertThat(Artifacts.duplicates(List.of(Path.of("guava-33.5.0-android.jar"), Path.of("guava-33.5.0.jar")), root))
                .isEmpty();
        assertThat(Artifacts.duplicates(List.of(Path.of("foo-1.0.jar"), Path.of("foo-1.0-tests.jar"),
                Path.of("simulator-0.1.0-SNAPSHOT.jar"),
                Path.of("listenablefuture-9999.0-empty-to-avoid-conflict-with-guava.jar")), root)).isEmpty();
        assertThat(Artifacts.duplicates(List.of(Path.of("foo-1.0-tests.jar"), Path.of("foo-2.0-tests.jar")), root))
                .hasSize(1);
        assertThat(Artifacts.duplicates(List.of(Path.of("guava-33.5.0-android.jar"),
                Path.of("guava-32.0.0-android.jar")), root)).hasSize(1);
        assertThat(Artifacts.duplicates(List.of(Path.of("grpc-protobuf-1.81.0.jar"),
                Path.of("grpc-protobuf-lite-1.81.0.jar")), root)).isEmpty();
    }

    @Test
    void resolvedValidatesAndCopiesItsProblemLists() {
        java.util.ArrayList<String> problems = new java.util.ArrayList<>(List.of("a", "b"));
        org.assertj.core.api.Assertions.assertThatThrownBy(() -> new Artifacts.Resolved(null, null, List.of(), List.of(),
                null, problems, List.of(Artifacts.Kind.STALE))).isInstanceOf(IllegalArgumentException.class);
        Artifacts.Resolved legacy = new Artifacts.Resolved(null, null, List.of(), List.of(), null, problems);
        assertThat(legacy.problemKinds()).containsExactly(Artifacts.Kind.MISSING, Artifacts.Kind.MISSING);
        problems.add("c"); // the record holds a defensive copy
        assertThat(legacy.problems()).containsExactly("a", "b");
    }

    @Test
    void explicitOptionsOverrideAndMustExist() throws Exception {
        completeBuild();
        Path other = touch("elsewhere/my-gateway.jar");
        Artifacts.Resolved r = Artifacts.resolve(options("--gateway-jar", other.toString()));
        assertThat(r.gatewayJar()).isEqualTo(other);
        Artifacts.Resolved bad = Artifacts.resolve(options("--gateway-jar", root.resolve("nope.jar").toString()));
        assertThat(bad.problems()).anyMatch(p -> p.contains("gateway jar not found"));
        Artifacts.Resolved cp = Artifacts.resolve(options("--simulator-classpath",
                other + java.io.File.pathSeparator + root.resolve("missing.jar")));
        assertThat(cp.problems()).anyMatch(p -> p.contains("classpath entry not found"));
    }

    @Test
    void artifactInfoHasSizeAndSha256() throws Exception {
        Path file = Files.writeString(root.resolve("abc.bin"), "abc");
        Artifacts.ArtifactInfo info = Artifacts.ArtifactInfo.of(file);
        assertThat(info.sizeBytes()).isEqualTo(3);
        assertThat(info.sha256()).isEqualTo("ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad");
        assertThat(info.toMap().keySet()).containsExactly("path", "sizeBytes", "sha256", "lastModified");
        assertThat(info.lastModified()).isEqualTo(Files.getLastModifiedTime(file).toInstant());
    }
}
