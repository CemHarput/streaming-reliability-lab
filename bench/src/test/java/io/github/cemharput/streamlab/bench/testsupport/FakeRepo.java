package io.github.cemharput.streamlab.bench.testsupport;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

/** A directory that looks like a built repository: pom.xml, module directories and tiny fake jars. */
public final class FakeRepo {

    private FakeRepo() {
    }

    public static Path layout(Path root) throws IOException {
        Files.writeString(root.resolve("pom.xml"), "<project/>");
        Files.createDirectories(root.resolve("gateway"));
        Files.createDirectories(root.resolve("loadgen"));
        return root;
    }

    public static Path build(Path root) throws IOException {
        layout(root);
        jar(root, "gateway/target/gateway-0.1.0-SNAPSHOT.jar");
        jar(root, "loadgen/target/loadgen-0.1.0-SNAPSHOT.jar");
        jar(root, "loadgen/target/lib/HdrHistogram-2.2.2.jar");
        jar(root, "bench/target/lib/a-dep-1.jar");
        jar(root, "bench/target/lib/simulator-0.1.0-SNAPSHOT.jar");
        jar(root, "bench/target/bench-0.1.0-SNAPSHOT.jar");
        return root;
    }

    private static void jar(Path root, String relative) throws IOException {
        Path p = root.resolve(relative);
        Files.createDirectories(p.getParent());
        Files.writeString(p, "fake jar " + relative);
    }
}
