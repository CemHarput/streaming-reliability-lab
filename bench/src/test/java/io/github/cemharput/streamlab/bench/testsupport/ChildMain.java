package io.github.cemharput.streamlab.bench.testsupport;

import java.io.File;
import java.nio.file.Path;
import java.util.concurrent.CountDownLatch;

/**
 * Test-only child JVM (JDK classes only). Prints READY and blocks forever.
 * Modes: {@code block}; {@code spawn} (also starts a blocking grandchild and prints GRANDCHILD pid);
 * {@code hook} (installs a shutdown hook that never finishes, so only a forced kill ends it on POSIX).
 */
public final class ChildMain {

    private ChildMain() {
    }

    public static void main(String[] args) throws Exception {
        String mode = args.length > 0 ? args[0] : "block";
        if (mode.equals("spawn")) {
            String java = Path.of(System.getProperty("java.home"), "bin",
                    System.getProperty("os.name").toLowerCase().contains("win") ? "java.exe" : "java").toString();
            String classes = Path.of(ChildMain.class.getProtectionDomain().getCodeSource().getLocation().toURI())
                    .toString();
            Process grandchild = new ProcessBuilder(java, "-Xshare:auto", "-cp", classes, ChildMain.class.getName(),
                    "block").redirectOutput(ProcessBuilder.Redirect.DISCARD)
                    .redirectError(ProcessBuilder.Redirect.DISCARD).start();
            System.out.println("GRANDCHILD " + grandchild.pid());
        }
        if (mode.equals("hook")) {
            Runtime.getRuntime().addShutdownHook(new Thread(() -> {
                try {
                    new CountDownLatch(1).await();
                } catch (InterruptedException ex) {
                    Thread.currentThread().interrupt();
                }
            }));
        }
        System.out.println("READY " + File.separator);
        System.out.flush();
        new CountDownLatch(1).await();
    }
}
