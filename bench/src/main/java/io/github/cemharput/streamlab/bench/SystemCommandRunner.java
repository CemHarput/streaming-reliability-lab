package io.github.cemharput.streamlab.bench;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.TimeUnit;

/** Real command execution: timeout 15 s by default, output bounded to 64 KiB (the rest is read and dropped). */
public final class SystemCommandRunner implements CommandRunner {

    public static final Duration DEFAULT_TIMEOUT = Duration.ofSeconds(15);
    private static final int MAX_OUTPUT = 64 * 1024;

    @Override
    public CommandResult run(List<String> command, Duration timeout) {
        Process process;
        try {
            process = new ProcessBuilder(command).redirectErrorStream(true).start();
        } catch (IOException ex) {
            return CommandResult.failed("cannot start " + command.get(0) + ": " + ex.getMessage());
        }
        ByteArrayOutputStream buffer = new ByteArrayOutputStream();
        Thread reader = new Thread(() -> read(process.getInputStream(), buffer), "command-output");
        reader.setDaemon(true);
        reader.start();
        try {
            if (!process.waitFor(timeout.toMillis(), TimeUnit.MILLISECONDS)) {
                process.destroyForcibly();
                process.waitFor(2, TimeUnit.SECONDS);
                reader.join(2000);
                return CommandResult.failed("timeout after " + timeout + ": " + command.get(0));
            }
            reader.join(2000);
        } catch (InterruptedException ex) {
            process.destroyForcibly();
            Thread.currentThread().interrupt();
            return CommandResult.failed("interrupted: " + command.get(0));
        }
        int exit = process.exitValue();
        String text;
        synchronized (buffer) {
            text = buffer.toString(StandardCharsets.UTF_8);
        }
        return new CommandResult(exit == 0, exit, text);
    }

    private static void read(InputStream in, ByteArrayOutputStream buffer) {
        byte[] chunk = new byte[4096];
        try (in) {
            int n;
            while ((n = in.read(chunk)) >= 0) {
                synchronized (buffer) {
                    int room = MAX_OUTPUT - buffer.size();
                    if (room > 0) {
                        buffer.write(chunk, 0, Math.min(room, n));
                    }
                }
            }
        } catch (IOException ex) {
            // process ended
        }
    }
}
