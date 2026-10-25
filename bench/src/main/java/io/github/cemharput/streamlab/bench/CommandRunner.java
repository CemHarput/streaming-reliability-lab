package io.github.cemharput.streamlab.bench;

import java.time.Duration;
import java.util.List;

/** Runs a short read-only system command (git, reg, powercfg, netstat ...). */
public interface CommandRunner {

    /** {@code ok} is true when the command ran and exited with 0; {@code output} is stdout+stderr. */
    record CommandResult(boolean ok, int exitCode, String output) {

        public static CommandResult failed(String reason) {
            return new CommandResult(false, -1, reason);
        }
    }

    CommandResult run(List<String> command, Duration timeout);
}
