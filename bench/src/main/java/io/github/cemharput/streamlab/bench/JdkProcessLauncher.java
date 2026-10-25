package io.github.cemharput.streamlab.bench;

import io.github.cemharput.streamlab.bench.ProcessSpecs.ProcessSpec;
import java.io.IOException;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

/** Starts real processes with {@link ProcessBuilder}: combined output, sanitised environment, bounded log. */
public final class JdkProcessLauncher implements ProcessLauncher {

    private final long logLimitBytes;

    public JdkProcessLauncher(long logLimitBytes) {
        this.logLimitBytes = logLimitBytes;
    }

    /**
     * Removes the names (compared case-insensitively, as Windows does) and then applies the overrides, so an
     * override always wins over a removed variable of the same name.
     */
    static void applyEnvironment(Map<String, String> env, Set<String> removed, Map<String, String> overrides) {
        Set<String> upper = removed.stream().map(n -> n.toUpperCase(Locale.ROOT)).collect(Collectors.toSet());
        env.keySet().removeIf(name -> upper.contains(name.toUpperCase(Locale.ROOT)));
        env.putAll(overrides);
    }

    @Override
    public ChildProcess launch(ProcessSpec spec) throws IOException {
        ProcessBuilder builder = new ProcessBuilder(spec.command());
        builder.directory(spec.workDir().toFile());
        builder.redirectErrorStream(true);
        applyEnvironment(builder.environment(), spec.envRemoved(), spec.envOverrides());
        Process process = builder.start();
        BoundedLogPump pump = new BoundedLogPump(process.getInputStream(), spec.logFile(), logLimitBytes,
                "log-pump-" + spec.name()).start();
        return new JdkChildProcess(process, pump);
    }
}
