package io.github.cemharput.streamlab.bench;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import io.github.cemharput.streamlab.bench.ProcessSpecs.ProcessSpec;
import io.github.cemharput.streamlab.bench.ProcessSupervisor.StopResult;
import io.github.cemharput.streamlab.bench.testsupport.ChildMain;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.Timeout.ThreadMode;
import org.junit.jupiter.api.io.TempDir;

/** Real child JVMs (three in total): the stop sequence on the actual platform. */
@Timeout(value = 120, unit = TimeUnit.SECONDS, threadMode = ThreadMode.SEPARATE_THREAD)
class ProcessSupervisorRealProcessTest {

    private static final boolean WINDOWS = System.getProperty("os.name").toLowerCase().contains("win");

    @TempDir
    Path dir;

    private final ProcessSupervisor supervisor = new ProcessSupervisor(new JdkProcessLauncher(1024 * 1024),
            Ticker.SYSTEM);

    @AfterEach
    void killLeftovers() {
        supervisor.killAll(Duration.ofSeconds(20));
    }

    private ProcessSpec spec(String name, String mode) throws Exception {
        String classes = Path.of(ChildMain.class.getProtectionDomain().getCodeSource().getLocation().toURI()).toString();
        return new ProcessSpec(name, List.of(ProcessSpecs.javaBinary(System.getProperty("java.home"), WINDOWS),
                "-cp", classes, ChildMain.class.getName(), mode), Map.of(), Set.of(), dir, dir.resolve(name + ".log"));
    }

    private void awaitLog(String name, String text) {
        await().atMost(Duration.ofSeconds(60)).pollInterval(Duration.ofMillis(100)).untilAsserted(
                () -> assertThat(Files.readString(dir.resolve(name + ".log"))).contains(text));
    }

    @Test
    void stopsABlockingChildOnThisPlatform() throws Exception {
        // POSIX: a hook that never finishes makes SIGTERM insufficient, so the forced step is exercised there.
        // Windows: destroy() already is TerminateProcess, so the destroy step suffices.
        ChildProcess child = supervisor.start(spec("blocking", WINDOWS ? "block" : "hook"));
        awaitLog("blocking", "READY");
        long pid = child.pid();
        StopResult result = supervisor.stop(child, Duration.ofSeconds(WINDOWS ? 20 : 2), Duration.ofSeconds(20));
        assertThat(result.method()).isEqualTo(WINDOWS ? "destroy" : "forced");
        assertThat(child.isAlive()).isFalse();
        assertThat(ProcessHandle.of(pid).map(ProcessHandle::isAlive).orElse(false)).isFalse();
        assertThat(child.logBytesWritten()).isPositive();
    }

    @Test
    void stopKillsTheGrandchildToo() throws Exception {
        ChildProcess child = supervisor.start(spec("parent", "spawn"));
        awaitLog("parent", "READY");
        String log = Files.readString(dir.resolve("parent.log"));
        long grandchildPid = Long.parseLong(log.lines().filter(l -> l.startsWith("GRANDCHILD ")).findFirst()
                .orElseThrow().substring("GRANDCHILD ".length()).trim());
        assertThat(ProcessHandle.of(grandchildPid).map(ProcessHandle::isAlive).orElse(false)).isTrue();

        supervisor.stop(child, Duration.ofSeconds(20), Duration.ofSeconds(20));

        assertThat(child.isAlive()).isFalse();
        await().atMost(Duration.ofSeconds(60)).untilAsserted(() -> assertThat(
                ProcessHandle.of(grandchildPid).map(ProcessHandle::isAlive).orElse(false)).isFalse());
    }
}
