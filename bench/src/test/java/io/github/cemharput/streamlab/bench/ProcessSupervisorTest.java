package io.github.cemharput.streamlab.bench;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.github.cemharput.streamlab.bench.ProcessSpecs.ProcessSpec;
import io.github.cemharput.streamlab.bench.ProcessSupervisor.StopResult;
import io.github.cemharput.streamlab.bench.testsupport.FakeChildProcess;
import io.github.cemharput.streamlab.bench.testsupport.FakeChildProcess.FakeKillable;
import io.github.cemharput.streamlab.bench.testsupport.FakeLauncher;
import io.github.cemharput.streamlab.bench.testsupport.FakeTicker;
import java.nio.file.Path;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.Timeout.ThreadMode;

@Timeout(value = 30, unit = TimeUnit.SECONDS, threadMode = ThreadMode.SEPARATE_THREAD)
class ProcessSupervisorTest {

    private static final ProcessSpec SPEC = new ProcessSpec("p", List.of("x"), Map.of(), Set.of(), Path.of("."),
            Path.of("p.log"));
    private static final Duration GRACEFUL = Duration.ofSeconds(5);
    private static final Duration FORCED = Duration.ofSeconds(5);

    private FakeChildProcess child;
    private final FakeLauncher launcher = new FakeLauncher(spec -> child);
    private final ProcessSupervisor supervisor = new ProcessSupervisor(launcher, new FakeTicker());

    @Test
    void destroySufficesWhenTheChildExitsAtOnce() throws Exception {
        child = new FakeChildProcess(1, true);
        ChildProcess started = supervisor.start(SPEC);
        StopResult r = supervisor.stop(started, GRACEFUL, FORCED);
        assertThat(r.method()).isEqualTo("destroy");
        assertThat(r.exitCode()).isEqualTo(143);
        assertThat(child.calls()).doesNotContain("destroyForcibly");
        assertThat(supervisor.registered()).isEmpty();
    }

    @Test
    void escalatesToForcedKillWhenDestroyDoesNotEndTheChild() throws Exception {
        child = new FakeChildProcess(1, true).exitsOnDestroy(false);
        supervisor.start(SPEC);
        StopResult r = supervisor.stop(child, GRACEFUL, FORCED);
        assertThat(r.method()).isEqualTo("forced");
        assertThat(child.calls()).containsSubsequence("destroy", "waitFor", "destroyForcibly");
        assertThat(child.isAlive()).isFalse();
    }

    @Test
    void descendantsAreKilledEvenWhenTheChildExitedGracefully() throws Exception {
        FakeKillable grandchild = new FakeKillable(99);
        child = new FakeChildProcess(1, true).withDescendant(grandchild);
        supervisor.start(SPEC);
        supervisor.stop(child, GRACEFUL, FORCED);
        assertThat(grandchild.isAlive()).isFalse();
    }

    @Test
    void reportsAChildThatSurvivesEverything() throws Exception {
        child = new FakeChildProcess(1, true).exitsOnDestroy(false).exitsOnForce(false);
        supervisor.start(SPEC);
        StopResult r = supervisor.stop(child, GRACEFUL, FORCED);
        assertThat(r.method()).isEqualTo("survived");
        assertThat(r.exitCode()).isNull();
    }

    @Test
    void theLogIsDrainedBeforeCountersAreReadForAChildThatDiedOnItsOwn() throws Exception {
        child = new FakeChildProcess(1, false).exited(2).logVisibleAfterDrain();
        supervisor.start(SPEC);
        assertThat(child.logBytesWritten()).isZero(); // the pump has not finished yet
        supervisor.stop(child, GRACEFUL, FORCED);
        assertThat(child.calls()).contains("awaitLogDrained");
        assertThat(child.logBytesWritten()).isEqualTo(10);
    }

    @Test
    void theLogIsDrainedAfterAStopOfALivingChildToo() throws Exception {
        child = new FakeChildProcess(1, true).logVisibleAfterDrain();
        supervisor.start(SPEC);
        supervisor.stop(child, GRACEFUL, FORCED);
        assertThat(child.logBytesWritten()).isEqualTo(10);
    }

    @Test
    void anAlreadyExitedChildIsReportedAsSuch() throws Exception {
        FakeKillable orphan = new FakeKillable(7);
        child = new FakeChildProcess(1, false).exited(3).withDescendant(orphan);
        supervisor.start(SPEC);
        StopResult r = supervisor.stop(child, GRACEFUL, FORCED);
        assertThat(r.method()).isEqualTo("already_exited");
        assertThat(r.exitCode()).isEqualTo(3);
        assertThat(orphan.isAlive()).isFalse();
    }

    @Test
    void killAllRunsTheCallbackOnceAfterTheKillWasIssued() throws Exception {
        child = new FakeChildProcess(1, true).exitsOnDestroy(false);
        supervisor.start(SPEC);
        java.util.concurrent.atomic.AtomicInteger runs = new java.util.concurrent.atomic.AtomicInteger();
        java.util.concurrent.atomic.AtomicReference<List<String>> callsAtCallback =
                new java.util.concurrent.atomic.AtomicReference<>();
        supervisor.killAll(Duration.ofSeconds(10), () -> {
            runs.incrementAndGet();
            callsAtCallback.set(child.calls());
        });
        assertThat(runs.get()).isEqualTo(1);
        assertThat(callsAtCallback.get()).contains("destroyForcibly").doesNotContain("waitFor");
        assertThat(child.isAlive()).isFalse();
    }

    @Test
    void killAllSurvivesAChildThatThrowsAndStillKillsTheRest() throws Exception {
        FakeChildProcess bad = new FakeChildProcess(1, true).failing();
        FakeChildProcess good = new FakeChildProcess(2, true).exitsOnDestroy(false);
        FakeKillable grandchild = new FakeKillable(99);
        good.withDescendant(grandchild);
        java.util.Iterator<FakeChildProcess> next = List.of(bad, good).iterator();
        ProcessSupervisor sup = new ProcessSupervisor(new FakeLauncher(spec -> next.next()), new FakeTicker());
        sup.start(SPEC);
        sup.start(SPEC);
        java.util.concurrent.atomic.AtomicInteger callbackRuns = new java.util.concurrent.atomic.AtomicInteger();

        sup.killAll(Duration.ofSeconds(10), callbackRuns::incrementAndGet);

        assertThat(good.isAlive()).isFalse();
        assertThat(grandchild.isAlive()).isFalse();
        assertThat(callbackRuns.get()).isEqualTo(1);
        assertThat(sup.isClosed()).isTrue();
    }

    @Test
    void killAllKillsEverythingRegisteredAndClosesTheSupervisor() throws Exception {
        FakeKillable grandchild = new FakeKillable(99);
        child = new FakeChildProcess(1, true).exitsOnDestroy(false).withDescendant(grandchild);
        supervisor.start(SPEC);
        supervisor.killAll(Duration.ofSeconds(10));
        assertThat(child.isAlive()).isFalse();
        assertThat(grandchild.isAlive()).isFalse();
        assertThat(supervisor.isClosed()).isTrue();
        assertThat(supervisor.registered()).isEmpty();
        assertThatThrownBy(() -> supervisor.start(SPEC)).isInstanceOf(IllegalStateException.class);
        assertThat(launcher.specs()).hasSize(1);
    }
}
