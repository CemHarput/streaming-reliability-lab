package io.github.cemharput.streamlab.gateway.stream;

import static io.github.cemharput.streamlab.gateway.stream.StreamContract.DRAIN_TIMEOUT;
import static io.github.cemharput.streamlab.gateway.stream.StreamTestSupport.chunk;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import io.github.cemharput.streamlab.gateway.stream.StreamContract.Observation;
import io.github.cemharput.streamlab.gateway.stream.StreamContract.Scenario;
import io.github.cemharput.streamlab.gateway.stream.StreamContract.Violation;
import io.github.cemharput.streamlab.gateway.stream.StreamTestSupport.CountingLease;
import io.github.cemharput.streamlab.gateway.stream.StreamTestSupport.FakeUpstreamStarter;
import io.github.cemharput.streamlab.gateway.stream.StreamTestSupport.RecordingEmitter;
import io.github.cemharput.streamlab.gateway.stream.StreamUnderTest.Kind;
import io.github.cemharput.streamlab.gateway.stream.StreamUnderTest.Wiring;
import io.github.cemharput.streamlab.tokenstream.v1.GenerateRequest;
import java.lang.System.Logger.Level;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

/**
 * Completion racing a cancel, on both implementations.
 *
 * <p>LEAKY: the race is built into the handler's terminal step ({@link LeakyStreamHandler#armTerminalRace}), a latch
 * both terminal paths must reach after passing the check, so both always pass it; the outcome depends on the latch
 * structure, not on timing. REAL has no hook: it is raced from the outside with a {@link CyclicBarrier} for many
 * iterations and the invariant is asserted per iteration, never who wins. A stress test cannot prove the absence of a
 * race; the argument for {@link StreamBridge} is the compare-and-set in its {@code claim()}.
 */
@Timeout(value = 60, threadMode = Timeout.ThreadMode.SEPARATE_THREAD)
class TerminalRaceContractTest {

    private static final System.Logger LOGGER = System.getLogger(TerminalRaceContractTest.class.getName());
    private static final int CAPACITY = 16;
    private static final int REAL_ITERATIONS = 200;
    private static final int LEAKY_ITERATIONS = 5;
    private static final int SPIN_STEPS = 25;
    private static final int SPIN_UNIT = 1500;
    private static final Duration QUIET = Duration.ofMillis(500);

    private final List<StreamTimeouts> timeoutsStarted = new CopyOnWriteArrayList<>();

    @AfterEach
    void tearDown() {
        for (StreamTimeouts timeouts : timeoutsStarted) {
            timeouts.stop();
        }
    }

    @ParameterizedTest
    @EnumSource(Kind.class)
    void completionRacesShutdownCancel(Kind kind) throws Exception {
        Race race = run(kind, Scenario.RACE_COMPLETION_VS_SHUTDOWN, kind == Kind.REAL ? REAL_ITERATIONS : LEAKY_ITERATIONS);
        if (kind == Kind.LEAKY) {
            for (Observation o : race.observations()) {
                assertThat(terminals(o)).as("LEAKY: both terminal paths wrote an event: %s", StreamContract.names(o.events())).isEqualTo(2);
                assertThat(o.leaseCloses()).as("lease closes").isEqualTo(2);
                assertThat(o.permitReleases()).as("permit releases").isEqualTo(2);
            }
        }
    }

    @ParameterizedTest
    @EnumSource(Kind.class)
    void completionRacesClientGone(Kind kind) throws Exception {
        Race race = run(kind, Scenario.RACE_COMPLETION_VS_CLIENT_GONE, kind == Kind.REAL ? REAL_ITERATIONS : LEAKY_ITERATIONS);
        if (kind == Kind.LEAKY) {
            for (Observation o : race.observations()) {
                assertThat(terminals(o)).as("LEAKY: only the completion writes: %s", StreamContract.names(o.events())).isEqualTo(1);
                assertThat(o.leaseCloses()).as("lease closes").isEqualTo(2);
                assertThat(o.permitReleases()).as("permit releases").isEqualTo(2);
            }
        }
    }

    private static int terminals(Observation o) {
        return (int) o.events().stream().filter(StreamContract::isTerminal).count();
    }

    private record Race(List<Observation> observations, Map<String, Integer> winners) {
    }

    private Race run(Kind kind, Scenario scenario, int iterations) throws Exception {
        boolean shutdownRace = scenario == Scenario.RACE_COMPLETION_VS_SHUTDOWN;
        StreamTimeouts timeouts = StreamTestSupport.startedTimeouts(Duration.ofSeconds(30), Duration.ofSeconds(30));
        timeoutsStarted.add(timeouts);
        GenerateRequest request = GenerateRequest.newBuilder().setRequestId("race").build();
        List<StreamUnderTest> units = new ArrayList<>();
        List<RecordingEmitter> emitters = new ArrayList<>();

        ExecutorService executor = Executors.newVirtualThreadPerTaskExecutor();
        CyclicBarrier barrier = new CyclicBarrier(2);
        try {
            for (int i = 0; i < iterations; i++) {
                RecordingEmitter emitter = RecordingEmitter.responsive();
                FakeUpstreamStarter starter = new FakeUpstreamStarter();
                StreamUnderTest unit = StreamUnderTest.create(kind, new Wiring(request, emitter, new CountingLease(),
                        timeouts, CAPACITY, starter, Duration.ofSeconds(60), DRAIN_TIMEOUT));
                assertThat(unit.admit()).as("HARNESS: permit admitted (iteration %d)", i).isTrue();
                unit.start();
                assertThat(starter.calls).as("HARNESS: one upstream call (iteration %d)", i).hasSize(1);
                CountDownLatch arrivals = new CountDownLatch(2);
                assertThat(unit.armTerminalRaceHook(arrivals))
                        .as("HARNESS: only the flawed handler has a race hook").isEqualTo(kind == Kind.LEAKY);
                var call = starter.calls.get(0);

                // The first chunk goes in before the race so that the real bridge has a stream in progress when the
                // last chunk (the COMPLETED claim) and the cancel compete. Whether its drainer is still running by then
                // is not guaranteed and does not matter: the invariant is the same for every interleaving.
                call.upstream().deliver(call.observer(), chunk(0, false));
                // Moves the cancel across the completion window from one iteration to the next (a busy spin, no
                // sleep), so that both outcomes are exercised; the invariant is asserted whoever wins.
                int spins = (i % SPIN_STEPS) * SPIN_UNIT;
                barrier.reset();
                Future<?> completion = executor.submit(() -> {
                    barrier.await(5, TimeUnit.SECONDS);
                    call.upstream().deliver(call.observer(), chunk(1, true));
                    call.observer().onCompleted();
                    return null;
                });
                Future<?> cancel = executor.submit(() -> {
                    barrier.await(5, TimeUnit.SECONDS);
                    for (int spin = 0; spin < spins; spin++) {
                        Thread.onSpinWait();
                    }
                    if (shutdownRace) {
                        unit.cancelForShutdown();
                    } else {
                        unit.clientGone();
                    }
                    return null;
                });
                completion.get(10, TimeUnit.SECONDS);
                cancel.get(10, TimeUnit.SECONDS);
                assertThat(unit.raceHookBroken())
                        .as("HARNESS: the race hook was passed by both paths (iteration %d)", i).isFalse();

                // The iteration is over once capacity is released and no timer is armed (and, for a shutdown race,
                // the terminal event is written); later writes are caught by the final stability wait.
                await().pollDelay(Duration.ZERO).pollInterval(Duration.ofMillis(1)).atMost(Duration.ofSeconds(5))
                        .until(() -> unit.leaseCloses() >= 1 && unit.permitReleases() >= 1
                                && unit.pendingTimers() == 0
                                && (!shutdownRace || emitter.events.stream().anyMatch(StreamContract::isTerminal)));
                units.add(unit);
                emitters.add(emitter);
            }
        } finally {
            barrier.reset();
            executor.shutdownNow();
        }
        assertThat(executor.awaitTermination(5, TimeUnit.SECONDS)).as("HARNESS: race threads ended").isTrue();

        Supplier<List<Observation>> snapshot = () -> {
            List<Observation> all = new ArrayList<>();
            for (int i = 0; i < units.size(); i++) {
                StreamUnderTest unit = units.get(i);
                all.add(new Observation(List.copyOf(emitters.get(i).events), -1, unit.leaseCloses(),
                        unit.permitReleases(), unit.permitsHeld(), unit.pendingTimers(), -1, -1, null, -1, -1, -1, -1,
                        null));
            }
            return all;
        };
        AtomicReference<List<Observation>> last = new AtomicReference<>();
        await().pollInterval(Duration.ofMillis(50)).during(QUIET).atMost(QUIET.multipliedBy(10)).until(() -> {
            List<Observation> current = snapshot.get();
            return current.equals(last.getAndSet(current));
        });
        List<Observation> observations = last.get();

        Map<String, Integer> winners = new TreeMap<>();
        List<String> mismatches = new ArrayList<>();
        for (int i = 0; i < observations.size(); i++) {
            Observation o = observations.get(i);
            List<Violation> violations = StreamContract.check(scenario, o, CAPACITY);
            winners.merge(winner(o), 1, Integer::sum);
            if (!StreamContract.clausesOf(violations).equals(StreamContract.expected(kind, scenario))) {
                mismatches.add("iteration " + i + ": " + StreamContract.report(kind, scenario, violations));
            }
            if (kind == Kind.LEAKY) {
                LOGGER.log(Level.INFO, "contract violation [{0} / {1}] iteration {2}: {3}", kind, scenario, i,
                        violations);
            }
        }
        LOGGER.log(Level.INFO, "race {0} / {1}: {2} iteration(s), outcomes {3}, pendingTimers {4}", kind, scenario,
                observations.size(), winners, timeouts.pendingTimers());
        assertThat(mismatches).as("%s / %s: every iteration must match the expectation %s", kind, scenario,
                StreamContract.expected(kind, scenario)).isEmpty();
        assertThat(observations).hasSize(iterations);
        assertThat(timeouts.pendingTimers()).as("no timer is left armed").isZero();
        return new Race(observations, winners);
    }

    private enum Winner {
        COMPLETE, ERROR, NONE
    }

    /** Which terminal event the client saw (distribution is logged, never asserted). */
    private static String winner(Observation o) {
        boolean complete = o.events().stream().anyMatch(e -> e.contains("event:complete"));
        boolean error = o.events().stream().anyMatch(e -> e.contains("event:error"));
        if (complete && error) {
            return "BOTH";
        }
        return (complete ? Winner.COMPLETE : error ? Winner.ERROR : Winner.NONE).name();
    }
}
