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
import io.grpc.Status;
import java.lang.System.Logger.Level;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.Callable;
import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.awaitility.core.ConditionTimeoutException;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

/**
 * The other terminal-path pairs of {@link StreamBridge} (timeout, upstream error, overflow, shutdown cancel, client
 * gone, completion) raced against each other, plus the registry's admission against {@code stop()}. REAL only.
 *
 * <p>Every iteration builds a new stream and runs {@link StreamContract#check} on it (the REAL expectation is the
 * empty clause set). On top of that the test checks that the pair of (terminal event, upstream cancel message) is one
 * the pair allows: if the stream did not end by itself, upstream was cancelled with the winner's message; if it ended
 * by itself (completion, upstream error) it was not cancelled. Which side wins is never asserted, only logged. A stress
 * test cannot prove the absence of a race; the argument for {@link StreamBridge} is the compare-and-set in
 * {@code claim()}.
 */
@Timeout(value = 60, threadMode = Timeout.ThreadMode.SEPARATE_THREAD)
class TerminalPathRaceTest {

    private static final System.Logger LOGGER = System.getLogger(TerminalPathRaceTest.class.getName());
    private static final int CAPACITY = 16;
    private static final int OVERFLOW_CAPACITY = 4;
    private static final Duration IDLE = Duration.ofMillis(10);
    private static final int TIMER_STEPS = 20;
    private static final int SPIN_STEPS = 25;
    private static final int SPIN_UNIT = 1500;
    private static final Duration ADMIT_DRAIN_TIMEOUT = Duration.ofMillis(20);
    private static final Duration QUIET = Duration.ofMillis(500);
    private static final Duration WAIT = Duration.ofSeconds(5);
    private static final Pattern CODE = Pattern.compile("code=([A-Z_]+)");

    private static final String CLIENT_GONE_MESSAGE = "client disconnected";
    private static final String OVERFLOW_MESSAGE = "per-request queue overflow";

    private final List<StreamTimeouts> timeoutsStarted = new CopyOnWriteArrayList<>();
    private final List<RecordingEmitter> emitters = new CopyOnWriteArrayList<>();

    @AfterEach
    void tearDown() {
        for (RecordingEmitter emitter : emitters) {
            emitter.gate.countDown();
        }
        for (StreamTimeouts timeouts : timeoutsStarted) {
            timeouts.stop();
        }
    }

    @Test
    void timeoutRacesCompletion() throws Exception {
        run(Pair.TIMEOUT_VS_COMPLETION, 100);
    }

    @Test
    void timeoutRacesClientGone() throws Exception {
        run(Pair.TIMEOUT_VS_CLIENT_GONE, 100);
    }

    @Test
    void upstreamErrorRacesClientGone() throws Exception {
        run(Pair.UPSTREAM_ERROR_VS_CLIENT_GONE, 200);
    }

    @Test
    void upstreamErrorRacesShutdownCancel() throws Exception {
        run(Pair.UPSTREAM_ERROR_VS_SHUTDOWN, 200);
    }

    @Test
    void overflowRacesClientGone() throws Exception {
        run(Pair.OVERFLOW_VS_CLIENT_GONE, 200);
    }

    @Test
    void overflowRacesShutdownCancel() throws Exception {
        run(Pair.OVERFLOW_VS_SHUTDOWN, 200);
    }

    @Test
    void admissionRacesStop() throws Exception {
        run(Pair.ADMIT_VS_STOP, 200);
    }

    /**
     * What a pair needs. {@code shape} carries the contract shape ({@code EXACTLY_ONE_ANY} or {@code AT_MOST_ONE});
     * {@code allowed} are the permitted "terminal|cancel message" keys, see {@link #key}.
     */
    private enum Pair {
        TIMEOUT_VS_COMPLETION(Scenario.RACE_COMPLETION_VS_SHUTDOWN, CAPACITY, false, true,
                Set.of("complete|null", "error:DEADLINE_EXCEEDED|" + TimeoutKind.IDLE.message())),
        TIMEOUT_VS_CLIENT_GONE(Scenario.RACE_COMPLETION_VS_CLIENT_GONE, CAPACITY, false, true,
                Set.of("error:DEADLINE_EXCEEDED|" + TimeoutKind.IDLE.message(), "none|" + CLIENT_GONE_MESSAGE)),
        UPSTREAM_ERROR_VS_CLIENT_GONE(Scenario.RACE_COMPLETION_VS_CLIENT_GONE, CAPACITY, false, false,
                Set.of("error:INTERNAL|null", "none|" + CLIENT_GONE_MESSAGE)),
        UPSTREAM_ERROR_VS_SHUTDOWN(Scenario.RACE_COMPLETION_VS_SHUTDOWN, CAPACITY, false, false,
                Set.of("error:INTERNAL|null", "error:CANCELLED|" + StreamBridge.SHUTDOWN_MESSAGE)),
        OVERFLOW_VS_CLIENT_GONE(Scenario.RACE_COMPLETION_VS_CLIENT_GONE, OVERFLOW_CAPACITY, true, false,
                Set.of("error:RESOURCE_EXHAUSTED|" + OVERFLOW_MESSAGE, "none|" + CLIENT_GONE_MESSAGE)),
        OVERFLOW_VS_SHUTDOWN(Scenario.RACE_COMPLETION_VS_SHUTDOWN, OVERFLOW_CAPACITY, true, false,
                Set.of("error:RESOURCE_EXHAUSTED|" + OVERFLOW_MESSAGE,
                        "error:CANCELLED|" + StreamBridge.SHUTDOWN_MESSAGE)),
        ADMIT_VS_STOP(Scenario.RACE_COMPLETION_VS_SHUTDOWN, CAPACITY, false, false,
                Set.of("complete|null", "error:CANCELLED|" + StreamBridge.SHUTDOWN_MESSAGE));

        final Scenario scenario;
        final int capacity;
        final boolean stalled;
        final boolean timer;
        final Set<String> allowed;

        Pair(Scenario scenario, int capacity, boolean stalled, boolean timer, Set<String> allowed) {
            this.scenario = scenario;
            this.capacity = capacity;
            this.stalled = stalled;
            this.timer = timer;
            this.allowed = allowed;
        }

        boolean overflow() {
            return this == OVERFLOW_VS_CLIENT_GONE || this == OVERFLOW_VS_SHUTDOWN;
        }

        /** Pairs where the race outcome always ends with exactly one terminal event. */
        boolean terminalAlways() {
            return scenario == Scenario.RACE_COMPLETION_VS_SHUTDOWN;
        }
    }

    /** What one iteration leaves for the judgement after the final stability wait. */
    private record Iteration(StreamUnderTest unit, RecordingEmitter emitter, FakeUpstreamStarter starter,
            boolean refused, Integer permitsHeldAtStop, int leaseClosesWhileStuck, int permitReleasesWhileStuck) {
    }

    private void run(Pair pair, int iterations) throws Exception {
        StreamTimeouts timeouts = pair.timer
                ? StreamTestSupport.startedTimeouts(Duration.ofSeconds(30), IDLE)
                : StreamTestSupport.startedTimeouts(Duration.ofSeconds(30), Duration.ofSeconds(30));
        timeoutsStarted.add(timeouts);
        GenerateRequest request = GenerateRequest.newBuilder().setRequestId("race").build();
        Duration drainTimeout = pair == Pair.ADMIT_VS_STOP ? ADMIT_DRAIN_TIMEOUT : DRAIN_TIMEOUT;
        List<Iteration> done = new ArrayList<>();
        boolean terminated = false;
        ExecutorService executor = Executors.newVirtualThreadPerTaskExecutor();
        CyclicBarrier barrier = new CyclicBarrier(2);
        try {
            for (int i = 0; i < iterations; i++) {
                RecordingEmitter emitter = pair.stalled ? RecordingEmitter.stalled() : RecordingEmitter.responsive();
                emitters.add(emitter);
                FakeUpstreamStarter starter = new FakeUpstreamStarter();
                StreamUnderTest unit = StreamUnderTest.create(Kind.REAL, new Wiring(request, emitter,
                        new CountingLease(), timeouts, pair.capacity, starter, Duration.ofSeconds(60), drainTimeout));
                int iteration = i;
                boolean refused = false;
                Integer heldAtStop = null;
                int leaseWhileStuck = -1;
                int releaseWhileStuck = -1;
                try {
                    if (pair != Pair.ADMIT_VS_STOP) {
                        assertThat(unit.admit()).as("HARNESS: permit admitted (iteration %d)", i).isTrue();
                        unit.start();
                        assertThat(starter.calls).as("HARNESS: one upstream call (iteration %d)", i).hasSize(1);
                    }
                    long base = System.nanoTime();
                    prepare(pair, unit, emitter, starter, i);

                    int k = (i % (2 * SPIN_STEPS + 1)) - SPIN_STEPS;
                    int spinsA = k < 0 ? -k * SPIN_UNIT : 0;
                    int spinsB = k > 0 ? k * SPIN_UNIT : 0;
                    barrier.reset();
                    Future<Object> sideA = null;
                    Future<Object> sideB;
                    if (pair.timer) {
                        long offset = (iteration % TIMER_STEPS) * (4 * IDLE.toNanos() / TIMER_STEPS);
                        sideB = executor.submit(() -> {
                            while (System.nanoTime() < base + offset) {
                                Thread.onSpinWait();
                            }
                            actB(pair, unit, starter);
                            return null;
                        });
                    } else {
                        sideA = executor.submit(() -> {
                            barrier.await(5, TimeUnit.SECONDS);
                            spin(spinsA);
                            return actA(pair, unit, starter, iteration);
                        });
                        sideB = executor.submit(() -> {
                            barrier.await(5, TimeUnit.SECONDS);
                            spin(spinsB);
                            return actB(pair, unit, starter);
                        });
                    }
                    Object resultA = sideA == null ? null : join(sideA, pair, "A");
                    Object resultB = join(sideB, pair, "B");
                    if (pair == Pair.ADMIT_VS_STOP) {
                        refused = Boolean.FALSE.equals(resultA);
                        heldAtStop = (Integer) resultB;
                    }
                    if (pair.overflow()) {
                        // Both sides have returned; the drainer is still stuck in the chunk 0 write under sendLock.
                        leaseWhileStuck = unit.leaseCloses();
                        releaseWhileStuck = unit.permitReleases();
                    }
                } finally {
                    emitter.gate.countDown();
                }
                if (!refused) {
                    // The iteration is over once capacity is released and no timer is armed (and, where a terminal
                    // event is certain, it is written); later writes are caught by the final stability wait. A
                    // timeout here is not thrown: a leak must show up as a contract violation, not as a timeout.
                    boolean settled = settle(unit, emitter, pair.terminalAlways());
                    if (!settled) {
                        LOGGER.log(Level.INFO, "race [{0}] iteration {1}: not settled in 5 s", pair, i);
                    }
                }
                done.add(new Iteration(unit, emitter, starter, refused, heldAtStop, leaseWhileStuck,
                        releaseWhileStuck));
            }
        } finally {
            barrier.reset();
            for (RecordingEmitter emitter : emitters) {
                emitter.gate.countDown();
            }
            executor.shutdownNow();
            terminated = executor.awaitTermination(5, TimeUnit.SECONDS);
        }
        assertThat(terminated).as("HARNESS: race threads ended").isTrue();

        Supplier<List<Observation>> snapshot = () -> {
            List<Observation> all = new ArrayList<>();
            for (Iteration it : done) {
                all.add(new Observation(List.copyOf(it.emitter().events), -1, it.unit().leaseCloses(),
                        it.unit().permitReleases(), it.unit().permitsHeld(), it.unit().pendingTimers(), -1, -1, null,
                        -1, -1, -1, -1, null));
            }
            return all;
        };
        AtomicReference<List<Observation>> last = new AtomicReference<>();
        await().pollInterval(Duration.ofMillis(50)).during(QUIET).atMost(QUIET.multipliedBy(10)).until(() -> {
            List<Observation> current = snapshot.get();
            return current.equals(last.getAndSet(current));
        });
        List<Observation> observations = last.get();

        Map<String, Integer> outcomes = new TreeMap<>();
        List<String> mismatches = new ArrayList<>();
        AtomicInteger heldWhileCompleting = new AtomicInteger();
        for (int i = 0; i < observations.size(); i++) {
            Observation o = observations.get(i);
            Iteration it = done.get(i);
            String prefix = "iteration " + i + " [" + pair + ", shape " + pair.scenario.shape + "]: ";
            String cancel = it.starter().calls.isEmpty() ? "no call"
                    : String.valueOf(it.starter().calls.get(0).upstream().cancelMessage.get());
            if (it.refused()) {
                outcomes.merge("REFUSED", 1, Integer::sum);
                checkRefused(prefix, o, it, mismatches);
                continue;
            }
            outcomes.merge(label(o), 1, Integer::sum);
            List<Violation> violations = StreamContract.check(pair.scenario, o, pair.capacity);
            if (!StreamContract.clausesOf(violations).isEmpty()) {
                mismatches.add(prefix + StreamContract.report(Kind.REAL, pair.scenario, violations)
                        + " events " + StreamContract.names(o.events()) + " cancel " + cancel);
            }
            String key = key(o.events()) + "|" + cancel;
            if (!pair.allowed.contains(key)) {
                mismatches.add(prefix + "outcome (terminal|cancel message) '" + key + "' is not one of "
                        + pair.allowed + "; events " + StreamContract.names(o.events()));
            }
            if (pair.overflow()) {
                long chunks = o.events().stream().filter(StreamContract::isChunk).count();
                if (chunks != 1) {
                    mismatches.add(prefix + "expected exactly 1 chunk event (queued chunks dropped), saw " + chunks
                            + ": " + StreamContract.names(o.events()));
                }
                if (it.leaseClosesWhileStuck() != 1 || it.permitReleasesWhileStuck() != 1) {
                    mismatches.add(prefix + "capacity not released while the chunk 0 write was stuck: leaseCloses="
                            + it.leaseClosesWhileStuck() + " permitReleases=" + it.permitReleasesWhileStuck());
                }
            }
            if (pair == Pair.ADMIT_VS_STOP) {
                // A forced cancel that wins the claim releases capacity inside stop(), so 0 is certain. If a COMPLETED
                // claim won, stop() may return between that claim and the drainer's releaseCapacity(): 1 is then a
                // legal reading (the permit is released exactly once right after, which RELEASE_ONCE checks on the
                // settled snapshot).
                int allowedHeld = o.events().stream().anyMatch(e -> e.contains("event:complete")) ? 1 : 0;
                if (it.permitsHeldAtStop() < 0 || it.permitsHeldAtStop() > allowedHeld) {
                    mismatches.add(prefix + "permitsHeld after stop() returned (read on the stop thread) = "
                            + it.permitsHeldAtStop() + ", allowed up to " + allowedHeld);
                }
                if (it.permitsHeldAtStop() == 1) {
                    heldWhileCompleting.incrementAndGet();
                }
            }
        }
        LOGGER.log(Level.INFO, "race [{0}]: {1} iteration(s), outcomes {2}, pendingTimers {3}", pair,
                observations.size(), outcomes, timeouts.pendingTimers());
        if (pair == Pair.ADMIT_VS_STOP) {
            LOGGER.log(Level.INFO, "race [{0}]: permitsHeld was 1 when stop() returned in {1} iteration(s)", pair,
                    heldWhileCompleting.get());
        }
        assertThat(mismatches).as("%s: every iteration must satisfy the contract and the outcome table", pair)
                .isEmpty();
        assertThat(observations).hasSize(iterations);
        assertThat(timeouts.pendingTimers()).as("no timer is left armed").isZero();
    }

    /** Brings the stream to the state the race starts from, and checks the harness preconditions. */
    private static void prepare(Pair pair, StreamUnderTest unit, RecordingEmitter emitter,
            FakeUpstreamStarter starter, int i) {
        if (pair == Pair.ADMIT_VS_STOP) {
            return;
        }
        var call = starter.calls.get(0);
        call.upstream().deliver(call.observer(), chunk(0, false));
        switch (pair) {
            case TIMEOUT_VS_COMPLETION, TIMEOUT_VS_CLIENT_GONE -> awaitHarness(
                    "the idle timer is armed (or has already fired)", i,
                    () -> unit.pendingTimers() >= 1 || unit.leaseCloses() >= 1);
            case UPSTREAM_ERROR_VS_CLIENT_GONE, UPSTREAM_ERROR_VS_SHUTDOWN -> awaitHarness(
                    "chunk 0 was written (drainer idle)", i, () -> emitter.events.size() == 1);
            case OVERFLOW_VS_CLIENT_GONE, OVERFLOW_VS_SHUTDOWN -> {
                awaitHarness("the drainer is blocked writing chunk 0", i, () -> emitter.sendInProgress);
                for (int seq = 1; seq <= OVERFLOW_CAPACITY; seq++) {
                    call.upstream().deliver(call.observer(), chunk(seq, false));
                }
                assertThat(unit.bufferedChunks())
                        .as("HARNESS: blocked chunk 0 plus a full queue (iteration %d)", i)
                        .isEqualTo(OVERFLOW_CAPACITY + 1);
            }
            default -> throw new IllegalStateException(pair.name());
        }
    }

    /** Side A: the trigger of the pair that is not a plain cancel (timer pairs have no side A). */
    private static Object actA(Pair pair, StreamUnderTest unit, FakeUpstreamStarter starter, int iteration) {
        switch (pair) {
            case UPSTREAM_ERROR_VS_CLIENT_GONE, UPSTREAM_ERROR_VS_SHUTDOWN -> {
                var call = starter.calls.get(0);
                call.observer().onError(Status.INTERNAL.withDescription("race").asRuntimeException());
                return null;
            }
            case OVERFLOW_VS_CLIENT_GONE, OVERFLOW_VS_SHUTDOWN -> {
                var call = starter.calls.get(0);
                call.upstream().deliver(call.observer(), chunk(OVERFLOW_CAPACITY + 1, false));
                return null;
            }
            case ADMIT_VS_STOP -> {
                boolean admitted = unit.admit();
                if (admitted) {
                    unit.start();
                    if (iteration % 2 == 1) {
                        var call = starter.calls.get(0);
                        call.upstream().deliver(call.observer(), chunk(0, true));
                        call.observer().onCompleted();
                    }
                }
                return admitted;
            }
            default -> throw new IllegalStateException(pair.name());
        }
    }

    /** Side B: the competing path. */
    private static Object actB(Pair pair, StreamUnderTest unit, FakeUpstreamStarter starter) {
        switch (pair) {
            case TIMEOUT_VS_COMPLETION -> {
                var call = starter.calls.get(0);
                call.upstream().deliver(call.observer(), chunk(1, true));
                call.observer().onCompleted();
                return null;
            }
            case TIMEOUT_VS_CLIENT_GONE, UPSTREAM_ERROR_VS_CLIENT_GONE, OVERFLOW_VS_CLIENT_GONE -> {
                unit.clientGone();
                return null;
            }
            case UPSTREAM_ERROR_VS_SHUTDOWN, OVERFLOW_VS_SHUTDOWN -> {
                unit.cancelForShutdown();
                return null;
            }
            case ADMIT_VS_STOP -> {
                unit.shutdown();
                // Read on the stop() thread, after stop() returned.
                return unit.permitsHeld();
            }
            default -> throw new IllegalStateException(pair.name());
        }
    }

    private static void spin(int spins) {
        for (int spin = 0; spin < spins; spin++) {
            Thread.onSpinWait();
        }
    }

    private static boolean settle(StreamUnderTest unit, RecordingEmitter emitter, boolean terminalExpected) {
        try {
            await().pollDelay(Duration.ZERO).pollInterval(Duration.ofMillis(1)).atMost(WAIT)
                    .until(() -> unit.leaseCloses() >= 1 && unit.permitReleases() >= 1 && unit.pendingTimers() == 0
                            && (!terminalExpected || emitter.events.stream().anyMatch(StreamContract::isTerminal)));
            return true;
        } catch (ConditionTimeoutException ex) {
            return false;
        }
    }

    private static void awaitHarness(String what, int iteration, Callable<Boolean> condition) {
        try {
            await().pollDelay(Duration.ZERO).pollInterval(Duration.ofMillis(1)).atMost(WAIT).until(condition);
        } catch (ConditionTimeoutException ex) {
            throw new AssertionError("HARNESS: precondition not reached in 5 s: " + what + " (iteration "
                    + iteration + ")", ex);
        }
    }

    private static <T> T join(Future<T> future, Pair pair, String side) {
        try {
            return future.get(10, TimeUnit.SECONDS);
        } catch (TimeoutException ex) {
            throw new AssertionError("HARNESS: side " + side + " of " + pair + " did not return in 10 s", ex);
        } catch (InterruptedException ex) {
            Thread.currentThread().interrupt();
            throw new AssertionError("HARNESS: interrupted while waiting for side " + side + " of " + pair, ex);
        } catch (ExecutionException ex) {
            Throwable cause = ex.getCause();
            if (cause instanceof BrokenBarrierException || cause instanceof TimeoutException) {
                throw new AssertionError("HARNESS: barrier failed on side " + side + " of " + pair + ": " + cause,
                        cause);
            }
            throw new AssertionError("[" + pair + "] side " + side + " threw", cause);
        }
    }

    private static void checkRefused(String prefix, Observation o, Iteration it, List<String> mismatches) {
        if (!it.starter().calls.isEmpty()) {
            mismatches.add(prefix + "REFUSED but " + it.starter().calls.size() + " upstream call(s) were started");
        }
        if (!o.events().isEmpty()) {
            mismatches.add(prefix + "REFUSED but events were written: " + StreamContract.names(o.events()));
        }
        if (o.leaseCloses() != 0 || o.permitReleases() != 0 || o.permitsHeld() != 0 || o.pendingTimers() != 0) {
            mismatches.add(prefix + "REFUSED but leaseCloses=" + o.leaseCloses() + " permitReleases="
                    + o.permitReleases() + " permitsHeld=" + o.permitsHeld() + " pendingTimers="
                    + o.pendingTimers());
        }
        if (it.permitsHeldAtStop() != 0) {
            mismatches.add(prefix + "REFUSED but permitsHeld after stop() = " + it.permitsHeldAtStop());
        }
    }

    /** "complete", "error:CODE", "none" or "multiple"; taken from the event text the way {@link StreamContract} does. */
    private static String key(List<String> events) {
        List<String> terminals = events.stream().filter(StreamContract::isTerminal).toList();
        if (terminals.isEmpty()) {
            return "none";
        }
        if (terminals.size() > 1) {
            return "multiple";
        }
        String terminal = terminals.get(0);
        if (terminal.contains("event:complete")) {
            return "complete";
        }
        Matcher matcher = CODE.matcher(terminal);
        return "error:" + (matcher.find() ? matcher.group(1) : "?");
    }

    /** Which terminal event the client saw, for the logged distribution (never asserted). */
    private static String label(Observation o) {
        return key(o.events()).replace("complete", "COMPLETE").replace("error:", "ERROR:").replace("none", "NONE")
                .replace("multiple", "MULTIPLE");
    }
}
