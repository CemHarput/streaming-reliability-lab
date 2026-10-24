package io.github.cemharput.streamlab.gateway.stream;

import static io.github.cemharput.streamlab.gateway.stream.StreamContract.DRAIN_TIMEOUT;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.awaitility.Awaitility.await;

import io.github.cemharput.streamlab.gateway.config.GatewayProperties;
import io.github.cemharput.streamlab.gateway.config.GatewayProperties.ConnectionMode;
import io.github.cemharput.streamlab.gateway.config.GatewayProperties.Limits;
import io.github.cemharput.streamlab.gateway.config.GatewayProperties.Timeouts;
import io.github.cemharput.streamlab.gateway.stream.StreamContract.Clause;
import io.github.cemharput.streamlab.gateway.stream.StreamContract.Observation;
import io.github.cemharput.streamlab.gateway.stream.StreamContract.Scenario;
import io.github.cemharput.streamlab.gateway.stream.StreamContract.Violation;
import io.github.cemharput.streamlab.gateway.stream.StreamTestSupport.CountingLease;
import io.github.cemharput.streamlab.gateway.stream.StreamTestSupport.RecordingEmitter;
import io.github.cemharput.streamlab.gateway.stream.StreamUnderTest.Kind;
import io.github.cemharput.streamlab.gateway.stream.StreamUnderTest.Wiring;
import io.github.cemharput.streamlab.gateway.upstream.ChannelProvider;
import io.github.cemharput.streamlab.gateway.upstream.PerRequestChannelProvider;
import io.github.cemharput.streamlab.gateway.upstream.SharedChannelProvider;
import io.github.cemharput.streamlab.simulator.RequestLimits;
import io.github.cemharput.streamlab.simulator.SimulatorConfig;
import io.github.cemharput.streamlab.simulator.SimulatorServer;
import io.github.cemharput.streamlab.testsupport.CountingChannelFactory;
import io.github.cemharput.streamlab.testsupport.CountingChannelFactory.CountingChannel;
import io.github.cemharput.streamlab.tokenstream.v1.FaultSpec;
import io.github.cemharput.streamlab.tokenstream.v1.FaultType;
import io.github.cemharput.streamlab.tokenstream.v1.GenerateRequest;
import io.github.cemharput.streamlab.tokenstream.v1.GetStatsRequest;
import io.github.cemharput.streamlab.tokenstream.v1.SimulatorAdminServiceGrpc;
import io.github.cemharput.streamlab.tokenstream.v1.SimulatorStats;
import io.github.cemharput.streamlab.tokenstream.v1.StreamProfile;
import io.github.cemharput.streamlab.tokenstream.v1.TokenStreamServiceGrpc;
import io.grpc.ManagedChannel;
import io.grpc.inprocess.InProcessChannelBuilder;
import io.grpc.inprocess.InProcessServerBuilder;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;
import java.util.stream.Stream;
import org.awaitility.core.ConditionTimeoutException;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.MethodSource;
import org.springframework.context.SmartLifecycle;

/**
 * The stream contract against the real in-process simulator, run on {@link StreamBridge} (REAL) and on the flawed
 * {@link LeakyStreamHandler} (LEAKY). REAL must violate nothing; for LEAKY the violated clauses must equal
 * {@link StreamContract#EXPECTED_LEAKY} exactly. The terminal races are in {@link TerminalRaceContractTest}.
 *
 * <p>Every scenario checks its preconditions first and reports a failure there as a harness error, so a scenario
 * cannot pass because it never got into the state it claims to test. A fresh simulator per test keeps its counters
 * exact.
 */
@Timeout(value = 60, threadMode = Timeout.ThreadMode.SEPARATE_THREAD)
class StreamContractTest {

    private static final Duration QUIET = Duration.ofMillis(500);
    private static final Duration PRECONDITION_WAIT = Duration.ofSeconds(10);
    private static final Duration SAMPLE_WINDOW = Duration.ofMillis(500);

    private SimulatorServer server;
    private String serverName;
    private ManagedChannel channel;
    private ManagedChannel adminChannel;
    private final List<RecordingEmitter> emitters = new CopyOnWriteArrayList<>();
    private final List<StreamTimeouts> timeoutsStarted = new CopyOnWriteArrayList<>();

    @BeforeEach
    void setUp() throws Exception {
        String name = InProcessServerBuilder.generateName();
        serverName = name;
        server = SimulatorServer.start(
                InProcessServerBuilder.forName(name),
                new SimulatorConfig(0, Duration.ofSeconds(2), 2, RequestLimits.DEFAULT));
        channel = InProcessChannelBuilder.forName(name).build();
        adminChannel = InProcessChannelBuilder.forName(name).build();
    }

    @AfterEach
    void tearDown() throws Exception {
        for (RecordingEmitter emitter : emitters) {
            emitter.gate.countDown();
        }
        for (StreamTimeouts timeouts : timeoutsStarted) {
            timeouts.stop();
        }
        // shutdownNow cancels a call the flawed handler leaked, which lets the simulator end its generation.
        channel.shutdownNow().awaitTermination(5, TimeUnit.SECONDS);
        adminChannel.shutdownNow().awaitTermination(5, TimeUnit.SECONDS);
        server.stop();
    }

    // ---- the contract on both implementations -------------------------------------------------------------

    @ParameterizedTest
    @EnumSource(Kind.class)
    void normalCompletionEndsOnce(Kind kind) {
        runAndAssert(kind, Scenario.COMPLETION);
    }

    @ParameterizedTest
    @EnumSource(Kind.class)
    void upstreamErrorEndsOnce(Kind kind) {
        runAndAssert(kind, Scenario.UPSTREAM_ERROR);
    }

    @ParameterizedTest
    @EnumSource(Kind.class)
    void clientGoneCancelsUpstream(Kind kind) {
        Observation observation = runAndAssert(kind, Scenario.CLIENT_GONE);
        // Whatever the implementation: capacity and timer are freed and nothing is sent to the gone client.
        assertThat(observation.leaseCloses()).isEqualTo(1);
        assertThat(observation.permitReleases()).isEqualTo(1);
        assertThat(observation.pendingTimers()).isZero();
        assertThat(observation.events()).hasSize(observation.eventsAtTrigger());
        if (kind == Kind.LEAKY) {
            // Flaw (a), stated directly: the generation stays active for the whole window and nobody cancelled it.
            assertThat(observation.upstreamStopped()).isFalse();
            assertThat(observation.active()).isEqualTo(1);
            assertThat(observation.cancelled()).isZero();
        } else {
            assertThat(observation.upstreamStopped()).isTrue();
            assertThat(observation.active()).isZero();
            assertThat(observation.cancelled()).isEqualTo(1);
        }
    }

    @ParameterizedTest
    @EnumSource(Kind.class)
    void shutdownCancelsUpstreamWithinBound(Kind kind) {
        runAndAssert(kind, Scenario.SHUTDOWN_CANCEL);
    }

    @ParameterizedTest
    @EnumSource(Kind.class)
    void firstChunkTimeoutCancelsUpstream(Kind kind) {
        runAndAssert(kind, Scenario.FIRST_CHUNK_TIMEOUT);
    }

    @ParameterizedTest
    @EnumSource(Kind.class)
    void slowConsumerStaysWithinQueueBound(Kind kind) {
        runAndAssert(kind, Scenario.SLOW_CONSUMER);
    }

    // ---- the contract in both connection modes ---------------------------------------------------------------

    static Stream<Arguments> modesAndScenarios() {
        Scenario[] scenarios = {Scenario.COMPLETION, Scenario.UPSTREAM_ERROR, Scenario.CLIENT_GONE,
                Scenario.SHUTDOWN_CANCEL, Scenario.FIRST_CHUNK_TIMEOUT, Scenario.SLOW_CONSUMER};
        return Arrays.stream(ConnectionMode.values())
                .flatMap(mode -> Arrays.stream(scenarios).map(scenario -> Arguments.of(mode, scenario)));
    }

    /**
     * The same REAL contract with the channel taken from the real provider of each mode. On top of the contract:
     * per-request opens one channel and shuts it down once (never shutdownNow); shared opens one channel at start
     * and does not touch it until the provider stops.
     */
    @ParameterizedTest(name = "{0} / {1}")
    @MethodSource("modesAndScenarios")
    void contractHoldsInEveryConnectionMode(ConnectionMode mode, Scenario scenario) throws Exception {
        CountingChannelFactory factory = new CountingChannelFactory(
                target -> InProcessChannelBuilder.forName(serverName).build());
        GatewayProperties properties = StreamTestSupport.properties(mode, DRAIN_TIMEOUT, Limits.defaults(),
                Timeouts.defaults());
        ChannelProvider provider = mode == ConnectionMode.SHARED
                ? new SharedChannelProvider(factory, properties)
                : new PerRequestChannelProvider(factory, properties);
        SmartLifecycle lifecycle = (SmartLifecycle) provider;
        lifecycle.start();
        try {
            CountingLease lease = new CountingLease(provider.acquire());
            UpstreamStarter starter = (upstreamRequest, deadline, observer) -> TokenStreamServiceGrpc
                    .newStub(lease.channel())
                    .withDeadlineAfter(deadline.toNanos(), TimeUnit.NANOSECONDS)
                    .generateTokens(upstreamRequest, observer);
            runAndAssert(Kind.REAL, scenario, lease, starter);

            assertThat(factory.created()).as("channels created").isEqualTo(1);
            CountingChannel upstream = factory.channels().get(0);
            if (mode == ConnectionMode.PER_REQUEST) {
                assertThat(upstream.shutdownCalls()).as("shutdown calls").isEqualTo(1);
                assertThat(upstream.shutdownNowCalls()).as("shutdownNow calls").isZero();
                await().atMost(PRECONDITION_WAIT).until(upstream::isTerminated);
            } else {
                assertThat(upstream.shutdownCalls()).as("shared channel shutdown before stop").isZero();
                assertThat(upstream.shutdownNowCalls()).isZero();
            }
        } finally {
            lifecycle.stop();
        }
        await().atMost(PRECONDITION_WAIT).until(() -> factory.channels().stream().allMatch(CountingChannel::isTerminated));
        if (provider instanceof PerRequestChannelProvider perRequest) {
            assertThat(perRequest.openChannels()).isZero();
        }
    }

    // ---- the harness itself ---------------------------------------------------------------------------------

    @Test
    void expectationTableCoversBothFlawsAndOnlyThem() {
        for (Kind kind : Kind.values()) {
            for (Scenario scenario : Scenario.values()) {
                assertThat(StreamContract.expected(kind, scenario)).as("%s / %s", kind, scenario).isNotNull();
            }
        }
        assertThat(StreamContract.EXPECTED_LEAKY).containsOnlyKeys(Scenario.values());
        for (Scenario scenario : Scenario.values()) {
            assertThat(StreamContract.expected(Kind.REAL, scenario)).as("REAL / %s", scenario).isEmpty();
        }
        for (Scenario scenario : Scenario.values()) {
            Set<Clause> expected = switch (scenario) {
                case CLIENT_GONE -> Set.of(Clause.UPSTREAM_CANCELLED);
                case RACE_COMPLETION_VS_SHUTDOWN -> Set.of(Clause.SINGLE_TERMINAL, Clause.RELEASE_ONCE);
                case RACE_COMPLETION_VS_CLIENT_GONE -> Set.of(Clause.RELEASE_ONCE);
                default -> Set.of();
            };
            assertThat(StreamContract.expected(Kind.LEAKY, scenario)).as("LEAKY / %s", scenario).isEqualTo(expected);
        }
    }

    @Test
    void checkReportsEachClauseOnSyntheticObservations() {
        List<String> good = List.of("event:chunk\nid:0", "event:chunk\nid:1", "event:chunk\nid:2",
                "event:chunk\nid:3", "event:chunk\nid:4", "event:complete\ndata:CompleteEvent");
        Observation base = new Observation(good, -1, 1, 1, 0, 0, -1, -1, true, 0, 0, 1, 0, null);
        assertThat(StreamContract.check(Scenario.COMPLETION, base, 16)).isEmpty();

        List<String> twoTerminals = new ArrayList<>(good);
        twoTerminals.add("event:error\ndata:code=CANCELLED");
        assertThat(clauses(Scenario.COMPLETION, copy(base, twoTerminals, 1, 1, 0, 0, true, -1, -1)))
                .containsExactly(Clause.SINGLE_TERMINAL);
        assertThat(clauses(Scenario.COMPLETION, copy(base, good, 2, 1, 0, 0, true, -1, -1)))
                .containsExactly(Clause.RELEASE_ONCE);
        assertThat(clauses(Scenario.COMPLETION, copy(base, good, 1, 2, 0, 0, true, -1, -1)))
                .containsExactly(Clause.RELEASE_ONCE);
        assertThat(clauses(Scenario.COMPLETION, copy(base, good, 1, 1, 1, 0, true, -1, -1)))
                .containsExactly(Clause.RELEASE_ONCE);
        assertThat(clauses(Scenario.COMPLETION, copy(base, good, 1, 1, 0, 0, false, -1, -1)))
                .containsExactly(Clause.UPSTREAM_CANCELLED);
        assertThat(clauses(Scenario.COMPLETION, copy(base, good, 1, 1, 0, 1, true, -1, -1)))
                .containsExactly(Clause.NO_TIMER_LEAK);
        assertThat(clauses(Scenario.SLOW_CONSUMER, copy(base, slowGood(), 1, 1, 0, 0, true, 5, 4)))
                .containsExactly(Clause.QUEUE_BOUNDED);
        assertThat(clauses(Scenario.SLOW_CONSUMER, copy(base, slowGood(), 1, 1, 0, 0, true, 4, 5)))
                .containsExactly(Clause.QUEUE_BOUNDED);
        assertThat(clauses(Scenario.SLOW_CONSUMER, copy(base, slowGood(), 1, 1, 0, 0, true, 4, 4))).isEmpty();
        // client gone: an event sent after the trigger is a violation
        Observation gone = new Observation(List.of("event:chunk\nid:0", "event:error\ndata:x"), 1, 1, 1, 0, 0,
                -1, -1, true, 0, 1, 0, 0, null);
        assertThat(StreamContract.check(Scenario.CLIENT_GONE, gone, 16)).extracting(Violation::clause)
                .containsExactly(Clause.SINGLE_TERMINAL);
    }

    @Test
    void checkReportsEveryBranchOfTheTerminalAndUpstreamClausesWhenViolated() {
        List<String> errorGood = List.of("event:chunk\nid:0", "event:chunk\nid:1", "event:error\ndata:code=INTERNAL");
        // UPSTREAM_ERROR is the scenario with an expected code and 2 chunk events before the error.
        assertThat(clauses(Scenario.UPSTREAM_ERROR, observation(errorGood, true, 0, 0, 1, null))).isEmpty();
        assertThat(clauses(Scenario.UPSTREAM_ERROR, observation(
                List.of("event:chunk\nid:0", "event:chunk\nid:1", "event:error\ndata:code=CANCELLED"),
                true, 0, 0, 1, null))).as("wrong error code").containsExactly(Clause.SINGLE_TERMINAL);
        assertThat(clauses(Scenario.UPSTREAM_ERROR, observation(
                List.of("event:chunk\nid:0", "event:error\ndata:code=INTERNAL"), true, 0, 0, 1, null)))
                .as("missing chunk before the error").containsExactly(Clause.SINGLE_TERMINAL);
        assertThat(clauses(Scenario.UPSTREAM_ERROR, observation(errorGood, true, 0, 0, 0, null)))
                .as("faulted_total mismatch").containsExactly(Clause.UPSTREAM_CANCELLED);

        // COMPLETION expects 5 chunk events and one complete event.
        assertThat(clauses(Scenario.COMPLETION, observation(
                List.of("event:chunk\nid:0", "event:chunk\nid:1", "event:chunk\nid:2", "event:chunk\nid:3",
                        "event:complete\ndata:x"), true, 0, 1, 0, null)))
                .as("chunk count mismatch").containsExactly(Clause.SINGLE_TERMINAL);
        assertThat(clauses(Scenario.COMPLETION, observation(
                List.of("event:chunk\nid:0", "event:chunk\nid:1", "event:chunk\nid:2", "event:chunk\nid:3",
                        "event:chunk\nid:4", "event:complete\ndata:x"), true, 0, 0, 0, null)))
                .as("completed_total mismatch").containsExactly(Clause.UPSTREAM_CANCELLED);

        // At most one terminal event: none and one pass, two do not.
        List<String> chunk = List.of("event:chunk\nid:0");
        assertThat(clauses(Scenario.RACE_COMPLETION_VS_CLIENT_GONE, raceObservation(chunk))).isEmpty();
        assertThat(clauses(Scenario.RACE_COMPLETION_VS_CLIENT_GONE, raceObservation(
                List.of("event:chunk\nid:0", "event:complete\ndata:x")))).isEmpty();
        assertThat(clauses(Scenario.RACE_COMPLETION_VS_CLIENT_GONE, raceObservation(
                List.of("event:chunk\nid:0", "event:complete\ndata:x", "event:error\ndata:code=CANCELLED"))))
                .as("two terminal events").containsExactly(Clause.SINGLE_TERMINAL);
        // The general "event after the first terminal" rule also fires for two terminals, so the distinct clause list
        // cannot tell the at-most-one branch apart; the raw violations can.
        assertThat(StreamContract.check(Scenario.RACE_COMPLETION_VS_CLIENT_GONE, raceObservation(
                List.of("event:chunk\nid:0", "event:complete\ndata:x", "event:error\ndata:code=CANCELLED")), 16))
                .extracting(Violation::detail).anyMatch(d -> d.contains("at most one terminal event"));

        // SHUTDOWN_CANCEL: exactly on the time bound passes, one millisecond over does not.
        Duration bound = StreamContract.DRAIN_TIMEOUT.plus(ActiveStreamRegistry.FORCED_CANCEL_WAIT)
                .plus(StreamContract.SHUTDOWN_SLACK);
        List<String> shutdownGood = List.of("event:chunk\nid:0", "event:chunk\nid:1",
                "event:error\ndata:code=CANCELLED");
        assertThat(clauses(Scenario.SHUTDOWN_CANCEL, observation(shutdownGood, true, 1, 0, 0, bound))).isEmpty();
        assertThat(clauses(Scenario.SHUTDOWN_CANCEL, observation(shutdownGood, true, 1, 0, 0,
                bound.plusMillis(1)))).as("shutdown over the bound").containsExactly(Clause.UPSTREAM_CANCELLED);
        // The counters are judged on their own, with the upstream reported as stopped.
        assertThat(clauses(Scenario.SHUTDOWN_CANCEL, observation(shutdownGood, true, 0, 0, 0, bound)))
                .as("cancelled_total mismatch alone").containsExactly(Clause.UPSTREAM_CANCELLED);
        assertThat(clauses(Scenario.SHUTDOWN_CANCEL, observation(shutdownGood, true, 1, 1, 0, bound)))
                .as("completed_total mismatch alone").containsExactly(Clause.UPSTREAM_CANCELLED);
        assertThat(clauses(Scenario.SHUTDOWN_CANCEL, observation(shutdownGood, false, 1, 0, 0, bound)))
                .as("upstream still active alone").containsExactly(Clause.UPSTREAM_CANCELLED);
    }

    /** A settled observation without release or timer problems; only the events and upstream counters vary. */
    private static Observation observation(List<String> events, boolean stopped, long cancelled, long completed,
            long faulted, Duration triggerTook) {
        return new Observation(events, -1, 1, 1, 0, 0, -1, -1, stopped, stopped ? 0 : 1, cancelled, completed,
                faulted, triggerTook);
    }

    /** A race observation: upstream is not measured there, so only events, release and timers count. */
    private static Observation raceObservation(List<String> events) {
        return new Observation(events, -1, 1, 1, 0, 0, -1, -1, null, -1, -1, -1, -1, null);
    }

    @Test
    void expectationIsComparedInBothDirections() {
        Violation upstream = new Violation(Clause.UPSTREAM_CANCELLED, "synthetic");
        // exact match passes
        StreamContract.assertMatchesExpectation(Kind.LEAKY, Scenario.CLIENT_GONE, List.of(upstream));
        StreamContract.assertMatchesExpectation(Kind.REAL, Scenario.CLIENT_GONE, List.of());
        // an expected violation that was not detected fails
        assertThatThrownBy(() -> StreamContract.assertMatchesExpectation(Kind.LEAKY, Scenario.CLIENT_GONE, List.of()))
                .isInstanceOf(AssertionError.class);
        // an unexpected violation fails, on the real implementation and on the flawed one
        assertThatThrownBy(() -> StreamContract.assertMatchesExpectation(Kind.REAL, Scenario.CLIENT_GONE,
                List.of(upstream))).isInstanceOf(AssertionError.class);
        assertThatThrownBy(() -> StreamContract.assertMatchesExpectation(Kind.LEAKY, Scenario.COMPLETION,
                List.of(new Violation(Clause.RELEASE_ONCE, "synthetic")))).isInstanceOf(AssertionError.class);
        // a different violation instead of the expected one fails too
        assertThatThrownBy(() -> StreamContract.assertMatchesExpectation(Kind.LEAKY, Scenario.CLIENT_GONE,
                List.of(new Violation(Clause.NO_TIMER_LEAK, "synthetic")))).isInstanceOf(AssertionError.class);
    }

    private static List<String> slowGood() {
        List<String> events = new ArrayList<>();
        for (int i = 0; i < 50; i++) {
            events.add("event:chunk\nid:" + i);
        }
        events.add("event:complete\ndata:x");
        return events;
    }

    private static Observation copy(Observation base, List<String> events, int leaseCloses, int permitReleases,
            int permitsHeld, int pendingTimers, boolean stopped, int maxInFlight, int maxBuffered) {
        return new Observation(events, base.eventsAtTrigger(), leaseCloses, permitReleases, permitsHeld,
                pendingTimers, maxInFlight, maxBuffered, stopped, base.active(), base.cancelled(), base.completed(),
                base.faulted(), base.triggerTook());
    }

    private static List<Clause> clauses(Scenario scenario, Observation observation) {
        return StreamContract.check(scenario, observation, scenario == Scenario.SLOW_CONSUMER ? 4 : 16).stream()
                .map(Violation::clause).distinct().toList();
    }

    // ---- scenario runner ----------------------------------------------------------------------------------------

    private SimulatorStats stats() {
        return SimulatorAdminServiceGrpc.newBlockingStub(adminChannel)
                .withDeadlineAfter(5, TimeUnit.SECONDS)
                .getStats(GetStatsRequest.getDefaultInstance());
    }

    private GenerateRequest request(Scenario scenario) {
        GenerateRequest.Builder builder = GenerateRequest.newBuilder()
                .setRequestId("contract-" + scenario)
                .setSeed(42);
        StreamProfile.Builder profile = StreamProfile.newBuilder().setFirstChunkDelayMs(1).setChunkIntervalMs(1);
        switch (scenario) {
            case COMPLETION -> profile.setChunkCount(5);
            case UPSTREAM_ERROR -> {
                profile.setChunkCount(5);
                builder.setFault(FaultSpec.newBuilder().setType(FaultType.FAULT_TYPE_INTERNAL).setAtSeq(2));
            }
            case CLIENT_GONE, SHUTDOWN_CANCEL -> {
                profile.setChunkCount(10).setChunkIntervalMs(10);
                builder.setFault(FaultSpec.newBuilder().setType(FaultType.FAULT_TYPE_HANG).setAtSeq(2));
            }
            case FIRST_CHUNK_TIMEOUT -> {
                profile.setChunkCount(5);
                builder.setFault(FaultSpec.newBuilder().setType(FaultType.FAULT_TYPE_HANG).setAtSeq(0));
            }
            case SLOW_CONSUMER -> profile.setChunkCount(50);
            default -> throw new IllegalArgumentException("not a simulator scenario: " + scenario);
        }
        return builder.setProfile(profile).build();
    }

    /** Fails as a harness error (not as a contract violation) when the scenario is not in the state it claims. */
    private static void precondition(String what, org.awaitility.core.ThrowingRunnable asserts) {
        try {
            await().pollInterval(Duration.ofMillis(10)).atMost(PRECONDITION_WAIT).untilAsserted(asserts);
        } catch (ConditionTimeoutException ex) {
            throw new AssertionError("HARNESS PRECONDITION FAILED (" + what + "): " + ex.getMessage(), ex);
        }
    }

    /** Waits for the condition for at most {@code window}; false when it did not hold in time (no exception). */
    private static boolean awaitQuietly(Duration window, Supplier<Boolean> condition) {
        try {
            await().pollInterval(Duration.ofMillis(20)).atMost(window).until(condition::get);
            return true;
        } catch (ConditionTimeoutException ex) {
            return false;
        }
    }

    /**
     * Waits until the measurement stops changing for {@link #QUIET}, then returns the last snapshot taken inside that
     * window. It encodes no expectation, so it is the same for both implementations, and anything written after the
     * signal that was waited for is part of the snapshot instead of being read later.
     */
    private static <T> T awaitStable(Supplier<T> snapshot) {
        AtomicReference<T> last = new AtomicReference<>();
        await().pollInterval(Duration.ofMillis(50)).during(QUIET).atMost(QUIET.multipliedBy(10)).until(() -> {
            T current = snapshot.get();
            T previous = last.getAndSet(current);
            return current.equals(previous);
        });
        return last.get();
    }

    private Observation runAndAssert(Kind kind, Scenario scenario) {
        CountingLease lease = new CountingLease();
        UpstreamStarter starter = (upstreamRequest, deadline, observer) -> TokenStreamServiceGrpc.newStub(channel)
                .withDeadlineAfter(deadline.toNanos(), TimeUnit.NANOSECONDS)
                .generateTokens(upstreamRequest, observer);
        return runAndAssert(kind, scenario, lease, starter);
    }

    private Observation runAndAssert(Kind kind, Scenario scenario, CountingLease lease, UpstreamStarter starter) {
        boolean slow = scenario == Scenario.SLOW_CONSUMER;
        int capacity = slow ? 4 : 16;
        RecordingEmitter emitter = slow ? RecordingEmitter.stalled() : RecordingEmitter.responsive();
        emitters.add(emitter);
        StreamTimeouts timeouts = StreamTestSupport.startedTimeouts(
                scenario == Scenario.FIRST_CHUNK_TIMEOUT ? Duration.ofMillis(300) : Duration.ofSeconds(30),
                Duration.ofSeconds(30));
        timeoutsStarted.add(timeouts);
        StreamUnderTest unit = StreamUnderTest.create(kind, new Wiring(request(scenario), emitter, lease, timeouts,
                capacity, starter, Duration.ofSeconds(60), DRAIN_TIMEOUT));

        assertThat(unit.admit()).as("HARNESS: the single permit must be admitted").isTrue();
        unit.start();

        // Preconditions: the scenario is in the state it claims to test before anything is triggered.
        precondition("upstream call started", () -> assertThat(stats().getStartedTotal()).isEqualTo(1));
        if (scenario == Scenario.CLIENT_GONE || scenario == Scenario.SHUTDOWN_CANCEL) {
            precondition("two chunks delivered, generation hanging, idle timer armed", () -> {
                assertThat(emitter.events.stream().filter(StreamContract::isChunk).count()).isEqualTo(2);
                assertThat(stats().getActiveGenerations()).isEqualTo(1);
                assertThat(unit.pendingTimers()).isEqualTo(1);
            });
        }
        if (slow) {
            precondition("simulator paused by flow control", () ->
                    assertThat(stats().getFlowControlPausesTotal()).isGreaterThanOrEqualTo(1));
        }

        int eventsAtTrigger = -1;
        Duration triggerTook = null;
        int maxInFlight = -1;
        int maxBuffered = -1;
        switch (scenario) {
            case CLIENT_GONE -> {
                eventsAtTrigger = emitter.events.size();
                unit.clientGone();
            }
            case SHUTDOWN_CANCEL -> {
                eventsAtTrigger = emitter.events.size();
                triggerTook = unit.shutdown();
            }
            case SLOW_CONSUMER -> {
                // The client reads nothing for a while: sample how far upstream got ahead of it, then let it read.
                int[] max = {0, 0};
                int[] samples = {0};
                try {
                    await().pollInterval(Duration.ofMillis(20)).during(SAMPLE_WINDOW).atMost(Duration.ofSeconds(5))
                            .until(() -> {
                                // stats first, then events: writes between the two reads cannot inflate the figure
                                long emitted = stats().getChunksEmittedTotal();
                                long written = emitter.events.stream().filter(StreamContract::isChunk).count();
                                max[0] = (int) Math.max(max[0], emitted - written);
                                max[1] = Math.max(max[1], unit.bufferedChunks());
                                samples[0]++;
                                return true;
                            });
                } finally {
                    emitter.gate.countDown();
                }
                assertThat(samples[0]).as("HARNESS: stalled-client samples").isGreaterThanOrEqualTo(5);
                assertThat(max[0]).as("HARNESS: upstream must have run ahead of the stalled client").isPositive();
                maxInFlight = max[0];
                maxBuffered = max[1];
            }
            default -> { }
        }

        Scenario current = scenario;
        boolean stopped = awaitQuietly(current.stopWindow, () -> stats().getActiveGenerations() == 0);
        int atTrigger = eventsAtTrigger;
        Duration took = triggerTook;
        int inFlight = maxInFlight;
        int buffered = maxBuffered;
        Observation observation = awaitStable(() -> {
            SimulatorStats s = stats();
            return new Observation(List.copyOf(emitter.events), atTrigger, unit.leaseCloses(), unit.permitReleases(),
                    unit.permitsHeld(), unit.pendingTimers(), inFlight, buffered, stopped, s.getActiveGenerations(),
                    s.getCancelledTotal(), s.getCompletedTotal(), s.getFaultedTotal(), took);
        });
        StreamContract.assertMatchesExpectation(kind, scenario, StreamContract.check(current, observation, capacity));
        return observation;
    }
}
