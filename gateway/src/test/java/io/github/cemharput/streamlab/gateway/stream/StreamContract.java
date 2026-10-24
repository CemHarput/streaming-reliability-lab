package io.github.cemharput.streamlab.gateway.stream;

import static org.assertj.core.api.Assertions.assertThat;

import io.github.cemharput.streamlab.gateway.stream.StreamUnderTest.Kind;
import java.lang.System.Logger.Level;
import java.time.Duration;
import java.util.ArrayList;
import java.util.EnumMap;
import java.util.EnumSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * The implementation-independent stream contract, as data plus a checker. {@link #check} turns one
 * {@link Observation} into a list of {@link Violation}s and never throws; the tests compare the violated clauses with
 * the expected ones ({@link #expected}) in both directions, so a missing expected violation and an unexpected one both
 * fail the build.
 *
 * <p>Clauses (numbers are those of the feature request):
 * <ol>
 *   <li>{@link Clause#SINGLE_TERMINAL}: exactly one terminal event, and nothing after it.</li>
 *   <li>{@link Clause#RELEASE_ONCE}: lease closed once, permit released once, no permit held at the end.</li>
 *   <li>{@link Clause#UPSTREAM_CANCELLED}: client gone, timeout and shutdown stop the upstream generation (the
 *       simulator's {@code active_generations} returns to 0); a normal end is counted by the matching counter.</li>
 *   <li>{@link Clause#NO_TIMER_LEAK}: no armed timer is left.</li>
 *   <li>{@link Clause#QUEUE_BOUNDED}: with a stalled client, neither the buffered chunks nor the chunks the simulator
 *       emitted ahead of the client exceed the queue capacity.</li>
 * </ol>
 */
final class StreamContract {

    private static final System.Logger LOGGER = System.getLogger(StreamContract.class.getName());

    /** Per-stream limits the scenarios are wired with. */
    static final Duration DRAIN_TIMEOUT = Duration.ofMillis(200);
    /** Tolerance on top of drain timeout + forced-cancel wait for a shutdown (a bound, not an expectation). */
    static final Duration SHUTDOWN_SLACK = Duration.ofSeconds(3);

    enum Clause {
        SINGLE_TERMINAL, RELEASE_ONCE, UPSTREAM_CANCELLED, NO_TIMER_LEAK, QUEUE_BOUNDED
    }

    record Violation(Clause clause, String detail) {
        @Override
        public String toString() {
            return clause + ": " + detail;
        }
    }

    /** What the terminal part of the event log must look like. */
    enum Shape {
        /** One complete event after exactly {@code chunks} chunk events. */
        COMPLETE,
        /** One error event with {@code errorCode} after exactly {@code chunks} chunk events. */
        ERROR,
        /** Nothing may be sent after the trigger (client gone). */
        NONE_AFTER_TRIGGER,
        /** Exactly one terminal event of any kind (race with shutdown). */
        EXACTLY_ONE_ANY,
        /** At most one terminal event (race with client gone: the client may have left before it). */
        AT_MOST_ONE
    }

    /** What the simulator counters must show once the stream is over. */
    enum UpstreamEnd {
        NOT_MEASURED, COMPLETED, FAULTED, CANCELLED
    }

    enum Scenario {
        COMPLETION(Shape.COMPLETE, 5, null, UpstreamEnd.COMPLETED, Duration.ofSeconds(10), false),
        UPSTREAM_ERROR(Shape.ERROR, 2, "INTERNAL", UpstreamEnd.FAULTED, Duration.ofSeconds(10), false),
        CLIENT_GONE(Shape.NONE_AFTER_TRIGGER, 2, null, UpstreamEnd.CANCELLED, Duration.ofSeconds(2), false),
        SHUTDOWN_CANCEL(Shape.ERROR, 2, "CANCELLED", UpstreamEnd.CANCELLED, Duration.ofSeconds(2), false),
        FIRST_CHUNK_TIMEOUT(Shape.ERROR, 0, "DEADLINE_EXCEEDED", UpstreamEnd.CANCELLED, Duration.ofSeconds(10), false),
        SLOW_CONSUMER(Shape.COMPLETE, 50, null, UpstreamEnd.COMPLETED, Duration.ofSeconds(10), true),
        RACE_COMPLETION_VS_SHUTDOWN(Shape.EXACTLY_ONE_ANY, -1, null, UpstreamEnd.NOT_MEASURED, null, false),
        RACE_COMPLETION_VS_CLIENT_GONE(Shape.AT_MOST_ONE, -1, null, UpstreamEnd.NOT_MEASURED, null, false);

        final Shape shape;
        final int chunks;
        final String errorCode;
        final UpstreamEnd upstreamEnd;
        /** How long the simulator gets to show active_generations == 0; null when upstream is not measured. */
        final Duration stopWindow;
        final boolean queueMeasured;

        Scenario(Shape shape, int chunks, String errorCode, UpstreamEnd upstreamEnd, Duration stopWindow,
                boolean queueMeasured) {
            this.shape = shape;
            this.chunks = chunks;
            this.errorCode = errorCode;
            this.upstreamEnd = upstreamEnd;
            this.stopWindow = stopWindow;
            this.queueMeasured = queueMeasured;
        }
    }

    /**
     * One settled measurement. Fields a scenario does not measure hold -1 / null. {@code eventsAtTrigger} is the number
     * of events logged when the trigger fired; {@code triggerTook} is the duration of the shutdown call.
     */
    record Observation(List<String> events, int eventsAtTrigger, int leaseCloses, int permitReleases, int permitsHeld,
            int pendingTimers, int maxInFlight, int maxBuffered, Boolean upstreamStopped, long active, long cancelled,
            long completed, long faulted, Duration triggerTook) {
    }

    /**
     * Expected violated clauses of the flawed handler per scenario. Exactly the two flaws show up here: (a) client gone
     * does not cancel upstream, (b) the terminal step is check-then-act, so completion racing a cancel produces two
     * terminal events and a double release, and completion racing client-gone produces a double release.
     */
    static final Map<Scenario, Set<Clause>> EXPECTED_LEAKY = leakyExpectations();

    private static Map<Scenario, Set<Clause>> leakyExpectations() {
        Map<Scenario, Set<Clause>> map = new EnumMap<>(Scenario.class);
        for (Scenario scenario : Scenario.values()) {
            map.put(scenario, Set.of());
        }
        map.put(Scenario.CLIENT_GONE, Set.of(Clause.UPSTREAM_CANCELLED));
        map.put(Scenario.RACE_COMPLETION_VS_SHUTDOWN, Set.of(Clause.SINGLE_TERMINAL, Clause.RELEASE_ONCE));
        map.put(Scenario.RACE_COMPLETION_VS_CLIENT_GONE, Set.of(Clause.RELEASE_ONCE));
        return map;
    }

    /** The real implementation must satisfy the whole contract: no expected violation at all. */
    static Set<Clause> expected(Kind kind, Scenario scenario) {
        return kind == Kind.REAL ? Set.of() : EXPECTED_LEAKY.get(scenario);
    }

    static boolean isTerminal(String event) {
        return event.contains("event:complete") || event.contains("event:error");
    }

    static boolean isChunk(String event) {
        return event.contains("event:chunk");
    }

    /** Never throws: a malformed observation is reported as violations, not as an exception. */
    static List<Violation> check(Scenario scenario, Observation o, int capacity) {
        List<Violation> violations = new ArrayList<>();
        checkSingleTerminal(scenario, o, violations);
        checkReleaseOnce(o, violations);
        if (scenario.upstreamEnd != UpstreamEnd.NOT_MEASURED) {
            checkUpstream(scenario, o, violations);
        }
        if (o.pendingTimers() != 0) {
            violations.add(new Violation(Clause.NO_TIMER_LEAK, "pendingTimers=" + o.pendingTimers()));
        }
        if (scenario.queueMeasured) {
            if (o.maxInFlight() > capacity || o.maxBuffered() > capacity) {
                violations.add(new Violation(Clause.QUEUE_BOUNDED, "maxInFlight=" + o.maxInFlight()
                        + " maxBuffered=" + o.maxBuffered() + " capacity=" + capacity));
            }
        }
        return violations;
    }

    private static void checkSingleTerminal(Scenario scenario, Observation o, List<Violation> violations) {
        List<String> events = o.events();
        int terminals = (int) events.stream().filter(StreamContract::isTerminal).count();
        int chunks = (int) events.stream().filter(StreamContract::isChunk).count();
        int firstTerminal = -1;
        for (int i = 0; i < events.size(); i++) {
            if (isTerminal(events.get(i))) {
                firstTerminal = i;
                break;
            }
        }
        if (firstTerminal >= 0 && firstTerminal != events.size() - 1) {
            violations.add(new Violation(Clause.SINGLE_TERMINAL, (events.size() - 1 - firstTerminal)
                    + " event(s) after the first terminal event (" + terminals + " terminal events in total)"));
        }
        switch (scenario.shape) {
            case COMPLETE -> {
                if (terminals != 1 || !hasTerminalOfKind(events, "event:complete")) {
                    violations.add(new Violation(Clause.SINGLE_TERMINAL,
                            "expected exactly one complete event, saw " + terminals + " terminal event(s): " + names(events)));
                } else if (chunks != scenario.chunks) {
                    violations.add(new Violation(Clause.SINGLE_TERMINAL,
                            "expected " + scenario.chunks + " chunk events, saw " + chunks));
                }
            }
            case ERROR -> {
                if (terminals != 1 || !hasTerminalOfKind(events, "event:error")
                        || events.stream().noneMatch(e -> isTerminal(e) && e.contains("code=" + scenario.errorCode))) {
                    violations.add(new Violation(Clause.SINGLE_TERMINAL, "expected exactly one error event with code "
                            + scenario.errorCode + ", saw " + terminals + " terminal event(s): " + names(events)));
                } else if (chunks != scenario.chunks) {
                    violations.add(new Violation(Clause.SINGLE_TERMINAL,
                            "expected " + scenario.chunks + " chunk events, saw " + chunks));
                }
            }
            case NONE_AFTER_TRIGGER -> {
                if (events.size() != o.eventsAtTrigger() || terminals != 0) {
                    violations.add(new Violation(Clause.SINGLE_TERMINAL, "client was gone after "
                            + o.eventsAtTrigger() + " events, but the log has " + events.size() + " (" + terminals
                            + " terminal)"));
                }
            }
            case EXACTLY_ONE_ANY -> {
                if (terminals != 1) {
                    violations.add(new Violation(Clause.SINGLE_TERMINAL,
                            "expected exactly one terminal event, saw " + terminals + ": " + names(events)));
                }
            }
            case AT_MOST_ONE -> {
                if (terminals > 1) {
                    violations.add(new Violation(Clause.SINGLE_TERMINAL,
                            "expected at most one terminal event, saw " + terminals + ": " + names(events)));
                }
            }
        }
    }

    /** Short form of an event log for messages: chunk events by name, terminal events with their payload. */
    static List<String> names(List<String> events) {
        return events.stream()
                .map(e -> isTerminal(e) ? e.lines().collect(Collectors.joining(" ")) : e.lines().findFirst().orElse(""))
                .toList();
    }

    private static boolean hasTerminalOfKind(List<String> events, String kind) {
        return events.stream().anyMatch(e -> e.contains(kind));
    }

    private static void checkReleaseOnce(Observation o, List<Violation> violations) {
        if (o.leaseCloses() != 1 || o.permitReleases() != 1 || o.permitsHeld() != 0) {
            violations.add(new Violation(Clause.RELEASE_ONCE, "leaseCloses=" + o.leaseCloses() + " permitReleases="
                    + o.permitReleases() + " permitsHeld=" + o.permitsHeld()));
        }
    }

    private static void checkUpstream(Scenario scenario, Observation o, List<Violation> violations) {
        List<String> problems = new ArrayList<>();
        if (!Boolean.TRUE.equals(o.upstreamStopped())) {
            problems.add("active_generations=" + o.active() + " after " + scenario.stopWindow.toSeconds()
                    + " s (expected 0)");
        }
        boolean cancelExpected = scenario.upstreamEnd == UpstreamEnd.CANCELLED;
        if (cancelExpected && (o.cancelled() != 1 || o.completed() != 0)) {
            problems.add("cancelled_total=" + o.cancelled() + " completed_total=" + o.completed()
                    + " (expected 1 and 0)");
        }
        if (scenario.upstreamEnd == UpstreamEnd.COMPLETED && o.completed() != 1) {
            problems.add("completed_total=" + o.completed() + " (expected 1)");
        }
        if (scenario.upstreamEnd == UpstreamEnd.FAULTED && o.faulted() != 1) {
            problems.add("faulted_total=" + o.faulted() + " (expected 1)");
        }
        if (scenario == Scenario.SHUTDOWN_CANCEL && o.triggerTook() != null) {
            Duration bound = DRAIN_TIMEOUT.plus(ActiveStreamRegistry.FORCED_CANCEL_WAIT).plus(SHUTDOWN_SLACK);
            if (o.triggerTook().compareTo(bound) > 0) {
                problems.add("shutdown took " + o.triggerTook().toMillis() + " ms (bound " + bound.toMillis() + " ms)");
            }
        }
        if (!problems.isEmpty()) {
            violations.add(new Violation(Clause.UPSTREAM_CANCELLED, String.join("; ", problems)));
        }
    }

    static Set<Clause> clausesOf(List<Violation> violations) {
        return violations.stream().map(Violation::clause).collect(Collectors.toCollection(
                () -> EnumSet.noneOf(Clause.class)));
    }

    static String report(Kind kind, Scenario scenario, List<Violation> violations) {
        return kind + " / " + scenario + ": expected violated clauses " + expected(kind, scenario)
                + ", detected " + clausesOf(violations) + " " + violations;
    }

    /**
     * Set equality between the detected and the expected clauses: an expected violation that did not show up fails
     * (the harness would be blind to the flaw), and so does a violation nobody expected. Also logs every violation,
     * which is how the flawed handler's findings show up in the build output.
     */
    static void assertMatchesExpectation(Kind kind, Scenario scenario, List<Violation> violations) {
        String report = report(kind, scenario, violations);
        for (Violation violation : violations) {
            LOGGER.log(Level.INFO, "contract violation [{0} / {1}] {2}", kind, scenario, violation);
        }
        if (violations.isEmpty()) {
            LOGGER.log(Level.INFO, "contract satisfied [{0} / {1}]", kind, scenario);
        }
        assertThat(clausesOf(violations)).as(report).isEqualTo(expected(kind, scenario));
    }

    private StreamContract() {
    }
}
