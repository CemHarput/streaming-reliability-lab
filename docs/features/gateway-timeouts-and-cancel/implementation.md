# Implementation: gateway-timeouts-and-cancel

Plan hash: f100ccc86c145ffec0b378fd73e2f2ec67faebefeb0249ee62a6650eb411fc35
Baseline HEAD: fdf7b85d56c11ade5db80c839be0ec34c49e12b3 (branch claude/changes)
Result: implemented (all acceptance criteria reported met, required checks passing)

Not committed. Review status is recorded in review.md once it exists.

## Summary
The gateway now has a first-chunk timeout and an idle-between-chunks timeout. They run on one shared `StreamTimeouts` scheduler (SmartLifecycle, phase DEFAULT_PHASE-768) with a leaf-locked per-stream `StreamTimer`. A timeout becomes a terminal outcome through the existing CAS claim: every CAS site now goes through a `claim()` helper that also closes the timer. The expiry path runs on the scheduler thread without holding a lock: claim, cancel upstream, `releaseCapacity()`, metric, `discardQueued()`, terminal write on the terminal-event executor (`DEADLINE_EXCEEDED`), `finish`. Idle timeouts measure upstream silence only: the idle timer is armed only while the gateway has granted upstream credit that is still outstanding. Cancellation of upstream is proven in tests through the simulator's own `active_generations` / `cancelled_total` for client disconnect, first-chunk timeout, idle timeout and shutdown. `proto/`, `simulator/`, `pom.xml` and `upstream/*` are unchanged.

## Files
Modified (under `gateway/src/...`, package `io/github/cemharput/streamlab/gateway`, except README):
- `README.md`: status line, module table, new "Timeouts and cancellation" subsection, `timeouts_total` metric row, two settings rows, Known limits a..f
- `main/.../config/GatewayProperties.java`: `Timeouts` record (5th component); top-level compact constructor rejects a timeout >= `upstream.stream-deadline`
- `main/.../LifecyclePhases.java`: `STREAM_TIMEOUTS = DEFAULT_PHASE-768` and javadoc
- `main/.../stream/StreamBridge.java`: outcomes `FIRST_CHUNK_TIMEOUT` / `IDLE_TIMEOUT`, `claim()`, `timedOut()`, timer hooks, `cancelMessage` switch, 7th constructor parameter, `timerArmed()`
- `main/.../stream/StreamController.java`: injects `StreamTimeouts` into the bridge
- `main/.../stream/StreamMetrics.java`: pre-registered `timeouts_total{kind}` and `timedOut(TimeoutKind)`
- `main/resources/application.yaml`: `lab.timeouts` 30s/30s
- Tests: `ConnectionModeConfigTest`, `stream/StreamTestSupport`, `stream/UpstreamFlowControlTest`, `testsupport/InProcessSimulatorConfig` (`stats()`)

Created:
- `main/.../stream/TimeoutKind.java`, `StreamTimeouts.java`, `StreamTimer.java`
- `test/.../stream/StreamTimerTest.java` (9 tests), `StreamBridgeTimeoutTest.java` (15 tests incl. parameterized), `test/.../gateway/TimeoutAndCancelTest.java` (8 tests)

## Acceptance mapping
| AC | Evidence (tests) |
|---|---|
| AC1 | `StreamBridgeTimeoutTest#firstChunkTimeoutClaimsOnceCancelsUpstreamAndReleasesCapacity`; `TimeoutAndCancelTest#firstChunkTimeoutOnHangCancelsUpstream`, `#slowFirstChunkTimesOut` |
| AC2 | `TimeoutAndCancelTest#idleTimeoutOnHangMidStreamCancelsUpstream`, `#slowIntervalTimesOut` |
| AC3 | (a) `StreamBridgeTimeoutTest#eachReceivedChunkResetsIdleTimer`; (b) `#idleTimerPausedWhileCreditExhausted` and `UpstreamFlowControlTest#stalledClientDoesNotTriggerIdleTimeout` (real gRPC); (c) `#upstreamEndAndLastChunkDisarmTimer`, `StreamTimerTest#lastChunkAndUpstreamEndLeaveNothingArmed`; (d) `StreamTimerTest#firstChunkArmedOnlyOnStart` |
| AC4 | `TimeoutAndCancelTest#clientDisconnectCancelsUpstream`, `#firstChunkTimeoutOnHangCancelsUpstream`, `#idleTimeoutOnHangMidStreamCancelsUpstream`, `#hangWithoutCancelStaysActiveUntilShutdownCancels` (shutdown cancel, with the negative control: `active_generations` stays 1 for 2 s and `pendingTimers() == 1`) |
| AC5 | `StreamBridgeTimeoutTest#timeoutDoesNotWaitForStalledClientWrite`, `#timersOfOtherStreamsFireWhileOneTerminalWriteIsStuck`, `#timeoutThenShutdownCancelGivesOneTerminal`, `#shutdownCancelThenTimerNeverFires`, `#lateUpstreamCallbacksAfterTimeoutSendNothing` (lease closed exactly once in each) |
| AC6 | `StreamBridgeTimeoutTest#noTimerLeftAfterAnyTerminalOutcome` (COMPLETED, UPSTREAM_ERROR, CLIENT_GONE, SHUTDOWN, OVERFLOW); `StreamTimerTest` (replace on first chunk, re-arm on credit, idempotent close, stale expiry ignored, stopped scheduler ignored, stop within 1 s); `GracefulShutdownTest` and `OverloadPolicyTest` unchanged and green |
| AC7 | `TimeoutAndCancelTest#timeoutsCounterIsPreRegisteredAndExposed` (0 at start, `/actuator/metrics/timeouts_total` 200); counter values asserted in the timeout tests |
| AC8 | `ConnectionModeConfigTest#codeDefaultsApplyWithoutApplicationYaml` (30s/30s), `#timeoutsAreBoundFromRelaxedNames`, `#invalidTimeoutsFailAtStartup` (5 parameters), `#timeoutNotShorterThanDeadlineFailsAtStartup` |
| AC9 | `./mvnw -B verify` green; `git diff --stat -- proto simulator` empty; no `Thread.sleep` under `gateway/src` |
| AC10 | README read: section, tables, Known limits a..f, hypotheses marked as hypotheses |

## Checks
- Implementer's report: `./mvnw -B verify` green (simulator 12/12, gateway 89/89), `./mvnw -B -pl gateway -am test` twice green, focused command 80/80.
- Coordinator re-ran independently: `./mvnw -B verify` exit 0 (simulator 12/12; gateway 89/89: ConnectionModeConfigTest 18, GracefulShutdownTest 2, OverloadPolicyTest 4, ReadinessDownTest 1, ActiveStreamRegistryTest 9, StreamBridgeTest 13, StreamBridgeTimeoutTest 15, StreamTimerTest 9, UpstreamFlowControlTest 2, StreamEndToEndTest 8, TimeoutAndCancelTest 8); `./mvnw -B -pl gateway -am test` twice more (see the review rounds below for later runs); `git diff --stat -- proto simulator` empty; `grep -rn Thread.sleep gateway/src`: no matches; `feature_state.py check`: in_progress, `approval_matches: true`.
- Not run: any test with a real TCP-level stalled client.

## Deviations from the plan and verified assumptions
- Verified in the jars: grpc-stub 1.81.0 calls `beforeStart` in the adapter constructor, before `call.start`, on the calling thread; Boot 4.1.1 `WebServerApplicationContext` constants are `GRACEFUL_SHUTDOWN_PHASE = MAX-1024` and `START_STOP_LIFECYCLE_PHASE = MAX-2048` (package `org.springframework.boot.web.server.context`), so phase -768 lies between the registry (-512) and the web graceful phase (-1024); `removeOnCancel` makes `pendingTimers()` reliable in the tests.
- Not inspected separately: `DefaultLifecycleProcessor` behaviour for a not-running bean; the shutdown test calls `registry.stop()` by hand and then `context.close()` ended without error.
- `StreamTimeouts.stop()` uses `shutdownNow()`, which may interrupt an expiry task that is running at that moment (accepted).
- Some timeouts in unit tests are 500 ms to 1 s instead of 300 ms, for CI margin; as a consequence `StreamBridgeTimeoutTest` takes about 23 s.
- `staleExpiryIsIgnored` uses a test subclass of `StreamTimeouts` (`CapturingTimeouts`) to run an already-queued stale task by hand.
- `timedOut`, like `overflow`, ends with `scheduleDrain()` so that a chunk offered after the discard is not left counted in the queue metric (not in the plan's list).

## Limitations
- `GracefulShutdownTest` has no `@Timeout` (it had none before; not changed here).
- On a timeout, chunks still waiting in the stream queue are discarded (plan D4); chunks already written come before the `error`. With a slow client, queued chunks are lost. README states this.
- The timers do not detect a client-side stalled write; with exhausted credit the idle timer is paused by design. The emitter `onTimeout` hypothesis and whether Tomcat detects a client disconnect without a write are unverified (README).
- A chunk arriving at the exact boundary can lose the race to the timer (accepted, README).
- Not reviewed independently yet; the previous two features needed multiple review rounds in the same classes.

## First-review fixes (after review.md pass 1, same plan)
Addressed the two P3 findings and the cheap observation of the first review; only `gateway/` and README.md changed (`proto/`, `simulator/`, `pom.xml`, `upstream/*` untouched).

- **P3-1 (README incomplete for the stuck intermediate write):** README now lists the idle timeout as a fourth way such a stream gives up its permit (only while upstream credit is outstanding and upstream stays silent; not once credit is exhausted or upstream has ended), the "Known limits" sentence that said the case "stays as described" is corrected, and the list of permit-releasing outcomes now includes timeout and queue overflow.
- **P3-2 (narrow test window):** in `StreamBridgeTimeoutTest#timeoutDoesNotWaitForStalledClientWrite` the cancel message, lease close, permit count, tracked count and `timeouts_total{idle}` assertions are all inside one `await().untilAsserted(...)`, because `timedOut()` cancels upstream before it releases capacity and counts the metric.
- **N1 (`LifecyclePhases` javadoc):** the class javadoc now says a lower phase starts first and a higher phase stops first, consistent with the stop-order paragraph and the `STREAM_TIMEOUTS` comment.
- **Not changed (optional observations):** N2 (a bridge-level test that an admitted bridge that never saw `beforeStart` has no armed timer; the real evidence for "`claim()` closes the timer" comes from the CLIENT_GONE, SHUTDOWN and timeout paths and the only CAS site is `claim()`), and N3 (the extra `scheduleDrain()` at the end of `timedOut` is unnecessary but harmless; it stays).

Checks after these fixes: see the review rounds recorded in review.md and the final numbers below.

## Second-review fixes (after review.md pass 2, same plan)
The second review found no behavioural defect and no production-logic change was needed; it reported one more test-stability finding of the same class as the first review's P3-2, plus two cosmetic README notes. All fixed here; only `gateway/` tests and README.md changed.

- **P3-1 (test window):** in `StreamBridgeTimeoutTest#idleTimerPausedWhileCreditExhausted` the check `bridge.timerArmed()` and the check `upstream.requested >= 1` are now awaited together in one `untilAsserted`. The bridge arms the timer (`creditGranted`) just before it calls `request(1)`, so waiting only for `timerArmed` and then asserting `requested` without waiting could fail on a loaded runner.
- **README notes:** the stuck-intermediate-write item now says the idle timeout does not claim the stream once credit is exhausted, the last chunk has been received, or upstream has ended; the idle row of the timer table now says "at the first chunk received, if credit is still outstanding then".

Checks after these fixes: `./mvnw -B verify` exit 0 (simulator 12/12, gateway 89/89); `./mvnw -B -pl gateway -am test` three more times, each 89/89; `git diff --stat -- proto simulator` empty; no `Thread.sleep` under `gateway/src`.

## Third-review fixes (after review.md pass 3, same plan)
Pass 3 swept every new test method; it found no production defect and two test-margin findings, plus one cosmetic README note. All fixed; only `gateway/` tests and README.md changed.

- **P3-1:** `StreamBridgeTimeoutTest#upstreamEndAndLastChunkDisarmTimer` waits for `sendInProgress` with a 5 ms poll interval (the 300 ms idle timer is already running at that point; the default 100 ms poll had eaten about a third of the margin).
- **P3-2:** `StreamBridgeTimeoutTest#eachReceivedChunkResetsIdleTimer` now uses idle 2 s and 8 gaps of 300 ms (`during(300ms)`, 20 ms poll), total about 2.4 s > 2 s, each gap about a seventh of the limit (was 1 s / 4 x 400 ms, about 2-2.5x). Expected event count updated to 9 (8 chunks + the idle error).
- **README note:** the Known-limits item now also says the idle timeout does not claim the stream after the last chunk has been received or upstream has ended.

Checks after these fixes: `./mvnw -B verify` exit 0 (simulator 12/12, gateway 89/89); `./mvnw -B -pl gateway -am test` three more times, each 89/89; `git diff --stat -- proto simulator` empty; no `Thread.sleep` under `gateway/src`.
