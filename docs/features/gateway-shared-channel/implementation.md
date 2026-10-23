# Implementation: gateway-shared-channel

Plan hash: 3a489d47a49d2b8b66e2a77e1d9333a9b884cc8a5ca6866b82b173e80ca62bca
Baseline HEAD: 1ba2e38b7e9e823415bfc0c7472dd2f5d89f27b4 (branch claude/changes)
Result: implemented (all acceptance criteria met, required checks passing)

Not committed. Not reviewed (`/feature-review` has not been run).

## Summary
Gateway bridges `GET /stream` to the simulator's `GenerateTokens` over a single lifecycle-managed gRPC `ManagedChannel` and emits SSE `chunk` / `complete` / `error` events. Readiness includes `upstreamChannel`. Graceful shutdown: reject new requests (503 + Retry-After), drain active streams up to the drain timeout, cancel the rest with `CANCELLED`, then close the channel. `proto/` and `simulator/` are unchanged.

## Files
Modified:
- `gateway/pom.xml` (test deps: spring-boot-starter-test, simulator, grpc-inprocess, awaitility)
- `gateway/src/main/resources/application.yaml` (`lab.*` settings, readiness group)
- `README.md` (gateway section, module table, status note)

Created (main, `gateway/src/main/java/io/github/cemharput/streamlab/gateway/`):
- `LifecyclePhases.java`
- `config/GatewayProperties.java`
- `upstream/ChannelProvider.java`, `ManagedChannelFactory.java`, `SharedChannelProvider.java`, `UpstreamChannelHealthIndicator.java`, `UpstreamConfiguration.java`
- `stream/ActiveStreamRegistry.java`, `StreamBridge.java`, `StreamParameters.java`, `FaultParam.java`, `SseEvents.java`, `StreamController.java`

Created (test):
- `gateway/src/test/java/io/github/cemharput/streamlab/testsupport/InProcessSimulatorConfig.java`, `SseClient.java`, `SseAssertions.java`
- `gateway/src/test/java/io/github/cemharput/streamlab/gateway/StreamEndToEndTest.java`, `ReadinessDownTest.java`, `GracefulShutdownTest.java`, `ConnectionModeConfigTest.java`

## Acceptance mapping
| AC | Evidence |
|---|---|
| AC1 | `StreamEndToEndTest#successStream` |
| AC2 | `#unavailableMidStream`, `#internalAtFirstChunk`, `#invalidChunkCount` |
| AC3 | `#badParameterRejected` (`fault=FOO`, `chunks=abc` -> 400) |
| AC4 | `#readinessUp` (test sets `show-components=always` to see `upstreamChannel`) |
| AC5 | `ReadinessDownTest` (readiness 503, liveness 200) |
| AC6 | `GracefulShutdownTest#activeStreamDrains` |
| AC7 | `GracefulShutdownTest#newRequestsRejectedAndHangingStreamCancelled` (503 + Retry-After during drain proves STREAM_ADMISSION ordering; HANG stream gets one `CANCELLED` error) |
| AC8 | `ConnectionModeConfigTest` (default SHARED starts; `per-request` fails at startup) |
| AC9 | `SseAssertions.assertSingleTerminal` used in all stream tests |
| AC10 | `git diff --stat -- proto simulator` empty; `./mvnw -B verify` BUILD SUCCESS |

## Checks (coordinator re-ran independently)
- `./mvnw -B verify`: exit 0. simulator 12/12, gateway 11/11 (ConnectionModeConfigTest 2, GracefulShutdownTest 2, ReadinessDownTest 1, StreamEndToEndTest 6), 0 failures/errors/skips.
- `git diff --stat -- proto simulator`: empty.
- `grep -rn Thread.sleep gateway/src`: no matches.
- `feature_state.py check`: plan hash still approved.
- Not run: the single-class command (`-Dtest=StreamEndToEndTest`), covered by the full build.
- The implementer's first focused run had 9 failures before two fixes (see deviations); the final runs are green.

## Deviations from the plan
- All assumed Spring Boot 4.1.1 package locations were verified in the jars; no import changes were needed. `WebServerGracefulShutdownLifecycle` phase verified as `DEFAULT_PHASE-1024`; no phase change.
- `@RequestParam` names are explicit (`name = "chunks"` etc.) because the parent POM does not pass `-parameters`; parent POM untouched.
- Test overrides of `lab.*` use `run("--lab....")` arguments instead of `.properties(...)`, because `SpringApplicationBuilder.properties` writes to default properties and `application.yaml` overrode them.
- Added `SseAssertions` test helper (not in the plan).

## Limitations and remaining work
- Out of scope and not done: bounded queue, semaphore, retry/reconnect, cancel-chain verification through simulator stats, loadgen, MQTT, per-request mode.
- The client-disconnect path (emitter `onCompletion`/`onTimeout`/`onError` cancels upstream) exists in code but has no dedicated test.
- (Superseded) The original report claimed a slow client's send delay on shutdown was "bounded by the drain timeout". The review showed that was wrong; fixed below.
- A slow SSE client blocks a gRPC callback thread; deferred to the bounded-concurrency feature.
- A client whose write is stuck can still miss its terminal event (the gateway waits at most `StreamBridge.SEND_LOCK_WAIT`, 5s, then logs and skips it; the emitter then ends via its own timeout, stream deadline + 5s). Correction after the second review: the registry phase of shutdown is no longer delayed by such a client, but this open response may stretch the web server's own graceful-shutdown phase (`timeout-per-shutdown-phase`, 20s). That Tomcat behaviour is a hypothesis, not verified. The limit is documented in README ("Known limits").
- Worst-case `close()` bound is drain + 2s terminal-event wait + channel termination (5s) + 2s after `shutdownNow` (the plan's AC7 text omitted the last 2s). The test asserts the observed bound (drain <= close < 7s for a 3s drain).

## Post-review fixes (after review.md, same plan)
Addressed review.md findings P2 and all P3s. Nothing outside gateway/ and README.md changed; `proto/` and `simulator/` untouched.

- **P2 (stalled client breaks shutdown bound):** `StreamBridge` no longer keeps terminal state under a lock that a blocking write can hold. The terminal outcome is an `AtomicReference` claimed by compare-and-set; the winner cancels upstream and frees the registry slot first. Only the terminal SSE event is written under a separate `sendLock` (keeps "nothing after the terminal event"), with `tryLock(SEND_LOCK_WAIT)`. `cancelForShutdown()` now returns a `CompletableFuture` finished on a virtual thread; `ActiveStreamRegistry.stop()` cancels all streams in parallel and waits at most `FORCED_CANCEL_WAIT` (2s) in total. The emitter callbacks (`onCompletion`/`onTimeout`/`onError`) only use the atomic claim, which also removes the hypothesised lock-ordering risk with Spring's internal async locks (that hypothesis was never verified).
- **P3 registry interrupt:** `stop()` still force-cancels remaining streams when interrupted, then restores the interrupt flag (`ActiveStreamRegistryTest#interruptedStopStillCancelsRemainingStreams`).
- **P3 negative `faultAtSeq`:** rejected with HTTP 400 in `StreamController` (`#negativeFaultAtSeqRejectedInsteadOfSilentlyIgnored`); README updated.
- **P3 AC8 default test:** `ConnectionModeConfigTest#codeDefaultsApplyWithoutApplicationYaml` runs with `--spring.config.name=nonexistent`, so only `@DefaultValue` can supply the values.
- **P3 AC7 bound:** `GracefulShutdownTest` measures the `close()` duration.
- **P3 client disconnect:** unit test `StreamBridgeTest#failedClientWriteCancelsUpstreamAndFreesTheSlot` and end-to-end `StreamEndToEndTest#clientDisconnectFreesTheStreamSlot`.
- **Review test nits:** AC1 now checks `requestId` on the `complete` event; AC2 checks the `INVALID_ARGUMENT` message; AC6 asserts `activeCount() == 1` when shutdown starts; the 400 for `fault=FOO` is also checked with `Accept: text/event-stream` (the review's second unverified hypothesis: it holds).
- New tests: `StreamBridgeTest` (4), `ActiveStreamRegistryTest` (2: interrupted stop; two stalled clients do not stretch shutdown beyond drain + 2s), plus the additions above. Test support: `StreamTestSupport`, `SseClient#disconnect`, `SseClient#getAcceptingEventStream`.

Checks after the fixes: `./mvnw -B verify` exit 0 (simulator 12/12, gateway 20/20, no failures/errors/skips); `./mvnw -B -pl gateway -am test` repeated twice, both 20/20; `git diff --stat -- proto simulator` empty; no `Thread.sleep` under `gateway/src`; `feature_state.py check` still `implemented` with `approval_matches: true`.

Limits of these checks: the new P2 test uses a fake emitter whose send blocks; it does not reproduce a real TCP-level stalled Tomcat connection. The earlier review (review.md) covered the code before these fixes; it has not been re-run, so its verdict does not cover the fixed code.

## Second-pass fixes (after the second review, same plan)
Addressed P3-1, P3-2, P3-3 and the nits of the second review.md pass. Only `gateway/`, README.md and this report changed; `proto/` and `simulator/` untouched.

- **P3-1 (stalled client, emitter not completed):** documented, not changed in code. README now has "Known limits" (the 5s terminal-event give-up, emitter timeout, possible 20s web-phase extension marked as unverified, worst-case `close()` formula, fake-emitter test limit). The "exactly one terminal event" sentence in README now names the exception; the incorrect statement in this report's Limitations section is corrected above.
- **P3-2 (hanging regression tests):** `@Timeout(10)` on `StreamBridgeTest` and `@Timeout(15)` on `ActiveStreamRegistryTest`; the blocking gates are released in `finally`.
- **P3-3 (last chunk vs shutdown):** `StreamBridge.onNext` now claims COMPLETED before writing the final chunk; if the write fails the slot is freed as CLIENT_GONE without cancelling the already-completed upstream call. New tests: `StreamBridgeTest#shutdownCannotCancelAStreamWhoseFinalChunkIsBeingWritten` (fails on the old order, where shutdown cancels upstream) and `#failedWriteOfTheFinalChunkStillFreesTheSlot`. Consequence documented in README: a stream claimed by shutdown before its final chunk ends with `error` / `CANCELLED` instead of the last chunk.
- **Nits:** interrupt during the terminal-event lock wait now still sends the event when the lock is free and restores the interrupt flag afterwards; `StreamParameters` javadoc updated; `GracefulShutdownTest` carries the close duration in the future (`supplyAsync`, so a failing `close()` is reported as that failure); `ConnectionModeConfigTest` asserts the `streamDeadline` and `channelTerminationTimeout` defaults.
- **Not addressed:** the nit that the emitter `onError`/`onCompletion` callback path is not separately proven. Spring's `ResponseBodyEmitter.Handler` is package-private, so a unit test cannot drive those callbacks; the `onNext` IOException path and the end-to-end disconnect test cover the same cleanup.

Checks after these fixes: `./mvnw -B verify` exit 0 (simulator 12/12, gateway 22/22, no failures/errors/skips); `./mvnw -B -pl gateway -am test` repeated three times, each 22/22; `git diff --stat -- proto simulator` empty; no `Thread.sleep` under `gateway/src`; `feature_state.py check` still `implemented`, `approval_matches: true`.

Limits: the second review (review.md) covered the code before these last fixes and has not been re-run; its verdict does not cover them. The P3-3 behaviour change touches the same class that review examined most closely.

## Third-pass fixes (after the third review, same plan)
Addressed P3-A, P3-B, P3-C and the nits of the third review.md pass. Only `gateway/`, README.md and this report changed; `proto/` and `simulator/` untouched. No new review was run, as requested.

- **P3-A (slot held while the final write is stuck):** README now states the exception: a stream whose `COMPLETED` outcome is claimed (final chunk or `complete` write in progress) cannot be cancelled by shutdown, keeps its slot and lease until the write returns, and neither shutdown nor the emitter timeout can free them. The sentence "once the final chunk is being written the stream completes normally" now says it ends with `complete` on success or is freed as a client disconnect (no `complete` event) on a failed write. `ActiveStreamRegistry.stop()` logs a separate WARNING ("N stream(s) still hold a slot after forced cancel ...") after the drain and forced cancel, and the earlier "cancelling" warning now says "cancelling those that can be cancelled". The behaviour is pinned by the new `ActiveStreamRegistryTest#streamWithAFinalWriteInProgressKeepsItsSlotButDoesNotDelayStop` (stop() returns after the drain, nothing cancelled upstream, slot count 1 until the write returns, then 0). The `released` flag that would let the emitter timeout free the slot was NOT added; it is left to the feature that makes the slot matter (per-request lease or admission limit).
- **P3-B (`@Timeout` and lock waits):** `StreamBridgeTest` and `ActiveStreamRegistryTest` use `@Timeout(..., threadMode = SEPARATE_THREAD)`, with comments saying why. This follows JUnit's documented semantics (the test is reported failed after the timeout even if its thread stays blocked); I did not exercise it by reintroducing a blocking design.
- **P3-C (hypothesis shown as fact):** README now says the response is "expected" to end through the emitter timeout, that no test covers a real emitter timeout during a blocked write, and that the 20s web-phase extension is unverified as well.
- **Nits:** comment on `lastSeen` being set before the claim; `failedWriteOfTheFinalChunkStillFreesTheSlot` now calls `onCompleted()`/`onError()` afterwards and uses a `CountingLease` to assert the lease is released exactly once and `emitter.completed` stays false; `ConnectionModeConfigTest` imports `java.time.Duration`.

Checks after these fixes: `./mvnw -B verify` exit 0, run twice (simulator 12/12, gateway 23/23, no failures/errors/skips); `./mvnw -B -pl gateway -am test` repeated three times, each 23/23; `git diff --stat -- proto simulator` empty; no `Thread.sleep` under `gateway/src`; `feature_state.py check` still `implemented`, `approval_matches: true`.

Log verification: the registry WARNINGs are emitted during a real Spring context close (seen when `GracefulShutdownTest` runs alone) and the new "still hold a slot" warning is emitted when `ActiveStreamRegistryTest` runs alone. In a full `verify` run they do not show in the console output after earlier Spring tests have run in the same surefire JVM; the experiment (Spring test first -> no output, test alone -> output) points to leftover logging state from the closed Spring contexts, but I did not isolate the cause. The "still hold a slot" warning has not been seen at a real stalled-final-write shutdown end to end; that scenario is only covered with the fake emitter.

Limits: the third review (review.md) covers the code before these last changes; nothing re-reviewed them.

## Fourth-review follow-up (README only)
The fourth review found no behavioural defect; its remaining minor finding and one nit were documentation. README "Known limits" now generalises the slot-held exception to any stream whose terminal outcome is claimed and whose last event is being written (final chunk or `complete` after COMPLETED, or the `error` event after an upstream error or an upstream that ended early), says `stop()` logs one aggregate warning with the number of streams (no per-stream entries or request ids), and the first bullet points to that exception. No code or test changed in this step. `./mvnw -B verify` exit 0 afterwards (simulator 12/12, gateway 23/23); `git diff --stat -- proto simulator` empty; `feature_state.py check`: implemented, `approval_matches: true`. As requested, no further review was run; the optional logger-capturing assertion for the new warning (review nit N-2) was not added.
