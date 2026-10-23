# Implementation: gateway-bounded-resources

Plan hash: b5527a390d5184086b5589a3ee08fba872619d3c356f64c3b7a873d4914365eb
Baseline HEAD: 9a0900a3eb5d5051089cb02f6311523ec2d53d80 (branch claude/changes)
Result: implemented (all acceptance criteria reported met, required checks passing)

Not committed. Not reviewed (`/feature-review` has not been run). `StreamBridge` changed substantially (398 lines now: bounded queue, serial drainer, split release), and the previous feature's four review rounds all found issues in this class, so a review is advisable.

## Summary
The gateway now has three limits: a global active-stream limit with `reject` or `wait` overload policy, a bounded per-stream chunk queue with manual upstream flow control (`disableAutoRequestWithInitial(capacity)` and `request(1)` after each written chunk), and Micrometer metrics. The registry is the single owner of admission. Terminal outcome is still claimed by CAS; the claim winner now frees the permit and lease before the terminal write (`releaseCapacity()`), so a stuck final write no longer holds capacity. `proto/`, `simulator/`, `pom.xml`, `LifecyclePhases` and `upstream/*` are unchanged.

## Files
Modified (under `gateway/src/...`, package `io/github/cemharput/streamlab/gateway`):
- `main/.../config/GatewayProperties.java`: `OverloadPolicy`, `Limits` record (validation, messages contain `lab.limits.<name>`), `Limits.defaults()`, fourth component `limits`
- `main/resources/application.yaml`: `lab.limits` block
- `main/.../stream/ActiveStreamRegistry.java`: tracked/admitted sets, `Admission`, `admit()` (reject/wait, `max-waiting`, `queue-wait`), `tryRegister`, `releasePermit`, `admittedCount`, `waitingCount`; `stop()` wakes waiters; warning text updated
- `main/.../stream/StreamBridge.java`: bounded `ArrayBlockingQueue` (offer only), per-stream serial virtual-thread drainer (wip counter), manual flow control, `upstreamEnd` handled after the queue drains, `overflow()` (single RESOURCE_EXHAUSTED error + upstream cancel), `release` split into `releaseCapacity()` and `finish()`, new constructor, `queuedChunks()`
- `main/.../stream/StreamController.java`: `registry.admit()`, `StreamMetrics`, queue capacity; rejections close the lease, count `rejected_total{reason}` and answer 503 + `Retry-After: 1`
- `test/.../stream/StreamTestSupport.java`, `StreamBridgeTest.java`, `ActiveStreamRegistryTest.java`, `ConnectionModeConfigTest.java`
- `README.md`: status line, module table, "Bounded resources" subsection, rewritten "Known limits", settings table

Created:
- `main/.../stream/StreamMetrics.java`
- `test/.../OverloadPolicyTest.java`, `test/.../stream/UpstreamFlowControlTest.java`

## Acceptance mapping
| AC | Evidence (tests) |
|---|---|
| AC1 | `ActiveStreamRegistryTest#rejectPolicyRejectsBeyondMaxActiveStreams`; `OverloadPolicyTest#rejectPolicyAnswers503AndCountsRejection` (503, Retry-After, no SSE, `rejected_total{limit}`, `active_streams`, `/actuator/metrics/rejected_total` 200, new stream completes after disconnect) |
| AC2 | `ActiveStreamRegistryTest#waitingRequestIsAdmittedWhenAPermitIsReleased`, `#waitingRequestTimesOutAfterQueueWait`, `#waitersBeyondMaxWaitingAreRejectedImmediately`; `OverloadPolicyTest#waitPolicyAdmitsWhenSlotFrees`, `#waitPolicyTimesOutWith503` |
| AC3 | `ActiveStreamRegistryTest#stopReleasesWaitersWithoutDelayingShutdown`; `OverloadPolicyTest#shutdownReleasesWaitingRequests`; `GracefulShutdownTest` unchanged and green |
| AC4 | `StreamBridgeTest#beforeStartDisablesAutoRequestWithQueueCapacity`, `#slowConsumerQueueNeverExceedsCapacity`; `UpstreamFlowControlTest#realUpstreamPausesWhileClientIsStalled` (in-process simulator: `flow_control_pauses_total >= 1`, `chunks_emitted_total <= 4`, all 50 chunks in order after the gate opens) |
| AC5 | `StreamBridgeTest#upstreamErrorIsDeliveredAfterQueuedChunks`, `#chunkBeyondGrantedCreditFailsWithResourceExhausted`, `#shutdownDiscardsQueuedChunks`, `#nothingIsSentAfterTheTerminalEvent`; `StreamEndToEndTest` unchanged and green |
| AC6 | `StreamBridgeTest#terminalClaimReleasesPermitAndLeaseBeforeTheFinalWrite`, `#stuckUpstreamErrorWriteDoesNotHoldThePermit`; `ActiveStreamRegistryTest#stuckFinalWriteDoesNotBlockAdmissionOfANewStream`; the earlier stuck-final-write tests stay green |
| AC7 | `StreamMetrics` gauges `active_streams`, `queue_depth{queue=chunks|admission}` and counters `rejected_total{reason}` (five reasons pre-registered); values asserted in the tests above and back to 0 when streams end |
| AC8 | `ConnectionModeConfigTest#codeDefaultsApplyWithoutApplicationYaml` (extended), `#waitPolicyIsBoundFromRelaxedNames`, `#invalidLimitsFailAtStartup` (7 parameters, each failure names the property) |
| AC9 | `./mvnw -B verify` green; `git diff --stat -- proto simulator` empty; no `Thread.sleep` under `gateway/src` |
| AC10 | README updated: both stuck-write cases documented, the emitter-timeout and Tomcat 20s effects marked unverified, no Prometheus registry, `rejected_total` naming in Prometheus unchecked |

## Checks
- The implementer's report: focused command 38/38, `./mvnw -B verify` green, `./mvnw -B -pl gateway -am test` twice green.
- Coordinator re-ran independently: `./mvnw -B verify` exit 0 (simulator 12/12; gateway 49/49: ConnectionModeConfigTest 11, GracefulShutdownTest 2, OverloadPolicyTest 4, ReadinessDownTest 1, ActiveStreamRegistryTest 9, StreamBridgeTest 13, UpstreamFlowControlTest 1, StreamEndToEndTest 8); `./mvnw -B -pl gateway -am test` twice more, each 49/49; `git diff --stat -- proto simulator` empty; `grep -rn Thread.sleep gateway/src`: no matches; `feature_state.py check`: in_progress, `approval_matches: true`.
- The implementer's first focused run had 4 failures (late `onCompleted` overwrote the upstream end after `onError`; queue discard was left to a stuck drainer; an early `return` in `writeChunk` skipped `request(1)`), all fixed. A first full `verify` failed with a stale-build `NoClassDefFoundError: SseClient`; it passed after `mvn -pl gateway clean`, with no code change.
- Not run: any test with a real TCP-level stalled client (fake emitter only).

## Deviations from the plan and verified assumptions
- Verified in the jars, no correction needed: `ClientCallStreamObserver.disableAutoRequestWithInitial(int)` is a non-abstract method valid only inside `beforeStart`; `ClientCall.request` may be called from any thread; Micrometer 1.17.1 package names; a `MeterRegistry` bean exists in a manually built `SpringApplicationBuilder` context; `/actuator/metrics/rejected_total` returned 200.
- Queue discard (`discardQueued()`) is done directly by the CAS winner (`clientGone`, `cancelForShutdown`, `overflow`) instead of being left to the drainer, because a drainer stuck in a write would otherwise keep `queue_depth` high (AC5). The drainer calls the same method when it sees a terminal outcome.
- `upstreamEnd`: the first writer wins (written only when null).
- `signalAll` instead of `signal` for `Admission` and `releasePermit`, to avoid lost wake-ups.
- On a credit violation the upstream cancel message is "per-request queue overflow".
- Test helpers `gauge` and `limits` were added to `StreamTestSupport`. The fake emitter records `data:` as the object's `toString`, not JSON, so chunk order is asserted through `id:<seq>`.

## Limitations
- If an intermediate chunk write is stuck, no terminal outcome is claimed, so the permit is held until the write returns, shutdown cancels the stream, or the emitter timeout fires (stream deadline + 5s). That the emitter timeout callback runs while another thread is blocked in a write is an unverified hypothesis; the Tomcat graceful-phase (20s) extension is also unverified. Both are marked in README.
- The limit bounds upstream calls and leases, not open HTTP responses: a stream whose terminal write is stuck holds no permit but stays tracked until the write returns.
- `wait` admission is not fair (a new request can overtake a waiter) and blocks a servlet thread (a virtual thread with the current setting; with virtual threads off it would use up to `max-waiting` platform threads).
- `max-waiting` was added by the planner and is not in the original request; it can be removed.
- No Prometheus registry dependency exists; the `prometheus` actuator exposure stays inert and how `rejected_total` would appear there was not checked.
- If a per-request lease arrives later, the lease/admission order in `StreamController` has to change.
- Nothing was reviewed independently; no real stalled-TCP-client test exists.

## First-review fixes (after review.md pass 1, same plan)
Addressed the four P3 findings of the first review. Only `gateway/` changed; `proto/`, `simulator/`, `pom.xml` untouched. README did not need a change (its text already said "finishing a final write").

- **P3-1 (`queue_depth` momentarily negative):** `StreamBridge.onNext` now counts the chunk (`chunkQueued()`) before `queue.offer`, and undoes it if the offer fails (`chunkDequeued()` before `overflow()`). A drainer that polls the chunk immediately can no longer count it out before it was counted in. The gauge can read one too high for an instant, never negative. No deterministic test for this micro-race was added (it is a scheduling window); the existing accounting tests (`slowConsumerQueueNeverExceedsCapacity`, `shutdownDiscardsQueuedChunks`, `chunkBeyondGrantedCreditFailsWithResourceExhausted`) cover the counts.
- **P3-2 (log and comment wording):** the `ActiveStreamRegistry.stop()` warning now says "N stream(s) still finishing a final event write after forced cancel: they hold no permit or lease any more but stay tracked until the write returns", and the comment states the permit/tracked distinction. This corrects a deviation from plan D4 that the first implementation report did not mention.
- **P3-3 (flaky wait-timeout test):** `OverloadPolicyTest#waitPolicyTimesOutWith503` uses `queue-wait=4s` (was 1s) so a slow runner has room to observe `queue_depth{queue=admission}==1`; the lower-bound assertion is now `>= 4s`.
- **P3-4 (unbound AC sub-claims):** `OverloadPolicyTest` reads the simulator's own stats through a new `InProcessSimulatorConfig#serverName()` and asserts `started_total` does not change when a request is rejected (reject test and wait-timeout test), proving the upstream call never starts. `StreamBridgeTest#failedClientWriteCancelsUpstreamAndFreesTheSlot` now also asserts `upstream.requested == 0` (no credit after a terminal outcome).

Checks after these fixes: `./mvnw -B verify` exit 0 (simulator 12/12, gateway 49/49); `./mvnw -B -pl gateway -am test` repeated twice, 49/49 each; `git diff --stat -- proto simulator` empty; no `Thread.sleep` under `gateway/src`; `feature_state.py check`: implemented, `approval_matches: true`.
