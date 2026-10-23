# Review: gateway-shared-channel (fourth pass, after the third-pass fixes)

Verdict: **CHANGES_REQUESTED**, but only minor and nit level findings, mostly documentation. The reviewer found no blocker, no major issue and no new behavioural defect in the code, and in its judgement nothing blocks accepting the feature once the two README corrections are made (or accepted as known limits).
Baseline HEAD: 1ba2e38b7e9e823415bfc0c7472dd2f5d89f27b4 (equal to HEAD at review time)
Snapshot: workspace fingerprints identical before and after the review (not stale).

History: passes 1-3 (a P2 stalled-client shutdown bound, then P3s about documented limits, test timeouts, last-chunk vs shutdown, slot held during a stalled final write) were addressed in turn; see implementation.md. This pass covers the code after the third-pass fixes and supersedes the earlier reports.

## Scope and limitations
- The reviewer read in full: the tracked diff snapshot, README.md, gateway/pom.xml, application.yaml, all 23 untracked files, request/plan/implementation (all three fix sections), the previous review, the workflow docs, and Generation.java, SimulatorServer.java, token_stream.proto for context. Nothing in scope was unreadable.
- The reviewer cannot run git, builds or tests. `./mvnw -B verify` exit 0 twice (simulator 12/12, gateway 23/23), three repeated gateway runs of 23/23, the empty `git diff -- proto simulator` and the observation that the new WARNING does not show in full-suite console output (cause not isolated) are coordinator reports, not independently verified.
- JUnit and Spring/Tomcat internals were not verified from jar sources.
- This review does not mean the feature is bug-free. No fixes were applied.

## Findings

### P3-1 (minor): the "Known limits" exception only names COMPLETED; a stuck UPSTREAM_ERROR write can hold the slot too
`stream/StreamBridge.java:137-152,182-208`, `README.md:120-122,129-135`
- Scenario: the client stopped reading but the socket buffer still has room, earlier chunks were written. Upstream delivers `onError` (fault, deadline or UNAVAILABLE), or `onCompleted` without a last chunk (INTERNAL). `onError` wins the UPSTREAM_ERROR claim (`:139`); `sendErrorAndComplete` takes the lock immediately (gRPC callbacks are serial, `onNext` is not active). If the buffer fills exactly at this write, `emitter.send` (`:199`) blocks indefinitely and `release` (`:141`) runs only after it returns. At shutdown `cancelForShutdown` loses the claim (`:160`) and the emitter callbacks lose it in `clientGone` (`:171`). Slot and lease are held until the write returns: the same mechanism as the documented COMPLETED case.
- Effect: README `:121` ("On shutdown the gateway still cancels that stream's upstream call and frees its slot") is wrong for this case, and the exception at `:129-131` only mentions a claimed `COMPLETED` outcome. The slot has no other effect today (no-op lease, no admission limit), so the impact is documentation and counter accuracy. The log text at `ActiveStreamRegistry.java:124` ("their final write is in progress") is general enough to cover it. This path predates the third-pass changes and was missed by earlier reviews.
- Suggested: generalise the exception to "a stream whose terminal outcome has been claimed and whose last event (final chunk, `complete` or upstream `error`) is being written". No code change needed.

### N-1 (nit): README says one warning per stream, the code writes one aggregate warning
`README.md:133-134` ("logs a separate warning for each stream still holding a slot"), `ActiveStreamRegistry.java:122-125`
- The code logs a single WARNING with the number of streams; no per-stream log and no request id. Suggested: reword the README to "logs one warning with the number of streams still holding a slot", or log per stream.

### N-2 (nit): nothing pins the new WARNING
- The warning conditions hold by source reading: it is written only in the `remaining > 0` branch after `cancelAll()`; a stream cancelled by shutdown is released synchronously before the future returns (`StreamBridge.java:163-164`), so it is not counted; the interrupt path behaves the same. Streams still counted are those with a terminal write in progress (message correct) and a very narrow window where `clientGone` has won the claim but not yet unregistered (message slightly off, negligible). `activeCount()` uses `lock.lock()`, which does not interfere with the interrupt path (the flag is still restored in `finally`).
- The new registry test does not assert the log content, and the coordinator reports the warning is absent from full-suite console output. Optional: a small logger-capturing assertion, or keep the README claim about the log short.

## Third-pass findings: verification (source reading)
- **P3-A: fixed**, with the P3-1 exception above. The stalled-final-write case was re-derived: `onNext(last)` wins the claim (`:98`) and blocks; `stop()` waits the drain, `cancelAll` returns a completed future and upstream is not cancelled; the warning is logged and `stop()` returns in about the drain time; slot and lease are released only when the send returns (successful send -> `complete` -> `release` at `:122-130`; IOException -> CLIENT_GONE + `release` at `:110-113`); the emitter timeout calls `clientGone` but loses the claim. The client sees the final chunk and `complete` on a successful send; on a failed send there is no `complete` and `completeEmitter` is not called. README `:129-135` and `:140-144` match for the COMPLETED case, including "nothing is ever sent after a terminal event". The new test `streamWithAFinalWriteInProgressKeepsItsSlotButDoesNotDelayStop` pins the documented behaviour (< 3 s, expected about 1 s; upstream not cancelled; count 1 until the gate opens, then 0). Flake risk is low (2 s margin).
- **P3-B: fixed.** `@Timeout(..., threadMode = SEPARATE_THREAD)` at class level (`StreamBridgeTest:22`, `ActiveStreamRegistryTest:19`). The thread-affine code in `interruptedStopStillCancelsRemainingStreams` (`interrupt()`, `stop()`, `Thread.interrupted()`) runs on the same (separate) test thread. On timeout the test thread is abandoned, `finally { gate.countDown(); }` does not run and a common-pool thread stays on `gate.await()`; common-pool threads are daemon, so the JVM does not hang. That JUnit's timeout thread is a daemon and a fresh executor per call is an assumption not checked in the jars. An interrupt flag left set by a failing assertion stays on the abandoned thread (better than SAME_THREAD).
- **P3-C: fixed.** README `:123-128` now says "expected", "has not been verified" and "not verified either".
- **Nits: fixed.** `lastSeen` comment (`:95-96`); the `CountingLease` test asserts the lease closes exactly once, no event is sent and `completed == false` (path traced: `onCompleted` is a no-op through `lastSeen`, `onError` loses the claim); `ConnectionModeConfigTest` imports `Duration`. `StreamTestSupport.CountingLease` compiles cleanly; fully qualified `AtomicInteger` and the mixed static/normal imports at `StreamBridgeTest:3-7` are style only.

## Earlier fixes still intact (source reading)
CAS terminal state; claim before the final write; parallel forced cancel with the 2 s bound (`ActiveStreamRegistry.java:136-158`); interrupted `stop()` still cancels remaining streams and restores the flag; negative `faultAtSeq` -> 400 (`StreamController.java:43-46`); AC8 default-value test; client-disconnect tests (unit and end to end); `GracefulShutdownTest` carries the close duration in a future. Scope discipline holds (no bounded queue, semaphore, retry, per-request mode, loadgen or MQTT); no `Thread.sleep` under `gateway/src`; no proto/ or simulator/ files in the snapshot or the untracked list.
Timing margins: `stalledClientsDoNotStretchShutdown...` < 4 s (expected about 3 s) remains the most sensitive; the new registry test < 3 s (expected about 1 s); `GracefulShutdownTest` 2.9-7 s.

## Acceptance criteria
- AC1-AC6, AC8, AC9: implemented and bound to tests (source reading).
- AC7: tested for a non-stalled client; the stalled-client and final-write windows are documented limits, which P3-1 completes.
- AC10: coordinator report only.
