# Review: gateway-bounded-resources (second pass, after the first-review fixes)

Verdict: **PASS**
Baseline HEAD: 9a0900a3eb5d5051089cb02f6311523ec2d53d80 (equal to HEAD at review time)
Snapshot: workspace fingerprints identical before and after the review (not stale).

History: the first pass returned CHANGES_REQUESTED with four P3 findings (P3-1 `queue_depth` momentarily negative, P3-2 `stop()` warning/comment wording, P3-3 flaky wait-timeout test, P3-4 AC sub-claims not bound to tests), no blocker. They were fixed (see implementation.md, "First-review fixes"). This pass supersedes the first report.

## Scope and limitations
- The reviewer read in full all 11 modified tracked files (README.md, GatewayProperties, ActiveStreamRegistry, StreamBridge, StreamController, application.yaml, ConnectionModeConfigTest, ActiveStreamRegistryTest, StreamBridgeTest, StreamTestSupport, InProcessSimulatorConfig) and the 3 new files (StreamMetrics, OverloadPolicyTest, UpstreamFlowControlTest), plus implementation.md, the first review and, as context, SimulatorMetrics and Generation.start. The diff headers contain no `proto/`, `simulator/` or `pom.xml`. No file was skipped.
- The reviewer cannot run git, builds or tests. Reported by the coordinator and not independently verified by the reviewer: `./mvnw -B verify` exit 0 (simulator 12/12, gateway 49/49), two further gateway runs at 49/49, the empty `git diff -- proto simulator`, the baseline HEAD equality. The reviewer verified itself: the metric accounting by tracing every path, no `Thread.sleep`/`TimeUnit.*.sleep` under `gateway/src`, the diff scope, and the `started_total` semantics.
- No test with a real TCP-level stalled client exists (documented in README). The earlier startup-window and emitter-`onTimeout` hypotheses are unverified and documented; the code does not contradict them.
- PASS means no problem needing a fix was found in the reviewed scope; it does not mean the feature is bug-free. The reviewer changed no file.

## First-pass findings: status
- **P3-1 (`queue_depth` could read negative): fixed.** Every path that changes the shared counter was re-derived: the `+1` now precedes `offer` (`StreamBridge.java:119`), so the drainer's `poll` and `-1` (`:173-175`) always follow that chunk's `+1`; a failed offer does `+1`, `-1`, then `overflow()` (net 0); `discardQueued()` (`:187-191`) subtracts only what `drainTo` actually removed, all of it previously counted; a chunk offered after a terminal claim is discarded by the drainer (`:167-169`), or when the drainer returns from a stuck write. The counter is always >= the number of queued items and never negative, and converges to 0. Code comment (`:117-118`) and implementation.md describe this correctly. No deterministic test exists for the micro-race (scheduling window); judged acceptable.
- **P3-2 (warning and comment wording): fixed.** `ActiveStreamRegistry.java:225-231` states the permit/tracked distinction. After a forced cancel only streams whose outcome is already claimed (`COMPLETED` or `UPSTREAM_ERROR`) and whose last write is running can remain in `activeCount()`, since `cancelForShutdown` and `clientGone` call `finish()` at once; "hold no permit or lease" matches the code and README `:176-182`, `:198-199`.
- **P3-3 (flaky wait-timeout test): fixed.** `queue-wait=4s` gives about a 4 s window against a 100 ms poll; `startedAt` is taken before the request so the `>= 4s` bound is safe; total test time is far below the class `@Timeout(60, SEPARATE_THREAD)`.
- **P3-4 (unbound AC sub-claims): fixed.** `InProcessSimulatorConfig#serverName()` returns the same instance field used by `simulatorServer()`; `getBean` finds the config class since it is the primary source; `simulatorStats()` opens its own channel and closes it with `shutdownNow`, so no resource leaks. The `requested == 0` assertion in `StreamBridgeTest` (`:176`) goes red if `request(1)` is made unconditional (a disconnected emitter makes the write fail, `writeChunkLocked` returns false, `request(1)` is never called).

## Findings (optional nits only, nothing blocking)
- **Nit-1:** README does not say that the chunk-queue gauge can read one too high for an instant. `README.md:154` ("The queue never holds more than its capacity") and `:166`; code `StreamBridge.java:119-121`. On a credit violation `chunkQueued()` runs and `offer` fails, so `queue_depth{queue="chunks"}` can show `capacity + 1` for that stream until `chunkDequeued()`; on the normal path a `+1` is visible between the increment and the offer. The queue itself never exceeds capacity; only the metric reads high for an instant. implementation.md documents it ("one too high for an instant"). Optional: add "may briefly read one higher per stream, never negative" to the metrics table.
- **Nit-2:** the `started_total` guard does not catch every regression deterministically. `OverloadPolicyTest.java:90-96` and `:147,159`. A faulty implementation that started `generateTokens` before admission and cancelled at once might not have the in-process server process `Generation.start()` before the 503 is returned, so the test would usually go red but not deterministically. For the current code the evidence is correct: the check at `StreamController.java:65-70` happens before `generateTokens`, and the holder stream has already started, so `started_total` must stay equal (`Generation.java:62`, `SimulatorMetrics.java:21-24`). No change required; a short `during(QUIET)` "unchanged" wait could be added.

## Acceptance criteria
| AC | Status |
|---|---|
| AC1 | Met; "upstream does not start" is now bound to a test (see Nit-2) |
| AC2 | Met; flake risk reduced |
| AC3, AC5, AC6, AC8 | Met (code and tests); unchanged in this round |
| AC4 | Met; no `request(1)` after a terminal outcome is now asserted |
| AC7 | Met; the negative value is gone, only a momentary over-count remains (Nit-1) |
| AC9 | Rests on the reported command results; absence of `Thread.sleep` was verified by the reviewer |
| AC10 | Met; the log text now matches README |
