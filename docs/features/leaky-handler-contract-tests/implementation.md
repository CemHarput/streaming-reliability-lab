# Implementation: leaky-handler-contract-tests

Plan hash: f8534be895792214209914ce8f5328311bc34f4fd3d0c49bd7e6cbfb2d0b060a
Baseline HEAD: 3187d71fda523fe13d4018e75d4dfb9edcd0c315 (branch main; worked directly on main, no branch created)
Result: implemented (all acceptance criteria reported met, required checks passing)

Not committed. Not reviewed (`/feature-review` has not been run); the two new documents have never been read by anyone but the implementer.

## Summary
One implementation-independent stream contract (one terminal event and nothing after it, permit and lease released once, upstream cancelled, no timer leak, bounded queue) is run parametrically on the real `StreamBridge` (`REAL`) and on a deliberately flawed `LeakyStreamHandler` (`LEAKY`, test code only). `StreamContract.check()` returns a violation list and never throws; the tests assert SET EQUALITY between the detected clauses and the expected set (empty for REAL; exactly `{UPSTREAM_CANCELLED}` for LEAKY/CLIENT_GONE, `{SINGLE_TERMINAL, RELEASE_ONCE}` for LEAKY/RACE_COMPLETION_VS_SHUTDOWN, `{RELEASE_ONCE}` for LEAKY/RACE_COMPLETION_VS_CLIENT_GONE, empty for every other LEAKY scenario), so a missing expected violation and an unexpected violation both fail while the build stays green. The two flaws are marked `// FLAW (a)` (client gone does not cancel the upstream call) and `// FLAW (b)` (check-then-act terminal step). The LEAKY race is made deterministic by a `CountDownLatch` hook inside the class armed only in the race tests; REAL is stressed with the same race through a test-level barrier for 200 iterations per race. No production code, `proto/` or `simulator/` file changed.

## Files
Created, under `gateway/src/test/java/io/github/cemharput/streamlab/gateway/stream/`:
- `StreamContract.java` (Clause, Violation, Shape, Scenario, Observation, `check()`, `EXPECTED_LEAKY`, `expected()`, `assertMatchesExpectation()`, `names()`)
- `LeakyStreamHandler.java` (with `PermitLedger`)
- `StreamUnderTest.java` (interface, Kind, Wiring, RealStream and LeakyStream adapters)
- `StreamContractTest.java` (15 tests: 12 parameterized invocations + 3 meta tests)
- `TerminalRaceContractTest.java` (4 tests)

Created docs: `docs/failure-analysis.md`, `docs/adr/0002-single-terminal-claim.md` (accepted, 2026-10-24).
Modified: `StreamTestSupport.java` (additions only: `CountingRegistry`, `startedCountingRegistry`), `README.md` (`### Stream contract tests` before Settings; ADR-0002 in the Decisions list).

## Acceptance mapping
| AC | Evidence |
|---|---|
| AC1 | `git diff --stat -- gateway/src/main proto simulator` is empty; changes only under `gateway/src/test`, `docs/` and `README.md` |
| AC2 | `StreamContractTest` normalCompletionEndsOnce, upstreamErrorEndsOnce, clientGoneCancelsUpstream, shutdownCancelsUpstreamWithinBound, firstChunkTimeoutCancelsUpstream, slowConsumerStaysWithinQueueBound on REAL: empty violation set, real in-process simulator and real stub |
| AC3 | `clientGoneCancelsUpstream[LEAKY]`: exactly `{UPSTREAM_CANCELLED}` (`active_generations` stays 1 and `cancelled_total` 0 for the 2 s window; lease closed once, permit released once, no armed timer, nothing sent after the trigger); the other 5 simulator scenarios on LEAKY satisfy the contract |
| AC4 | `TerminalRaceContractTest[LEAKY]` (5 iterations each): shutdown race = `{SINGLE_TERMINAL, RELEASE_ONCE}` (two terminal events, `leaseCloses == 2`, `permitReleases == 2`); client-gone race = `{RELEASE_ONCE}` (`permitsHeld = -1`); `raceHookBroken() == false` each time |
| AC5 | `TerminalRaceContractTest[REAL]`: 200 of 200 iterations with an empty set for both races; `pendingTimers() == 0` at the end |
| AC6 | `expectationTableCoversBothFlawsAndOnlyThem`, `expectationIsComparedInBothDirections`, `checkReportsEachClauseOnSyntheticObservations` (the last two were added beyond the plan: the checker is not vacuous and the comparison fails in both directions) |
| AC7 | no `Thread.sleep` under `gateway/src`; `@Timeout(60, SEPARATE_THREAD)` on both classes; barrier and latch waits bounded to 5 s so a missing party fails the test; gates released in `finally`/`@AfterEach`; a fresh simulator per test; preconditions fail as harness errors |
| AC8 | docs written with every claim tied to a file/method; the REAL stress limit stated; NOT yet reviewed |
| AC9 | `./mvnw -B verify` green: simulator 12, gateway 184 (165 + 19 new); no existing test removed or changed (only additions in `StreamTestSupport`) |
| AC10 | per-class times: `StreamContractTest` 12-15 s, `TerminalRaceContractTest` about 3 s (budget about 40 s) |

## Checks
- Implementer: focused command three times in a row (19 tests each, exit 0); `./mvnw -B -pl gateway clean` then `./mvnw -B verify` green; `./mvnw -B -pl gateway -am test` twice green.
- Coordinator re-ran independently: `./mvnw -B clean verify` exit 0 (simulator 12/12; gateway 184/184, `StreamContractTest` 15 in 14.16 s, `TerminalRaceContractTest` 4 in 2.85 s); focused command exit 0 (19 tests); `./mvnw -B -pl gateway -am test` twice more, each 184/184; `git diff --stat -- gateway/src/main proto simulator` empty; no `Thread.sleep`; `feature_state.py check`: in_progress, `approval_matches: true`; branch main, nothing committed.
- Not run: a review; any test with a real TCP-level stalled client.

## Deviations from the plan and verified assumptions
- A1-A5 verified in code/jars, no correction: a same-package test subclass overrides the package-private `releasePermit` and `StreamBridge` dispatches to it (REAL shows `permitReleases == 1`); events are recorded as text containing `event:complete` / `event:error` (an error shows `code=INTERNAL`); `shutdownNow` on the data channel makes the simulator cancel the generation and `server.stop()` does not wait out its 2 s grace (about 0.7-1 s per test); Awaitility `during` + `until` works as planned and `ConditionTimeoutException` is in `org.awaitility.core`; `-Dsurefire.failIfNoSpecifiedTests=false` is the right flag.
- **The REAL race was strengthened beyond the plan's "barrier only" design.** In the first version the same side always won (`ERROR=200` and `NONE=200`), so the stress was weak. Now chunk 0 is delivered before the race and the cancel side is delayed by `(i % 25) * 1500` calls of `Thread.onSpinWait()` (no sleep), so both outcomes occur (three runs: shutdown race COMPLETE 173/181/184 and ERROR 27/19/16; client-gone race COMPLETE 184-186 and NONE 14-16). N stayed at 200.
- Two meta tests were added to the plan (`checkReportsEachClauseOnSyntheticObservations`, `expectationIsComparedInBothDirections`), so `StreamContractTest` has 15 tests and the total is 19 new tests (the plan expected 182, the real count is 184).
- `Scenario` gained `stopWindow`, `chunks` and `errorCode` fields; LEAKY also writes terminal events under `sendLock` (for event ordering); race logs shorten event text with `names()`.
- The implementer noted a one-off `java.lang.Error: Unresolved compilation problem` at test time after an escape-character mistake (this is ECJ-style output, not javac); it was fixed and the root cause was not investigated. A `clean` build by the coordinator shows no such error.
- **Correction by the coordinator:** the implementer's docs said the violation log lines appear "in the build output". That holds for the focused command (15 `contract violation` lines, some of them from the meta tests' synthetic observations) but NOT in a full `./mvnw -B verify`, where the coordinator's run showed 0 such lines although every assertion ran and passed. `docs/failure-analysis.md` now says so and treats the log as a convenience and the assertions as the proof. The cause of the missing log lines in the full build was not isolated (earlier in this project, log output also went missing after Spring tests in the same test JVM).

## Limitations
- The REAL stress cannot prove the absence of a race; the argument is the compare-and-set in `StreamBridge.claim()` (stated in README and `docs/failure-analysis.md`).
- LEAKY is a simplification: its SLOW_CONSUMER path writes chunks synchronously on the gRPC callback thread and its timeout path writes on the timer thread, unlike production; neither is measured by a contract clause (`docs/failure-analysis.md`).
- `StreamBridge` logs a "Stream race finished" INFO line per iteration, so the focused output is noisy (about 400 lines).
- The documentation was written by the implementer and has not been reviewed.

## First-review fixes (after review pass 1)
- P3-1: `docs/failure-analysis.md` ("How REAL differs") and `docs/adr/0002-single-terminal-claim.md` now say that the error terminal event takes `sendLock` with `tryLock(SEND_LOCK_WAIT)` (`sendErrorAndComplete`), chunks and the `complete` event are written under `sendLock.lock()` (no time limit), and claim/cancel/release take no lock. The word "Only" was removed from the ADR.
- P3-2: "Both outcomes occur" became an observation (local runs on 2026-10-24, three runs; distribution not asserted, machine dependent, a collapse to one side would weaken the stress without failing a test).
- P3-3: new meta test `checkReportsEveryBranchOfTheTerminalAndUpstreamClausesWhenViolated` (wrong error code, missing chunk, chunk-count mismatch, `faulted_total` / `completed_total` / `cancelled_total` mismatches alone, `AT_MOST_ONE` with two terminals, shutdown at the time bound passes and bound + 1 ms fails, upstream still active alone). `StreamContractTest` now has 16 tests, 20 new tests in total.
- Nits: comment in `TerminalRaceContractTest` no longer claims the drainer is running; the weak `NONE_AFTER_TRIGGER` clause in `CLIENT_GONE` is documented as a limit in `docs/failure-analysis.md`.

## Second-review fixes (after review pass 2)
- P3-A: the sentence "claim, cancel, release take no lock" was wrong for the `COMPLETED` path and for a failed chunk write (they run inside `writeChunkLocked` under `sendLock`). Fixed in `docs/failure-analysis.md` and `docs/adr/0002-single-terminal-claim.md`.
- P3-B: the `AT_MOST_ONE` row passed through the general "event after the first terminal" rule, so the distinct clause list could not tell the branch apart; the meta test now also checks the raw violation detail ("at most one terminal event").

## Third-review fixes (after review pass 3)
- The lock sentence was rewritten per path (`COMPLETED`: claim and release under `sendLock`, no cancel; failed non-last chunk write: `clientGone` under `sendLock`; `handleUpstreamEnd`, `timedOut`, `cancelForShutdown`, `overflow`: no `sendLock` in `StreamBridge`; Spring emitter-callback threading not checked). Docs only.
