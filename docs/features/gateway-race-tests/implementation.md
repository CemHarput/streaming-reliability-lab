# Implementation: gateway-race-tests

Status: implemented (all required checks passed). Plan hash `1a93daf05171f2cda4db002017feddbdefac9856d43792fadb9c62136000a3c7`, baseline `65631ad817ff713c95698d07a038cd0dad625ee1`.

## Files
- New: `gateway/src/test/java/io/github/cemharput/streamlab/gateway/stream/TerminalPathRaceTest.java` (REAL only, `@Timeout(60, SEPARATE_THREAD)`, 7 tests: `timeoutRacesCompletion`, `timeoutRacesClientGone`, `upstreamErrorRacesClientGone`, `upstreamErrorRacesShutdownCancel`, `overflowRacesClientGone`, `overflowRacesShutdownCancel`, `admissionRacesStop`; `Pair` enum, generic `run(Pair, int)` re-written from the `TerminalRaceContractTest` skeleton, private helpers only).
- Changed: `docs/failure-analysis.md` (sentence in "Scenarios", new section "What the terminal-path stress adds", "Running it"), `README.md` (3 lines in "Stream contract tests").
- No change to existing test files, `StreamTestSupport`, `gateway/src/main`, `proto`, `simulator`.

## Acceptance mapping
| AC | Result |
|---|---|
| AC1 | `git diff --stat -- gateway/src proto simulator` empty; only README.md, failure-analysis.md modified, one new test file |
| AC2-AC6 | 7 tests green; per iteration empty clause set, outcome table, overflow extras (lease/permit released while the write is stuck, exactly one chunk event), `permitsHeld == 0` after `stop()`, REFUSED and ADMITTED both observed |
| AC7 | both outcomes observed for every pair in all implementer runs (table below) |
| AC8 | no `Thread.sleep` under `gateway/src`; bounded waits; gates and executor closed in `finally`/`@AfterEach`; `HARNESS:` prefix for harness errors |
| AC9 | `TerminalPathRaceTest` 10.66-10.96 s (JaCoCo on), below 30 s |
| AC10 | `./mvnw -B clean verify` three times exit 0 in the coordinator's runs; runs 2 and 3 logs show gateway 192/192, 0 failures (the run-1 log was overwritten by a colliding second invocation, so only its exit code 0 is known) |
| AC11 | self-check by the implementer; NOT independently reviewed yet |
| AC12 | not triggered, no product defect found |

## Checks
- Implementer: focused command ×3 (11 tests each, exit 0); `./mvnw -B clean verify` (exit 0, gateway 192); greps and git diffs as above.
- Coordinator: `feature_state.py check` approval_matches true; git diff of `gateway/src`, `proto`, `simulator` empty; no `Thread.sleep`; three full builds as in AC10. The first attempt at the repeat runs collided with a still running background loop (two Maven runs in the same tree); this was my mistake, not a test failure, and the colliding runs were discarded.

## Local distributions (2026-10-24, implementer, three focused runs; observation, machine dependent, not asserted)
| Test | Outcomes |
|---|---|
| timeoutRacesCompletion (100) | COMPLETE 46-49 / DEADLINE_EXCEEDED 51-54 |
| timeoutRacesClientGone (100) | DEADLINE_EXCEEDED 51-54 / NONE 46-49 |
| upstreamErrorRacesClientGone (200) | INTERNAL 90-92 / NONE 108-110 |
| upstreamErrorRacesShutdownCancel (200) | INTERNAL 92-93 / CANCELLED 107-108 |
| overflowRacesClientGone (200) | RESOURCE_EXHAUSTED 97 / NONE 103 |
| overflowRacesShutdownCancel (200) | RESOURCE_EXHAUSTED 94 / CANCELLED 106 |
| admissionRacesStop (200) | REFUSED 102-103 / COMPLETE 49 / CANCELLED 48-49 |

## Deviations
None from the plan.

## Limitations
- The outcome split is close to 50/50 and very stable between runs; the implementer's hypothesis (unverified) is that the spin skew direction mostly decides the winner, so few iterations are genuinely close races (recorded in the doc as a hypothesis).
- Window reasoned about but never observed: a forced shutdown cancel losing the claim to a `COMPLETED` claim whose drainer has not yet reached `releaseCapacity()` could leave `permitsHeld` at 1 when `stop()` returns; `admissionRacesStop` would surface it.
- Not raced: production `admit(bridge)`, the WAIT overload, retry vs cancel. Fakes for emitter and upstream, no container threads.
- Race INFO lines do not appear in a full build (known log loss in full builds); assertions are the evidence.
- Documentation not yet reviewed independently.

## First-review fixes (after review pass 1; coordinator, fast path as the user asked)
- P2-1 (deviation from AC5, decided by the coordinator because the user asked to fix review findings): `admissionRacesStop` required `permitsHeld == 0` right after `stop()` in every ADMITTED iteration. By the code that is not guaranteed when the forced cancel loses the claim to a `COMPLETED` claim whose drainer has not yet reached `releaseCapacity()`; `stop()` can return in between (permit released exactly once right after). The check now requires 0 for REFUSED and `error CANCELLED` and allows 0 or 1 for `complete`; the number of 1s is logged; the settled snapshot still requires exactly one release (`RELEASE_ONCE`). This corrects an over-specified assertion, not a product defect (no leak, no double release). plan.md is untouched.
- P2-2: `docs/failure-analysis.md` states this window as a fact from the code (not observed in the recorded runs).
- P3-1: docs/README say "six selected pairs" and list the pairs that are not raced.
- P3-2: documented that only the first upstream cancel message is recorded.
- P3-3: the timer precondition is now `pendingTimers() >= 1 || leaseCloses() >= 1`.
- P3-4: `awaitTermination` result is taken in the `finally` block and asserted afterwards.
- Nits: "once stop() has set accepting = false" wording; the Windows timer resolution is marked as hypothesis.

## Second-review fix (after review pass 2)
- P3: the "Pairs not raced" list in `docs/failure-analysis.md` now also names client gone against shutdown cancel and says it is not a complete list of all combinations. Docs only; the test code and the build result of the first-review fixes are unchanged.
