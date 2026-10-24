# Review: gateway-race-tests (final: pass 3)

Verdict: **PASS** (no open finding).
Baseline HEAD: 65631ad817ff713c95698d07a038cd0dad625ee1 (branch main, nothing committed).

## History
- Pass 1: CHANGES_REQUESTED. P2-1 (AC5 `permitsHeld == 0` right after `stop()` is not guaranteed when the forced cancel loses the claim to a `COMPLETED` claim whose drainer has not yet released capacity), P2-2 (doc stated it unconditionally), P3-1 (docs overstated which pairs are raced), P3-2 (only the first upstream cancel message is recorded), P3-3 (timer precondition read the shared `pendingTimers`), P3-4 (`awaitTermination` outside `finally`), nits. All fixed. P2-1 changed the test to allow 0 or 1 for `complete` outcomes (0 required for REFUSED and `error CANCELLED`), counts the 1s in a log line, and the settled snapshot still requires exactly one release; this deviates from plan AC5 and is recorded in implementation.md (coordinator decision, plan.md untouched).
- Pass 2: CHANGES_REQUESTED. One P3: the "Pairs not raced" list missed client gone against shutdown cancel. Fixed (docs only).
- Pass 3: PASS. No test races any of the listed non-raced pairs; the "six selected pairs" wording and the rest of the new section match the code.

## Evidence and limits
- Reviewers ran nothing. Coordinator: clean `./mvnw -B clean verify` exit 0 after the pass-1 fixes (gateway 192/192, `TerminalPathRaceTest` 7 tests 10.76 s), three full builds exit 0 before them, `git diff` of `gateway/src` tracked files, `proto`, `simulator` empty, no `Thread.sleep`. Pass-2 and pass-3 fixes touched documentation only.
- Whether `permitsHeld == 1` occurs in practice after the fix is unknown (the log line does not appear in full builds).
- A stress test cannot prove the absence of a race; the argument is the CAS in `StreamBridge.claim()` (ADR-0002).
- This review does not mean the feature is bug-free.
