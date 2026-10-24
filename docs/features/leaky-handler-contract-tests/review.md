# Review: leaky-handler-contract-tests (final: pass 5)

Verdict: **PASS** (no blocker, major or open finding).
Baseline HEAD: 3187d71fda523fe13d4018e75d4dfb9edcd0c315 (branch main, nothing committed).

## History
- Pass 1: CHANGES_REQUESTED. P3-1 (`tryLock` claimed for the `complete` event), P3-2 ("both outcomes occur" stated as fact), P3-3 (no negative synthetic rows for several checker branches), two nits. All fixed (new meta test, doc wording, comment, CLIENT_GONE limit documented).
- Pass 2: CHANGES_REQUESTED. P3-A (lock sentence wrong for the `COMPLETED` path), P3-B (`AT_MOST_ONE` row did not isolate its branch). Fixed (per-path wording; extra assertion on the raw violation detail).
- Pass 3: CHANGES_REQUESTED. P3 doc: lock sentence implied a cancel on `COMPLETED` and omitted `handleUpstreamEnd`. Rewritten per path.
- Pass 4: CHANGES_REQUESTED. Three P3 doc points ("upstream call has already ended", "non-last chunk" for `clientGone`, registry tracking after shutdown/client gone). Fixed.
- Pass 5: PASS. The three passages and their neighbours match `StreamBridge` and `ActiveStreamRegistry`. One optional wording remark (ADR could add "and calls `finish()`" for a failed last-chunk write), not applied.

## Evidence and limits
- Passes 2-5 were static reads; reviewers ran nothing. Coordinator ran clean `./mvnw -B clean verify` after the code/test changes (exit 0, gateway 185/185, `StreamContractTest` 16, `TerminalRaceContractTest` 4), `git diff --stat -- gateway/src/main proto simulator` empty, no `Thread.sleep` under `gateway/src`.
- Passes 3-5 changed documentation only after that build.
- Not checked: whether Spring runs `SseEmitter` callbacks on a thread that holds `sendLock` (the docs say so).
- This review does not mean the feature is bug-free; the REAL race stress cannot prove the absence of a race (argument: the CAS in `claim()`, ADR-0002).
