# Review: gateway-timeouts-and-cancel (fourth pass, after the third-pass fixes)

Verdict: **PASS**
Baseline HEAD: fdf7b85d56c11ade5db80c839be0ec34c49e12b3 (equal to HEAD at review time)
Snapshot: workspace fingerprints identical before and after the review (not stale).

History: pass 1 (README incomplete for the stuck intermediate write, a narrow assertion window, javadoc wording), pass 2 (a narrow window in `idleTimerPausedWhileCreditExhausted`, two cosmetic README notes) and pass 3 (a full sweep of every new test: two P3 margins in `upstreamEndAndLastChunkDisarmTimer` and `eachReceivedChunkResetsIdleTimer`, one README note) each returned CHANGES_REQUESTED with minor findings only; all were fixed (implementation.md). No production defect was found in any pass, and no production logic changed after the first review. This pass supersedes the earlier reports.

## Scope and limitations
- By instruction this pass did not repeat the whole sweep or re-derive the production design (verified in passes 1-3). The reviewer read in full `StreamBridgeTimeoutTest`, `StreamTimer`, `StreamTestSupport`, the previous review and the two file lists, and looked at the idle-related README sections (`README.md:170-258`) and the timer calls in `StreamBridge.java` (`:117, :127, :146, :155, :219, :353`).
- The reviewer could not check with git that production logic is unchanged since the earlier passes (coordinator report); this is a stated limitation.
- Not independently verifiable by the reviewer (coordinator reports): `./mvnw -B verify` exit 0 (simulator 12/12, gateway 89/89), three further gateway runs at 89/89, empty `git diff -- proto simulator`, no `Thread.sleep`.
- PASS means no problem needing a fix was found in the reviewed scope; it does not mean the feature is bug-free. The reviewer applied no fix.

## Findings
No blocker, major or minor finding.

### Verification of the changes (by reading source)
1. **`upstreamEndAndLastChunkDisarmTimer`** (`StreamBridgeTimeoutTest.java:183-221`): after `deliver(chunk0)` the capacity is 4 with 3 outstanding and the 300 ms IDLE timer is armed (`StreamTimer.java:71-72`). The wait for `sendInProgress` now polls every 5 ms (`:190`), so the disarming action waits only for the drain virtual thread to enter `send` plus about 5-10 ms: a margin of 30-60x against 300 ms (was about 3x). Disarming is synchronous and under the lock in both scenarios: "error" via `onError` -> `upstreamEnded` -> `disarm` (`StreamBridge.java:146`, `StreamTimer.java:93-100`); "last" via `chunkReceived(true)` -> `upstreamDone=true` -> `disarm`, no re-arm (`StreamTimer.java:67-73`). The following `during(1s)` is more than 3x 300 ms. Event indices are right: 2 events (chunk, error) in the "error" scenario, 3 events (chunk, chunk, complete) in the "last" scenario; capacity 4 with two chunks causes no overflow.
2. **`eachReceivedChunkResetsIdleTimer`** (`:118-140`): first-chunk and idle 2 s, capacity 16. Each round is `deliver` plus `during(300ms).pollInterval(20ms)`, so a real gap is about 300-320 ms, about 6.3-6.7x below 2 s. The total is about 8 x 310 ms = 2.5 s, longer than 2 s: without a reset, the timer armed at chunk 0 would fire at t=2.0 s, inside the `during` window of round 7 (i=6, about 1.86-2.17 s), and the test would fail; load enlarges that detection margin, it does not shrink it, so the test really proves the reset. Credit does not run out: a responsive emitter returns credit; even without any return, 16-8 = 8 > 0 and `chunkReceived` re-arms each time; `creditGranted` does not re-arm while `armed != null` (`StreamTimer.java:84`), so the wait does not stretch. After the loop: 8 chunks and 1 error make 9 events, `events.get(8)` is the error, `atMost(5s)` covers the final 2 s idle wait, and the `cancelMessage`, `idle == 1`, `first_chunk == 0` checks are consistent (the first-chunk timer is removed by the `disarm` in the first `chunkReceived`). Duration is about 2.5 s plus about 2 s = 4.5 s, well inside the class-level `@Timeout(30s)` (`:28`).
3. **README sentence** (`README.md:253-257`): "not after the last chunk has been received or upstream has ended" matches the code (the last chunk sets `upstreamDone` and nothing re-arms, `StreamTimer.java:67-71`; `onError`/`onCompleted` disarm via `upstreamEnded`, `StreamBridge.java:146, :155`, `StreamTimer.java:93-100`; `creditGranted` does not arm once `upstreamDone`, `:84`). No contradiction with the main item (`:225-228`) or the table row (`:184`).

### Optional note (not blocking)
- `StreamBridgeTimeoutTest.java:124-125`: the comment says each gap is "about a seventh" of the limit; with poll and scheduling overhead it is about 1/6.5. No functional effect.

## Acceptance coverage
The AC3a/AC3c margin findings of pass 3 appear fixed by source reading. AC10: README is consistent with the code. The other ACs stay as in pass 3: AC6's phase order is verified by code reading only; AC9 rests on the coordinator report.
