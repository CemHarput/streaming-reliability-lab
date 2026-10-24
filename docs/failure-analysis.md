# Failure analysis: the stream contract and a deliberately leaky handler

The gateway's stream guarantees are written down once as a contract and checked on two
implementations with the same scenarios: `StreamBridge` (REAL) and
`LeakyStreamHandler` (LEAKY, test code only, in
`gateway/src/test/java/io/github/cemharput/streamlab/gateway/stream/`). The point is to
show that the contract tests would notice the two classic stream bugs, without breaking
the build.

## The contract (`StreamContract`)

| Clause | What is measured | Violation |
|---|---|---|
| `SINGLE_TERMINAL` | the events the emitter recorded (`event:complete` / `event:error`) | not exactly one terminal event where one is required, or any event after it, or an event after the client was gone |
| `RELEASE_ONCE` | `CountingLease.closes`, permit releases, permits held | lease closed or permit released other than once, or a permit still held |
| `UPSTREAM_CANCELLED` | simulator `GetStats`: `active_generations` back to 0 within a window, plus `cancelled_total` / `completed_total` / `faulted_total`; for shutdown also the duration | the generation stays active, or the counters do not match the cause |
| `NO_TIMER_LEAK` | `StreamTimeouts.pendingTimers()` | not 0 at the end |
| `QUEUE_BOUNDED` | (slow consumer only) chunks the simulator emitted ahead of the client, and `bufferedChunks()` | above the queue capacity |

`StreamContract.check` returns a list of violations and never throws. The tests compare
the set of violated clauses with the expected set (`StreamContract.expected`) for equality:
REAL expects the empty set in every scenario; LEAKY expects `EXPECTED_LEAKY`. A missing
expected violation and an unexpected violation both fail the test, so the harness cannot
silently go blind and the build stays green while LEAKY is found guilty.

## Scenarios

`StreamContractTest` runs six scenarios against the real in-process simulator through the
real gRPC stub: normal completion, upstream error (`FAULT_TYPE_INTERNAL`), client gone
(`FAULT_TYPE_HANG`), shutdown (`ActiveStreamRegistry.stop()` on REAL, `cancelForShutdown()`
on LEAKY), first chunk timeout, slow consumer. Every scenario first checks a
precondition (for client gone and shutdown: two chunk events, `active_generations == 1`,
one armed timer) and reports a failure there as a harness error. After the trigger the
test waits until all measurements stop changing for 500 ms and judges that last
snapshot. `TerminalRaceContractTest` runs completion against a shutdown cancel and
completion against client gone. `TerminalPathRaceTest` races six selected pairs of terminal
paths and the registry's admission against `stop()` (see "What the terminal-path stress adds").

## The two flaws

(a) **Client gone does not cancel upstream** (`LeakyStreamHandler.clientGone`, marked
`FLAW (a)`). Timer and capacity are freed, nothing is sent, but `call.cancel` is never
called. Measured: `UPSTREAM_CANCELLED` in scenario `CLIENT_GONE` only; the generation
stays at `active_generations == 1` with `cancelled_total == 0` for the whole 2 s window.
Log line when the contract tests are run on their own (see "Running it"):
`contract violation [LEAKY / CLIENT_GONE] UPSTREAM_CANCELLED: active_generations=1 after 2 s (expected 0); cancelled_total=0 completed_total=0 (expected 1 and 0)`.
The assertions, not this log line, are the evidence: the line is informational.

(b) **Terminal step is check-then-act** (`LeakyStreamHandler.terminal`, marked
`FLAW (b)`: `if (terminated) return; ...; terminated = true; act.run()`). The test makes
the interleaving deterministic with a `CountDownLatch` armed only in the race tests
(`armTerminalRace`): both paths must pass the check and reach the latch before either
sets the flag. The outcome depends on the latch, not on timing; if one side never
arrives, `raceHookBroken()` is set after 5 s and the test fails as a harness error.
Measured: completion vs shutdown cancel gives `{SINGLE_TERMINAL, RELEASE_ONCE}` (two
terminal events, lease closed twice, permit released twice); completion vs client gone
gives `{RELEASE_ONCE}` (one `complete` event, lease and permit released twice).

All other LEAKY scenarios satisfy the contract (`expectationTableCoversBothFlawsAndOnlyThem`
pins the table to exactly these three non-empty rows).

## How REAL differs

- `StreamBridge.claim` is one compare-and-set; the winner closes the timer there.
- The winner cancels the live upstream call first and then calls `releaseCapacity()`,
  all before any write (`clientGone`, `timedOut`, `cancelForShutdown`, `overflow`).
  `COMPLETED` is claimed in `writeChunkLocked` before the last chunk is written.
- The error terminal event is written under `sendLock` with `tryLock(SEND_LOCK_WAIT)`
  (`sendErrorAndComplete`). The `complete` event is written under the same
  `sendLock.lock()` as the chunks (`writeChunk` / `writeChunkLocked`), which waits
  without a limit; capacity is nevertheless released at the claim, before that write.
  Which paths hold `sendLock` while they claim and release capacity:
  - `COMPLETED`: claim and release run in `writeChunkLocked` with the drainer holding
    `sendLock`; there is no cancel: the last chunk has been received, so the upstream call ends by
    itself (its `onCompleted` may still be pending when the claim happens).
  - a failed write of a non-last chunk: `clientGone()` is called from `writeChunkLocked`,
    so claim, cancel and release also run under `sendLock`.
  - upstream end/error (`handleUpstreamEnd`), `timedOut`, `cancelForShutdown`, `overflow`
    and a `clientGone` from an emitter callback: claim, release (and the cancel where the
    call is still live) are taken without `sendLock` as far as `StreamBridge` itself
    goes; whether Spring runs an emitter callback on a thread that holds the lock was not
    checked.
- Client gone writes no terminal event (so scenario `CLIENT_GONE` expects none).

A limit of the `CLIENT_GONE` scenario: its upstream is a HANG fault, so no chunk
arrives after the trigger anyway. The clause "nothing after the trigger" therefore only
catches an implementation that writes a terminal or another event to the disconnected
client; a defect that keeps writing chunks after the trigger would not show up here.

Simplifications of LEAKY that are not flaws and that the contract does not measure:
chunks are written synchronously on the gRPC callback thread (no queue and drainer), a
timeout writes its error on the timer thread, there is no retry and no overflow path.

## What the REAL stress does and does not show

`TerminalRaceContractTest` races REAL with a `CyclicBarrier` for 200 iterations per race
(a busy spin of 0 to 24 steps of 1500 `Thread.onSpinWait` shifts the cancel against the
completion from iteration to iteration; no sleep) and asserts the contract per
iteration, never who wins. In local runs on 2026-10-24 (three runs) both outcomes were
observed (the test logs the distribution, for example `COMPLETE=184, ERROR=16`); the
distribution is not asserted and depends on the machine, so on a single-core or very
slow runner it could collapse to one side and the stress would then be weaker without
any test failing. This cannot prove that no race exists: that
argument is the CAS in `claim()` (ADR-0002). A hypothesis, not verified, is that a
stronger scheduler perturbation than the spin would reach interleavings this run does
not.

## What the terminal-path stress adds

`TerminalPathRaceTest` (REAL only, no LEAKY) races six selected pairs of terminal paths of
`StreamBridge` (listed in the table below), and the registry's admission against `stop()`. Each iteration builds a new
stream, runs `StreamContract.check` (REAL expects the empty clause set) and additionally
checks that the pair (terminal event, upstream cancel message recorded by
`FakeUpstream.cancelMessage`) is one the pair allows. Which side wins is never asserted.
The existing scenarios `RACE_COMPLETION_VS_SHUTDOWN` (`EXACTLY_ONE_ANY`) and
`RACE_COMPLETION_VS_CLIENT_GONE` (`AT_MOST_ONE`) are used only as carriers of the shape; for
them `check` looks at `SINGLE_TERMINAL`, `RELEASE_ONCE` and `NO_TIMER_LEAK`. The
upstream cancel is not judged by the `UPSTREAM_CANCELLED` clause (that one reads simulator
counters) but by the test's own outcome table below.

| Test | Side A | Side B | Allowed (terminal event, cancel message) |
|---|---|---|---|
| `timeoutRacesCompletion` | IDLE timer (10 ms) fires `timedOut` on the scheduler thread | last chunk plus `onCompleted`, at 0 to 38 ms after chunk 0 (busy wait, 2 ms steps) | `complete`, none; or `error DEADLINE_EXCEEDED`, `idle timeout between chunks` |
| `timeoutRacesClientGone` | same timer | `clientGone` (emitter completion callback) at the same offsets | `error DEADLINE_EXCEEDED`, idle message; or no terminal event, `client disconnected` |
| `upstreamErrorRacesClientGone` | `onError(INTERNAL)`: `recordEnd` then a drainer runs `handleUpstreamEnd` | `clientGone` | `error INTERNAL`, none; or no terminal event, `client disconnected` |
| `upstreamErrorRacesShutdownCancel` | same | `cancelForShutdown` | `error INTERNAL`, none; or `error CANCELLED`, `gateway shutting down` |
| `overflowRacesClientGone` | a chunk beyond the full queue: `overflow` | `clientGone` | `error RESOURCE_EXHAUSTED`, `per-request queue overflow`; or no terminal event, `client disconnected` |
| `overflowRacesShutdownCancel` | same | `cancelForShutdown` | overflow error as above; or `error CANCELLED`, `gateway shutting down` |
| `admissionRacesStop` | `tryRegister`, then `start()` (odd iterations also deliver a last chunk and `onCompleted`) | `ActiveStreamRegistry.stop()` with a 20 ms drain timeout | refused (see below); or `complete`, none; or `error CANCELLED`, `gateway shutting down` |

Retry is disabled (`RetryPolicy.disabled()`), and INTERNAL is not retryable anyway.

**Which paths hold `sendLock`** (read from `StreamBridge`, not measured):

- Hold it: the error terminal event (`sendErrorAndComplete`, `tryLock(SEND_LOCK_WAIT)`, 5 s),
  chunks and the complete event (`writeChunk`, `sendLock.lock()`). The `COMPLETED` claim and
  `releaseCapacity()` run inside `writeChunkLocked` under it. A failed write of a non-last
  chunk calls `clientGone()` there too, so claim, cancel and release run under `sendLock`.
- Do not hold it for claim and release: `handleUpstreamEnd`, `timedOut`,
  `cancelForShutdown`, `overflow`, and `clientGone` called from an emitter callback (as far as
  `StreamBridge` goes; whether a container runs a callback on a thread that holds the lock was
  not checked).

In the overflow pairs the drainer is deliberately blocked in the chunk 0 write
(`RecordingEmitter.stalled()`) while holding `sendLock`, from before the race until both sides
have returned. Two extra checks follow from the code: lease close and permit release happen
exactly once before the gate opens (claim and release do not wait for `sendLock`), and the
log ends with exactly one chunk event (the queued chunks are discarded).

**Registry race.** `tryRegister` and `stop()` both take the registry `lock`, and `stop()` sets
`accepting = false` under it, so no admission can happen once `stop()` has set `accepting = false` (a `tryRegister` that gets the
lock before that still succeeds). The test
therefore asserts a two-way split, not a "late admit during drain": (REFUSED) `tryRegister`
returned false: no upstream call, no event, no lease close, no permit release, no permit held,
no timer. (ADMITTED) the contract holds with exactly one terminal event, and it is either
`complete` or `error CANCELLED` with `gateway shutting down`. `permitsHeld()` read on the `stop()` thread right after `stop()` returned is 0 for REFUSED
and for `error CANCELLED` (the forced cancel claims, cancels and releases inside `stop()`).
For `complete` it can legally be 1 (fact from the code, not observed in the recorded runs): if
the forced cancel loses the claim to a `COMPLETED` claim whose drainer has not yet reached
`releaseCapacity()` (`writeChunkLocked`), `stop()` returns in between and the permit is
released exactly once right after; the test allows 0 or 1 there and counts the 1s in a log
line, and the settled snapshot still requires exactly one release.

**What this does not show** (facts unless marked as hypothesis):

- It is not proof: no stress test can show that no race exists; the argument remains the
  compare-and-set in `claim()` (ADR-0002).
- The timer pairs depend on the scheduler's resolution (fact: IDLE is 10 ms and the window is
  0 to 38 ms); the sweep covers both sides on this machine, hypothesis for others (hypothesis:
  Windows timers have a coarser resolution than 1 ms).
- The distributions are machine dependent and not asserted. On a single core or a very busy
  machine a pair could collapse to one side and the stress would then be weaker without any
  test failing.
- Pairs not raced (not a complete list of all combinations): first-chunk timeout, timeout against
  shutdown, timeout against overflow or upstream error, client gone against shutdown cancel, and the
  `clientGone` that comes from a failed non-last chunk write under `sendLock`.
- Only the first upstream cancel message is recorded (`FakeUpstream.cancelMessage` keeps the
  first one), so a second cancel by the losing path would not be seen; a losing path doing
  extra release work is still caught by `RELEASE_ONCE`.
- Only `tryRegister` is raced; the production `admit(bridge)` does the same check under the
  same `lock` but is not raced separately, and the WAIT overload path is covered only by
  `ActiveStreamRegistryTest`.
- Retry versus cancel races are out of scope; no real container threads are used (fake emitter
  and fake upstream).
- Hypothesis, not verified: the outcomes in the table below follow the direction of the
  two-way spin skew closely (the two sides are nearly even), so the number of iterations in
  which both sides are really close to each other is probably small.

Local distributions on 2026-10-24 (observation, three runs of the focused command; machine
dependent, not asserted):

| Test | Outcomes (range over three runs) |
|---|---|
| `timeoutRacesCompletion` (100) | `COMPLETE` 46 to 49, `ERROR:DEADLINE_EXCEEDED` 51 to 54 |
| `timeoutRacesClientGone` (100) | `ERROR:DEADLINE_EXCEEDED` 51 to 54, `NONE` 46 to 49 |
| `upstreamErrorRacesClientGone` (200) | `ERROR:INTERNAL` 90 to 92, `NONE` 108 to 110 |
| `upstreamErrorRacesShutdownCancel` (200) | `ERROR:INTERNAL` 92 to 93, `ERROR:CANCELLED` 107 to 108 |
| `overflowRacesClientGone` (200) | `ERROR:RESOURCE_EXHAUSTED` 97, `NONE` 103 |
| `overflowRacesShutdownCancel` (200) | `ERROR:RESOURCE_EXHAUSTED` 94, `ERROR:CANCELLED` 106 |
| `admissionRacesStop` (200) | `REFUSED` 102 to 103, `COMPLETE` 49, `ERROR:CANCELLED` 48 to 49 |

The class took about 10.9 s (surefire, JaCoCo agent on) on that machine.

## Connection modes

`StreamContractTest#contractHoldsInEveryConnectionMode` runs the REAL expectation (no violations) for six
simulator scenarios (completion, upstream error, client gone, shutdown cancel, first-chunk timeout, slow
consumer) with the channel taken from the real shared provider and from the real per-request provider, against
the in-process simulator. The contract itself is unchanged. As an additional observation, per-request mode must
open one channel per stream and shut it down once, never with `shutdownNow()`; shared mode must not touch its
channel until the provider stops. The terminal-path race tests still use a fake upstream starter and no
provider, so they do not exercise the channel close; the lease races are in `PerRequestChannelProviderTest`.
Like the other stress tests, these cannot prove that no race exists. See
[ADR-0003](adr/0003-per-request-connection-mode.md).

## Running it

```
./mvnw -B -pl gateway -am verify -Dtest=StreamContractTest,TerminalRaceContractTest,TerminalPathRaceTest -Dsurefire.failIfNoSpecifiedTests=false
```

Violations of LEAKY and the race outcomes appear as `INFO` lines of
`StreamContract` / `TerminalRaceContractTest` / `TerminalPathRaceTest` (`System.Logger`) in the surefire output of
this focused command (15 `contract violation` lines on 2026-10-24; some come from the
meta tests' synthetic observations and are marked `synthetic`). In a full
`./mvnw -B verify` the same lines were not present in the console output on the same
day, although all assertions ran and passed. Earlier runs of this project showed log
output going missing after Spring tests in the same test JVM; the cause was not
isolated, so treat the log as a convenience and the test assertions as the proof.
