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
completion against client gone.

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

## Running it

```
./mvnw -B -pl gateway -am verify -Dtest=StreamContractTest,TerminalRaceContractTest -Dsurefire.failIfNoSpecifiedTests=false
```

Violations of LEAKY and the race outcomes appear as `INFO` lines of
`StreamContract` / `TerminalRaceContractTest` (`System.Logger`) in the surefire output of
this focused command (15 `contract violation` lines on 2026-10-24; some come from the
meta tests' synthetic observations and are marked `synthetic`). In a full
`./mvnw -B verify` the same lines were not present in the console output on the same
day, although all assertions ran and passed. Earlier runs of this project showed log
output going missing after Spring tests in the same test JVM; the cause was not
isolated, so treat the log as a convenience and the test assertions as the proof.
