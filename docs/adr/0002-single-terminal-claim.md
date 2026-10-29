# ADR-0002: Single terminal claim by CAS; capacity released before the terminal write

- Status: accepted
- Date: 2026-10-24

## Context

A stream can end in several ways at nearly the same time: the last chunk is written,
upstream fails, the client disconnects, a timer fires, shutdown force-cancels. Each
path must produce at most one terminal SSE event, release the permit and the lease
exactly once, and cancel the upstream call when the stream did not end by itself. The
client write is blocking and can be stuck on a client that stopped reading.

## Decision

- The terminal outcome is claimed with one `AtomicReference.compareAndSet`
  (`StreamBridge.claim`). Whoever wins closes the stream's timer in the same step.
- The winner cancels the upstream call (if it is still live) and then calls
  `releaseCapacity()` (lease close and `ActiveStreamRegistry.releasePermit`), all before
  any write. `clientGone`, `timedOut`, `cancelForShutdown` and `overflow` follow this
  order; the `COMPLETED` claim happens in `writeChunkLocked` before the last chunk is
  written.
- Writes go through `sendLock`, so events stay ordered: the error terminal event takes
  it with `tryLock(SEND_LOCK_WAIT)` (`sendErrorAndComplete`), while chunks and the
  `complete` event are written under `sendLock.lock()`, which waits without a limit
  (`writeChunk` / `writeChunkLocked`). The claim itself is a lock-free CAS. On the `COMPLETED` path (claim and release, no
  cancel) and when a non-last chunk write fails (`clientGone`; a failed write of the last chunk only
  sets `CLIENT_GONE`, capacity is already released at the `COMPLETED` claim) these steps happen inside
  `writeChunkLocked` while the drainer holds `sendLock`; on the other paths
  (`handleUpstreamEnd`, `timedOut`, `cancelForShutdown`, `overflow`) `StreamBridge` takes
  no lock for them. Losers of the claim do nothing.

## Alternatives considered

- **A check-then-act flag** (`if (terminated) return; terminated = true; ...`). Rejected:
  two paths can both pass the check. `LeakyStreamHandler.terminal` (test code) is exactly
  this and, with the race made deterministic by a latch, produces two terminal events
  and a double release (see `docs/failure-analysis.md`).
- **Doing the whole terminal step under `sendLock`.** Rejected: a stuck client write
  would hold shutdown, upstream cancellation and capacity hostage.
- **Releasing capacity after the terminal write, in `finish`.** Rejected: a stuck final
  write would leak the permit and the lease for as long as it is stuck.

## Consequences

- When the client is gone no terminal event is written (`clientGone` does not send one).
- Once `COMPLETED` is claimed, shutdown cannot cancel the stream (its `claim` fails).
- After a completion, upstream error, timeout or overflow a stream stays tracked in the
  registry until its terminal write returns, although it no longer holds a permit or
  lease (docs/gateway.md, "Known limits"); after a shutdown cancel or client gone it is removed
  by `finish()` before the terminal write.
- The guarantee comes from the CAS in `claim()`. The stress run in
  `TerminalRaceContractTest` cannot prove the absence of a race; it only checks the
  invariant in each iteration.

## Revisit when

A terminal path is added that does not go through `claim()`, or the terminal write has
to be ordered against a path that must not wait for `sendLock`.
