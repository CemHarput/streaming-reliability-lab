# ADR-0003: Per-request connection mode

- Status: accepted
- Date: 2026-10-24

## Context

The gateway used one shared, lifecycle-managed gRPC channel. To compare it later with a channel per stream, the
gateway needs a second mode that is selected by `lab.connection-mode`, without a second copy of the stream logic
in `StreamBridge`. The stream already takes a `ChannelProvider.Lease` and closes it exactly once, on the path that
wins the terminal claim (ADR-0002). That close can run while the drainer holds `sendLock`.

## Decision

- **Lazy channel in the lease.** `PerRequestChannelProvider.acquire()` returns a lease without a channel. The first
  `Lease.channel()` call creates it (CAS `EMPTY -> OPEN`), with no lock held; later calls return the same channel.
  The controller still acquires the lease before admission, but the channel is only created when the upstream call
  starts, after admission and permit. Rejected and waiting requests never open a channel. No change in
  `StreamController` or `StreamBridge` was needed.
- **Close on the existing release path.** `Lease.close()` (CAS to `CLOSED`) calls `ManagedChannel.shutdown()` only. It
  is idempotent and non-blocking, so it is safe where `sendLock` is held. The provider never calls
  `awaitTermination` there; channels that were shut down are tracked and waited for in `stop()`.
- **`shutdown()`, not `shutdownNow()`.** On cancel paths the call is already cancelled. On the `COMPLETED` claim
  the last chunk is still being written and trailers may not have arrived; `shutdownNow()` could cancel a call that
  is about to finish. A retry call that tries to start after the claim gets an `IllegalStateException` from
  `channel()` or is cancelled by the bridge's terminal check (read from the code, not stressed separately).
- **Retries reuse the stream's channel.** The lease already lives for the whole stream, so closing stays on one path
  and "once per stream" follows from the claim CAS. A channel per attempt would need a second close path in the
  bridge, and a fresh channel would skip the channel's own connect backoff, so the comparison would no longer
  isolate the cost of a channel per stream.
- **Provider stop as backstop.** `stop()` closes leases that are still open (normally none: streams are drained or
  force-cancelled in an earlier phase), waits for the shut-down channels within
  `lab.shutdown.channel-termination-timeout` as one deadline, then calls `shutdownNow()` on the rest and waits 2 s.
  If `stop()` runs while a `channel()` call is creating a channel, that channel is closed by the lease itself; it may
  be missing from the set `stop()` waits on, but it has been asked to shut down. If `close()` runs between the
  lease's state change to open and its registration in the open set, `channel()` re-reads the state after the
  registration, removes the entry itself and throws, so a closed lease never stays in the set.
- **Bean selection.** `UpstreamConfiguration` registers `SharedChannelProvider` or `PerRequestChannelProvider`
  with conditions that read `lab.connection-mode` through the `Binder`, so spelling rules match the properties
  binding. Exactly one `ChannelProvider` bean exists.
- **Readiness.** In per-request mode `state()` reports `READY` while the provider runs and `SHUTDOWN` after: there
  is no long-lived channel to probe. An idle probe channel would add a connection that the comparison does not have.
- **Observability.** One gauge `connection_mode{mode="shared"|"per_request"}` and one startup INFO line per provider.
  No per-request ids and no per-channel logs or tags.

## Alternatives considered

- **Eager channel in `acquire()`.** Simpler, but it would create channels for requests that are then rejected or
  queued, and it would run channel creation before admission.
- **Channel per attempt.** See above: second close path in `StreamBridge`, different retry behaviour.
- **`shutdownNow()` on close.** Can cancel a finishing call and distort the simulator counters and the contract.
- **Blocking `awaitTermination` in `close()`.** Would block while `sendLock` is held.
- **A channel pool.** A different mode with different semantics; out of scope.

## Consequences

- Both modes pass the six simulator scenarios of the stream contract (`StreamContractTest`); per-request additionally
  shows one channel and one `shutdown()` per stream (counting factory). The other contract and race tests use the
  test channel or fake starters and do not go through a provider.
- Not measured: latency, throughput, CPU and memory, connection setup cost, real Netty/TCP/HTTP/2, TLS, event-loop
  sharing, port usage and `TIME_WAIT`, connect backoff after a real network failure. All tests use in-process
  channels. After a real `UNAVAILABLE` a fresh channel can behave differently from a warm shared one; that is a
  hypothesis, not tested here.
- The lease races are stress-tested with a bounded number of iterations; a stress test does not prove their absence.
  The argument is the lease CAS.
- Under load the process creates many channels per second; whether that is acceptable is exactly what the later
  comparison is meant to show.
