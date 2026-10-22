# ADR-0001: Synthetic simulator on plain grpc-java

- Status: accepted
- Date: 2026-10-22

## Context

The lab needs an upstream token producer whose timing and failures are fully
controlled, so that gateway behaviour can be tested deterministically and
benchmarks can run without a paid API. The producer must also make it observable
whether upstream work actually stopped after a client cancel or deadline.

## Decision

- Implement the simulator with plain `grpc-java` (Netty transport), not Spring.
- Emit chunks from a small shared `ScheduledExecutorService`; no thread per stream.
- Respect transport flow control (`isReady` / `onReadyHandler`) instead of
  buffering when the consumer is slow.
- Make chunk content a pure function of `(seed, request_id, seq)`.
- Expose counters through a separate `SimulatorAdminService`.

## Alternatives considered

- **Spring gRPC server.** Rejected for the simulator: the server is a test
  fixture with no need for dependency injection, and a plain `main` keeps
  startup and shutdown order explicit.
- **Ignoring flow control.** Rejected: an upstream that writes regardless of
  readiness hides backpressure inside Netty buffers, which is exactly the
  behaviour the lab is meant to expose.
- **Random content.** Rejected: deterministic content lets tests detect gaps,
  duplicates and reordering after resume without extra bookkeeping.

## Consequences

- Real LLM providers do not always pause on backpressure; the simulator is a
  best case. A future `ignore_flow_control` option can model the worst case.
- Latency numbers from the simulator are synthetic and must not be reported as
  model time-to-first-token.

## Revisit when

The gateway needs a producer behaviour the request fields cannot express, or the
emit scheduler becomes a measured bottleneck in benchmarks.
