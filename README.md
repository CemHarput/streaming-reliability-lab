# streaming-reliability-lab

A small but real transport layer for token streaming, used to show connection
lifecycle, bounded concurrency, backpressure and recovery after failure.
Everything is synthetic: no real LLM, no API key.

> Status: **stage 1 in progress.** The gRPC contract, the synthetic simulator
> and the gateway's shared-channel SSE bridge are done and tested. The load
> generator is a skeleton.

## Problem

A streaming gateway sits between many clients and an upstream token producer.
Under load and failure it must keep these contracts:

- queues and active requests are bounded; overload is rejected or delayed explicitly
- cancelling or timing out a client request stops the upstream work
- every request reaches exactly one terminal state
- shutdown finishes within a known time

The lab compares two connection strategies (a new channel per request vs. one
shared, lifecycle-managed channel) under the same load, payload and failure policy.

## Out of scope

Chat UI, user accounts, RAG, vector databases, multi-cloud, Kubernetes.

## Modules

| Module | Purpose | State |
|---|---|---|
| `proto/` | `TokenStreamService` and `SimulatorAdminService` contract | done |
| `simulator/` | Synthetic gRPC token producer with timing and fault injection | done |
| `gateway/` | Spring Boot SSE gateway: lifecycle, bounded queue, gRPC adapter | shared channel + SSE bridge done; bounded queue pending |
| `loadgen/` | Closed- and open-model load generator (HdrHistogram) | skeleton |

## Simulator

Every chunk is a pure function of `(seed, request_id, seq)`, so a stream resumed
with `resume_from_seq` is byte-identical to the original suffix.

| Request field | Effect |
|---|---|
| `profile.first_chunk_delay_ms` | delay before the first chunk |
| `profile.chunk_count` | total chunks, `seq` 0..n-1 |
| `profile.chunk_interval_ms` | delay between chunks |
| `profile.chunk_size_bytes` | approximate payload size |
| `fault.type` / `fault.at_seq` | `UNAVAILABLE`, `INTERNAL`, or `HANG` instead of chunk `at_seq` |
| `resume_from_seq` | first seq to emit |

The simulator respects gRPC flow control: when the transport is not ready it
pauses instead of buffering, and counts the pause. `SimulatorAdminService.GetStats`
exposes `active_generations`, so tests can prove that upstream work stopped after
a cancel, deadline or shutdown.

Abrupt connection loss is not simulated in-process. It will be injected with
Toxiproxy between gateway and simulator.

## Run

Requirements: JDK 26.

```bash
./mvnw verify
```

Start the simulator (port `50051`, override with `SIMULATOR_PORT`):

```bash
./mvnw -q -pl simulator -am install -DskipTests
./mvnw -q -pl simulator exec:java
```

With reflection enabled you can call it using `grpcurl`:

```bash
grpcurl -plaintext -d '{"request_id":"demo","seed":7,"profile":{"chunk_count":5,"chunk_interval_ms":100}}' \
  localhost:50051 streamlab.tokenstream.v1.TokenStreamService/GenerateTokens
```

| Environment variable | Default |
|---|---|
| `SIMULATOR_PORT` | `50051` |
| `SIMULATOR_SHUTDOWN_GRACE_SECONDS` | `10` |
| `SIMULATOR_SCHEDULER_THREADS` | available processors |

## Gateway

`GET /stream` opens one upstream `GenerateTokens` call on a single shared,
lifecycle-managed gRPC channel and relays chunks as Server-Sent Events.

| Query parameter | Default | Meaning |
|---|---|---|
| `chunks` | `10` | total chunks |
| `firstDelayMs` | `0` | delay before the first chunk |
| `intervalMs` | `50` | delay between chunks |
| `fault` | `NONE` | `NONE`, `UNAVAILABLE`, `INTERNAL`, `HANG` |
| `faultAtSeq` | `0` | seq at which the fault is injected |
| `seed` | `0` | deterministic content seed |

Malformed values (e.g. `fault=FOO`) and a negative `faultAtSeq` return HTTP 400. Other semantic limits are
enforced by the simulator and surface as an `error` event. The response carries
an `X-Request-Id` header.

| SSE event | Data |
|---|---|
| `chunk` (`id` = seq) | `{"requestId","seq","text","last"}` |
| `complete` | `{"requestId","chunks"}` |
| `error` | `{"requestId","code","message"}` where `code` is a gRPC status code |

Every stream ends with exactly one `complete` or `error` event, except when the
client disconnected or stopped reading (see "Known limits"). Readiness
(`/actuator/health/readiness`) includes the upstream channel and is DOWN while it
is not READY; liveness is unaffected. On shutdown the gateway answers new
streams with 503 + `Retry-After`, drains active streams, force-cancels the rest
(`error` / `CANCELLED`), then closes the channel.

Known limits of the gateway:

- A client that stops reading can block a write indefinitely. On shutdown the
  gateway still cancels that stream's upstream call and frees its slot (except in
  the case described in the next point), and waits
  at most 2 s in total for terminal events. The stuck stream's own `error` event is
  given up on after 5 s. The response is then expected to end through the emitter
  timeout (stream deadline + 5 s), but that has not been verified: a real emitter
  timeout while another thread is blocked in a write is not covered by any test.
  Whether such an open response also stretches the web server's own
  graceful-shutdown phase (`spring.lifecycle.timeout-per-shutdown-phase`, 20 s) has
  not been verified either; treat 20 s as the possible extra bound.
- Exception to the previous point: a stream whose terminal outcome has already been
  claimed and whose last event is being written (the final chunk or `complete` event
  after `COMPLETED`, or the `error` event after an upstream error or an upstream that
  ended early) cannot be cancelled by shutdown. If that write is stuck, the stream
  keeps its registry slot and lease until the write returns; neither shutdown nor the
  emitter timeout can free them. `stop()` stays bounded and logs one warning with the
  number of streams still holding a slot after the drain (no per-stream entries, no
  request ids). Today nothing else depends on the slot; a future per-request lease or
  admission limit would need to release it.
- For streams that do not stall, `close()` takes at most drain timeout + 2 s +
  channel termination (5 s) + 2 s after the forced shutdown.
- Stalled-client behaviour is covered by unit tests with a fake emitter that blocks,
  not by a real TCP-level stalled connection.
- A stream whose final chunk has not been written yet when shutdown claims it ends
  with `error` / `CANCELLED` instead of the last chunk. Once the final chunk write
  has begun, shutdown no longer cancels the stream: it ends with `complete` if the
  write succeeds, or is freed as a client disconnect (no `complete` event) if the
  write fails. Nothing is ever sent after a terminal event.

| Setting (`application.yaml`) | Default |
|---|---|
| `lab.connection-mode` | `shared` (only value for now) |
| `lab.upstream.target` | `localhost:50051` |
| `lab.upstream.stream-deadline` | `60s` |
| `lab.shutdown.drain-timeout` | `10s` |
| `lab.shutdown.channel-termination-timeout` | `5s` |

## Decisions

- [ADR-0001: Synthetic simulator on plain grpc-java](docs/adr/0001-simulator-on-plain-grpc-java.md)

## License

MIT
