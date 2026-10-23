# streaming-reliability-lab

A small but real transport layer for token streaming, used to show connection
lifecycle, bounded concurrency, backpressure and recovery after failure.
Everything is synthetic: no real LLM, no API key.

> Status: **stage 1 in progress.** The gRPC contract, the synthetic simulator
> and the gateway's shared-channel SSE bridge with bounded resources (active
> stream limit, per-stream queue, metrics) are done and tested. The load
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
| `gateway/` | Spring Boot SSE gateway: lifecycle, bounded queue, gRPC adapter | shared channel + SSE bridge + bounded resources (limit, per-stream queue, metrics) done |
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

### Bounded resources

Three bounds apply to every gateway instance.

**Active stream limit.** `lab.limits.max-active-streams` caps the streams that hold
a *permit*. A stream takes a permit when it is admitted and gives it back as soon as
its terminal outcome is claimed (completed, upstream error, client gone, shutdown),
before the last event is written. When no permit is free, `lab.limits.overload-policy`
decides:

| Policy | Behaviour |
|---|---|
| `reject` (default) | answer 503 + `Retry-After: 1` immediately |
| `wait` | the request thread waits up to `lab.limits.queue-wait` for a permit; at most `lab.limits.max-waiting` requests wait at once, further ones get 503 without waiting |

Waiting is not fair: a newly arriving request may take a freed permit ahead of a
waiting one. Request threads are virtual threads, so waiting does not use up
platform threads (with `spring.threads.virtual.enabled=false` it would occupy up to
`max-waiting` of them). A rejected request opens no SSE response and makes no
upstream call. The 503 reason is counted in `rejected_total`:

| `reason` | Cause |
|---|---|
| `limit` | no permit (reject policy), or `max-waiting` reached |
| `queue_timeout` | waited `queue-wait` without getting a permit |
| `shutdown` | shutdown started (also wakes waiting requests immediately; they never delay shutdown) |
| `upstream_unavailable` | no upstream channel lease could be acquired |
| `interrupted` | the waiting request thread was interrupted |

The 400 responses for malformed parameters are not counted.

**Per-stream queue and upstream flow control.** Each stream has a bounded queue of
`lab.limits.stream-queue-capacity` chunks. Upstream flow control is manual: the
gateway grants the simulator that many chunks up front and one more after each
chunk was written to the client, so a slow client makes the upstream pause instead
of the gateway buffering. The queue never holds more than its capacity; if upstream
sends more than it was granted, the stream ends with one `error` event
(`RESOURCE_EXHAUSTED`) and the upstream call is cancelled. Queued chunks are always
written before the upstream's `error` or end; a client disconnect or shutdown
cancellation drops queued chunks. Writes to the client run on a separate virtual
thread per stream, never on the gRPC callback thread.

**Metrics** (Micrometer, `/actuator/metrics/<name>`):

| Metric | Meaning |
|---|---|
| `active_streams` | gauge: streams holding a permit |
| `queue_depth{queue="chunks"}` | gauge: chunks queued across all streams |
| `queue_depth{queue="admission"}` | gauge: requests waiting for a permit |
| `rejected_total{reason}` | counter, reasons above |

No Prometheus registry is on the classpath; `prometheus` appears in the exposure
list but has no effect today. How a Prometheus registry would render the
`rejected_total` name has not been checked.

Known limits of the gateway:

- The permit and the lease are released when the terminal outcome is claimed, before
  the terminal event is written, so a client whose final write is stuck does not leak
  capacity. The stream stays *tracked* (it counts for `activeCount`, shutdown drain and
  forced cancel) until that write returns. `stop()` stays bounded and logs one warning
  with the number of streams still finishing a final write (no per-stream entries, no
  request ids). Such a stream cannot be cancelled by shutdown, because its outcome is
  already claimed.
- A stuck write of an *intermediate* chunk is different: no outcome is claimed, and the
  upstream's `error` or deadline signal waits behind it in the queue. The stream
  therefore keeps its permit until the write returns, shutdown cancels it, or the
  emitter timeout (stream deadline + 5 s) ends it. That the emitter timeout callback
  runs while another thread is blocked in a write is a hypothesis; no test covers it.
  This is not new (a stuck write already blocked delivery of upstream callbacks), but
  now the blocked thread is the stream's own drainer and the upstream pauses by flow
  control. Until then the stream counts against `max-active-streams`.
- On shutdown the gateway cancels the upstream call of every stream whose outcome is
  not yet claimed and frees its permit, and waits at most 2 s in total for terminal
  events. A stuck stream's own `error` event is given up on after 5 s. The response is
  then expected to end through the emitter timeout (stream deadline + 5 s), but that
  has not been verified. Whether such an open response also stretches the web
  server's own graceful-shutdown phase (`spring.lifecycle.timeout-per-shutdown-phase`,
  20 s) has not been verified either; treat 20 s as the possible extra bound.
- The limit bounds upstream calls and leases, not open HTTP responses: a stream
  finishing its last write has already given up its permit.
- For streams that do not stall, `close()` takes at most drain timeout + 2 s +
  channel termination (5 s) + 2 s after the forced shutdown. Waiting requests do not
  add to this.
- Stalled-client behaviour is covered by unit tests with a fake emitter that blocks,
  not by a real TCP-level stalled connection.
- A stream whose final chunk has not been written yet when shutdown claims it ends
  with `error` / `CANCELLED` instead of the last chunk. Once the final chunk write
  has begun, shutdown no longer cancels the stream: it ends with `complete` if the
  write succeeds, or is freed as a client disconnect (no `complete` event) if the
  write fails. Nothing is ever sent after a terminal event.
- With a per-request connection mode (not implemented) the lease would have to be
  acquired after admission, not before as it is now with the no-op shared lease.

| Setting (`application.yaml`) | Default |
|---|---|
| `lab.connection-mode` | `shared` (only value for now) |
| `lab.upstream.target` | `localhost:50051` |
| `lab.upstream.stream-deadline` | `60s` |
| `lab.shutdown.drain-timeout` | `10s` |
| `lab.shutdown.channel-termination-timeout` | `5s` |
| `lab.limits.max-active-streams` | `100` (>= 1) |
| `lab.limits.overload-policy` | `reject` (`reject` or `wait`) |
| `lab.limits.queue-wait` | `5s` (positive; used by `wait`) |
| `lab.limits.max-waiting` | `100` (>= 1; used by `wait`) |
| `lab.limits.stream-queue-capacity` | `16` (1..4096) |

Invalid values stop the application at startup with a message naming the property.

## Decisions

- [ADR-0001: Synthetic simulator on plain grpc-java](docs/adr/0001-simulator-on-plain-grpc-java.md)

## License

MIT
