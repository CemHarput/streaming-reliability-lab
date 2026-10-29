# streaming-reliability-lab

A small but real transport layer for token streaming, used to show connection
lifecycle, bounded concurrency, backpressure and recovery after failure.
Everything is synthetic: no real LLM, no API key.

> Status: **stage 1 in progress.** The gRPC contract, the synthetic simulator
> and the gateway's SSE bridge with bounded resources (active
> stream limit, per-stream queue, metrics), upstream timeouts with
> cancellation checks and two selectable connection modes (shared channel,
> channel per request) are done and tested. The load generator (closed model, raw JSONL output) is done and
> tested against an in-test server, and it drove the measurements of the result set described below.
>
> The benchmark protocol runner (`bench/`, [protocol](docs/benchmark-protocol.md)) is done: it starts fresh
> simulator and gateway JVMs per run, drives the load generator and writes raw results plus a manifest. It has been
> checked with a smoke preset on Windows 11 and has run the full default set once, on Linux (see below).
>
> The benchmark report tool (`report/`) is done and tested with synthetic result sets: it reads one result set of the
> runner and writes a descriptive report, derived tables and charts.
>
> **One full result set has been run and analysed:** 24 runs (concurrency 50, 100 and 150, both connection modes, 4
> repetitions each, ABBA order) on one Azure VM (Standard_D4als_v6, 4 vCPU on 2 physical cores, Ubuntu 24.04, Sweden
> Central). All 24 runs were usable and no run carries a stability flag. The report is
> [docs/benchmark-report.md](docs/benchmark-report.md). In that set the per-request mode had a higher time to first
> chunk than the shared mode at every concurrency (p50 medians 0.6 to 1.0 ms higher, on streams of about 1955 ms), and
> at concurrency 150 about 90 % of the requests were rejected by the active stream limit of 100 in both modes. This
> describes that set on that machine only: it is synthetic, runs on one host, has 4 repetitions per cell and states no cause.

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
| `gateway/` | Spring Boot SSE gateway: lifecycle, bounded queue, gRPC adapter | shared channel + selectable connection modes (shared / per-request) + SSE bridge + bounded resources (limit, per-stream queue, metrics) + first-chunk/idle timeouts and upstream-cancel checks + bounded reconnect (retry with backoff, resume) done |
| `loadgen/` | Load generator client: closed model + linear ramp, raw JSONL + convenience summary (HdrHistogram); open model deferred | closed model done; no performance claim |
| `bench/` | Benchmark protocol runner: per-run fresh JVMs, pinned settings, ABBA run order, raw results + manifest (no analysis) | runner + protocol v1 done; one full set run (see status) |
| `report/` | Benchmark report tool: reads one result set read-only, writes a descriptive report, CSV/JSON tables and SVG dot plots (no inference) | tool done; one result set analysed ([report](docs/benchmark-report.md)); describes that set only |

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

`verify` also measures test coverage with JaCoCo (0.8.15, the first release that supports
Java 26). Each module with tests writes an HTML report to `<module>/target/site/jacoco/index.html`
and an XML report to `<module>/target/site/jacoco/jacoco.xml`, which the Sonar step in the CI
workflow reads. The `proto` module has no tests and produces no report.

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

`GET /stream` opens one upstream `GenerateTokens` call on a gRPC channel (one shared,
lifecycle-managed channel by default; see "Connection modes") and relays chunks as
Server-Sent Events.

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
client disconnected or stopped reading (see "Known limits"). A timeout ends the
stream with an `error` event (see "Timeouts and cancellation"). Readiness
(`/actuator/health/readiness`) includes the upstream channel and is DOWN while it
is not READY; liveness is unaffected. On shutdown the gateway answers new
streams with 503 + `Retry-After`, drains active streams, force-cancels the rest
(`error` / `CANCELLED`), then closes the channel.

### Bounded resources

Three bounds apply to every gateway instance.

**Active stream limit.** `lab.limits.max-active-streams` caps the streams that hold
a *permit*. A stream takes a permit when it is admitted and gives it back as soon as
its terminal outcome is claimed (completed, upstream error, client gone, shutdown,
timeout, queue overflow), before the last event is written. When no permit is free, `lab.limits.overload-policy`
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
| `timeouts_total{kind}` | counter: streams ended by a timeout, `kind` = `first_chunk` or `idle` (see "Timeouts and cancellation") |
| `retries_total{reason}` | counter: retry calls started, `reason` = `restart` or `resume` (see "Bounded reconnect") |
| `retry_exhausted_total` | counter: streams that ended with the upstream error after retries or budget ran out |
| `connection_mode{mode}` | gauge, always 1 for the configured mode; `mode` = `shared` or `per_request` (see "Connection modes") |

No Prometheus registry is on the classpath; `prometheus` appears in the exposure
list but has no effect today. How a Prometheus registry would render the
`rejected_total` name has not been checked.

### Timeouts and cancellation

Two timers bound how long the *upstream* may stay silent. Neither measures a slow
client.

| Timer | Starts | Is reset by | Armed while |
|---|---|---|---|
| first chunk (`lab.timeouts.first-chunk`) | when the upstream call starts, i.e. after admission (waiting for a permit is not counted) | the first chunk received | no chunk has been received |
| idle (`lab.timeouts.idle-between-chunks`) | at the first chunk received, if credit is still outstanding then | each chunk received from upstream (not written to the client) | the gateway has granted upstream credit that is still unused, and no last chunk or upstream end was received |

When flow control has paused the upstream because the client is slow (credit 0), the
idle timer is not armed and cannot fire; it is armed again when a credit is handed
back. When the last chunk or the upstream's `error`/end has been received, no timer
is armed, so a slow client at the end of a stream cannot turn into a timeout.

When a timer expires the stream takes the same single terminal outcome as every other
ending: the upstream call is cancelled (with the message `first chunk timeout` or
`idle timeout between chunks`), the permit and lease are released once, chunks that
were still queued (not yet written) are dropped, and the client gets one `error`
event with `code` `DEADLINE_EXCEEDED` and that message. Chunks that were already
written come before it. The timers run on one shared daemon scheduler thread
(`stream-timeouts`), which never waits for a client write. Every terminal outcome
(completed, upstream error, client gone, shutdown, overflow, timeout) cancels the
stream's timer; the tests check that no timer is left armed afterwards. A deadline
error from gRPC itself (`lab.upstream.stream-deadline`) is reported with the gRPC
message instead of the two messages above, which tells the two apart.

`timeouts_total{kind=first_chunk|idle}` counts these endings (registered at startup,
so it reads 0). Only the outcome's winner counts, so a stream counts at most once.

The tests prove that cancellations reach the upstream by reading the simulator's
`active_generations` and `cancelled_total` (`SimulatorAdminService.GetStats`) for four
causes: client disconnect, first chunk timeout, idle timeout and shutdown. A stream
that nobody cancels stays at `active_generations == 1`. Timers keep running while
shutdown drains, so a stream the upstream no longer answers can end with a timeout
(`DEADLINE_EXCEEDED`) instead of the forced shutdown cancel (`CANCELLED`).

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
  therefore keeps its permit until one of these happens: the write returns; shutdown
  cancels it; the idle timeout fires (only while upstream credit is outstanding and
  upstream stays silent: the timeout claims the stream and frees its permit even
  though the write is still stuck, but not once credit is exhausted, the last chunk
  has been received, or upstream has ended); or the emitter timeout (stream deadline + 5 s) ends it. That the emitter
  timeout callback runs while another thread is blocked in a write is a hypothesis;
  no test covers it.
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
- The timers do not detect a stuck client write. They measure upstream silence only,
  and the idle timer stops when credit runs out. The stuck-intermediate-write case
  above therefore holds its permit until the write returns, shutdown, the emitter
  timeout, or an idle timeout while upstream credit is still outstanding and upstream
  is silent (not after the last chunk has been received or upstream has ended).
- The emitter `onTimeout` hypothesis above is unchanged: it stays a hypothesis.
- A chunk that arrives at the very instant a timer fires can lose the race: the
  timeout wins and the chunk is dropped.
- All timers share one scheduler thread: a timer never fires earlier than configured,
  but under heavy load it can fire late.
- A stream whose upstream stays silent while it still holds credit (for example `HANG`)
  and whose client is gone ends at the latest with the timeout. Whether Tomcat notices
  the disconnect without a write has not been verified; the disconnect test uses a
  continuously streaming upstream.
- Both timeouts must be shorter than `lab.upstream.stream-deadline`: lowering the
  deadline below 30 s means lowering both timeouts too, or the application does not
  start.
- In per-request mode the lease is still acquired before admission, but it does not
  create a channel: the channel is created by the first `channel()` call, which the
  stream makes after admission, so rejected and waiting requests never open one.

### Connection modes

`lab.connection-mode` selects how streams get their upstream channel. The default is `shared`.
An invalid or empty value stops the application at startup (an unset property uses the
default). The configured mode is logged once at startup (INFO) and exposed as
`connection_mode{mode}`.

- **`shared`**: one channel is created at startup, kept warm by the channel watcher, and used
  by all streams. A stream's lease does nothing on close. The channel is shut down once when
  the application stops.
- **`per-request`**: every stream has its own channel. The channel is created by the lease's
  first `channel()` call, i.e. when the first upstream call of an admitted stream starts, with no
  lock held; it is not created for rejected or waiting requests. The stream's terminal path
  (completion, upstream error, client gone, timeout, overflow, shutdown cancel) closes the
  lease once, which calls `ManagedChannel.shutdown()` (graceful, non-blocking). Retries and
  resumes of the stream reuse its channel, so one stream opens one channel. If the channel
  cannot be created, that stream ends with an `INTERNAL` error event and other streams are not
  affected. On application stop, channels still open are closed as a backstop and waited for
  up to `lab.shutdown.channel-termination-timeout` before `shutdownNow()`.
- Readiness in per-request mode reflects the application lifecycle only: there is no long-lived
  channel to probe. In shared mode it reflects the channel's connectivity state.
- Six simulator scenarios of the stream contract (single terminal event, release once, upstream
  cancelled, no timer leak, bounded queue) run against the real simulator in both modes, and the
  tests count how often each channel is shut down; the other contract and race tests do not go
  through a channel provider.

**Not measured by the tests.** This feature makes the two modes comparable; it makes no performance claim by itself.
One benchmark result set (see "Benchmark report") describes both modes on one machine.
All tests use in-process channels, which have no sockets, no event-loop threads and no connect
backoff. Not measured or not tested:

- latency and throughput of either mode
- CPU and memory cost
- the cost of establishing a connection per stream
- real Netty / TCP / HTTP/2 behaviour
- TLS
- sharing of Netty event loops between channels
- ports and `TIME_WAIT` under many short streams
- the effect of connect backoff after a real network failure

### Bounded reconnect

When the upstream call ends with `UNAVAILABLE`, the gateway starts a new call for the
same stream instead of failing it, a bounded number of times. The client sees no
sign of it: chunk ids stay consecutive and there is still exactly one `complete` or
`error` event (no SSE hint is sent for a retry).

| Setting | Default | Meaning |
|---|---|---|
| `lab.retry.max-attempts` | `3` (0..10) | number of **retries**, i.e. extra calls after the first one, so at most `max-attempts + 1` calls per stream. `0` turns retrying off. This differs from gRPC service-config `maxAttempts`, which counts all calls. |
| `lab.retry.initial-backoff` | `100ms` | ceiling of the first retry wait; positive, not above `max-backoff` |
| `lab.retry.max-backoff` | `2s` | ceiling of any wait; positive, shorter than `lab.upstream.stream-deadline` |

- **Backoff** is exponential with full jitter: the wait before retry k (k = 0 for the
  first retry) is a random value in `[0, min(max-backoff, initial-backoff * 2^k)]`. With
  the defaults the ceilings are 100, 200, 400 ms. The wait can be 0.
- **Budget:** the whole stream, all calls included, stays within
  `lab.upstream.stream-deadline`. The budget starts when the first call starts (after
  admission). Every retry call gets the *remaining* budget as its gRPC deadline. No
  retry is made when the next wait is not shorter than the remaining budget, or when
  the budget is spent by the time the wait ends.
- **Which endings are retried:** only `UNAVAILABLE`, and only while no `last` chunk has
  been received. `INTERNAL`, `INVALID_ARGUMENT`, `DEADLINE_EXCEEDED` (including the
  gRPC deadline), `RESOURCE_EXHAUSTED`, `CANCELLED`, `UNKNOWN`, an OK end without a
  `last` chunk and the first-chunk and idle timeouts of the gateway are never retried.
- **Resume position:** a retry call carries the same `request_id`, `seed`, profile and
  fault. With no chunk received it starts from the beginning (`resume_from_seq` unset,
  metric reason `restart`); otherwise it sets `resume_from_seq` to *the last chunk
  received + 1* (reason `resume`). Chunks that are received but still queued are
  written first, so the client sees no gap and no duplicate. This deliberately differs
  from "last chunk written + 1": resuming from the last written chunk would send the
  queued chunks a second time. If chunks were received but none written yet, the
  stream is resumed after them and not restarted. The simulator replays text
  deterministically, and the end-to-end test checks that the resumed stream has
  byte-for-byte the text of an uninterrupted call.
- **Flow control:** the initial credit of a new call is the queue capacity minus the
  chunks still buffered, which can be 0. Credit freed while the new call is starting
  is held back and granted once the call has started. Upstream therefore never has
  more credit than the queue has room for, also on a resumed call.
- **While waiting** the stream keeps its permit and lease and stays registered; the
  backoff is the only timer armed, so the first-chunk and idle timers cannot fire.
  `StreamTimeouts#pendingTimers` counts a backoff too. The next call is started on a
  virtual thread (`stream-retry`), never inside a gRPC callback and never on the
  scheduler thread. Each new call arms its own first-chunk timer, but only when it
  holds credit; with credit 0 it is armed when the first credit is granted. The time
  to first chunk of a resumed call is therefore also counted in
  `timeouts_total{kind="first_chunk"}`.
- **Stops:** a pending wait is cancelled, and no new call is started, by a terminal
  outcome of any kind (client gone, a failed write, shutdown cancel, timeout). The
  *start* of shutdown does not cancel a wait that is already running: when that wait
  ends, no call is started and the stream ends with the last `UNAVAILABLE`. During
  drain no retry is started either: an `UNAVAILABLE` seen while the gateway is shutting
  down ends the stream with that `UNAVAILABLE` at once. A wait in progress can therefore
  extend the drain by up to `lab.retry.max-backoff`; if `max-backoff` is larger than
  `lab.shutdown.drain-timeout`, the forced cancel of the drain can win and the stream
  ends with `CANCELLED` instead of `UNAVAILABLE`.
- **Exhaustion:** when the retries or the budget run out, the client gets one `error`
  event with the *last* upstream status (`UNAVAILABLE` and its description).

| Metric | Meaning |
|---|---|
| `retries_total{reason}` | counter: retry calls started; `reason` = `restart` (from the beginning) or `resume` (after received chunks) |
| `retry_exhausted_total` | counter: streams that ended with the upstream error after the retries or the budget ran out (not counted for `max-attempts=0` or a stop by shutdown) |

Both are registered at startup and read 0. Against the simulator, `UNAVAILABLE` at
seq 3 is replayed by each resumed call: the client gets chunks 0..2 and then the
error of the simulator, and the simulator counts 4 started and 4 faulted calls.

Known limits of the reconnect:

- A real LLM upstream may not support resuming at a position or reproducing the same
  output; the lab relies on the deterministic simulator. Whether a retry is safe for
  another upstream is not tested here.
- An `UNAVAILABLE` after partial output can make the upstream repeat work that was
  already done.
- A stream that could have been rescued during drain ends with `UNAVAILABLE`.
- The resume position trusts `seq` to be consecutive; an upstream that skips or
  repeats a `seq` is not detected.
- A retry call may be started and cancelled right away when shutdown or a client
  disconnect wins the race with the end of the wait; the simulator then counts one
  extra started and cancelled call.
- Whether Tomcat notices a disconnected client during a wait, when nothing is being
  written, has not been verified (hypothesis); the wait is bounded by the budget.
- In per-request mode the lease lives across the retries of a stream: all calls of one
  stream use the same channel, which is closed once when the stream ends.
- The jitter can be 0, so a retry can follow immediately. The end-to-end tests assert
  counts and content, not durations.
- The emitter timeout (stream deadline + 5 s) also includes the time spent waiting for
  admission (unchanged).
- The transparent retry of gRPC itself stays enabled. It is not configured by this
  feature, and the tests use a client interceptor that it does not affect (not
  verified against a real network failure).

### Stream contract tests

The gateway's stream guarantees (one terminal event, permit and lease released once,
upstream cancelled, no timer leak, bounded queue) are written as one contract in the
tests and run on `StreamBridge` and on a deliberately flawed `LeakyStreamHandler`
(test code only). The real bridge must violate nothing; for the flawed handler the test
asserts that exactly the two known flaws are detected, so the build stays green. See
[docs/failure-analysis.md](docs/failure-analysis.md). The REAL race stress cannot prove
that no race exists; that argument is the compare-and-set in `StreamBridge.claim()`.
Six more pairs of terminal paths (timeout, upstream error and queue overflow against a competing
completion, client gone or shutdown cancel; the exact pairs are listed in that document) and the admission-versus-shutdown
race of the registry are stressed the same way, per iteration and
without asserting the winner; details in the same document.

### Settings

| Setting (`application.yaml`) | Default |
|---|---|
| `lab.connection-mode` | `shared` (`shared` or `per-request`) |
| `lab.upstream.target` | `localhost:50051` |
| `lab.upstream.stream-deadline` | `60s` |
| `lab.shutdown.drain-timeout` | `10s` |
| `lab.shutdown.channel-termination-timeout` | `5s` |
| `lab.limits.max-active-streams` | `100` (>= 1) |
| `lab.limits.overload-policy` | `reject` (`reject` or `wait`) |
| `lab.limits.queue-wait` | `5s` (positive; used by `wait`) |
| `lab.limits.max-waiting` | `100` (>= 1; used by `wait`) |
| `lab.limits.stream-queue-capacity` | `16` (1..4096) |
| `lab.timeouts.first-chunk` | `30s` (positive, shorter than `lab.upstream.stream-deadline`) |
| `lab.timeouts.idle-between-chunks` | `30s` (positive, shorter than `lab.upstream.stream-deadline`) |
| `lab.retry.max-attempts` | `3` (0..10; number of retries, `0` = off) |
| `lab.retry.initial-backoff` | `100ms` (positive, not above `max-backoff`) |
| `lab.retry.max-backoff` | `2s` (positive, shorter than `lab.upstream.stream-deadline`) |

Invalid values stop the application at startup with a message naming the property.

## Load generator

`loadgen/` is a command line client that opens many SSE streams against the gateway's `GET /stream` and writes
one raw record per request. It is a measurement tool, not a result: **this feature makes no performance claim**,
and it does not compare the two connection modes (that is a later feature).

**Model: closed only.** `--concurrency` workers (virtual threads) each open one stream, wait for it to end and
open the next one immediately (no think time, except the `--reject-pause` after a 503). Worker `w` starts at `ramp-up * w / concurrency` (linear ramp).
The run ends after `--duration` (no new request starts after it; open streams get `--drain-timeout`, the rest is
recorded as `INTERRUPTED`), after `--requests` requests, or on SIGINT/SIGTERM (the same drain; the JVM stays alive until the JSONL footer, the stdout summary and the
`--summary-output` file are written, waiting at most `--drain-timeout` plus 20 s; the exit status of a
signalled JVM is set by the JVM, normally 128 plus the signal number, for example 130 for SIGINT; not tested). The open model
(fixed arrival rate) is deferred, see [ADR-0004](docs/adr/0004-loadgen-closed-model-raw-output.md); each record
already has a `scheduledOffsetNanos` field so it can be added without changing the schema.

### Usage

Start the simulator and the gateway first (the gateway defaults to the simulator on `localhost:50051` and
listens on port 8080):

```bash
./mvnw -q -pl simulator -am install -DskipTests
./mvnw -q -pl simulator exec:java                     # terminal 1
./mvnw -B -pl gateway -am package -DskipTests
java -jar gateway/target/gateway-0.1.0-SNAPSHOT.jar   # terminal 2
```

Then build and run the load generator (`target/lib/` must stay next to the jar, the manifest points to it):

```bash
./mvnw -B -pl loadgen package -DskipTests
mkdir -p results/raw
java -jar loadgen/target/loadgen-0.1.0-SNAPSHOT.jar \
  --target http://localhost:8080/stream --concurrency 10 --duration 30s --ramp-up 5s \
  --chunks 20 --interval-ms 20 --seed 42 \
  --output results/raw/run.jsonl --summary-output results/raw/summary.json
```

This command is documented, not asserted in CI. The tests use an in-test HTTP server; there is no end-to-end test
with the real gateway (the gateway is a repackaged Spring Boot jar and its in-process simulator wiring lives in
its test sources, so loadgen cannot depend on either without coupling the modules).

| Option | Default | Meaning |
|---|---|---|
| `--target URL` | required | `http`/`https` URL of the stream endpoint, no query (the parameters are generated) |
| `--concurrency N` | `1` | workers, 1..10000 |
| `--duration D` / `--requests M` | exactly one required | run time (`500ms`, `30s`, `2m`, at most `7d`) / number of requests (1..100000000) |
| `--ramp-up D` | `0s` | linear start-up time of the workers; shorter than `--duration` |
| `--chunks N\|A-B` | `10` | chunks per stream, 1..1000000 |
| `--interval-ms N\|A-B` | `50` | 0..60000 |
| `--first-delay-ms N\|A-B` | `0` | 0..60000 |
| `--fault KIND` | `NONE` | `NONE`, `UNAVAILABLE`, `INTERNAL`, `HANG` |
| `--fault-at-seq N\|A-B` | `0` | seq of the fault, >= 0 |
| `--fault-rate P` | `1.0` | share of requests that carry the fault (ignored for `NONE`) |
| `--seed L` | `0` | seed of the parameter sequence |
| `--output PATH` | required | JSONL file; the directory must exist, an existing file is an error without `--overwrite` |
| `--summary-output PATH` | none | JSON file with the convenience summary (same rule) |
| `--overwrite` | off | allow replacing existing output files |
| `--connect-timeout D` | `5s` | HttpClient connect timeout |
| `--idle-timeout D` | `90s` | no progress (no response header or body bytes) for this long ends the request with `TIMEOUT`; after a terminal event the outcome is kept and only `bodyEnd` becomes `timeout` |
| `--drain-timeout D` | `30s` | how long open streams may finish after the run ended |
| `--reject-pause D` | `100ms` | pause of a worker after an HTTP 503 (without it a limit smaller than the concurrency becomes a hot loop) |

Ranges (`A-B`) are drawn per request. Invalid, missing, duplicate or unknown arguments, "both" or "none" of
`--duration`/`--requests`, and an existing output file print a message line (and `Try --help.`) to stderr and exit with code **2**; no
output file is created. `--help` exits with 0. An I/O or unexpected error exits with 1. **A run that completed
exits with 0 whatever the outcomes were** (even if every request failed); scripts must check the summary or
the JSONL. The idle timeout default is larger than the gateway's own 30 s first-chunk and idle timeouts and the
65 s emitter timeout, because the gateway sends no heartbeat.

The generated parameters are a pure function of `(seed, request index)` (SplitMix64, fixed draw order, golden
values pinned in `RequestVariationTest`). A drawn `faultAtSeq` that is not below the drawn `chunks` never fires,
so such a request streams to completion although a fault was requested. **Not deterministic:** timing, which
worker gets which index, the outcomes and the gateway's request ids.

### Output

One JSON object per line, `schemaVersion` 1, flat objects (no nesting), fixed field order, all durations in
integer nanoseconds from `System.nanoTime`, offsets relative to the run start. First line `type=header`, last line
`type=footer`, everything in between `type=request`. A write error ends the run early (workers stop starting
requests, open streams are drained as usual): the file then has no footer, no summary is printed or written, stderr gets
`loadgen: I/O error: ...` and the exit code is 1.

**header:** `type, schemaVersion, tool, toolVersion, startedAt` (wall clock, ISO-8601 UTC), `target, loadModel`
(`closed`), the run parameters (`concurrency, durationMs, requests, rampUpMs, chunks, intervalMs, firstDelayMs,
faultAtSeq` as strings such as `"5-20"`, `fault, faultRate, seed, connectTimeoutMs, idleTimeoutMs,
drainTimeoutMs, rejectPauseMs`), `httpVersion` (`HTTP/1.1`), `javaVersion, javaVendor, javaVmName, osName,
osArch, osVersion, availableProcessors, maxHeapBytes, timeUnit, clock, coordinatedOmission, commandLine`.

**request:**

| Fields | Meaning |
|---|---|
| `index, worker` | request index (unique, `0..M-1` with `--requests`) and worker |
| `paramChunks, paramIntervalMs, paramFirstDelayMs, paramFault, paramFaultAtSeq, paramSeed` | what was sent |
| `scheduledOffsetNanos, startOffsetNanos, endOffsetNanos` | planned start, actual start, record closed (including the wait for the body to end after a terminal event). `startOffsetNanos - scheduledOffsetNanos` is the generator's own delay, not the server's |
| `outcome, status, retryAfter, requestId` | outcome (below), HTTP status, raw `Retry-After` header, `X-Request-Id` header |
| `ttfbNanos` | request start until the HttpClient delivered the complete response header block |
| `firstBodyByteNanos` | until the first non-empty body buffer |
| `ttfcNanos` | until the buffer that completed the first `chunk` event |
| `totalNanos` | until the outcome was decided (terminal event; for a response without a stream the later of the headers and the first body byte; otherwise the end of the wait) |
| `chunks, lastSeq` | `chunk` events counted, id of the last one |
| `interChunkMinNanos, interChunkMaxNanos` | smallest and largest gap between consecutive `chunk` events (null with fewer than 2) |
| `terminal, errorCode, errorMessage, completeChunks` | `complete`, `error` or `none`; fields of the terminal event (message cut at 512 characters) |
| `protocolViolations` | events after the terminal event, a second terminal event, unknown event names, non-numeric or non-consecutive chunk ids, an `error` event without `code`, unparsable terminal data |
| `timeoutKind, errorPhase, errorClass, errorDetail, bodyEnd` | `connect`/`headers`/`idle`; `before_headers`/`body`; exception class and message (512); how the body ended: `eof`, `reset`, `timeout`, `interrupted` or `limit` |

Ordering `ttfb <= firstBodyByte <= ttfc <= total` holds for the values that are present (for a rejected, HTTP-error or
wrong-content-type response `total` is the later of the headers and the first body byte). `errorPhase` and
`errorClass` stay empty when the request got its answer (`COMPLETED`, `ERROR_EVENT`, `REJECTED`, `HTTP_ERROR`,
`PROTOCOL_ERROR`) even if the body failed afterwards; that is visible in `bodyEnd` only. `errorDetail` is empty for
the first four and carries the reason for `PROTOCOL_ERROR` (`unexpected content type`, or the parser limit). Several events that arrive in one buffer share one
timestamp, so their gap is 0 ns: the resolution is the arrival of buffers, not of events.

**Outcomes** (exactly one per request): `COMPLETED` (200 and a `complete` event), `ERROR_EVENT` (200 and an `error`
event), `REJECTED` (503), `HTTP_ERROR` (other status), `PROTOCOL_ERROR` (200 without `text/event-stream`, or an SSE
line over 1 MiB or the data of one event over 16 Mi characters, which is 16 times the line limit), `NO_TERMINAL` (clean end of the body without a terminal event), `RESET` (I/O error after the
headers and before a terminal event, or before the headers other than connect/timeout), `TIMEOUT`, `CONNECT_FAILURE`,
`INTERRUPTED` (drain timeout or signal), `CLIENT_ERROR` (anything else). What happens after the terminal event
(`bodyEnd`, `protocolViolations`) never changes the outcome. A TCP reset and a connection closed in the middle of a
chunked body both surface in the JDK HttpClient as the same `IOException` ("chunked transfer encoding, state:
READING_LENGTH"), so loadgen cannot tell them apart; both are `RESET`. This was observed on Windows only; other platforms were not
checked.

**footer:** `type, endedAt, elapsedNanos, stopReason` (`duration`, `requests`, `signal`), `requestsStarted,
recordsWritten, interrupted, interChunkCount, interChunkHistogramUnit` (`ns`), `interChunkHistogram`
(HdrHistogram, `encodeIntoCompressedByteBuffer`, Base64) with all inter-chunk gaps of the run.

**Convenience summary.** Stdout (and `--summary-output`, a flat `type=summary` object) shows the counts per
outcome and p50/p90/p99/p99.9/max in microseconds of `ttfc` and `total` (COMPLETED requests only) and of the
inter-chunk gaps (all streams), headed "CONVENIENCE SUMMARY - not a benchmark result". The JSONL file is the result;
the summary is a quick look. HdrHistogram with 3 significant digits, so values are accurate to about 0.1 %.

### What is measured, what is not

Measured (client side, one monotonic clock): the times above, counts and outcomes.

**Not measured:** server-side time (the gateway's queueing, the simulator's work), GC pauses or JIT warm-up on
either side, the network (the intended use has client and gateway on one machine, so loopback), the client's own
bottleneck (one `SelectorManager` thread inside the JDK HttpClient serves all connections; whether it limits a
run is not measured), connection reuse (HTTP/1.1 keep-alive; the API does not tell whether a request used a new
or a pooled connection, so a connect cost may or may not be inside `ttfbNanos`), and the reason of a 503 (the
gateway answers without a body; only its `rejected_total{reason}` metric tells). `ttfbNanos` is **not** the first
byte on the wire: it is the moment the HttpClient had parsed the whole header block, and it includes connection
set-up if the connection was not pooled, the request write and the gateway's admission. The JDK may also retry an
idempotent GET once and invisibly when a pooled connection turned out to be stale; that time would be inside the
measurement (this is a hypothesis about JDK behaviour that was not observed or tested here). Running loadgen on the
same machine as the gateway means both compete for CPU, which is a bias of the setup, not something loadgen can
remove.

**Coordinated omission is not corrected.** In a closed model a worker only sends the next request after the
previous one ended, so when the server gets slow the generator sends fewer requests and the requests that "should
have" waited in a queue are never made and never measured. Latency is measured from the actual send. Percentiles of a
slow period are therefore optimistic compared to an open-model run at the same offered rate. The header and the
stdout summary say so. No correction is applied.

### Concurrency limits of one JVM

Each client is a virtual thread and the load per client is small, and the JDK's HTTP/1.1 pool has no per-host
limit, but there are practical limits: the single HttpClient selector thread, the OS ephemeral port range
(roughly 16k ports on Windows and 28k on Linux by default; the numbers were not re-checked), client-side
`TIME_WAIT` after timeouts, cancels and resets, the Tomcat `max-connections` setting (default 8192, not verified
here) and the gateway's `lab.limits.max-active-streams` (100; more concurrent streams get 503). With keep-alive
and a closed model the number of ports in use is about the concurrency. `--concurrency` is limited to 10000.
The record writer queue is unbounded on purpose (a bounded queue would block workers and disturb the timing),
so memory grows if the disk cannot keep up.

### Findings about the gateway (not changed)

- This README did not say how to start the gateway; the commands above (`java -jar`, port 8080) were checked
  against a local run (the simulator was started from its classes there, not through `exec:java`).
- A 503 carries no reason in the response, only in `rejected_total{reason}`.
- Semantic parameter errors (for example `chunks=0`) are a 200 with an `error` event `INVALID_ARGUMENT`, not a 400,
  so loadgen validates the ranges itself (its limits mirror `RequestLimits.DEFAULT` of the simulator and may drift).
- The gateway sends no SSE heartbeat, so a client idle timeout has to be larger than the gateway's own.

## Benchmark protocol

`bench/` runs the written [benchmark protocol](docs/benchmark-protocol.md) (protocol version 1): for every cell
(concurrency x connection mode x repetition) it starts a fresh simulator and gateway JVM, waits for readiness within
fixed bounds, verifies the connection mode from the `connection_mode` metric, runs a warm-up and a measured loadgen
run, takes metric snapshots before and after, stops the processes and checks that the ports are free. Raw files and a
`manifest.json` land in `bench/results/<run-id>/` (ignored by git). **This harness and its results make no performance
claim; it compares nothing by itself.** The runner analyses nothing and ranks nothing; stability problems are only
recorded as flags (see the protocol).

```bash
./mvnw -B package -DskipTests                                        # the runner never builds; run this first
java -jar bench/target/bench-0.1.0-SNAPSHOT.jar --dry-run            # plan and exact commands, starts nothing
java -jar bench/target/bench-0.1.0-SNAPSHOT.jar --preset smoke --dry-run
java -jar bench/target/bench-0.1.0-SNAPSHOT.jar --preset smoke --checklist yes   # short harness check (4 runs)
java -jar bench/target/bench-0.1.0-SNAPSHOT.jar                      # default set: 24 runs, nominal duration printed by --dry-run
java -jar bench/target/bench-0.1.0-SNAPSHOT.jar --help
```

- Before a run set the operator checklist (other applications closed, AC power, power plan) is asked; `--checklist ask`
  needs an interactive terminal (otherwise exit code 2), `--checklist yes|skip` records the answer without asking.
  A `no` or a skipped checklist hides nothing, it only adds a flag.
- Exit codes: 0 all planned runs completed (flags do not change it), 1 harness or run failure (the manifest says
  `failed`; exceptions: if the result directory cannot be created there is neither directory nor manifest, and if not even a
  minimal manifest can be written, for example on a full disk, there is none, and if writing the manifest keeps failing
  during the run the runner makes one direct write (second temp file, atomic move) of the complete manifest state with its current status,
  and if that fails too `manifest.json` keeps its last written status, e.g. `running`, and the full state stays in
  `manifest.json.direct.tmp` (path printed) if that file could be written; a leftover `manifest.json.tmp` may remain), 2 invalid arguments or failed precondition (nothing started, no directory created).
- Windows: `Process.destroy()` is a hard termination, so the graceful shutdown of gateway and simulator is not exercised
  (the recorded stop method is `destroy`). Press Ctrl+C in the console to interrupt; the runner then kills its children
  and marks the manifest `interrupted` (observed in a plain Windows console; PowerShell, Windows Terminal and Git Bash
  (mintty) were not verified separately); if the
  runner is killed hard, no cleanup runs and leftover `java` processes must be stopped by hand (their PIDs are in the
  manifest).
- Archive a result directory with `tar -czf bench-<run-id>.tar.gz -C bench/results <run-id>` (or `Compress-Archive`) and
  attach it to a release or issue; do not commit it.

## Benchmark report

`report/` reads **one** result set written by the runner (`bench/results/<run-id>/`, archived and extracted, never
committed) and writes a descriptive report. **The report describes one result set on one machine; no performance claim.**
It shows what was measured and under which recorded conditions, which runs were excluded and why, per run metrics, and
per concurrency the two modes side by side (per repetition, with run index and ABBA position, median and range). Its
sentences come from a closed set of templates (`{...}` are numbers, mode names, the metric and the concurrency; values are
shown with enough decimals that the facts the rule used stay visible):

- higher: "In this result set, at concurrency {c}, the median {metric} of {m1} ({v1}) was higher than that of {m2} ({v2});
  the ranges did not overlap and the difference between the medians ({d}) was larger than the largest within-mode range
  ({s}). With n={n1} and n={n2} repetitions this describes this set only."
- cannot separate, ranges overlap: "In this result set, at concurrency {c}, the median {metric} was {v1} for {m1} and {v2}
  for {m2}; the ranges overlapped; this set cannot separate the two modes on this metric. With n={n1} and n={n2}
  repetitions this describes this set only."
- cannot separate, spread: "In this result set, at concurrency {c}, the median {metric} was {v1} for {m1} and {v2} for
  {m2}; the ranges did not overlap, but the difference between the medians ({d}) was not larger than the largest
  within-mode range ({s}); this set cannot separate the two modes on this metric. With n={n1} and n={n2} repetitions this
  describes this set only."
- equal medians: "In this result set, at concurrency {c}, the median {metric} of {m1} ({v1}) and of {m2} ({v2}) were
  equal; this set cannot separate the two modes on this metric. With n={n1} and n={n2} repetitions this describes this
  set only."
- insufficient: "In this result set, at concurrency {c}, there were fewer than two usable repetitions for {modes} for the
  {metric}; no comparison is stated."

The report has no p-values, no confidence intervals, no ranking and no pooled
percentiles, and it states when a set cannot separate the modes. The protocol's "What it cannot answer" list is quoted
verbatim in it. A scan for forbidden phrases and for absolute paths runs over every output file before anything is written (the standard output summary is only scanned for paths). It never modifies the
result set.

```bash
./mvnw -B package -DskipTests                                                                  # builds report/target
java -jar report/target/report-0.1.0-SNAPSHOT.jar --help
java -jar report/target/report-0.1.0-SNAPSHOT.jar --results <result-set-dir> --check            # read and validate only
java -jar report/target/report-0.1.0-SNAPSHOT.jar --results <result-set-dir> --out docs --overwrite
java -jar report/target/report-0.1.0-SNAPSHOT.jar --placeholder --out docs --overwrite          # no result set analysed yet
```

- `--out docs` writes `docs/benchmark-report.md` (fully generated, do not edit) and `docs/benchmark/` (`runs.csv`,
  `cells.csv`, `analysis.json`, four SVG charts). The file list is fixed; an existing file is only replaced with
  `--overwrite`; `--out` (also a `benchmark/` link below it) must not resolve into the result set. A smoke set needs `--allow-smoke`. In the CSV files, text cells that start with `=`, `+`, `-`, `@`, a tab, a carriage return or a line feed are prefixed with a single quote (spreadsheet formula injection); numeric columns are written as they are. Replaced output files are new files (permissions `rw-r--r--` subject to the umask on POSIX); an existing, more restrictive mode is not preserved.
- Exit codes: 0 success (excluded runs are reported, they are not an error), 1 unexpected error (I/O, a guard violation),
  2 invalid usage or invalid input (an unsupported `schemaVersion` / `protocolVersion`, a missing manifest; nothing is
  written). The last line of the standard output is one line of JSON (`report-summary`).
- **Regenerate from an archived result set:** extract the archive into a new, empty directory outside the repository
  (`tar -xzf bench-<run-id>.tar.gz -C <new-empty-directory>`; the extracted `<run-id>` directory contains `manifest.json`),
  run `--check` and then `--out docs --overwrite` as above. The same input gives the same output, byte for byte (the
  report prints the raw size of files it only lists, which depends on the line ends of the input).
- `docs/benchmark-report.md` and `docs/benchmark/*` are generated from one result set
  (`20261029T105937Z-b117ff0-dirty`, git working tree dirty at run time). When a new set has been analysed, those are the
  only files to commit; the raw set is not committed. `--placeholder` still writes the version without numbers.
- The runner and manifest findings RF1 to RF8 found while building the tool (the manifest `schemaVersion` changed without
  a version bump, no power setting on Linux, the harness never ran on Linux, ...) are listed in
  [ADR-0006](docs/adr/0006-benchmark-report.md); they are reported, not fixed.

## Decisions

- [ADR-0001: Synthetic simulator on plain grpc-java](docs/adr/0001-simulator-on-plain-grpc-java.md)
- [ADR-0002: Single terminal claim by CAS; capacity released before the terminal write](docs/adr/0002-single-terminal-claim.md)
- [ADR-0003: Per-request connection mode](docs/adr/0003-per-request-connection-mode.md)
- [ADR-0004: Closed-model load generator with raw output](docs/adr/0004-loadgen-closed-model-raw-output.md)
- [ADR-0005: Benchmark protocol runner as a plain-Java module](docs/adr/0005-benchmark-runner.md)
- [ADR-0006: Benchmark report as a separate plain-Java module](docs/adr/0006-benchmark-report.md)

## License

MIT
