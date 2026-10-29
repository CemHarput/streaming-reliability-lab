# Gateway

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

## Bounded resources

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

## Timeouts and cancellation

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

## Connection modes

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

**Not covered by the tests.** The tests use in-process channels (no sockets, no event-loop threads, no connect
backoff), so they say nothing about latency, throughput, CPU or memory cost, real Netty / TCP / HTTP/2 behaviour, TLS,
sharing of event loops between channels, or connect backoff after a real network failure. The two modes are compared
in one benchmark result set (see [Results](../README.md#results)); ports and `TIME_WAIT` under many short streams are only
recorded there, not analysed.

## Bounded reconnect

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

## Stream contract tests

The gateway's stream guarantees (one terminal event, permit and lease released once,
upstream cancelled, no timer leak, bounded queue) are written as one contract in the
tests and run on `StreamBridge` and on a deliberately flawed `LeakyStreamHandler`
(test code only). The real bridge must violate nothing; for the flawed handler the test
asserts that exactly the two known flaws are detected, so the build stays green. See
[failure-analysis.md](failure-analysis.md). The REAL race stress cannot prove
that no race exists; that argument is the compare-and-set in `StreamBridge.claim()`.
Six more pairs of terminal paths (timeout, upstream error and queue overflow against a competing
completion, client gone or shutdown cancel; the exact pairs are listed in that document) and the admission-versus-shutdown
race of the registry are stressed the same way, per iteration and
without asserting the winner; details in the same document.

## Settings

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
