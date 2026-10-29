# Load generator

`loadgen/` is a command line client that opens many SSE streams against the gateway's `GET /stream` and writes
one raw record per request. It is a measurement tool, not a result: **this feature makes no performance claim**,
and it does not compare the two connection modes (that is a later feature).

**Model: closed only.** `--concurrency` workers (virtual threads) each open one stream, wait for it to end and
open the next one immediately (no think time, except the `--reject-pause` after a 503). Worker `w` starts at `ramp-up * w / concurrency` (linear ramp).
The run ends after `--duration` (no new request starts after it; open streams get `--drain-timeout`, the rest is
recorded as `INTERRUPTED`), after `--requests` requests, or on SIGINT/SIGTERM (the same drain; the JVM stays alive until the JSONL footer, the stdout summary and the
`--summary-output` file are written, waiting at most `--drain-timeout` plus 20 s; the exit status of a
signalled JVM is set by the JVM, normally 128 plus the signal number, for example 130 for SIGINT; not tested). The open model
(fixed arrival rate) is deferred, see [ADR-0004](adr/0004-loadgen-closed-model-raw-output.md); each record
already has a `scheduledOffsetNanos` field so it can be added without changing the schema.

## Usage

Start the simulator and the gateway first, as in [Quick start](../README.md#quick-start) (the gateway defaults to the simulator on
`localhost:50051` and listens on port 8080). Then build and run the load generator (`target/lib/` must stay next to the
jar, the manifest points to it):

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

## Output

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

## What is measured, what is not

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

## Concurrency limits of one JVM

Each client is a virtual thread and the load per client is small, and the JDK's HTTP/1.1 pool has no per-host
limit, but there are practical limits: the single HttpClient selector thread, the OS ephemeral port range
(roughly 16k ports on Windows and 28k on Linux by default; the numbers were not re-checked), client-side
`TIME_WAIT` after timeouts, cancels and resets, the Tomcat `max-connections` setting (default 8192, not verified
here) and the gateway's `lab.limits.max-active-streams` (100; more concurrent streams get 503). With keep-alive
and a closed model the number of ports in use is about the concurrency. `--concurrency` is limited to 10000.
The record writer queue is unbounded on purpose (a bounded queue would block workers and disturb the timing),
so memory grows if the disk cannot keep up.

## Findings about the gateway (not changed)

- A 503 carries no reason in the response, only in `rejected_total{reason}`.
- Semantic parameter errors (for example `chunks=0`) are a 200 with an `error` event `INVALID_ARGUMENT`, not a 400,
  so loadgen validates the ranges itself (its limits mirror `RequestLimits.DEFAULT` of the simulator and may drift).
- The gateway sends no SSE heartbeat, so a client idle timeout has to be larger than the gateway's own.
