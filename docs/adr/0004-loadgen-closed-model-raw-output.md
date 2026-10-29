# ADR-0004: Closed-model load generator with raw output

- Status: accepted
- Date: 2026-10-25

## Context

The comparison of the two gateway connection modes (ADR-0003) needs a client that opens many SSE streams and leaves
a stable, scriptable record of what happened. The later benchmark protocol (warmup, repetitions, comparison) is a
separate feature. This one only has to produce raw data and has to be honest about what that data can and cannot
say. Constraints: no dependency on gateway or simulator code, few dependencies, Java 26, runs on one machine.

## Decision

- **Closed model with a linear ramp; the open model is deferred.** N workers each open a stream, wait for its end
  and open the next one (no think time, except the pause after a 503). Worker `w` starts at `ramp-up * w / N`. An open model (fixed arrival rate)
  needs a latency measured from the planned start, a backlog policy for a generator that falls behind and a
  protection against unbounded concurrency; that is a design of its own. The record already contains
  `scheduledOffsetNanos`, so the open model can be added without changing the schema.
- **Coordinated omission is disclosed, not corrected.** In the closed model latency is measured from the actual
  send; when the server slows down the offered load drops and the missing requests are never measured. The header
  (`loadModel`, `coordinatedOmission`), the stdout summary and docs/loadgen.md say so. The difference between
  `scheduledOffsetNanos` and `startOffsetNanos` only shows the generator's own delay.
- **JDK `HttpClient`, HTTP/1.1 only, virtual-thread executor.** No new dependency. HTTP/1.1 makes every open
  stream one TCP connection; HTTP/2 multiplexing would hide the connection cost this lab wants to look at. The
  default cached platform pool would grow with the number of streams, so the client gets a virtual-thread executor.
- **A push `BodySubscriber` with an incremental byte parser, not `ofInputStream`/`ofLines`.** Both return when the
  headers arrive and then read in a blocking way; closing a blocked read from another thread is not guaranteed to
  unblock it, and a line API hides CR/CRLF splits and the arrival time of bytes. The subscriber parses in `onNext`
  without blocking, requests one buffer at a time, and the worker thread only waits on a future with a
  progress-based timeout. Cancelling closes the connection (checked in `StreamClientTest`: after the client gave
  up, the server's writes fail).
- **Timestamps are buffer arrival times.** An event is stamped with the `onNext` of the buffer that completed it, so
  events in one buffer share a timestamp. `ttfbNanos` is the moment the HttpClient delivered the parsed header
  block, not the first byte on the wire.
- **Hand-written JSON.** The records are flat, so a small writer (RFC 8259 escaping) and a flat reader (for the
  gateway's `data` payloads and for tests) are enough. Jackson is managed in the Spring Boot BOM but would be a heavy
  runtime dependency for a tool whose requirement is "minimal". The runtime classpath is HdrHistogram only.
- **Raw JSONL is the result, the summary is a convenience.** Header, one record per request, footer; flat objects,
  fixed field order, `schemaVersion` 1, integer nanoseconds. The footer carries the compressed inter-chunk
  histogram. Every request gets exactly one outcome; deviations from the stream contract are counted separately
  (`protocolViolations`) and never change the outcome.
- **Seeded variation.** The request parameters are a pure function of `(seed, index)`, drawn with a SplitMix64
  implemented in the module (`java.util.Random` and `ThreadLocalRandom` are not stable across versions or not
  seedable per index). The draw order is fixed so that changing one option does not shift the other values. Each
  index owns eight draw slots so that neighbouring indexes do not share draws. Timing, worker assignment, outcomes
  and the gateway's request ids are not deterministic.
- **Failing fast, exit codes.** Hand-written option parsing; invalid input exits with 2 before any file is created.
  A run that completed exits with 0 whatever the outcomes were, because outcomes are data; a script reads the
  summary. An existing output file is an error unless `--overwrite` is given. After a 503 a worker pauses
  (`--reject-pause`, default 100 ms) so that a gateway limit below the concurrency does not turn into a hot loop;
  this is a deliberate think time and is part of the record (`rejectPauseMs`).
- **Packaging: jar plus `lib/`.** `maven-jar-plugin` writes `Main-Class` and a `lib/` class path, and
  `maven-dependency-plugin` copies the runtime dependencies to `target/lib` in the `package` phase. No shade or
  assembly. `exec:java` was rejected: it would measure inside Maven's JVM and a `System.exit` would end Maven.
- **No end-to-end test with the real gateway.** The gateway artifact is a repackaged Spring Boot jar (classes under
  `BOOT-INF`), so it cannot be a dependency, its in-process simulator wiring is in its test sources, and the
  feature must not depend on gateway code. Tests use an in-test HTTP server and a raw socket server (reset,
  truncated chunked body, clean EOF). The real-gateway run is a documented manual command; script level smoke
  tests belong to the benchmark protocol feature.

## Consequences

- The data can show shape and failures of a closed-loop run. It cannot show queueing delay that the generator
  itself avoided, server-side time, GC, or network effects, and it makes no performance claim.
- A TCP reset and a connection closed in the middle of a chunked body are indistinguishable through the JDK
  HttpClient (same `IOException`); both are `RESET`. Observed on Windows only.
- The record writer queue is unbounded so that disk speed never delays a worker; memory grows if the disk cannot
  keep up.
- The client has a single selector thread inside the JDK; whether it limits a given run is not measured.
- The limits of the generated parameters mirror the simulator's `RequestLimits.DEFAULT` and may drift.
