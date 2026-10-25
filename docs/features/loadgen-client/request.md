# Request: loadgen-client

Goal
Turn the empty loadgen module into a small load generator client that opens N concurrent SSE streams against the gateway, ramps the load, and records per-request measurements as raw data. It produces raw results only: no benchmark protocol, no comparison between connection modes, no analysis, no report, no performance claim.

Context
- loadgen is currently an empty skeleton Maven module (README status: skeleton; pom mentions HdrHistogram). The gateway exposes the SSE endpoint (planner: verify path, query parameters such as chunks, intervalMs, fault, faultAtSeq, event names chunk/complete/error, status codes 503 with Retry-After for rejection, error event codes) in gateway/src/main/.../stream/StreamController and README. The simulator and gateway are separate processes in a real run; loadgen only talks HTTP to the gateway.
- A later feature (benchmark-protocol-run) will define warmup, repetitions, seeds, machine info and run the comparison of the two connection modes; this feature must give it a stable raw output format and a CLI that can be driven by a script.
- Planner: decide the client stack (JDK java.net.http.HttpClient with virtual threads vs another library already in the repo's dependency management; keep dependencies minimal, check the root pom for managed versions, Java 26), how SSE is parsed (events split by blank line; id/event/data), and how many concurrent streams one JVM can reasonably hold.

Measurements per request (planner: define exact fields and units)
- time to first byte of the response, time to first chunk event, inter-chunk intervals (store as a compact histogram per run, and optionally per-request min/max), total stream duration, number of chunk events, terminal event kind (complete, error with code, none) or HTTP status when rejected, client-side error class (connect failure, timeout, reset, protocol error). Use monotonic time (System.nanoTime), and say in the docs that client and gateway share one machine in the intended use.
- Closed-model and open-model are both named in the README skeleton row; planner: decide whether this feature implements closed model only (N concurrent clients, each starts a new stream when the previous one ends) plus a simple ramp, and defers the open model (arrival rate) explicitly, or implements both if it stays small. State the decision.
- Coordinated omission: planner must state how the chosen model avoids or discloses it in the output (e.g. record the intended start time in the open model; in the closed model say what is not measured).

Output
- Raw per-request records as JSON Lines (one object per request) written to a file given by a CLI option, plus a small summary file or stdout summary (counts per outcome, HdrHistogram percentiles of time-to-first-chunk and total duration) that is clearly labelled as a convenience, not a result. Include a run header record (tool version, parameters, start time, target URL, Java version, available processors) so a later run can be reproduced.
- Deterministic parameters where possible (seeded request parameter variation); document what is not deterministic.

CLI
- A runnable jar or Maven exec entry: --target URL, --concurrency N, --duration or --requests, --ramp-up, --chunks, --interval-ms, --fault options passed through, --output path, --seed. Invalid arguments fail fast with a clear message and non-zero exit. Planner: choose the argument parsing approach with no heavy dependency.

Tests (no real gateway process required for unit tests)
- SSE parser tests (split chunks across reads, id/event/data, comments, CRLF, terminal events, malformed input).
- Client tests against a small in-test HTTP server (JDK com.sun.net.httpserver or the existing in-process gateway if cheap) that streams controlled SSE: normal completion, error event, 503 with Retry-After, connection reset mid-stream, slow server; assert the JSONL records and the summary counts; bounded waits, no Thread.sleep, @Timeout(SEPARATE_THREAD), all resources closed.
- One end-to-end smoke test is optional: gateway + in-process simulator started in the test JVM (see gateway tests' InProcessSimulatorConfig for the pattern, planner: verify whether loadgen can depend on gateway test code or whether this belongs elsewhere) with a handful of streams; if that couples modules awkwardly, leave it out and document it.

Acceptance criteria (planner to refine, numbered)
- Builds and tests green with ./mvnw -B verify three times; loadgen has no dependency on gateway main code.
- CLI runs against a local gateway (manual command documented in the README, not asserted in CI) and writes JSONL plus summary.
- README: loadgen section with usage, output format, what is measured, what is NOT measured (server-side time, GC, network), coordinated omission note, and an explicit statement that no performance claim is made.

Out of scope
- Benchmark protocol, warmup/repetition policy, comparison of shared vs per-request, result analysis, charts, the report.
- Changing gateway, simulator or proto. If the gateway lacks something loadgen needs, report it as a finding instead of changing it here.
- MQTT, showcase.

Constraints
- Work on main, no new branch, no commits or pushes.
