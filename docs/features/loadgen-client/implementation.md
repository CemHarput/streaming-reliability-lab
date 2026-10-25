# Implementation: loadgen-client

Status: implemented (required checks passed). Plan hash `7d92f1c46fb1330a0a9b4a04eb2ec718587d31c515c4a9419b21dd30a0222271`, baseline `24d617cdfa2d3a1bc0bf3117140fe2e7043a5053`.

## Files
- Changed: `pom.xml` (maven-dependency-plugin 3.8.1 property + pluginManagement), `loadgen/pom.xml` (description, test deps, jar manifest, copy-dependencies to `target/lib`), `loadgen/.../package-info.java`, `README.md` (status, module row, JaCoCo line, "Load generator" section, ADR-0004 in Decisions).
- New main (`loadgen/src/main/java/io/github/cemharput/streamlab/loadgen/`): `LoadgenMain`, `Options`, `OptionsParser`, `UsageException`, `RequestVariation`, `LoadRun`, `RecordWriter`, `Summary`, `client/{StreamClient,StreamSubscriber,StreamParams,StreamResult,Outcome}`, `sse/{SseParser,SseEvent,SseListener,SseProtocolException}`, `json/{JsonLine,FlatJson}`.
- New tests: `sse/SseParserTest` (21), `json/JsonLineTest` (8), `json/FlatJsonTest` (5), `OptionsParserTest` (10), `RequestVariationTest` (9), `client/StreamClientTest` (19), `LoadRunTest` (9), `LoadgenMainTest` (8), `testsupport/SseTestServer`, `testsupport/RawSocketServer`.
- New docs: `docs/adr/0004-loadgen-closed-model-raw-output.md`.
- Unchanged: `proto`, `simulator`, `gateway`.

## Acceptance mapping
AC1-AC12 met per the implementer's evidence, with the exceptions below. Key evidence: runtime dependency is HdrHistogram only; no gateway/simulator/tokenstream import; no `Thread.sleep`/`.sleep(` under loadgen; every test class has `@Timeout(SEPARATE_THREAD)`; module test time about 4.7 s; manual runs against a real simulator + gateway (not part of the docs as results): 10 workers 8 s run gave 126 COMPLETED with 0 protocol violations, a 150-worker run with `--fault UNAVAILABLE --fault-rate 0.3` gave COMPLETED, ERROR_EVENT (UNAVAILABLE) and REJECTED (503, `max-active-streams` 100).
- AC1 "three green full builds": the implementer ran one green full build (a first attempt failed in `proto` at `maven-jar-plugin` assembling a class, apparently a Windows file lock, and the immediate retry was green); the coordinator ran another independent clean build. Met by the later builds listed in "Checks after the review fixes" below (three green clean full builds on the final code, loadgen 89 tests).

## Checks
- Implementer: focused command x3 (85 tests, BUILD SUCCESS, about 7 s each); `package` + manifest + `target/lib` + `java -jar ... --help` (exit 0) + invalid arg (exit 2, no file); `dependency:tree` (compile scope HdrHistogram only); full `./mvnw -B clean verify` green (3:02 min, gateway 233, loadgen 85); greps clean.
- Coordinator: `feature_state.py check` approval_matches true; independent `./mvnw -B clean verify` exit 0 (simulator 12, gateway 233, loadgen 85); `git diff --stat -- proto simulator gateway` empty; greps clean.

## Deviations
1. `StreamClient` takes a `LongConsumer` inter-chunk sink (4th constructor argument), `Summary` owns the histogram.
2. SplitMix64 start state is `seed + index * 8 * GAMMA` (not `index * GAMMA`): with the plan's formula neighbouring indexes would share shifted raw values. Documented in Javadoc and ADR; golden values match (computed with an independent Python implementation).
3. RST, truncated-chunked and clean-EOF tests send 2 chunks and release the server after the client saw the second, so the RST cannot discard unread data.
4. `bodyEnd` has a fifth value `limit`.
5. Duration parser accepts `ms|s|m` only and rejects anything over 7 days.
6. Summary percentiles for ttfc/total are over COMPLETED requests only (stated in stdout text and README); inter-chunk covers all streams.
7. `RecordWriter.finish` takes a `Supplier<String>` for the footer.
8. Only `Retry-After`, `X-Request-Id` and `Content-Type` response headers are read.

## Limitations
- Not tested: `TIMEOUT/connect`, `CLIENT_ERROR`, a writer failure in the middle of a run, a real SIGINT through the shutdown hook (the hook is wired, `stop("signal")` is tested).
- RST and FIN are indistinguishable in the JDK HttpClient on Windows (identical exception); both map to RESET, README says so. The invisible HttpClient retry hypothesis is not verified.
- Port/TIME_WAIT limits and Tomcat max-connections figures in the README are unverified.
- The `exec:java` path for starting the simulator was not tried (the manual runs used `java -cp`); the README says so.
- The summary JSON is written after the run, a failure there exits 1 after the JSONL is complete.
- Gateway findings F1-F4 are only reported (README).
- Not run before this step: a review. Reviewer attention points from the implementer: `StreamClient.classify`, `StreamSubscriber` synchronisation between HttpClient threads and the worker, `LoadRun` drain/interrupt logic.

## First-review fixes (after review pass 1; coordinator)
- P2-1: the shutdown hook now waits for a latch that `LoadgenMain.run` counts down after the stdout summary and `--summary-output` are written (it used to wait for the end of `LoadRun.run`, so a signal could end the JVM before the summary existed). The extra wait after the drain timeout is 20 s (5 s for workers, 5 s for the HttpClient, 5 s for its executor, margin). The hook is a testable `LoadgenMain.shutdownHook(...)`; new test `shutdownHookStopsTheRunAndKeepsTheJvmAliveUntilTheOutputIsWritten`. README documents the signal behaviour. A real SIGINT of a JVM is still not tested.
- P3-1: for REJECTED, HTTP_ERROR and wrong-content-type responses `totalNanos` is the later of the headers and the first body byte, so `ttfb <= firstBodyByte <= total` holds for all present values; asserted in `otherStatusIsHttpError`; README states the rule.
- P3-2: `StreamClient.execute` reads the subscriber once at the moment of the timeout decision (`seen`), so a response whose headers arrive after the decision is still a `headers` timeout; a subscriber created later is still cancelled.
- P3-3: `errorPhase`/`errorClass`/`errorDetail` stay empty for COMPLETED, ERROR_EVENT, REJECTED, HTTP_ERROR and PROTOCOL_ERROR even if the body failed afterwards (visible in `bodyEnd`); new test with a new `RawSocketServer` ending `TRUNCATED_CHUNKED_AFTER_COMPLETE`.
- P3-4: with `--requests`, workers still waiting for their ramp slot are woken when the budget is used up (`wake` latch in `LoadRun`); new test `requestBudgetEndsTheWaitOfWorkersStillInTheRampUp` (4 workers, 10 m ramp, 1 request).
- P3-5: README/ADR/package-info wording (two stderr lines, "no think time except the pause after a 503", idle timeout after a terminal event, RST vs FIN observed on Windows only).
- P3-6: the data of one SSE event is capped at 16 times the line limit (16 Mi characters by default) with `SseProtocolException`; new parser test; README mentions the limit.
- P3-7: `Files.list` closed in `OptionsParserTest`; BOM written as `\uFEFF`; `errorClass="internal"` in the unreachable no-subscriber branch.
- Loadgen test count after the fixes: 88 + 1 (hook test) = 89 expected; the focused run showed 88 before the hook test.

## Second-review fixes (after review pass 2; coordinator)
- P3-a/b/d (README): `errorDetail` rule (carries the reason for `PROTOCOL_ERROR`), `total` rule incl. wrong content type, `totalNanos` row, parser event limit in characters (16 times the line limit), signalled JVM exit status stated as "normally 128 plus the signal number, not tested"; `stopReason` `writer_error` only exists internally (no footer is written after a write error, see below). The `PROTOCOL_ERROR` detail text for the parser limit is now "SSE parser limit exceeded (line or event data)".
- P3-c: the raw U+FEFF character in `SseParser` is now written as a unicode escape (backslash, u, FEFF) (the first attempt had not taken effect); the same raw character in the earlier text of this file was replaced.
- P3-e: a failing record write now stops the run (`RecordWriter.onFailure` -> `LoadRun.stop("writer_error")`), so waiting workers wake up and the run ends with exit 1 instead of idling to the end of the ramp. Not tested (a mid-run write failure is hard to provoke portably).
- P3-f: test counts in this file corrected (SseParserTest 21, StreamClientTest 19, LoadRunTest 9, LoadgenMainTest 8; loadgen total 89).

## Third-review fixes (after review pass 3; coordinator)
- P3-1: README no longer lists `writer_error` among the footer `stopReason` values; it states that a write error ends the run early, leaves no footer, prints no summary, and exits with 1 (`RecordWriter.finish` throws the write failure before the footer).
- P3-2: `SseProtocolException` Javadoc mentions the event data limit; the limit unit is "16 Mi characters" here as in the README.

## Checks after the review fixes (coordinator)
- Clean `./mvnw -B clean verify`, exit 0, simulator 12 / gateway 233 / loadgen 89 tests, on successive versions: after the first-review fixes, after the second-review fixes (README, BOM escape, writer-failure stop), and after the third-review doc fixes. No Java source changed after that last build; two further clean builds on exactly the final Java code (after the fourth and fifth review passes) were also green, so the final code has three green clean full builds. Two further attempts failed in the `clean` phase because `simulator/target` could not be deleted (a transient Windows file lock); the immediate retry was green each time. This is the AC1 "three green builds" evidence.
- `git diff --stat -- proto simulator gateway` empty; no `Thread.sleep` / `.sleep(` under loadgen.
- Not tested: a mid-run write failure, a real SIGINT, `TIMEOUT/connect`, `CLIENT_ERROR`. A failed write may leave a cut-off last JSONL line.
