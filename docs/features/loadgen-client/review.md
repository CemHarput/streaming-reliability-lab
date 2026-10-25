# Review: loadgen-client (final: pass 5 plus a confirming re-read)

Verdict: **PASS** (no open finding).
Baseline HEAD: 24d617cdfa2d3a1bc0bf3117140fe2e7043a5053 (branch main, nothing committed).

## History
- Pass 1: CHANGES_REQUESTED. P2-1 (shutdown hook let the JVM exit before the summary was written); P3: time-order claim vs `total` for 503/400/wrong content type, `holder.get()` read twice around timeout/interrupt, error fields filled after a terminal event, `--requests` ramp waiters not woken, README/ADR sentences, unbounded SSE event data, hygiene. All fixed.
- Pass 2: CHANGES_REQUESTED, P3 only: README rules (errorDetail, total, exit status of a signalled JVM), parser-limit text, raw U+FEFF still in `SseParser`, a failing record write did not stop the run, stale counts. All fixed.
- Pass 3: CHANGES_REQUESTED, P3 only: README listed `writer_error` as a footer value that can never appear (a write error leaves no footer); Javadoc and limit unit wording. Fixed.
- Pass 4: CHANGES_REQUESTED, one P3: stale AC1 build evidence. Fixed.
- Pass 5: CHANGES_REQUESTED, one P3: wording of the build evidence. Fixed; the reviewer re-read implementation.md and answered PASS.

## Evidence and limits
- Reviewers ran nothing. Coordinator: clean `./mvnw -B clean verify` exit 0 (simulator 12, gateway 233, loadgen 89) on successive versions and twice more on exactly the final Java code; three clean-phase failures overall were a transient Windows file lock on `simulator/target` (the immediate retries were green). `git diff --stat -- proto simulator gateway` empty; no `Thread.sleep` under loadgen.
- The implementer ran the CLI manually against a real simulator and gateway (outputs are not benchmark results and are not in the docs); the coordinator did not repeat the manual run.
- Not tested: a mid-run write failure, a real SIGINT (hook logic is unit-tested), `TIMEOUT/connect`, `CLIENT_ERROR`; the invisible JDK HttpClient retry and the RST/FIN distinction are hypotheses or Windows-only observations (documented).
- The graph refresh (`bce.exe index`) failed with a database connection timeout after the implementation; it does not affect the feature state.
- This review does not mean the feature is bug-free; no performance claim is made.
