# Review: benchmark-protocol-run (final: pass 8 plus a confirming re-read)

Verdict: **PASS** (no open finding; remaining ideas are far-fetched failures during failure handling, listed as hypotheses).
Baseline HEAD: 1fa78dc54878befc63d2d39a86eb7543a520b0ee (branch main, nothing committed).

## History
- Pass 1: CHANGES_REQUESTED, P3 only (12 items, 3 hypotheses): log pump stopped draining after a write error, interruption integrity of the manifest, 1 s graceful stop for loadgen, `--operator-note` not stored, loadgen log counters, env sanitisation untested, privacy sentence, manifest-less exit 1, stale jars, per-run HttpClient, weak test asserts, doc nits. All fixed.
- Pass 2: P3 only: final loadgen record lost after an interrupt, `LOG_TRUNCATED` after a write error, `hookKillMs` not recorded, manifest ticker, precondition wording, jar-name parsing, `killAll` robustness. Fixed.
- Pass 3: P3 only: log drain for a child that died on its own, exit 1 without a directory documented, one shared hook deadline, `AMBIGUOUS:` label, wider catch around manifest creation. Fixed.
- Pass 4: P3 only: permanent mid-run manifest write failure, `--help` wording, stronger concurrent-hook test, nits. Fixed.
- Pass 5: P2 (the pass-4 fallback replaced the last good manifest with a minimal one, not atomic) plus P3. Fixed: the fallback writes the full state through a second temporary file; `Artifacts.Resolved` validates its lists.
- Pass 6: P2 (non-atomic direct move could delete the manifest) plus P3 (interrupted-but-unwritten manifest, cleanup aborting on a failed record, `running` runs in a failed manifest, stale wording). Fixed: atomic move, `dirty` flag, robust cleanup.
- Pass 7: P3 only: direct-write outcome reporting (`WRITTEN`/`TMP_KEPT`/`NOTHING`), stale comments. Fixed.
- Pass 8: P3 only (javadoc and comments contradicting `NOTHING`, seam comment on the wrong method). Fixed; the reviewer re-read the spots and answered PASS.

## Evidence and limits
- Reviewers ran nothing. Coordinator: independent clean `./mvnw -B clean verify` exit 0 on the final code (simulator 12, gateway 233, loadgen 89, bench 205 tests; a first attempt failed on a transient Windows file lock while assembling the `proto` jar, the immediate retry was green); the pass-8 comment-only edits were followed by a green focused run (205). `git diff --stat -- gateway simulator proto loadgen` empty; no `Thread.sleep` in bench tests; no listener on 8080/50051 after the runs.
- The implementer ran the smoke preset several times against the real simulator and gateway (shape only, results gitignored under `bench/results/`), including port-busy, wrong-jar and a plain-console Ctrl+C scenario; the coordinator did not repeat the manual runs. The default 24-run set was never run. Ctrl+C in Windows Terminal, PowerShell and Git Bash (mintty) is unverified.
- Not tested: failures while handling failures beyond what the tests cover (for example the clock throwing inside `finish` on the direct path).
- Graph refresh (`bce.exe index`) failed with a database connection timeout after the implementation; it does not affect the feature state.
- This review does not mean the feature is bug-free. The feature makes no performance claim and the repo contains no measurement results.
