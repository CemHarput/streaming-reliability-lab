# Implementation: benchmark-protocol-run

Status: implemented (required checks passed). Plan hash `75f39af23361207ef902c68814ecf6bf0c6a503e5262fc9001a2816b20296335`, baseline `1fa78dc54878befc63d2d39a86eb7543a520b0ee`. The implementer's first session was interrupted; the resumed session continued from the 21 files already on disk (one compile error in `Artifacts.java` fixed).

## Files
- Modified: `pom.xml` (module `bench` after loadgen), `.gitignore` (`bench/results/`), `README.md` (status, `bench/` module row, "Benchmark protocol" section with "This harness and its results make no performance claim; it compares nothing by itself", ADR-0005 link).
- New docs: `docs/benchmark-protocol.md` (first line "Protocol version: 1"), `docs/adr/0005-benchmark-runner.md`.
- New module `bench/`: `pom.xml` (proto, grpc-netty-shaded, simulator runtime scope, test deps; jar manifest with Main-Class and `lib/` classpath; `copy-dependencies` to `bench/target/lib`); main classes under `bench/src/main/java/io/github/cemharput/streamlab/bench/`: `BenchMain`, `BenchRunner`, `DryRun`, `Flags`, `BenchOptions`, `BenchOptionsParser`, `Preset`, `ConnectionMode`, `UsageException`, `PlanBuilder`, `RunId`, `ResultLayout`, `GitInfo`, `Manifest`, `ManifestStore`, `Snapshot`, `Artifacts`, `ProcessSpecs`, `ChildProcess`, `JdkChildProcess`, `ProcessLauncher`, `JdkProcessLauncher`, `ProcessSupervisor`, `BoundedLogPump`, `PortProbe`, `Ticker`, `Readiness`, `GatewayApi`, `GatewayProbe`, `SimulatorApi`, `SimulatorStatsClient`, `EnvironmentCapture`, `CommandRunner`, `SystemCommandRunner`, `CpuLoadSampler`, `OperatorChecklist`, `LoadgenOutputInspector`, `RunFlags`, `json/Json`; tests: 22 classes, 160 tests, helpers in `testsupport/` (`FakeTicker`, `FakeChildProcess`, `FakeLauncher`, `FakeActuatorServer`, `FakeRepo`, `ChildMain`).
- Unchanged: `gateway`, `simulator`, `proto`, `loadgen`.

## Acceptance mapping
AC1-AC14 met per the implementer's evidence: 42 option-parser cases, ABBA plan tests, dry-run test with fake artefacts (launcher never called, tree unchanged), manifest key-set test, protocol/README/code consistency test, supervisor tests with fakes and three real test-only child JVMs (child and grandchild killed), no `Thread.sleep` in bench tests, `@Timeout(SEPARATE_THREAD)` everywhere, `bench` total test time about 6 s.

## Checks
- Implementer: focused command x3 stable (160 tests), `dependency:tree` (bench compile/runtime deps only proto, grpc-netty-shaded and the simulator closure), three green clean full builds.
- Coordinator: `feature_state.py check` approval_matches true; independent `./mvnw -B clean verify` exit 0 (simulator 12, gateway 233, loadgen 89, bench 160); `git diff --stat -- gateway simulator proto loadgen` empty; no `Thread.sleep` in bench tests; `git check-ignore` confirms `bench/results/`; no java process or listener on 8080/50051 left after the implementer's runs.
- Manual (implementer): dry-run (default, smoke); smoke run exit 0 with 4 completed runs (`001-c4-shared-r1`, `002-c4-per-request-r1`, `003-c4-per-request-r2`, `004-c4-shared-r2`, order labels AB, AB, BA, BA), 16 expected files per run, 17 top-level manifest keys, run-set flags `SMOKE_PRESET`, `GIT_DIRTY`, `ARTIFACT_OLDER_THAN_HEAD` (heuristic: the loadgen jar mtime is older than the HEAD commit time), run flags empty, `portsFreed` true; second smoke created a new directory and left the first manifest's sha256 unchanged; port-busy scenario exit 2 with no directory; wrong `--gateway-jar` scenario exit 1, run 1 `failed`, the others `not_run`, no orphan; Ctrl+C in a plain Windows console: manifest `interrupted`, all child PIDs gone, no listener (a first attempt, hard-killed with `taskkill /T /F`, left its manifest at `running`, which illustrates the hard-kill gap). No numbers or interpretation are in the repo docs; the coordinator did not repeat the manual run.

## Deviations
- `PlanBuilder.RunPlan`/`PlannedRun` are nested records; extra classes `GatewayApi`, `SimulatorApi`, `Snapshot`, `GitInfo`, `Flags`, `DryRun` for injection and testing.
- The supervisor's stop method is named `destroy` (not "graceful"): on Windows `destroy()` is a hard termination; documented.
- A loadgen with a non-zero exit code or an exceeded wait fails the run (flag also recorded); likewise `GATEWAY_EXITED`, `SIMULATOR_EXITED`, `CONNECTION_MODE_MISMATCH`.
- The manifest has an extra `harnessFailure` string after a harness exception; `parameters.gateway.properties` records `lab.connection-mode` as a per-run placeholder; dry-run placeholders for missing artefacts no longer use `<...>` (invalid path on Windows).

## Findings (not fixed, described in `docs/benchmark-protocol.md` section 10 and ADR-0005)
F1 no runnable simulator artefact; F2 simulator stats only via gRPC; F3 no graceful stop on Windows; F4 `server.port` not set and upstream target `localhost`; F5 scheduler thread default is machine dependent; F6 chunk size not exposed; F7 loadgen exit 0 does not reflect outcomes.

## Limitations
- The default 24-run set was not run, only the smoke preset. Ctrl+C was observed only in a plain Windows console (Windows Terminal, PowerShell, Git Bash/mintty unverified). A hard kill skips the hook (manifest stays `running`; PIDs are recorded and the next run checks the ports).
- The runner JVM prints harmless grpc-netty `sun.misc.Unsafe`/Epoll INFO warnings to stderr while polling the stats.
- Five gitignored smoke result directories created by the implementer remain under `bench/results/` (one `running`, one `failed`, one `interrupted`, two `completed`).
- Review: see review.md.

## First-review fixes (after review pass 1; done by the implementer on the coordinator's request)
- 1: `BoundedLogPump` keeps draining after a write or open failure, records the first error (`ChildProcess.logWriteError()` -> `log.writeError`, flag `LOG_WRITE_ERROR`); tests with a failing OutputStream.
- 2: interruption: `abort()` force-kills, then marks the manifest `interrupted`, then waits; an interrupt that arrives before the manifest exists is applied when it is created; stop results, log counters, `portsFreed` and flags are still recorded after an interrupt (`updateRunAlways`); concurrent hook test. The hook waits up to 10 s for the runner thread's final writes.
- 3: loadgen graceful wait before a forcible stop is 40 s (30 s drain + 10 s margin), forced wait 10 s; recorded in `parameters.plan.timeouts` and the protocol doc.
- 4: `--operator-note` is stored as top-level `operatorNote` (null when absent). 5: loadgen processes record `log{bytesWritten, bytesDropped, writeError}`.
- 6: env sanitisation extracted to `JdkProcessLauncher.applyEnvironment` and tested (mixed-case names, overrides after removal). H3: `LOGGING_*` added to the removed prefixes.
- 7: protocol doc privacy sentence (absolute paths, OS user name, check archives before sharing). 8: a minimal `failed` manifest is written when building the manifest fails; if even that fails, exit 1 without a manifest (documented). 9: precondition (exit 2) for two jars of the same artifact id in `bench/target/lib` or `loadgen/target/lib`. 10: the per-run HttpClient is closed after each run. 11: meaningless asserts replaced, versions checked against their own documented values. 12: doc nits (`portsFreed` null, `gatewayIdleWait` on pre/post only, flags vs failures and exit code).
- H1: `netstat` output with TCP lines but no recognised state returns null and `TIME_WAIT_UNAVAILABLE`. H2: manifest move retried up to 5 times through the `Ticker` (fake ticker in tests).
- Checks: focused bench tests x3 (179 tests), clean full build green (simulator 12, gateway 233, loadgen 89, bench 179; the coordinator's independent clean build also green), `git diff --stat -- gateway simulator proto loadgen` empty, no `Thread.sleep` in bench tests, smoke re-run shape verified (new fields present, no java process or listener left). The real Ctrl+C console scenario was not repeated after the fixes (fake-based tests cover the logic).

## Second-review fixes (after review pass 2; done by the implementer on the coordinator's request)
- The final write of a loadgen phase after an interrupt uses `updateRunAlways` (exit code, log counters, outcomes, footer, flags are kept even if the hook wins the race; deterministic concurrent-hook test).
- `LOG_TRUNCATED` only for the 16 MiB limit: bytes lost after a log write or open error are counted separately (`bytesLostAfterWriteError`); `LOG_WRITE_ERROR` is independent.
- `hookKillMs` (10 s) recorded in `parameters.plan.timeouts` and documented as the best-effort bound for post-interrupt records; a consistency test checks every timeout key is documented.
- `BenchRunner` passes the injected ticker to the `ManifestStore` (test: always failing move = 5 attempts, 4 virtual sleeps, no real sleep).
- Precondition wording "artifact problems:" and `STALE:`/`MISSING:` in the dry run; duplicate-jar check parses `<id>-<version>[-<classifier>]` so same-version classifier pairs are not flagged (tests incl. guava/-android, foo-tests, grpc-protobuf vs -lite, listenablefuture-9999.0).
- `ProcessSupervisor.killAll` catches `RuntimeException` per child and around the callback; direct `killAll` tests (callback once after the kill, a throwing child does not stop the others).
- Documented: an interrupt during environment capture may leave the manifest `running` or the directory without manifest (best effort).
- Checks: focused bench tests x3 (186 tests); the implementer's clean full build green and the coordinator's independent run recorded in the line below; `git diff --stat -- gateway simulator proto loadgen` empty; no `Thread.sleep` in bench tests; smoke re-run clean in shape (new log keys, `hookKillMs` 10000), no java process or listener left.

## Third-review fixes (after review pass 3; done by the implementer on the coordinator's request)
- Log drain also for a child that died on its own (`ChildProcess.awaitLogDrained`, bounded 2 s, used on both stop paths; counters are read after the drain; fake-based tests).
- Exit 1 without a directory or manifest (result directory cannot be created) is documented in the protocol doc, README and `BenchMain` javadoc.
- The hook shares one deadline (`hookKillMs`) between the kill wait and the wait for the runner thread (plus at most 2 s per child for log draining); test with virtual time.
- Dry-run labels `AMBIGUOUS:` (two jars), `STALE:` and `MISSING:`; the `createManifest` catch covers RuntimeException and the minimal failed manifest survives a throwing clock; the duplicate-jar parser never reports a name it cannot parse (documented limit); ADR/protocol doc say the PID is written when the child is registered (narrow window on an interrupt).
- Checks: focused bench tests x3 (191 tests); coordinator's independent clean build green (simulator 12, gateway 233, loadgen 89, bench 191); `git diff --stat -- gateway simulator proto loadgen` empty; no `Thread.sleep` in bench tests; smoke re-run clean in shape; no java process or listener left.

## Fourth-review fixes (after review pass 4; done by the implementer on the coordinator's request)
- A permanent manifest write failure mid-run now tries one direct write of a minimal `failed` manifest (not through the failing move path); the remaining exception (the file can keep its last written status, e.g. `running`) is documented in the protocol doc, README and `BenchMain` javadoc; test with a mover that fails permanently after the first write.
- The `--help` text echoes the exit-1 exceptions (result directory cannot be created, manifest cannot be written); tests tie USAGE to the documented wording.
- The concurrent-hook test reads the manifest right after the hook returns and asserts the simulator stop record and `portsFreed`, so skipping the runner-thread wait would fail it.
- Nits: exact `MISSING:` label assertion, misplaced Javadoc moved, artifact problems carry a kind (`Artifacts.Kind`) so dry-run labels no longer depend on message text.
- Checks: focused bench tests x3 (194 tests); the coordinator's independent clean build green (simulator 12, gateway 233, loadgen 89, bench 194); `git diff --stat -- gateway simulator proto loadgen` empty; no `Thread.sleep` in bench tests; no java process or listener left. Test counts quoted in the earlier sections of this file are historical (160/179/186/191).

## Fifth-review fixes (after review pass 5; done by the implementer on the coordinator's request)
- P2: the direct fallback after a permanent mid-run manifest write failure no longer writes a minimal manifest over the last good file. `ManifestStore.writeDirect` serialises the full in-memory state first, writes it to `manifest.json.direct.tmp` and replaces the target with a plain (non-atomic) move; on any failure the temp file is deleted and `manifest.json` stays untouched. Only a still `running` manifest becomes `failed` (with `harnessFailure`); `completed`, `failed` and `interrupted` keep their status. The minimal manifest is used only when no store exists. Docs (protocol doc, README, `BenchMain` javadoc) state what the fallback writes, that it is not atomic and that a leftover `manifest.json.tmp` may remain; tests assert that environment, parameters, artifacts and completed runs survive, that an interrupted status is kept, and that a failing direct write leaves the file unchanged.
- P3: `Artifacts.Resolved` validates `problems.size() == problemKinds.size()` and copies defensively.
- Checks: focused bench tests x3 (197 tests); the coordinator's independent clean build green (simulator 12, gateway 233, loadgen 89, bench 197); `git diff --stat -- gateway simulator proto loadgen` empty; no `Thread.sleep` in bench tests; no java process or listener left.

## Sixth-review fixes (after review pass 6; done by the implementer on the coordinator's request)
- P2: the direct write uses an atomic move (`ATOMIC_MOVE` with `REPLACE_EXISTING`; without atomic support a plain move only when the target does not exist), so a failure never deletes the previous file; the fully written `manifest.json.direct.tmp` is kept on failure and its path printed (`ManifestStore.directTmp()`). Docs (protocol doc, README, `BenchMain` and `ManifestStore` javadoc, ADR-0005) state this exactly; the earlier "plain, non-atomic move" wording of the fifth-review section is superseded.
- P3: `ManifestStore` has a `dirty` flag (set by a failed write, cleared by a successful one); at the end of `execute`, also in the interrupted branch, a direct write is attempted when the store is dirty; the console message says what really happened.
- P3: `cleanup` stops both processes and checks the ports even when a stop or `portsFreed` record cannot be written (first exception rethrown at the end); `finish` turns a still `running` run into `failed` with `endedAt` and a `harness` failure, so a failed or interrupted manifest never contains a `running` run.
- Checks: focused bench tests x3 (203 tests); the coordinator's independent clean build green (see the final line); `git diff --stat -- gateway simulator proto loadgen` empty; no `Thread.sleep` in bench tests; no listener on 8080/50051. The implementer's first clean attempt hit the transient `simulator/target` file lock; the retry was green.

## Seventh-review fixes (after review pass 7; done by the implementer on the coordinator's request)
- `ManifestStore.writeDirect` reports three outcomes (`WRITTEN`, `TMP_KEPT`, `NOTHING`); a partially written direct tmp is deleted; the console message and the docs (protocol doc, README, `BenchMain` javadoc) say the full state is in `manifest.json.direct.tmp` only "if that file could be written"; tests for a failing tmp write at store and runner level.
- Stale "plain move" comments fixed; the end-of-run "interrupted" console message is chosen from the manifest status, not from the abort flag.
- Checks: focused bench tests x3 (205 tests); the coordinator's independent clean build green (simulator 12, gateway 233, loadgen 89, bench 205); `git diff --stat -- gateway simulator proto loadgen` empty; no `Thread.sleep` in bench tests; no listener on 8080/50051.
- Correction to the line above: the coordinator's first clean build after these fixes failed in `proto` at `maven-jar-plugin` ("Error assembling JAR", the transient Windows file lock seen before); the immediate retry was green (simulator 12, gateway 233, loadgen 89, bench 205).

## Eighth-review fixes (comments only, coordinator)
- Javadoc and code comments in `ManifestStore` (`directTmp`, `writeDirect`) and `BenchRunner` (fallback comment, the two test-seam comments) now say that the complete state stays in `manifest.json.direct.tmp` only when the move failed, and that a tmp that could not be written is removed. No behaviour change; the focused bench tests (205) were re-run green after the edit.
