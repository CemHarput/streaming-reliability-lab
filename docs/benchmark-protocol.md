Protocol version: 1

# Benchmark protocol

This document is the written protocol that the `bench/` runner implements. The runner produces **raw data and a
manifest, nothing else**. It does not analyse, rank or compare, and neither this document nor the runner makes a
performance claim. Interpreting a result set is a separate step that is not part of this protocol version.

## 1. The question

Which connection strategy of the gateway, one shared lifecycle-managed gRPC channel (`shared`) or one channel per
stream (`per-request`), behaves differently under the same load, the same payload and the same failure policy, when
both run on one machine against the synthetic simulator?

The protocol only defines how to produce comparable raw measurements for that question: identical fresh JVMs,
identical pinned settings, a warm-up, alternating run order, recorded environment and recorded stability flags.

## 2. What it cannot answer

- Anything about a real LLM, a real network, TLS, several hosts or production hardware: everything is synthetic and
  runs on `127.0.0.1` on one machine.
- Absolute capacity. The load generator, the gateway and the simulator share the CPU, memory and loopback stack of
  one machine. That is a bias of the set-up that no flag removes.
- Latency under a fixed arrival rate. The load model is closed (a fixed number of workers, each opening the next
  stream when the previous one ended), so coordinated omission is not corrected.
- Differences smaller than the run-to-run variation of the machine. The protocol records several repetitions per
  cell; whether they suffice is a question for the analysis step.
- Causes. The raw data can show that two modes differ; it cannot show why.

## 3. Variables and pinned settings

**Independent variables:** the connection mode (`lab.connection-mode`: `shared`, `per-request`) and the number of
concurrent workers (`--concurrency` list). **Repetitions** are the measured runs per mode and concurrency.

**Everything else is pinned** and identical in both modes. Each run starts fresh JVMs (simulator, gateway) so that
no JIT, GC or connection state leaks from one run to the next. All hosts are `127.0.0.1`.

Gateway: `java -Xms1g -Xmx1g -XX:+UseG1GC -Xlog:gc:file=gateway-gc.log:uptime,time,level,tags:filecount=2,filesize=8M -jar <gateway.jar>` followed by

```
--server.port=8080
--spring.threads.virtual.enabled=true
--server.tomcat.max-connections=8192
--server.tomcat.accept-count=100
--lab.connection-mode=<shared|per-request>
--lab.upstream.target=127.0.0.1:50051
--lab.upstream.stream-deadline=60s
--lab.limits.max-active-streams=100
--lab.limits.overload-policy=reject
--lab.limits.queue-wait=5s
--lab.limits.max-waiting=100
--lab.limits.stream-queue-capacity=16
--lab.timeouts.first-chunk=30s
--lab.timeouts.idle-between-chunks=30s
--lab.retry.max-attempts=3
--lab.retry.initial-backoff=100ms
--lab.retry.max-backoff=2s
--lab.shutdown.drain-timeout=10s
--lab.shutdown.channel-termination-timeout=5s
```

Only `--lab.connection-mode` differs between the modes. The ports are the defaults 8080 and 50051 and can be
overridden (`--gateway-port`, `--simulator-port`); the values used are recorded.

Simulator: `java -Xms512m -Xmx512m -XX:+UseG1GC -Xlog:gc:file=simulator-gc.log:uptime,time,level,tags:filecount=2,filesize=8M -cp <jars> io.github.cemharput.streamlab.simulator.SimulatorMain`
with the environment

```
SIMULATOR_PORT=50051
SIMULATOR_SHUTDOWN_GRACE_SECONDS=10
SIMULATOR_SCHEDULER_THREADS=<availableProcessors of the runner machine>
```

The classpath is the explicit list of the jars in `bench/target/lib`, sorted by file name. The scheduler thread count
is written out explicitly because its default depends on the machine.

Load generator (one JVM per phase): `java -Xms1g -Xmx1g -XX:+UseG1GC -Xlog:gc:file=<phase>-loadgen-gc.log:uptime,time,level,tags:filecount=2,filesize=8M -jar <loadgen.jar>` with

```
--target http://127.0.0.1:8080/stream --concurrency <C> --duration <D> --ramp-up <R>
--chunks 40 --interval-ms 50 --first-delay-ms 0 --fault NONE --fault-at-seq 0 --fault-rate 1.0 --seed 42
--connect-timeout 5s --idle-timeout 90s --drain-timeout 30s --reject-pause 100ms
--output <phase>.jsonl --summary-output <phase>-summary.json
```

`<phase>` is `warmup` or `measured`. The values are fixed, there are no ranges. The gateway does not expose the
chunk size, so every chunk carries the simulator's default payload (one word).

**Environment sanitising.** `JAVA_TOOL_OPTIONS`, `JDK_JAVA_OPTIONS`, `_JAVA_OPTIONS` and every variable starting with
`SPRING_`, `LAB_`, `MANAGEMENT_`, `SERVER_`, `SIMULATOR_` or `LOGGING_` are removed from the environment of every child before the
explicit values above are set. Only the names of the removed variables are recorded.

**Seed policy.** The seed is 42 in every run and mode and is never varied. Because all other parameters are fixed, the
seed only determines the synthetic text; every mode and repetition receives the same requests.

**Stream length.** 40 chunks at 50 ms are streams of roughly two seconds. A `per-request` stream opens one TCP
connection to the simulator, so short streams keep the connection rate and the number of lingering TIME_WAIT sockets
small. That this stays below the ephemeral port range is an assumption, not a proof; the TIME_WAIT count is recorded
before every run for this reason.

**Concurrency sweep (default `50,100,150`)** is chosen relative to `lab.limits.max-active-streams=100`: below the
limit, at the limit, and above the limit, where the rejection path (`REJECTED`, `rejected_total{reason=limit}`)
becomes visible. The `reject-pause` bounds how fast a rejected worker retries.

**Defaults.** 4 repetitions per mode and concurrency, measured duration 60 s, warm-up 20 s with the same concurrency,
ramp-up 5 s, 30 s cooldown between runs, CPU sample window 3 s. This is 24 runs (3 concurrency values, 2 modes,
4 repetitions); the dry run prints the nominal total.

**Smoke preset (`--preset smoke`).** Concurrency `4`, 2 repetitions (4 runs), duration 3 s, warm-up 2 s, ramp-up 0 s,
cooldown 1 s, CPU window 1 s. All pinned settings are unchanged. The result directory id ends with `-smoke` and the run
set carries the flag `SMOKE_PRESET`. A smoke run only shows that the harness works end to end; it is not a measurement.

## 4. Procedure

**Order.** Concurrency is the outer loop. For every concurrency value, repetition `r` (0 based) runs `[A, B]` when `r`
is even and `[B, A]` when `r` is odd, where A and B are the first and second mode of `--mode-order`
(default `shared,per-request`). Four repetitions give `ABBA ABBA`, so both modes occupy each position equally often.
The alternation is recorded in `parameters.plan.alternation` (`ABBA`) and in the `orderLabel` of every run (`AB` or
`BA`).

**Preconditions (exit code 2, nothing is created):** the gateway jar, the loadgen jar with its `lib/` and the simulator
jar are present (build first with `./mvnw -B package -DskipTests`; the runner never builds), the arguments are valid,
no listener is present on the gateway and simulator ports, `bench/target/lib` and `loadgen/target/lib` contain no two
jars of the same artifact with different versions (a stale directory; run `./mvnw -B clean package -DskipTests`), and
`--checklist ask` has an interactive terminal. The dry run reports a missing artifact as a `MISSING:` line, an ambiguous
auto-detection (two matching jars) as an `AMBIGUOUS:` line and a stale lib directory as a `STALE:` line; a real run fails with `artifact problems: ...`.

**Per run (one cell):**

1. Both ports must have no listener; the TIME_WAIT counts are recorded.
2. Start the simulator and wait until `SimulatorAdminService.GetStats` answers (at most 30 s).
3. Start the gateway; wait until `/actuator/health/readiness` returns 200 with status `UP` (at most 90 s), then check
   that the `mode` tag values of `/actuator/metrics/connection_mode` are exactly the expected tag (`shared` or
   `per_request`) with value 1.0. A mismatch is the flag `CONNECTION_MODE_MISMATCH` and fails the run. A process that
   dies while the runner waits ends the wait at once.
4. Take snapshot `ready`.
5. Run the warm-up loadgen (the wait is bounded by duration + 30 s drain + 20 s + 30 s) and inspect its output. A
   loadgen that does not finish in time is stopped: it gets 40 s after `destroy()` (its pinned 30 s drain timeout plus a
   10 s margin, so that summary and footer can still be written on POSIX), then a forced kill for at most 10 s.
6. Wait (at most 10 s) until the gateway has no active streams and no queued items; take snapshot `pre`.
7. Take a coarse CPU load sample.
8. Run the measured loadgen and inspect its output. The runner does not poll during the measurement.
9. Wait (at most 10 s) until the gateway is idle and the simulator reports `active_generations == 0`; take snapshot
   `post`; check that both processes are still alive.
10. Stop the gateway (wait 45 s after `destroy`, then force for at most 10 s), then the simulator (20 s / 10 s). Descendants
    of a child are killed as well. Verify within 30 s that no listener is left on either port.
11. Update the manifest, then wait for the cooldown.

A run that cannot be completed (a process dies, a wait times out, the loadgen fails, the connection mode does not
match) is marked `failed`, its processes are cleaned up, the remaining runs are `not_run` and the exit code is 1.
Flags themselves never change the exit code and never delete or hide a run or a file. The run failures that can
accompany some of them do end the set with exit code 1: `LOADGEN_EXIT_NONZERO`, `LOADGEN_WAIT_EXCEEDED`,
`GATEWAY_EXITED`, `SIMULATOR_EXITED` and `CONNECTION_MODE_MISMATCH` come with a `failed` run, because the data of that run
would be unusable. All other flags only annotate a completed run.

## 5. Environment record

The manifest `environment` object records: the operating system, the CPU model with the source it came from, logical
processors and (best effort) physical cores, total and free memory, Java version, vendor, VM and home, the git commit
with the dirty state and the commit time, the Windows power plan (`powercfg /getactivescheme`), the TIME_WAIT counts at
the start (total and per port; `netstat -an -p tcp` on Windows, `ss -tan` elsewhere), and the names of the environment
variables that were removed from the children. `cpuLoadBefore` of each run holds the CPU load samples (one priming
read, then one sample every 500 ms over the window, negative readings are `null`). A value that cannot be determined is
`null`; none of these are fatal.

Host names are not recorded, and neither are the values of environment variables. The manifest does contain absolute
paths (jars, process commands, result directory) that may include the operating system user name: check an archive
before sharing it.

## 6. Operator checklist

Before a run set the operator confirms three things, recorded under `operatorChecklist.answers`:

| Key | Question |
|---|---|
| `otherAppsClosed` | Are other applications (browsers, IDEs, sync clients) closed? |
| `onAcPower` | Is the machine on AC power? |
| `samePowerPlan` | Is the same power plan active as in earlier run sets? |

`--checklist ask` (default) asks interactively and needs a terminal (three invalid answers count as `unanswered`);
`--checklist yes` records `yes` with source `flag`; `--checklist skip` records `unanswered` with source `skipped`.
A `no` or `unanswered` answer hides no data: it adds the flag `CHECKLIST_ANSWER_NO` or `CHECKLIST_NOT_CONFIRMED` to
the run set.

## 7. Stability flags

Flags are fixed strings in `flags[]` (run set level in the manifest root, run level in each run, and per phase for
loadgen output). They record an observation; they are not a verdict and the runner never acts on them. `<NAME>` is an
outcome name of the loadgen summary and `<R>` an upper-cased `rejected_total` reason.

| Flag | Meaning |
|---|---|
| `OUTCOME_<NAME>_NONZERO` | An `outcome_*` count other than `COMPLETED` and `REJECTED` is greater than 0 in the phase summary (for example `CONNECT_FAILURE`, `RESET`, `TIMEOUT`, `PROTOCOL_ERROR`). Generic, so new loadgen outcomes are covered. |
| `REJECTED_AT_OR_BELOW_LIMIT` | `REJECTED` outcomes occurred although the concurrency does not exceed `max-active-streams`. |
| `LOADGEN_EXIT_NONZERO` | The loadgen exited with a code other than 0. |
| `LOADGEN_WAIT_EXCEEDED` | The loadgen did not finish within the bounded wait and was stopped. |
| `SUMMARY_MISSING` | The summary file is missing or not a loadgen summary. |
| `JSONL_HEADER_UNEXPECTED` | The first JSONL line is not a header. |
| `JSONL_FOOTER_MISSING` | The last JSONL line is not a footer (for example after an I/O error). |
| `STOP_REASON_NOT_DURATION` | The footer `stopReason` is not `duration`. |
| `FOOTER_INTERRUPTED_NONZERO` | The footer reports interrupted requests. |
| `ACTIVE_STREAMS_NOT_ZERO_BEFORE` | `active_streams` was not 0 in snapshot `pre`. |
| `ACTIVE_STREAMS_NOT_ZERO_AFTER` | `active_streams` was not 0 in snapshot `post`. |
| `QUEUE_DEPTH_NOT_ZERO_AFTER` | `queue_depth` (chunks or admission) was not 0 in snapshot `post`. |
| `REJECTED_REASON_<R>_DELTA_NONZERO` | `rejected_total{reason}` changed between `pre` and `post` for a reason other than `limit`. |
| `TIMEOUTS_DELTA_NONZERO` | `timeouts_total` changed between `pre` and `post`. |
| `RETRIES_DELTA_NONZERO` | `retries_total` changed between `pre` and `post`. |
| `RETRY_EXHAUSTED_DELTA_NONZERO` | `retry_exhausted_total` changed between `pre` and `post`. |
| `SIMULATOR_ACTIVE_GENERATIONS_NOT_ZERO_AFTER` | The simulator still had active generations in snapshot `post`. |
| `SIMULATOR_FAULTED_DELTA_NONZERO` | `faulted_total` of the simulator changed between `pre` and `post`. |
| `SIMULATOR_REJECTED_DELTA_NONZERO` | `rejected_total` of the simulator changed between `pre` and `post`. |
| `SIMULATOR_STATS_UNAVAILABLE` | `GetStats` could not be read for `pre` or `post`. |
| `GATEWAY_EXITED` | The gateway process was no longer alive at the end of the measurement. |
| `SIMULATOR_EXITED` | The simulator process was no longer alive at the end of the measurement. |
| `CONNECTION_MODE_MISMATCH` | The `connection_mode` metric did not match the mode of the run. |
| `PORT_NOT_FREED` | A listener remained on a port within 30 s after the processes were stopped. |
| `CPU_BUSY_BEFORE_RUN` | The mean CPU load sample before the measurement was above 0.25 (a fixed marker, recorded as `cpuBusyThreshold`). |
| `CPU_LOAD_UNAVAILABLE` | No usable CPU load sample. |
| `TIME_WAIT_UNAVAILABLE` | The TIME_WAIT count could not be determined (command failed, or its output contained no recognisable TCP state, for example because it was localised). |
| `LOG_TRUNCATED` | A process log reached the 16 MiB per-stream limit; the dropped byte count (`log.bytesDropped`) is in the manifest. Bytes lost to a write error are not counted here. |
| `LOG_WRITE_ERROR` | Writing a process log failed (for example a full disk). The runner keeps draining the process output so the child never blocks, the log is incomplete, the error text is in `log.writeError` and the discarded bytes are in `log.bytesLostAfterWriteError` (not in `bytesDropped`, so `LOG_TRUNCATED` keeps its meaning). |
| `GIT_DIRTY` | The working tree had uncommitted changes. |
| `GIT_UNAVAILABLE` | git facts could not be read. |
| `ARTIFACT_OLDER_THAN_HEAD` | A jar is older than the HEAD commit time (a heuristic: the build may be stale). |
| `OUTPUT_NOT_GITIGNORED` | The output directory is not ignored by git. |
| `CHECKLIST_NOT_CONFIRMED` | A checklist answer is `unanswered` (including `--checklist skip`). |
| `CHECKLIST_ANSWER_NO` | A checklist answer is `no`. |
| `SMOKE_PRESET` | The run set used `--preset smoke`. |
| `INTERRUPTED` | The runner was interrupted (Ctrl+C or termination). |

## 8. Layout and schema

```
bench/results/<run-id>/
  manifest.json
  runs/<NNN>-c<C>-<mode>-r<R>/
    simulator.log  simulator-gc.log*  gateway.log  gateway-gc.log*
    warmup.jsonl  warmup-summary.json  warmup-loadgen.log  warmup-loadgen-gc.log*
    measured.jsonl  measured-summary.json  measured-loadgen.log  measured-loadgen-gc.log*
    snapshot-ready.json  snapshot-pre.json  snapshot-post.json
```

`<run-id>` is `yyyyMMdd'T'HHmmss'Z'-<shortCommit|nogit>[-dirty][-smoke]` (UTC). The directory is created atomically; if
the name exists, `-2` to `-99` is tried and then the runner fails. An existing directory or file is never overwritten.
`NNN` is the 1 based run index, `R` the 1 based repetition. `*` marks rotated GC log files. Stdout and stderr of a
process are merged into its `.log`, limited to 16 MiB per process; the rest is read and discarded and its size is
recorded. `bench/results/` is ignored by git.

`manifest.json` is written through a temporary file that replaces it with an atomic move (retried up to 5 times because a
Windows scanner can briefly lock the target; if every attempt fails the previous file stays as it was) after every state change: plan, process start with its PID (written when the child is registered; in the very narrow window where an interrupt wins
before that record, the child is still killed but its PID may be missing), run end, completion
and interrupt. After an interrupt the stop results, the final loadgen record, log counters and `portsFreed` of the
interrupted run are still recorded; its status stays `interrupted`. These post-interrupt records are
best effort within that wait: the shutdown hook first force-kills the children, marks the manifest `interrupted`, and then waits at most
`hookKillMs` (10 s, recorded in `parameters.plan.timeouts` together with the other bounds: `simulatorReadyMs`,
`gatewayReadyMs`, `idleMs`, `gatewayGracefulMs`, `gatewayForcedMs`, `simulatorGracefulMs`, `simulatorForcedMs`,
`portsFreedMs`, `loadgenExtraMs`, `loadgenGracefulMs`, `loadgenForcedMs`) for the runner thread to finish them; the JVM
may exit before that. The hook as a whole shares one deadline of `hookKillMs` (the kill wait and the wait for the runner
thread together), plus at most 2 s per child for draining a log. An interrupt that arrives while the environment is still being captured, before the manifest
exists, is applied as soon as the manifest is created; if the process is cut off earlier, the result directory can be
empty or contain a manifest that still says `running` (best effort). If the full manifest cannot even be created (for example an unreadable jar
when hashing), the runner writes a minimal manifest with `status` `failed` and `harnessFailure` and exits 1; if that
fails too (a full disk), exit 1 happens without any manifest. If writing the manifest keeps failing in the middle of the
run, the runner makes one direct write of the complete in-memory manifest (environment, artifacts, parameters and all run
records) with its current status: a still `running` manifest becomes `failed` with `harnessFailure`, an `interrupted`
one stays `interrupted`. The same happens at the end of the run when an earlier write had failed and was swallowed (for example in the
shutdown hook). That fallback goes through a second temporary file (`manifest.json.direct.tmp`) and an atomic move (a
plain move only on a platform without atomic move and only when `manifest.json` does not exist, so the old file is never
removed before the new content is in place); if it fails too, `manifest.json` is left untouched and keeps its last
written status, for example `running`, while the complete state stays in `manifest.json.direct.tmp` (whose path is
printed) if that file could be written; if not even that works, a partial direct tmp file is removed and no copy of the
final state exists. A failed manifest never contains a run in status `running`: such a run is turned into `failed` with a
`harness` failure. A leftover `manifest.json.tmp` from the failed normal path may remain in the directory. `schemaVersion` is 1 and `protocolVersion` is 1.

**Top-level keys of `manifest.json`:**

| Key | Content |
|---|---|
| `schemaVersion` | 1 |
| `protocolVersion` | 1 |
| `runId` | the directory name |
| `tool` | `streamlab-bench` |
| `toolVersion` | version of the runner jar (`dev` outside a jar) |
| `status` | `running`, `completed`, `failed` or `interrupted` |
| `createdAt`, `finishedAt` | UTC instants; `finishedAt` is `null` while running |
| `note` | `Raw data only. No analysis, no performance claim.` |
| `preset` | `default` or `smoke` |
| `commandLine` | the arguments |
| `operatorNote` | the `--operator-note` text, or `null` |
| `operatorChecklist` | `source`, `answers`, `note` |
| `environment` | see section 5: `os`, `cpu{model, modelSource, logicalProcessors, physicalCores}`, `memory{totalBytes, freeBytesAtStart}`, `java{version, vendor, vmName, vmVersion, home}`, `git{commit, shortCommit, dirty, commitTime, statusLines}`, `powerPlan`, `timeWaitAtStart{total, gatewayPort, simulatorPort}`, `removedEnvironmentVariables` |
| `artifacts` | `gatewayJar`, `loadgenJar`, `loadgenLib[]`, `simulatorClasspath[]`, `benchJar`; each `{path, sizeBytes, sha256, lastModified}` |
| `parameters` | `gateway{properties}`, `simulator{env}`, `jvm{gateway[], simulator[], loadgen[]}`, `loadgen{target, fixedArguments}`, `plan{concurrency[], repetitions, durationMs, warmupMs, rampUpMs, cooldownMs, modeOrder[], alternation, seed, ports, timeouts, logLimitBytes, cpuBusyThreshold, cpuWindowMs}` (`lab.connection-mode` is recorded as a per-run placeholder) |
| `runs` | array of run objects |
| `flags` | run set level flags |

A manifest of a run set that ended in a harness failure may carry an additional `harnessFailure` string.

**Keys of a run object:**

| Key | Content |
|---|---|
| `index`, `id`, `dir` | 1 based index, directory name, relative directory (`runs/<id>`) |
| `mode` | `shared` or `per-request` |
| `concurrency`, `repetition`, `orderLabel` | the cell; `orderLabel` is `AB` or `BA` |
| `status` | `planned`, `running`, `completed`, `failed`, `interrupted` or `not_run` |
| `startedAt`, `endedAt` | UTC instants |
| `failure` | `null` or `{phase, message}` |
| `processes` | `simulator` and `gateway`: `{pid, command[], envOverrides, startedAt, stop{method, exitCode, durationMs}, log{file, bytesWritten, bytesDropped, bytesLostAfterWriteError, writeError}}`; `stop.method` is `already_exited`, `destroy`, `forced` or `survived` |
| `timeWaitBefore` | TIME_WAIT counts before the run, or `null` |
| `cpuLoadBefore` | the CPU load samples |
| `warmup`, `measured` | `{warmup, loadgen{pid, command[], exitCode, durationMs, files{jsonl, summary, log}, log{bytesWritten, bytesDropped, bytesLostAfterWriteError, writeError}}, outcomes{}, footer{stopReason, requestsStarted, recordsWritten, interrupted}, flags[]}` |
| `snapshots` | `ready`, `pre`, `post`: the file name and the capture time; `pre` and `post` also hold `gatewayIdleWait` (`ready` has none) |
| `portsFreed` | whether both ports were free within 30 s after stopping; `null` when no process was started (for example a failed port check) |
| `flags` | run level flags |

**Snapshot files** hold `capturedAt`, `gateway` (a list of `{path, status, value, raw}` for the readiness endpoint and
the metrics `active_streams`, `queue_depth` per queue, `rejected_total` in total and per reason, `timeouts_total` per
kind, `retries_total` per reason, `retry_exhausted_total`, `connection_mode`, `jvm.memory.used` (heap),
`jvm.threads.live`, `process.cpu.usage`, `system.cpu.usage`; a 404 is recorded, not an error) and `simulator` (the eight
`GetStats` fields, or `{unavailable: <reason>}`).

No example manifest is committed: invented numbers could be mistaken for a result.

## 9. Archiving

Result directories are not committed. Archive one with `tar -czf bench-<run-id>.tar.gz -C bench/results <run-id>` (Git
Bash and the `tar.exe` of Windows 11) or PowerShell `Compress-Archive`, and attach the archive to a release or an issue.

## 10. Harness limits and observations

Findings about the other modules (reported, not changed):

- The simulator has no runnable artifact: its jar has no classpath manifest and no `lib/`, and the README only shows
  `exec:java`, which runs it inside the Maven JVM. The runner takes the jars from `bench/target/lib`
  (`bench` depends on `simulator` with runtime scope; `copy-dependencies` fills that directory during `package`).
- Simulator statistics are only readable through gRPC (`GetStats`); there is no HTTP surface. The runner uses the proto
  module for a small client.
- On Windows, `Process.destroy()` and `Process.destroyForcibly()` are both a hard termination. The graceful shutdown of
  the gateway (Spring graceful shutdown) and of the simulator (JVM shutdown hook) therefore cannot be triggered from
  outside, and the actuator `shutdown` endpoint is not exposed. The runner stops the processes after all data has been
  collected; shutdown behaviour is not part of the protocol. In the observed smoke run the logs of both processes show no
  shutdown messages. On Linux, `destroy()` sends SIGTERM and the hooks run. The recorded `stop.method` `destroy` therefore
  means "exited after `destroy()`", not "exited gracefully".
- `server.port` is not set in the gateway `application.yaml` (the default 8080 is implicit) and `lab.upstream.target`
  defaults to `localhost`; the runner pins `127.0.0.1` to avoid IPv4/IPv6 resolution differences.
- `SIMULATOR_SCHEDULER_THREADS` defaults to the processor count; the runner sets it explicitly.
- The gateway does not expose the chunk size (`chunk_size_bytes` is never set).
- The loadgen exit code 0 does not reflect request outcomes; the runner inspects the summary and the JSONL footer.

Observations about the harness on Windows 11 with JDK 26 (what was seen, not benchmark results):

- `getCpuLoad()` returned samples within [0, 1] (A4). `reg query`, `powercfg /getactivescheme`, `netstat -an -p tcp` and
  the PowerShell CIM query for the physical core count worked without administrator rights (A5).
- Spring Boot 4.1.1 answers `/actuator/metrics/<name>` with `measurements[].statistic/value` and `availableTags`, and
  supports `?tag=key:value` (A1). `connection_mode` carries exactly one `mode` tag value, the configured mode.
- `destroy()` ended the gateway and simulator without any shutdown output in their logs (A2).
- A connect to a closed loopback port was refused within milliseconds here; the probe timeout is 3 s regardless.
- Interrupt: a Ctrl+C sent to the console of a runner started in its own Windows console (GenerateConsoleCtrlEvent)
  during a measured phase ended the runner through its shutdown hook: the manifest said `interrupted` (the run
  `interrupted`, the later runs `not_run`, flag `INTERRUPTED`), the simulator, gateway and loadgen processes were gone and
  both ports were free. The JVM's exit status was `0xC000013A` (STATUS_CONTROL_C_EXIT), i.e. chosen by the JVM, not by the
  runner. Whether the children also receive the console event was not examined; the hook kills them either way. The
  "ignore Ctrl+C" attribute of a console process is inherited: a runner started from a parent that ignores Ctrl+C never
  sees it. If the runner itself is killed hard (window closed, `taskkill /F`, `TerminateProcess`), no hook runs; the
  manifest then stays `running` and the children may stay alive. The PIDs in the manifest and the port check before
  the next run are the mitigation. Behaviour in PowerShell, Windows Terminal and mintty (Git Bash) was not verified
  separately.
- `Console.isTerminal()` is false when standard input or output is redirected; `--checklist ask` then fails with exit
  code 2. Behaviour in an interactive mintty window was not verified; use `--checklist yes|skip` there (A6).

Limits of the harness itself:

- The runner JVM and every child run on the same machine; the runner is idle during a measurement.
- TIME_WAIT accumulation of `per-request` runs may carry over into the next run. The 2 s streams, the cooldown, the
  alternation and the recorded counts reduce and expose this; they do not prove its absence.
- Antivirus, indexing and other background load are only flagged (checklist, CPU sample), never controlled.
- The `ARTIFACT_OLDER_THAN_HEAD` check is a heuristic, not proof that the jars match the checkout.
- The hand-written JSON parser reads only known producers (actuator, loadgen).
- Disk use: with 503 responses the JSONL grows; the default set can take hundreds of MB. Logs are limited to 16 MiB per
  process and GC logs to 2 x 8 MiB.

## 11. Running it

```bash
./mvnw -B package -DskipTests                                                # build the jars (the runner never builds)
java -jar bench/target/bench-0.1.0-SNAPSHOT.jar --dry-run                    # plan and exact commands, starts nothing
java -jar bench/target/bench-0.1.0-SNAPSHOT.jar --preset smoke --dry-run
java -jar bench/target/bench-0.1.0-SNAPSHOT.jar --preset smoke --checklist yes   # harness check (4 short runs)
java -jar bench/target/bench-0.1.0-SNAPSHOT.jar                              # the default set (asks the checklist)
```

Exit codes: 0 all planned runs completed; 1 a harness or run failure after the result directory was created (the manifest
says `failed`, except when not even a minimal manifest could be written, see section 8, and except when the result
directory itself cannot be created, for example an unwritable `--output-dir` or no free name among `-2` to `-99`: then
there is neither a directory nor a manifest and the reason is printed, and except when writing the manifest keeps
failing during the run: the runner then makes one direct write of the complete in-memory manifest with its current status
(a still `running` manifest becomes `failed`; `completed`, `failed` and `interrupted` keep their status), see section 8); 2 invalid arguments or a failed precondition (nothing was started, no directory was created). After an
interrupt the manifest says `interrupted`; the exit status is then chosen by the JVM and is not tested.
`--help` prints all options.
