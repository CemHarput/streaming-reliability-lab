# ADR-0005: Benchmark protocol runner as a plain-Java module

- Status: accepted
- Date: 2026-10-25

## Context

The comparison of the two gateway connection modes (ADR-0003) with the closed-model load generator (ADR-0004) needs a
repeatable procedure: fresh JVMs per run, pinned settings, a warm-up, alternating run order, recorded environment and
raw results in a fixed layout. The procedure is written down in [the protocol](../benchmark-protocol.md); something has
to execute it. It must run on Windows (the development machine) and on Linux (CI), must control child process trees,
must stop everything on Ctrl+C, and must not add third-party artefacts.

## Decision

- **A new Maven module `bench` with a plain-Java runner (`BenchMain`), JDK APIs only.** It is a runnable jar with its
  `lib/` directory, like `loadgen`. No new third-party dependency: it uses `proto` and `grpc-netty-shaded` (already in
  the reactor) for the simulator's `GetStats`, the JDK `HttpClient` for the actuator and `ProcessBuilder` /
  `ProcessHandle` for processes. A small nested JSON parser/writer is written instead of adding Jackson, because
  `bench/target/lib` is also the simulator's classpath and must not be polluted (same reasoning as ADR-0004).
- **Alternatives rejected.**
  - *Bash (Git Bash):* MSYS PIDs are not Windows PIDs, killing a process tree needs `taskkill /T /F`, `trap INT` under
    mintty is unreliable for native Java processes, JSON handling needs `jq` and testing needs `bats`; none of it can
    be unit-tested in `./mvnw verify`.
  - *PowerShell:* Windows only (or `pwsh` in CI), needs Pester for tests, a second implementation for Linux.
  - *Python:* not in the toolchain; a new runtime.
- **The runner never builds.** `./mvnw -B package -DskipTests` is a documented separate step. This avoids the
  `mvnw` / `mvnw.cmd` split, building while measuring and Maven JVM noise.
- **Children are started directly with `<java.home>/bin/java`**, not through the Maven wrapper, so there is no
  intermediate process tree. The gateway uses its Spring Boot jar, the loadgen its jar with `lib/`. The simulator has no
  runnable artefact (finding F1), so `bench` depends on `simulator` with runtime scope and `copy-dependencies` fills
  `bench/target/lib`; the simulator is started with an explicit, name-sorted classpath of exactly those jars.
- **Bounded stop sequence** (`ProcessSupervisor`): snapshot the descendants, `destroy()`, wait a bounded time,
  `destroyForcibly()`, force-kill the snapshot plus the current descendants, wait a bounded time. On Windows `destroy()`
  is `TerminateProcess` as well, so the graceful shutdown paths of the gateway and the simulator cannot be triggered from
  outside (finding F3) and are not part of the protocol; the recorded method `destroy` means "exited after `destroy()`".
- **Cleanup on interrupt:** a shutdown hook kills all registered children, forbids new starts and marks the manifest
  `interrupted`. A hard kill of the runner itself cannot run the hook; the PID of every process is written to the
  manifest as soon as the child is registered (in the very narrow window where an interrupt wins before that record, the child is still killed but its PID may be missing) and the next run set refuses to start while a listener is present on the ports.
- **Port checks use a connect probe**, not a bind test: Windows `SO_REUSEADDR` / TIME_WAIT semantics can make a bind
  test report the wrong answer.
- **Manifest and flags.** `manifest.json` has a fixed schema (`schemaVersion` 1, `protocolVersion` 1) and is written
  through a temporary file and an atomic move after every state change. Observations are recorded as flags from a fixed dictionary; a flag never changes
  the exit code, hides a run or deletes a file. The runner analyses nothing and makes no performance claim.
- **Tests do not start the real gateway, simulator or loadgen.** All external effects (processes, HTTP, gRPC, ports, time,
  system commands) are behind interfaces with fakes; the only real child processes are three test-only JVMs
  (`ChildMain`) in the process supervisor test.

## Consequences

- One portable implementation with unit tests in `./mvnw verify`; the price is more code than a script.
- The runner JVM sits idle on the same machine during a measurement; it does not poll while the loadgen runs.
- The runner is only as honest as the protocol: results from a machine with background load are flagged, never
  corrected.
- Graceful shutdown behaviour of gateway and simulator on Windows stays untested by the harness (F3).
- The hand-written JSON parser is a deliberate duplication of a small amount of code; its inputs come from known
  producers.
