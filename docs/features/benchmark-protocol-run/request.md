# Request: benchmark-protocol-run

Goal
Define a written benchmark protocol and a script that runs it, so that the two gateway connection modes (shared and per-request) can be compared under identical conditions and the raw results are saved in a reproducible layout. This feature produces the PROTOCOL and the RAW DATA only: no analysis, no charts, no report, no ranking of the modes, no performance claim anywhere (docs, README, script output).

Context
- Existing pieces: simulator (gRPC token producer with fault injection), gateway (SSE bridge, lab.connection-mode = shared | per-request, actuator metrics incl. rejected_total, queue_depth, active_streams, connection_mode), loadgen (closed-model CLI, JSONL records plus header/footer, convenience summary, exit 0 when the run completed). A later feature (benchmark-report) will read the raw results and write the analysis.
- Planner: verify how to start simulator and gateway as separate processes on this repo (README manual commands, ports 50051 and 8080 as assumed, gateway properties for the upstream target, max-active-streams, timeouts, actuator exposure), what the gateway needs so that only the connection mode differs between runs, and which settings must be pinned and recorded (JVM flags, heap, GC, server threads, lab.limits.*, lab.timeouts.*, stream parameters).
- The developer machine is Windows 11 with Git Bash and PowerShell; CI is ubuntu. The script must run on the developer machine; planner decides the language/shell (a single portable implementation preferred over two parallel ones; a small Java or shell runner is acceptable, avoid new heavy dependencies) and states why.

Protocol (write it as docs/benchmark-protocol.md, planner to refine)
- Question the protocol is allowed to ask: how do the two connection modes differ in the raw measurements loadgen records, under a closed-model load, on one machine, with the in-process-free real network path (TCP to localhost). It must state what the protocol cannot answer (production behaviour, network effects, other hardware, open-model arrival rates, coordinated omission).
- Variables: connection mode is the only independent variable; everything else pinned. A concurrency sweep is allowed as a second axis (planner: choose a small set, e.g. below and at and above the gateway max-active-streams so that rejection behaviour is visible, and justify the values).
- Procedure per scenario: start simulator and gateway fresh, wait for readiness (bounded), warm-up run (recorded but marked as warm-up), N measured repetitions, stop processes (bounded, verify ports freed). Order of modes alternates across repetitions (for example A B B A) to reduce drift; the order used is recorded. Same seed for every repetition and mode so request parameters are identical; planner: decide whether to vary the seed between repetitions and record it.
- Stability checks recorded, not interpreted: loadgen exit code, outcome counts, gateway rejected_total and active_streams before and after, simulator stats if reachable without changing the simulator (planner: verify what is available), process still alive, a note when a run had a non-zero count of CONNECT_FAILURE/RESET/TIMEOUT/PROTOCOL_ERROR. The script flags such runs in the manifest but never deletes or hides data.
- Environment capture: machine (OS, CPU model and cores, RAM), Java version and vendor, git commit and dirty flag, exact command lines and gateway/simulator settings, wall-clock start, power/background-load notes the operator must confirm (a short checklist the script prints and records the answer to: other apps closed, plugged in, same power plan), and a coarse CPU-load sample before each run so noisy runs can be identified later.

Output layout (planner to define precisely, stable schema with schemaVersion)
- bench/results/<run-id>/ with manifest.json (protocol version, environment, parameters, ordered list of runs, flags), one directory per measured run containing the loadgen JSONL and summary JSON, process logs (gateway/simulator stdout/stderr, size-bounded), and metric snapshots. bench/results must be gitignored by default (large raw data); planner decides whether a tiny example or schema file is committed, and how a result set can be archived or attached without committing it.
- run-id derived from time plus short git commit; never overwrite an existing run directory.

Script behaviour
- Options: scenario set (default sweep and a quick smoke preset with tiny durations for checking the harness), repetitions, duration per run, warm-up duration, concurrency list, output directory, mode order, --dry-run (print the plan and the exact commands, start nothing), --help. Fail fast with clear messages on invalid options; clean up child processes on error and on Ctrl+C (no orphaned java processes, ports freed), bounded waits, no endless loops.
- Never changes gateway, simulator or loadgen code; if the harness needs something they lack (for example a way to read simulator stats, a fixed actuator exposure, a deterministic port), report it as a finding instead of changing them here.

Tests and verification
- The script logic that is not process-heavy (option parsing, plan generation including the alternating order, manifest and path building, run-id uniqueness, flag detection from loadgen output, cleanup logic with fake processes if the language allows) is unit-tested in the normal build (./mvnw -B verify stays green and does not start the real gateway/simulator).
- The smoke preset is run manually by the implementer once against the real simulator and gateway: report that it works, the resulting directory listing and the manifest shape; do NOT include numbers or interpretation of the measurements in the repo docs.
- Acceptance criteria (planner to refine and number): dry-run prints a complete plan; smoke run produces a valid manifest and per-run files for both modes; processes are cleaned up after success, failure and interruption; a second run never overwrites the first; docs/benchmark-protocol.md complete (question, variables, procedure, environment capture, stability flags, what it cannot answer); README section with the commands and an explicit 'no performance claim' statement; bench/results ignored by git.

Out of scope
- Analysis of results, charts, tables of numbers, the report, any statement about which mode is better or faster (that is benchmark-report, and even there only with the limits above).
- Open-model load, other transports, CI integration of the benchmark, multi-machine setups, MQTT, showcase.
- Changing gateway, simulator, proto or loadgen.

Constraints
- Work on main, no new branch, no commits or pushes.
