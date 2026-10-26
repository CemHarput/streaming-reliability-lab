# ADR-0006: Benchmark report as a separate plain-Java module

- Status: accepted
- Date: 2026-10-26

## Context

The runner ([ADR-0005](0005-benchmark-runner.md)) writes raw data and a manifest and nothing else. Turning one result set
into something a reader can check needs an analysis step that states what was measured, under which recorded
conditions, and what cannot be concluded. The real result set will come from a quiet Linux machine and will be read on
the Windows development machine, while the harness has only ever run on Windows. The report must not claim that either
connection mode is generally better, and it must not be possible for a number in the report to drift from the data.

## Decision

- **A new Maven module `report` with a plain-Java tool (`ReportMain`), not a sub-command of `bench`.** Decoding the
  inter-chunk histogram of the JSONL footer needs HdrHistogram. Adding it to `bench` would put it into `bench/target/lib`,
  which is the simulator's classpath (and the `simulatorClasspath` of the manifest), and would change the pinned artefact
  list of the protocol. `report` depends on the `bench` jar with an all-exclusion (`*:*`), so `report/target/lib` holds
  only the bench jar and HdrHistogram (the managed version). It reuses `Json`, `Flags`, the manifest and layout
  constants, `GatewayProbe` path constants, `ConnectionMode` and `PlanBuilder` (to rebuild the expected run list). These
  classes use the JDK only, so they load without gRPC on the classpath (a child JVM in a test and the packaged jar show
  it).
- **Alternatives rejected.** A hand-written HdrHistogram decoder (a copy of a binary format); Python or R (a new
  runtime); a `bench` sub-command (see above, and it would touch the runner).
- **Descriptive output only.** Per run: outcome counts and shares, completed streams per second over the window
  `[rampUp, duration)`, exact nearest-rank percentiles of the COMPLETED requests, the footer inter-chunk histogram,
  snapshot deltas. Between modes: only within one concurrency, per-repetition values side by side with the run index
  and the ABBA position, median and range per mode, and one sentence from a closed template set. No p-values, no
  confidence intervals, no ranking, no pooled percentiles. The rule that decides the sentence (fewer than two
  repetitions, equal medians, overlapping ranges, difference not larger than the largest within-mode range, otherwise
  "higher") is applied to exact values; the values of a cell are shown with enough decimals that the two medians and
  the comparison of the difference with the range stay visible. The difference and the range are descriptive, because a handful of repetitions per
  cell gives no stable estimate of the variation.
- **Visible exclusion.** Every flag of the runner's dictionary is classified `exclude`, `caution` or `info` in one table
  (`FlagPolicy`, generated into the report and compared with `Flags.DOCUMENTED` by a test, so a new flag in the runner
  breaks the build of this module). Unknown flags exclude, except in a warm-up phase, where every flag is only a caution. Runs whose status is not
  `completed` are excluded. Every run appears in the inventory with its class and reasons; excluded runs keep their
  metrics in the tables and are left out of the comparisons and the charts. The tool prints the excluded runs and a last
  line of JSON.
- **Honest wording is enforced by code.** A `WordingGuard` scans every output file (the report, the CSV, JSON and SVG
  files) for a fixed list of forbidden phrases; the only exemption is the block of the protocol's "What it cannot
  answer" items, which is quoted verbatim and compared with the protocol document by a test. A `PrivacyGuard` scans
  every output file and the standard output summary for absolute paths, the result set path and the home directory
  (the standard output is not wording-scanned). A violation stops the tool (exit 1, nothing written). The tool never
  reads `java.home`, command lines, the text of the operator note, failure messages or harness failure texts into any
  output, and of the git status lines only the count.

## Consequences

- The analysis is reproducible from an archived result set with two commands. `CommittedReportTest` compares the
  committed placeholder byte for byte with the generated one; for an analysed report it only checks the generation
  marker, both guards and the quoted block, so its numbers are verified by regenerating the report from the archived
  result set, not by a test.
- The module depends on the runner's flag dictionary and manifest shape. A change of the manifest `schemaVersion` or
  `protocolVersion` makes the tool refuse the set (exit 2); a new flag breaks the build until it is classified.
- The tool follows the loadgen format through a contract test that runs the real `RecordWriter` and `Summary`.
- The statistics are deliberately weak. They show that two modes differ in this set or that this set cannot separate
  them; they do not say why, and they say nothing about another machine.
- The comparison needs both modes with at least two usable repetitions in a cell; otherwise it says so.
- `RunAssessment` builds the expected run ids itself instead of reading them from `PlanBuilder`, because the runner
  formats them with `String.format` and the default locale (a test caught non-Latin digits under `ar-SA`).

## Findings about the runner and the manifest (reported, not changed)

- **RF1.** `schemaVersion` 1 changed without a version bump: old manifests record `stop.method` `graceful` (today
  `already_exited`, `destroy`, `forced`, `survived`) and some lack `bytesLostAfterWriteError` / `writeError` in the log
  records. The tool tolerates this (an unknown stop method is information), but there is no policy that says when a
  field change needs a new version.
- **RF2.** On Linux nothing replaces the Windows power plan (no CPU governor, no virtualisation information, no
  dedicated-core information); `powerPlan` is always null. The report cannot say anything about power settings there.
- **RF3.** ARM `/proc/cpuinfo` has no `model name`, so the CPU model is null. On virtual machines `physicalCores`
  reflects the vCPU topology.
- **RF4.** The harness was never run on Linux. The exit code after SIGTERM (for example 143) and the meaning of
  `destroy` ("shutdown hooks ran") are unverified; the report shows both as recorded.
- **RF5.** The checklist questions (`onAcPower`, `samePowerPlan`) are written for a desktop or Windows machine; on a Linux
  virtual machine their meaning is unclear.
- **RF6.** Run from a tarball instead of a git clone, the runner records `GIT_UNAVAILABLE` and a null commit, so the
  identification block stays incomplete. Run it from a clone on Linux.
- **RF7.** `failure.message` and `harnessFailure` can contain local paths. The report prints neither (only the presence
  of a harness failure).
- **RF8.** A non-smoke set has never been produced on Linux; the `ss -tan` output is counted by `TIME-WAIT` (the code was
  read, not run there).
