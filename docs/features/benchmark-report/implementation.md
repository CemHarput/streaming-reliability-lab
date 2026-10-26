# Implementation: benchmark-report

Status: implemented (required checks passed; the real result set analysis is the remaining step, the generated placeholder report is the accepted outcome until a set is supplied). Plan hash `a2b53f7b2660d136e1131bb0a53a841a6471151e64b136901e73ab458ed2f7df`, baseline `37a09b7c46c5d8ea22a0d3d40ea1ecde4097c867`.

## Files
- Modified: `pom.xml` (module `report` after bench), `README.md` (status, module row, "Benchmark report" section, ADR-0006 in Decisions), `docs/benchmark-protocol.md` (cross reference in section 1, regenerate command in section 9; protocol version unchanged).
- New docs: `docs/adr/0006-benchmark-report.md` (decision, rejected alternatives, consequences, runner findings RF1-RF8 reported not fixed), `docs/benchmark-report.md` (generated placeholder, no numbers).
- New module `report/`: `pom.xml` (bench dependency with a `*:*` exclusion, HdrHistogram, test deps loadgen/junit/assertj, jar manifest, copy-dependencies to `report/target/lib`); 31 main classes under `report/src/main/java/io/github/cemharput/streamlab/report/` (`ReportMain`, `ReportOptions`, `ReportOptionsParser`, `UsageException`, `InputException`, `ResultSetReader`, `ManifestView`, `SafePaths`, `BoundedLineReader`, `JsonlScanner`, `InterChunk`, `SnapshotReader`, `Jv`, `Percentiles`, `FlagPolicy`, `Checks`, `Severity`, `RunAssessment`, `RunMetrics`, `CellComparison`, `ComparedMetric`, `Statement`, `Fmt`, `ReportText`, `ReportRenderer`, `CsvWriter`, `AnalysisJson`, `SvgDotPlot`, `OutputWriter`, `WordingGuard`, `PrivacyGuard`); 29 test classes in the report package at the end of the review rounds (the first implementation had 26 classes and 232 tests with 1 skipped; later sections give the current counts) plus the contract test in loadgen's package plus `testsupport/` (`ResultSetBuilder`, `Cli`, `Scenarios`, `Analyses`, `Scans`) and `ReportLoadgenFormatContractTest` in loadgen's package.
- Unchanged: `bench`, `loadgen`, `gateway`, `simulator`, `proto`. No smoke result data was read or used.

## Acceptance mapping
AC1-AC16 and AC18 met per the implementer's evidence; AC17 (analysis of a real result set) is open by design. Key evidence: `report/target/lib` holds exactly the bench jar and HdrHistogram; the only child process in tests is the analyser in `BoundedMemoryTest` (`-Xmx32m`, at least 96 MiB JSONL, exit 0); determinism across locales tr-TR/de-DE/ar-SA/th, time zones UTC/Tokyo/LA, shuffled records and different temp dirs; forbidden-phrase and privacy scans over every generated output; Linux-flavoured and Windows-flavoured fixtures give identical CSV/JSON/SVG; `FlagPolicyTest` ties the classification table to `Flags.DOCUMENTED`.

## Checks
- Implementer: focused command x3 (232 tests, 1 skipped, stable), `dependency:tree`, full clean build green, manual `--help`, `--placeholder --out docs` (rerun with `--overwrite` identical), `--check` and `--out` against a fixture set (exit 0, 0, and 2 without `--overwrite`).
- Coordinator: `feature_state.py check` approval_matches true; independent `./mvnw -B clean verify` exit 0 (simulator 12, gateway 233, loadgen 89, bench 205, report 232 with 1 skipped); `git diff --stat -- bench gateway simulator proto loadgen` empty; no `Thread.sleep` in `report/src`; `report/target/lib` = `HdrHistogram-2.2.2.jar` + `bench-0.1.0-SNAPSHOT.jar`; the committed `docs/benchmark-report.md` is the placeholder.
- Not run: a review (follows); analysis of a real result set.

## Deviations and decisions
1. The report prints raw file sizes for files it only lists (warm-up files, logs); sizes differ between LF and CRLF inputs, so the report differs between them only in those values (CSV/JSON/SVG are byte-identical); tests mask them, README and ADR say so.
2. A locale defect in `bench`'s `PlanBuilder` (run ids formatted with the default locale, non-Latin digits under `ar-SA`) is worked around in `RunAssessment.expectedId` and recorded in ADR-0006 (bench not changed).
3. The plan named checks C1-C12 without listing them; `Checks.CROSS_CHECKS` defines them and the data-quality table is generated from it.
4. Non-completed runs get reason `STATUS:STATUS_<X>`; stdout and JSON use the plan's `source:code` format. `Fmt.cell` escapes `<` and `>`.

## Limitations
- No real result set was analysed. When a path is supplied: extract into a new empty directory outside the repository, run `--check`, record sha256, run `--out docs --overwrite`, generate again into a temp dir and compare, re-hash, run `./mvnw -B verify`, check that git status shows only the report and `docs/benchmark/*`.
- The symlink-escape test in `SafePathsTest` is skipped on this Windows account (no symlink privilege); the path logic exists and the case is covered where symlinks work (for example Linux CI).
- Statistics are deliberately weak: cells with n<=4 per mode only get "cannot separate" or "higher" descriptions, no inference.
- Runner/manifest findings RF1-RF8 are reported in ADR-0006.

## First-review fixes (after review pass 1; done by the implementer on the coordinator's request)
- Output writing is all-or-nothing as far as practical: every directory and every file is written to a unique JDK temp file first, moves happen afterwards, only own temp files are deleted, the message names already replaced files (exit 1); `<out>/benchmark` resolving into (or containing) the result set is refused with exit 2; user-owned `*.tmp` files are untouched (`HardeningTest`).
- ADR-0006 and README state exactly what is scanned (`WordingGuard` and `PrivacyGuard` over every output file, `PrivacyGuard` also over the stdout summary) and that an analysed report is verified by regenerating it from the archived set (the placeholder is checked byte for byte).
- The stdout summary prints counts per run status (`statuses: ...`); an oversized, unparsable or wrong-type summary yields the caution `CHECK_SUMMARY_UNREADABLE` (rows C4, C8, C9).
- Untrusted manifest text: one-place table escaping (`<`, `>`, `|`), `Fmt.code`/`Fmt.line`, a safe run id in the HTML comment, CSV cells starting with `= + - @` get a leading single quote, an invalid `modeOrder` falls back to first appearance with a `CHECK_PLAN_MISMATCH` banner, statement templates fill placeholders in one pass.
- Smaller items: INSUFFICIENT sentence names both modes, `TIME_WAIT_UNAVAILABLE` scope "set and run", overlap/spread sentences state no direction, rates and shares compared exactly at 30 decimals (CSV/JSON carry those values), `PrivacyGuard` also covers `/opt/`, `/srv/`, `/data/`, `/etc/`, `/proc/`, `/sys/`, `/usr/`.
- A real defect found by the extended guard: the Linux CPU model source text `/proc/cpuinfo model name` tripped `PrivacyGuard` and would have made every Linux set exit 1; it is now rendered as "proc cpuinfo model name".
- Checks: focused report tests x3 (247 tests, 2 skipped symlink tests); the coordinator's independent clean build green (simulator 12, gateway 233, loadgen 89, bench 205, report 247 with 2 skipped); `git diff --stat -- bench gateway simulator proto loadgen` empty; no `Thread.sleep` in `report/src`; the two remaining `java.exe` processes are VS Code language servers.

## Second-review fixes (after review pass 2; done by the implementer on the coordinator's request)
- Display scale per comparison cell chosen so that the exact difference stays visible (the rules still use the exact 30-decimal values); output files get normal POSIX permissions (`rw-r--r--` subject to umask) instead of the temp-file default 0600; README and ADR quote the real sentence templates; the unknown-flag wording says an unknown flag from a warm-up phase is only a caution; stale `OutputWriter` Javadoc fixed; table cells are escaped once and code spans escape only the pipe (backslash handling cannot split a cell; the UNC pattern of `PrivacyGuard` now needs a `\server\share` shape); CSV neutralisation also for cells starting with tab, CR or LF; an empty `benchmark/` directory created by a failed run is removed.
- Checks: focused report tests x3 (259 tests, 3 skipped: POSIX permissions and two symlink tests, skipped on this Windows account); the coordinator's independent clean build green (see below); `git diff --stat -- bench gateway simulator proto loadgen` empty; no `Thread.sleep` in `report/src`.

## Third-review fixes (after review pass 3; done by the implementer on the coordinator's request)
- Display scale: chosen from the rounded median/min/max of each mode so that a HIGHER sentence keeps "ranges did not overlap" visible and the sentence's difference and range are computed from the displayed values, exactly as a reader would from the table.
- Markdown cells: every backtick in untrusted text becomes an apostrophe; code spans in cells come only from `Fmt.code` (private-use markers), with only the pipe escaped inside; `Fmt.codeInline` for lines outside tables.
- `PrivacyGuard` UNC detection covers markdown-escaped and JSON-escaped forms, the `\?\UNC\` prefix and the slash-converted `//server/share` form (URLs and `docs//x` do not match).
- README: CSV neutralisation list names tab, CR and LF (also in the report's data-files prose); replaced files are new files with `rw-r--r--` subject to the umask, an existing stricter mode is not preserved.
- Checks: focused report tests x3 (265 tests, 3 skipped on this Windows account); the coordinator's independent clean build green (see the final line); `git diff --stat -- bench gateway simulator proto loadgen` empty; no `Thread.sleep` in `report/src`.
- Coordinator's independent clean build after these fixes: the first attempt failed in the clean phase on a transient Windows file lock (`simulator/target`), the immediate retry was green (simulator 12, gateway 233, loadgen 89, bench 205, report 265 with 3 skipped).
