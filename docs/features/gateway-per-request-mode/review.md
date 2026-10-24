# Review: gateway-per-request-mode (final: pass 3)

Verdict: **PASS** (no open finding; one plan deviation was reported to the user as a decision and later closed, see the addendum).
Baseline HEAD: 9136fa4b8b68d99863ba7f4412040e835798863c (branch main, nothing committed).

## History
- Pass 1: CHANGES_REQUESTED. P2-1 (`channel()`/`close()` race could leave a CLOSED lease in the `open` set, making `openChannels()`/`isTerminated()` wrong and the race test flaky), P3-1 (empty `lab.connection-mode` falls back to shared instead of failing: deviation from plan AC1), P3-2 (no test that a WAIT-queued request opens no channel), P3-3 (overflow, mid-stream idle timeout, retry-exhausted and failed-non-last-write paths not run with a per-request lease), P3-4 (docs over-generalized the contract coverage, stale `acquire()` javadoc). Fixed: state re-read after registering in `open`; new E2E test `waitingRequestOpensItsChannelOnlyAfterAdmission`; docs/javadoc; P3-3 accepted as a documented gap; P3-1 left and reported.
- Pass 2: CHANGES_REQUESTED, P3 only: the new test waited on the sum of two `queue_depth` gauges, README module table, stale counts in implementation.md. Fixed.
- Pass 3: PASS.

## Evidence and limits
- Reviewers ran nothing. Coordinator: independent clean `./mvnw -B clean verify` exit 0 before and after the fixes (gateway 231 then 233 tests); implementer ran three green full builds; diff of proto, simulator and `gateway/src/main/.../gateway/stream` empty; no `Thread.sleep`. A transient `clean` failure (file lock on `simulator/target`) was followed by a green retry.
- Not covered: Netty/TCP/HTTP2, connect backoff, TLS, performance (in-process channels only; README "Not measured yet").
- Deviation at the time of the PASS (closed afterwards, see the addendum): an empty `lab.connection-mode=` was treated as unset (shared) instead of failing startup.
- This review does not mean the feature is bug-free.

## Addendum: empty-mode fail-fast
The "open deviation" above was closed after the PASS: an empty or blank `lab.connection-mode` now fails startup (see implementation.md, "Fail-fast for an empty mode"). This small change was reviewed in a separate pass (below, if present) or by the coordinator's full build only.
