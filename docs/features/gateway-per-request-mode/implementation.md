# Implementation: gateway-per-request-mode

Status: implemented (required checks passed). Plan hash `4779099200dd7efcaed4964f4d7268b61172d7265b02f5633d18080b4a3f6f7d`, baseline `9136fa4b8b68d99863ba7f4412040e835798863c`.

## Files
Main: `config/GatewayProperties.java` (enum `SHARED("shared")`, `PER_REQUEST("per_request")`, `tag()`, null check), new `upstream/PerRequestChannelProvider.java` (SmartLifecycle at `UPSTREAM_CHANNEL`; lease CAS EMPTY/OPEN/CLOSED; channel created lazily on first `channel()` with no lock held; `close()` only calls non-blocking `shutdown()`; `stop()` backstop closes leftover leases, waits for closing channels against one deadline, then `shutdownNow()` + 2 s), `upstream/UpstreamConfiguration.java` (`@Conditional` shared/per-request beans reading `lab.connection-mode` via `Binder`), new `upstream/ConnectionModeMetrics.java` (gauge `connection_mode{mode}`), `SharedChannelProvider` (startup INFO line), `ChannelProvider` (javadoc), `application.yaml` (comment).
Tests: new `testsupport/CountingChannelFactory`, `InProcessSimulatorConfig` (additive wrap), `StreamTestSupport` (additive overloads), `StreamContractTest` (additive overload + `contractHoldsInEveryConnectionMode`, 12 cases), new `upstream/PerRequestChannelProviderTest` (8 unit + 3 races x 400 iterations), new `ConnectionModeEndToEndTest` (13 cases after the first-review fixes, 11 at first), `ConnectionModeConfigTest` (replaced `perRequestModeFailsAtStartup`).
Docs: README ("Connection modes", "Not measured yet", settings/metrics rows, known limits), new `docs/adr/0003-per-request-connection-mode.md`, `docs/failure-analysis.md` note.
Unchanged: `StreamBridge`, `StreamController`, `ActiveStreamRegistry`, `RetryPolicy`, proto, simulator.

## Acceptance mapping
AC1-AC9 met per implementer evidence (details below); AC10 met (`git diff --stat -- proto simulator gateway/src/main/java/.../gateway/stream` empty; listed test files unchanged; no `Thread.sleep`); AC11 met (3 green full builds by the implementer, one more clean build by the coordinator); AC12 not triggered.
- Overflow and the retry-then-end path are covered by source inspection and the resume E2E only; there is no dedicated per-terminal-path channel test for overflow, timeout mid-stream or upstream error with retries (the contract mode test covers completion, upstream error, client gone, shutdown cancel, first-chunk timeout, slow consumer).

## Checks
- Implementer: focused command x3 (81 tests, exit 0); `./mvnw -B clean verify` then `./mvnw -B verify` twice, all exit 0, gateway 231 tests.
- Coordinator: `feature_state.py check` approval_matches true; independent `./mvnw -B clean verify` exit 0, gateway 231/231 (ConnectionModeConfigTest 31 in 6.2 s, ConnectionModeEndToEndTest 11 in 8.5 s, StreamContractTest 28 in 23.7 s, PerRequestChannelProviderTest 11 in 0.4 s); diffs and grep as in AC10.
- Not run: a review (follows).

## Deviation
- An empty `lab.connection-mode=` does not fail at startup: Boot treats it as unset, `@DefaultValue("shared")` applies and the condition reads the same value. The plan expected a failure. The test asserted the fallback and the README said an empty value counts as unset (closed later: see the section "Fail-fast for an empty mode" at the end). `pooled` fails with `lab.connection-mode` in the stack trace.
- The Binder-based condition worked in Boot 4; the single-bean fallback was not needed.

## Limitations
- In-process channels only: Netty/TCP/HTTP2, connect backoff, TLS and performance are untested (README "Not measured yet").
- A stop racing a `channel()` call may leave a new channel out of the set `stop()` waits on; the lease still shuts it down (ADR-0003).
- Per-request readiness reflects the lifecycle only (S1); no strict thread assert (S2).
- Startup INFO lines are not asserted by any test.

## First-review fixes (after review pass 1; coordinator)
- P2-1: `PerRequestLease.channel()` re-reads the state after registering in the `open` set; if `close()` won between the CAS and the add, the call removes the entry itself and throws, so a closed lease never stays in `open` (`openChannels()` / `isTerminated()` contract, and the race test's `openChannels() == 0`). ADR-0003 states the window.
- P3-2: new E2E test `waitingRequestOpensItsChannelOnlyAfterAdmission` (both modes, `overload-policy=wait`): while a request waits, `created == 1`; after the holder disconnects it completes and per-request shows `created == 2`, each channel shut down once. (The first version used a hanging holder, whose disconnect is not noticed without a write, and ran 30 s per case; it now uses a long stream like `OverloadPolicyTest`; class time 12 s.) The test file now has 13 cases.
- P3-4: README and ADR-0003 say that six simulator scenarios run in both modes (the other contract and race tests do not go through a provider); `ChannelProvider.acquire()` javadoc fixed.
- P3-1 (empty `lab.connection-mode=` falls back to shared instead of failing, a deviation from plan AC1) is left as is and reported to the user; the null check in `GatewayProperties` is still reachable only by programmatic construction.
- P3-3 (overflow, mid-stream idle timeout, retry-exhausted and failed-non-last-write paths are not run with a per-request lease): accepted gap, all of them go through the same `releaseCapacity()` -> `lease.close()` call, which the lease unit tests and race tests cover; RELEASE_ONCE already measures these paths in the existing tests.

## Second-review fixes (after review pass 2; coordinator)
- P3-A: the new E2E test now waits on the admission queue gauge only (`queue_depth{queue=admission} == 1`), not the sum with the `chunks` gauge.
- P3-B: README module table mentions the selectable connection modes.
- P3-C: this file's counts updated. Checks after the first-review fixes: clean `./mvnw -B clean verify` exit 0, gateway 233/233, `ConnectionModeEndToEndTest` 13 cases about 12 s; the final full build after the second-review fixes is recorded below.
- Final full build after the second-review fixes: `./mvnw -B clean verify` exit 0, gateway 233/233 (a first attempt failed in the clean phase because `simulator/target` could not be deleted, a transient file lock; the immediate retry was green).

## Fail-fast for an empty mode (after review PASS; user decision, fast path by the coordinator)
- The user chose fail-fast for the deviation above. `UpstreamConfiguration.configuredMode` now throws `IllegalStateException("lab.connection-mode must be shared or per-request, but it is empty")` when the raw property is present but blank (an unset property still uses the default `shared`). `ConnectionModeConfigTest.emptyConnectionModeFailsAtStartup` replaces the fallback test (empty and whitespace-only values); README says an invalid or empty value stops the application. Plan AC1 is now met without deviation. The `GatewayProperties` null check stays for programmatic construction.
