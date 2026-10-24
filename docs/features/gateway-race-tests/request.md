# Request: gateway-race-tests

Goal
Extend the REAL-side race stress of TerminalRaceContractTest to the other terminal-path pairs of StreamBridge and to the permit admission versus shutdown race in ActiveStreamRegistry. Every iteration asserts the stream contract (StreamContract.check, expected set empty for REAL), never who wins.

Context
- Existing: TerminalRaceContractTest races completion vs shutdown cancel and completion vs client gone (200 iterations each, CyclicBarrier plus Thread.onSpinWait skew, no sleep). ADR-0002 says the guarantee comes from the CAS in StreamBridge.claim(); stress cannot prove absence of a race.
- Reuse the existing run(...) skeleton, StreamUnderTest(Kind.REAL), StreamTestSupport (CountingLease, FakeUpstreamStarter, RecordingEmitter, CountingRegistry) and StreamContract. Add Scenario values only if the existing shapes (EXACTLY_ONE_ANY, AT_MOST_ONE) are not enough.

Pairs in scope (planner: verify each has a real race window in the code and drop any that do not)
1. timedOut vs completion; timedOut vs client gone.
2. upstream error (handleUpstreamEnd) vs client gone; upstream error vs shutdown cancel.
3. overflow vs client gone; overflow vs shutdown cancel.
4. ActiveStreamRegistry: admit/register vs stop() (a permit admitted while shutdown is draining must be released exactly once and the stream must end with exactly one terminal event or be refused cleanly).

Acceptance criteria
- Per iteration: at most/exactly one terminal event as the pair allows, nothing after it, lease closed once, permit released once, permits held 0, pendingTimers 0, upstream cancelled where the stream did not end by itself.
- Both outcomes of each pair observed in local runs where the pair can end two ways; distribution is logged, not asserted, and documented as machine dependent.
- No Thread.sleep, no unbounded waits, @Timeout(SEPARATE_THREAD), all barrier/latch/future waits bounded, a harness failure is reported as a harness error.
- Total added test time budget: about 30 s. No flakiness in three consecutive full builds.
- Docs: extend docs/failure-analysis.md (what the new stress shows and does not show), no sentence that contradicts the code; README test section updated briefly.

Out of scope
- Retry/resume attempt vs cancel (separate feature: needs a deterministic upstream hook).
- Any change to gateway/src/main, proto or simulator. If a test finds a real defect: stop, report it as a finding with evidence, do not fix it inside this feature.
- Adding flaws to LEAKY or changing the contract clauses.
- Per-request mode, loadgen, MQTT.

Constraints
- Work on main, no new branch, no commits or pushes.
