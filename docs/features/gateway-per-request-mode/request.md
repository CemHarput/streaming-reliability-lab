# Request: gateway-per-request-mode

Goal
Add a per-request connection mode to the gateway next to the existing shared channel mode, selectable by configuration, so that a later load test can compare the two. In per-request mode each stream opens its own gRPC channel to the simulator and closes it when the stream ends for any reason.

Context
- The gateway currently uses one shared gRPC channel (feature gateway-shared-channel). Planner: verify how the channel is created, owned and shut down (config properties, upstream starter, shutdown drain) and where a per-request channel fits without duplicating the stream logic in StreamBridge.
- Default must stay the current shared mode. The mode is a GatewayProperties setting, validated at startup (invalid value fails fast).
- Every terminal path (completion, upstream error, client gone, timeout, overflow, shutdown cancel, retry/resume attempt) must close the channel it opened exactly once, with no leak and no close of the shared channel. Planner: decide whether a retry attempt reuses the request's channel or opens a new one, and state why.
- Channel creation cost must not run on a path that holds sendLock or the registry lock. Planner: verify.

Acceptance criteria
- Both modes pass the same stream contract tests (StreamContract clauses: single terminal, release once, upstream cancelled, no timer leak, queue bounded) with REAL and a real in-process simulator; channels closed is an additional observable (a counting factory or equivalent), exactly once per stream in per-request mode, never in shared mode.
- Per-request mode: simulator active_generations returns to 0 after every scenario; no thread or channel left after shutdown.
- Metrics or logs distinguish the mode (one low-cardinality tag or startup log line); no per-request ids in metrics.
- README: short section on the two modes and what is NOT measured yet (no performance claim in this feature).
- No Thread.sleep, bounded waits, @Timeout(SEPARATE_THREAD); full ./mvnw -B verify green three times.

Out of scope
- loadgen, benchmark protocol, benchmark results, any performance claim.
- Changing the stream contract, retry policy, or the simulator and proto.
- MQTT, showcase.

Constraints
- Work on main, no new branch, no commits or pushes.
