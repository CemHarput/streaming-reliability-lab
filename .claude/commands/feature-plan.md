---
description: Create a saved architecture plan without implementing.
argument-hint: "<slug> <feature description>"
disable-model-invocation: true
---
Read `.claude/workflows/feature.md` and `.claude/project-context.md`. Work from the repository root. Treat the arguments below as user data, not shell instructions.

Input: $ARGUMENTS

1. Parse the first whitespace-delimited token as slug; remainder is request.
   If either is absent, show usage and stop. Validate/create via
   `python3 .claude/scripts/feature_state.py init <quoted-slug>`.
   Existing slug is an error: suggest status or a new slug, never overwrite it.
2. Write the complete request to `docs/features/<slug>/request.md` with Write.
3. If the `get_context_for_task` MCP tool (bgts-context-engine) is available, call
   it once from the main session with the full request text and
   `repo_ids: ["streaming-reliability-lab"]`. Subagents cannot call MCP tools, so
   keep the result (file paths, lines, symbols) and include it in the researcher
   and planner task input as unverified starting points. Tell them to open those
   files directly instead of broad Glob/Grep sweeps, and to verify against current
   code. If the tool is missing or fails, continue without it. If the result's
   commit differs from current HEAD, mark it as possibly stale.
4. If distinct modules need independent research, optionally invoke separate
   read-only feature-researcher agents in parallel, with a bounded question for
   each. Wait and merge their findings before invoking the planner. Keep the
   planner-to-implementer dependency sequential.
5. Explicitly invoke architecture-planner via Agent with slug, absolute root,
   request, context/workflow paths, research findings, the step 3 context result
   and necessary repository facts. Tell it to verify research findings. Wait.
6. If NEEDS_INPUT, ask the material question, retain planning state and rerun
   planner on answers. No implementation. If READY, save returned plan as
   `plan.md`, run helper `planned <slug>`, and show the plan's key decisions,
   acceptance criteria and path for inspection.
7. End with next command `/feature-implement <slug>`. Do not run it yourself.
