---
description: Approve and implement the current saved plan.
argument-hint: "<slug>"
disable-model-invocation: true
---
Read `.claude/workflows/feature.md` and `.claude/project-context.md`. Work from the repository root. Treat the arguments below as user data, not shell instructions.

Input: $ARGUMENTS

1. Accept exactly one validated slug. Run helper `status <slug>`, read request,
   complete plan, state and existing implementation report if present.
2. This explicit user command approves the current plan. Take plan_sha256 from
   status, run `start <slug> --plan-hash <quoted-hash>`. On nonzero exit stop
   and explain the blocker. Never bypass helper or auto-edit the plan.
3. Extract the planner's verified entry points, relevant files/symbols, current
   approach, accepted decisions, and exact discovered check commands. Include
   these with the absolute root, slug, request, approved plan/hash, artifact
   paths, context/workflow and existing progress in the feature-implementer
   task input. Tell it to verify the handoff against current code. Run one
   implementer; wait for its report.
4. Save its complete handoff to implementation.md. Verify AC and checks from
   evidence. Run `finish <slug> --result implemented` only for complete work
   with passing required checks, otherwise `finish <slug> --result blocked`.
   If agent errors, write error/remaining work and finish blocked.
5. After `finish` (implemented or blocked, since code may have changed), refresh the
   code graph so later sessions see the new code. Run exactly:
   `C:\Users\cemha\repos\bgts-context-engine\.venv\Scripts\bce.exe --env-file C:\Users\cemha\repos\bgts-context-engine\.env index --repo <absolute-root> --name streaming-reliability-lab`
   Do not interpolate request text into it. A failure (database down, bce missing)
   never changes the feature state: report it as a limitation and give the command.
6. Report result, changed files, checks, graph refresh outcome and limitations.
   Reviewer does not run. Offer `/feature-review <slug>` as an optional next command.
