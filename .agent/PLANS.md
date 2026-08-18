# ExecPlan format

An ExecPlan is a living, self-contained execution record for work that spans several
files, subsystems, or sessions. Store active plans in `.agent/execplans/`. A reader
with only a clean checkout and the plan must be able to understand the purpose,
continue safely, verify completed claims, and recover from an interrupted step.

Update the plan whenever a milestone completes, a decision changes, a material fact
is discovered, a benchmark is run, or verification evidence changes. Use ISO 8601 UTC
timestamps. Never claim a live check, test, measurement, backup, restore, migration,
or clean-environment result that was not actually performed.

## Required sections

1. **Purpose and outcome** — the user-visible result, non-goals, and safety/licensing
   boundaries.
2. **Current status** — one timestamped statement naming the active milestone and
   blockers.
3. **Milestones** — ordered observable outcomes marked `[ ]`, `[~]`, or `[x]` for
   pending, active, or complete. A created file is not an outcome by itself.
4. **Decisions** — dated decision, rationale, considered alternatives, and the ADR or
   evidence that owns the durable explanation. Preserve superseded decisions.
5. **Discoveries and risks** — observed facts, surprises, external blockers, and
   implications. Separate inference from evidence.
6. **Benchmark results** — exact command, profile, hardware/software, dataset, runtime,
   peak memory, rate/latency, plans, result artifact, and limitations. Write `not
   measured` until evidence exists.
7. **Verification evidence** — exact command, environment, timestamp, pass/fail/skip
   counts, and relevant hashes/revisions. A skipped required integration is not a pass.
8. **Recovery instructions** — safe checkpoint, persistent services/data, temporary
   resources, and precise resume/cleanup/replay procedure.
9. **Remaining work** — concrete work required by the definition of done, including
   approvals or external state.

## Working rules

- Read the plan before substantial work and update it before handing the task to
  another agent/session.
- Keep canonical user/operator instructions under `docs/`; the plan records progress
  and evidence, not a second manual.
- Use append-only dated evidence when history matters. Correct a false statement
  explicitly instead of silently preserving a green claim.
- Link local repository artifacts with relative paths and external evidence with exact
  URLs/versions. Do not include credentials, signed URLs, dumps, or personal data.
- Record destructive targets and safety checks before migrations, cleanup, restore, or
  scale benchmark work. Include recovery steps before beginning the risky action.
- Mark a milestone complete only when its behavior and required documentation have
  been verified. `TODO`, stub, mock-only, fixture-only, or an unrun command is not
  completion.

## Minimal template

```markdown
# <Outcome-oriented title>

## Purpose and outcome

<Scope, non-goals, invariants.>

## Current status

<UTC timestamp> — <active work and blockers>.

## Milestones

- [x] <verified behavior>
- [~] <active behavior-level milestone>
- [ ] <remaining behavior-level milestone>

## Decisions

- <date> — <decision, rationale, alternatives, durable reference>.

## Discoveries and risks

- <fact/inference, evidence, implication>.

## Benchmark results

Not measured.

## Verification evidence

- <timestamp> — `<exact command>` — <environment and exact result>.

## Recovery instructions

<Safe restart and cleanup procedure.>

## Remaining work

- <Unmet definition-of-done item>.
```
