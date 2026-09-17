# Engine operations on the public WorkflowRuntime SPI

## Goal

Move all engine operations from the `private[atomicflow]` trait `WorkflowExecution` onto the
public `WorkflowRuntime` trait, accessed through an opaque, path-dependent type member.
`WorkflowExecution` is deleted. `WorkflowContext` exposes the runtime handle directly.

## Approved decisions

1. **`WorkflowRuntime` gains `type CurrentExecution`** — unbounded, opaque. Contract points the
   scaladoc must state (wording up to the implementer): the runtime creates the handle at run
   start; its identity is stable for the run's lifetime; its contents are owned and updatable by
   the runtime (e.g. lease protocols that rotate fencing values); it may be shared across
   parallel-branch threads, so internal mutation must be thread-safe; it should carry only what
   the runtime cannot derive from its own configuration.
2. **~35 engine ops become public methods on `WorkflowRuntime`**, each taking
   `run: CurrentExecution` as first parameter. This includes the four existing
   `private[atomicflow]` members (`upsertWakeup`, `readStep`, `startChild`,
   `getWorkflowInstanceInfo`) which become public.
3. **`startChild`**: the `(parentId, parentGeneration)` parameters are replaced by
   `run: CurrentExecution`; `enclosingScopePath` and all other parameters stay.
4. **`renewLease(run)`**, **`throwIfCancelled(run, uncancellableDepth: Int)`**,
   **`def durableRetryThreshold: FiniteDuration`** (public). `now` is NOT moved — call sites use
   `runtime.clock.instant()`.
5. **Types** currently `private[atomicflow]` in `WorkflowExecution.scala` become public types in
   package `atomicflow`: `StoredStep`, `AwaitSignalCandidate`, `AwaitTimerCandidate`,
   `UpdateCandidate`, `AwaitUpdateDecision`, `AwaitRaceLeaf` (+ 3 leaf types),
   `AwaitRaceCandidate`, `AwaitRaceDecision`. File organization is the implementer's choice.
6. **`WorkflowContext`**: `val runtime: WorkflowRuntime` (val, not def — a def cannot prefix a
   dependent type), `def currentExecution: runtime.CurrentExecution`; the `execution` member is
   removed. `DerivedWorkflowContext` delegates both and keeps the lexical-state members.
7. **Postgres**: the handle class (today `PostgresExecution`) becomes public and opaque, keeps
   `workerId`, `fencingToken`, `generation`, and the instance identity (workflowId, key, scope);
   drops `runLeaseDuration` (the runtime reads its own `leaseDuration` setting), `now`, and
   `durableRetryThreshold` (runtime fields). It no longer extends any trait. The runtime refines
   `type CurrentExecution` and implements all ops. Context construction sets
   `val runtime = this` and `def currentExecution`.
8. **Core call sites** (Workflow.scala ×3, Step.scala ×12): `val rt = ctx.runtime`;
   `val run = ctx.currentExecution`; `rt.op(run, ...)`; `execution.now` → `rt.clock.instant()`;
   `startAsChild` passes `ctx.currentExecution` (its `ctx.execution.generation` read disappears).
9. **Docs**: spec edits in `running-workflows.md` (trait framing, ~lines 70 and 305) and
   `core-types.md` (WorkflowContext shape, ~lines 52-56); DEVIATIONS.md entry (numbered, house
   style: what / rationale / consequences / benefit) covering the public SPI decision, the
   visibility flips, and the `startChild` signature change.

## Constraints

- **Wording ban** in all written content (code comments, scaladoc, docs, commits, replies): never
  use "seam", "load bearing", "vestigial", or "minted".
- Comment/doc wording is the implementer's choice; the plan pins only the required content
  points.

## Tasks

- **Task 1 — the move (core + db, one compile unit)**: all of decisions 1-8; delete
  `core/src/main/scala/atomicflow/internal/WorkflowExecution.scala`; ends with full
  `sbt test` green and `grep -rn WorkflowExecution core/src/main db/src/main` returning nothing.
  Compile risk to verify first with a tiny spike: the `DerivedWorkflowContext` delegation of
  `currentExecution` (`= underlying.currentExecution` under singleton widening); if the compiler
  rejects it, a single localized cast is the accepted fallback.
- **Task 2 — docs**: decision 9 only.

## Environment (from previous sessions' ledgers)

- Every sbt invocation needs
  `--java-home /opt/homebrew/opt/openjdk/libexec/openjdk.jdk/Contents/Home` (PATH java is 11).
- munit `-z` test filters do not work through this sbt setup — run whole suites.
- Docker must be running (testcontainers).
- No scalafmt plugin; format manually, maxColumn 120.
- Do not touch the commented-out `core/src/main/scala/atomicflow/impl/memory/InMemoryWorkflowRuntime.scala`.
- Slash syntax for sbt axes (`core/compile`, `db/Test/compile`).
