# Context-Carried Scope & Uncancellable State Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Move the transient `Workflow.scoped` path and `Workflow.uncancellable` depth out of `PostgresExecution`'s ThreadLocals onto the immutable `WorkflowContext`, passed to region bodies via derived copies (the `local` of a reader monad), so the state travels with the context value instead of the executing thread.

**Architecture:** `WorkflowContext` gains two `private[atomicflow]` members (`scopePath: Vector[String]`, `uncancellableDepth: Int`) with defaults, a concrete `currentScope` string view, and a concrete `derive(...)` method implemented by a private wrapper class. Every region function (`Workflow.scoped`, `Workflow.uncancellable`, `Workflow.restartable`/`loop`, `Workflow.parallel`, `Step.firstToRunWithoutSuspension`) applies its body with a derived context; bodies/branches become context functions (`WorkflowContext ?=> R`) resolved at application time. `WorkflowExecution` loses all state-carrying members (`currentScope`, `pushScope`/`popScope`, `snapshotBranchContext`/`restoreBranchContext`, `enterUncancellable`/`exitUncancellable`, `BranchContextSnapshot`); `throwIfCancelled` takes the depth as a parameter. `PostgresExecution` loses its two ThreadLocals entirely.

**Tech Stack:** Scala 3.7.3, direct style, com.softwaremill.ox 0.7.0, doobie, sbt. No new dependencies.

**Spec:** `spec/` (all files; the design intent is the reader-monad context passing described by the project owner). Deviations are recorded in `DEVIATIONS.md` per project practice.

## Global Constraints

- No `ThreadLocal` anywhere after this change (`grep -rn ThreadLocal core db` must return nothing in main sources).
- The state lives on `WorkflowContext`, NOT on `WorkflowExecution`. Do not add scope/depth fields or state to `WorkflowExecution` or `PostgresExecution`.
- New `WorkflowContext` members are `private[atomicflow]`; no public API additions.
- Behavior must be identical for all sanctioned paths: identical `StepId`s, identical scope strings, identical DB rows. The 24 existing db test suites must pass unchanged (modulo the mechanical `() =>` removal at branch call sites required by the new branch types).
- Branch/bodies of `Workflow.parallel`, `Step.firstToRunWithoutSuspension`, `Workflow.restartable`, and `Workflow.loop` become context functions; `Workflow.scoped`/`uncancellable`/`runToSuspension` signatures are unchanged (already context functions).
- The working tree contains a pre-existing uncommitted rename `checkCancellation` → `throwIfCancelled` (Step.scala, WorkflowExecution.scala, PostgresWorkflowRuntime.scala). It will be committed as a standalone commit before Task 1; build on it, do not revert it.
- Build/test commands (from repo root; Docker must be running for testcontainers): `sbt "core/compile"`, `sbt "db/compile"`, `sbt "example/compile"`, `sbt "db/testOnly test.ScopedParallelSuite"`, `sbt test`. Sbt uses slash syntax for project axes (`core/compile`, NOT `core:compile`).
- Formatting follows `.scalafmt.conf` (maxColumn 120, no scalafmt sbt plugin — format by hand in the existing style).
- Do not touch the commented-out `core/src/main/scala/atomicflow/impl/memory/InMemoryWorkflowRuntime.scala` (kept for reference per DEVIATIONS.md entry 1).
- Do not modify anything under `spec/`. Record the signature changes in `DEVIATIONS.md` (Task 3).

---

### Task 1: Red regression tests for context-carried state

**Files:**
- Modify: `db/src/test/scala/test/ScopedParallelSuite.scala` (append two tests)

**Interfaces:**
- Consumes: current public API (`Workflow.scoped`, `Workflow.uncancellable`, `Step.atLeastOnce`, `Step.await`, `Signal`, `rt.cancel`, `rt.createWorkflowInstance`, `rt.runWorkflowInstance`) — no new interfaces.
- Produces: two tests that FAIL against the current ThreadLocal implementation and PASS after Task 2. Both use the context-function type `WorkflowContext ?=> R` (already valid against current code — the deferral mechanism is the same one `Workflow.scoped` bodies use) and apply a captured context on a foreign `java.lang.Thread`.

- [ ] **Step 1: Add the two tests to `ScopedParallelSuite`**

Append inside `class ScopedParallelSuite extends PostgresWorkflowRuntimeSuite { ... }` (the file already imports `atomicflow.*`, `java.util.concurrent.atomic.AtomicInteger`, and has `given Cacheable[Throwable]` plus the `stepScopePaths` helper):

```scala
  test("a step applied from a captured context on a foreign thread records the enclosing scope") {
    val rt = newRuntime
    val counter = new AtomicInteger(0)
    val wf = Workflow[String, String](id = "foreign-thread-scope") { in =>
      Workflow.scoped("outer") {
        val body: WorkflowContext ?=> String =
          Step.atLeastOnce[String]("foreign") { counter.incrementAndGet(); "v" }
        val ctx = summon[WorkflowContext]
        val thread = new Thread(() => body(using ctx))
        thread.start()
        thread.join()
      }
      "done"
    }
    rt.createWorkflowInstance(wf, "k", "in")
    assertEquals(rt.runWorkflowInstance(wf, WorkflowInstanceId(wf.id, "k")), WorkflowRunResult.Result("done"))
    assertEquals(counter.get(), 1, "the step body executes exactly once")
    assertEquals(
      stepScopePaths(wf.id, "k"),
      Vector("outer"),
      "the step row lands under the enclosing scope carried by the context, not the foreign thread's empty scope"
    )
  }

  test("uncancellable suppression travels with the context onto a foreign thread") {
    val rt = newRuntime
    val sig = Signal[String]("gate")
    val counter = new AtomicInteger(0)
    val wf = Workflow[String, String](id = "foreign-thread-uncancellable") { in =>
      Workflow.uncancellable {
        Step.await[String]("gate", Awaitable.SignalEvent(sig))
        val body: WorkflowContext ?=> String =
          Step.atLeastOnce[String]("compensate") { counter.incrementAndGet(); "ok" }
        val ctx = summon[WorkflowContext]
        val thread = new Thread(() => try body(using ctx) catch { case _: WorkflowCancelledException => () })
        thread.start()
        thread.join()
      }
      Step.atLeastOnce[Int]("after") { 2 }
      "finished"
    }
    val id = rt.createWorkflowInstance(wf, "k", "in").id
    assertEquals(rt.runWorkflowInstance(wf, id), WorkflowRunResult.WorkflowSuspended)
    rt.cancel(id)
    sig.send(id, "go")(using rt)
    assertEquals(rt.runWorkflowInstance(wf, id), WorkflowRunResult.WorkflowCancelled)
    assertEquals(
      counter.get(),
      1,
      "the compensation step executes despite the pending cancel: the uncancellable depth travels with the context"
    )
  }
```

- [ ] **Step 2: Run them and verify both FAIL (red)**

Run: `sbt "db/testOnly test.ScopedParallelSuite -- -z foreign thread"`
Expected: 2 failures. Test 1 fails at `stepScopePaths` (row under scope `""` instead of `"outer"` — the foreign thread's ThreadLocal is empty). Test 2 fails at `counter.get()` (cancellation is delivered on the foreign thread, depth 0 there, so the body never runs; `WorkflowCancelledException` is caught by the try in the Runnable). The run results themselves are already as asserted in both.

- [ ] **Step 3: Commit**

```bash
git add db/src/test/scala/test/ScopedParallelSuite.scala
git commit -m "Add red regression tests: scope and uncancellable depth must travel with the context"
```

---

### Task 2: The refactor — state moves from ThreadLocals to derived WorkflowContexts

**Files:**
- Modify: `core/src/main/scala/atomicflow/WorkflowContext.scala` (rewrite; 26 lines → ~80)
- Modify: `core/src/main/scala/atomicflow/internal/WorkflowExecution.scala` (remove `BranchContextSnapshot` + 7 members; change `throwIfCancelled`)
- Modify: `core/src/main/scala/atomicflow/Workflow.scala` (`startAsChild`, `restartable`, `loop`, `regionLoop`, `uncancellable`, `withScope`, `parallel`, `runBranch`)
- Modify: `core/src/main/scala/atomicflow/Step.scala` (`runStep`, `getExecutionState`, `firstToRunWithoutSuspension` overloads, `firstToRun0`, `awaitSignal`, `awaitUpdate0`, `awaitTimer`, `awaitRace0`)
- Modify: `db/src/main/scala/atomicflow/impl/db/PostgresWorkflowRuntime.scala` (`PostgresExecution`)
- Modify: `example/src/main/scala/example/DocumentProcessingAtomicflow.scala` (one `Workflow.parallel` call site)
- Modify: all db test files that fail `db/Test/compile` with `() => R` vs `WorkflowContext ?=> R` mismatches (expected: `ScopedParallelSuite`, `FirstToRunSuite`, possibly `ChildrenSuite`, `TerminateSuite`, `RestartableSuite`)

**Interfaces:**
- Consumes: nothing from Task 1 (the tests only assert behavior).
- Produces (for tests and any future runtime):
  - `WorkflowContext`: `private[atomicflow] def scopePath: Vector[String] = Vector.empty`, `private[atomicflow] def uncancellableDepth: Int = 0`, `private[atomicflow] def currentScope: String = scopePath.mkString("/")`, `private[atomicflow] def derive(scopePath: Vector[String] = scopePath, uncancellableDepth: Int = uncancellableDepth): WorkflowContext`
  - `WorkflowExecution.throwIfCancelled(uncancellableDepth: Int): Unit`
  - `Workflow.parallel[R](branches: Seq[WorkflowContext ?=> R])(using ctx: WorkflowContext): Seq[R]` (+ vararg `(WorkflowContext ?=> R)*`)
  - `Step.firstToRunWithoutSuspension[R: Cacheable](stepId, invalidateOn, ensureUnchanged)(branches: Seq[WorkflowContext ?=> R])(using ctx, throwableCodec): R` (+ vararg)
  - `Workflow.restartable[S: Cacheable, R](id, initialState)(body: (S, RestartableScope[S]) => WorkflowContext ?=> R)(using ctx): R`
  - `Workflow.loop[S: Cacheable, R](id, initialState)(body: (S, LoopScope[R]) => WorkflowContext ?=> S)(using ctx): R`

- [ ] **Step 1: Rewrite `WorkflowContext.scala`**

Replace the entire file content with:

```scala
package atomicflow

import atomicflow.internal.WorkflowExecution

/** The runtime context materialized only during workflow execution. It carries
  * the instance identity, the version of the definition captured at creation, the
  * runtime services, and (via [[execution]]) the per-run engine seam.
  *
  * A new context is materialized for every run and is not retained between runs.
  * The context is immutable: the transient lexical state of the run — the
  * enclosing `Workflow.scoped` path ([[scopePath]]) and the `Workflow.uncancellable`
  * depth ([[uncancellableDepth]]) — lives on the context, and every region
  * function (`Workflow.scoped`, `Workflow.uncancellable`, `Workflow.restartable` /
  * `Workflow.loop`, `Workflow.parallel`, `Step.firstToRunWithoutSuspension`)
  * passes a derived copy to its body (the `local` of a reader monad). The state
  * therefore travels with the context value, not with the executing thread.
  */
trait WorkflowContext {
  def instanceId: WorkflowInstanceId

  /** The `Workflow.version` recorded when the instance was created. The current
    * body may branch on it internally to adapt to the definition version that
    * created the instance (see `spec/workflow-evolution.md`).
    */
  def versionAtCreation: Long

  def runtime: WorkflowRuntime

  /** The per-run engine seam: fencing identity and, in later tasks, the services
    * a running body needs from the engine.
    */
  private[atomicflow] def execution: WorkflowExecution

  /** The path of enclosing `Workflow.scoped` (and region / parallel-branch)
    * segments at this context's lexical position; `Vector.empty` at the top
    * level. Transient per-run state carried immutably through derived
    * contexts; never persisted directly. The exact contents are an
    * implementation detail of the runtime and are not portable across runtime
    * implementations — [[currentScope]] is the composed string the durable
    * records key on.
    */
  private[atomicflow] def scopePath: Vector[String] = Vector.empty

  /** The depth of enclosing `Workflow.uncancellable` regions at this context's
    * lexical position; `0` outside any region. Transient per-run state carried
    * immutably through derived contexts; a non-zero depth suppresses
    * cancellation delivery at checkpoints (`WorkflowExecution.throwIfCancelled`).
    */
  private[atomicflow] def uncancellableDepth: Int = 0

  /** The current step scope path: [[scopePath]]'s segments joined by `/` (`""`
    * at the top level). Step/Await IDs and derived child scopes key on this
    * string.
    */
  private[atomicflow] def currentScope: String = scopePath.mkString("/")

  /** Derives a copy of this context carrying the given transient lexical state,
    * leaving everything else identical — the `local` of a reader monad. Region
    * functions call this and pass the derived context to their body.
    */
  private[atomicflow] def derive(
      scopePath: Vector[String] = scopePath,
      uncancellableDepth: Int = uncancellableDepth
  ): WorkflowContext =
    new DerivedWorkflowContext(this, scopePath, uncancellableDepth)
}

/** The [[WorkflowContext.derive]] implementation: an immutable wrapper that
  * overrides exactly the two transient state members and delegates everything
  * else to the underlying context.
  */
private[atomicflow] final class DerivedWorkflowContext(
    underlying: WorkflowContext,
    override val scopePath: Vector[String],
    override val uncancellableDepth: Int
) extends WorkflowContext {
  def instanceId: WorkflowInstanceId = underlying.instanceId
  def versionAtCreation: Long = underlying.versionAtCreation
  def runtime: WorkflowRuntime = underlying.runtime
  private[atomicflow] def execution: WorkflowExecution = underlying.execution
}
```

- [ ] **Step 2: Update `WorkflowExecution.scala`**

In `core/src/main/scala/atomicflow/internal/WorkflowExecution.scala`:
1. Delete the `BranchContextSnapshot` case class (lines ~110-117) entirely.
2. Delete the members `currentScope`, `pushScope`, `popScope`, `snapshotBranchContext`, `restoreBranchContext`, `enterUncancellable`, `exitUncancellable` from the trait (lines ~137-205 region), together with their doc comments.
3. Replace `throwIfCancelled` with:

```scala
  /** A cancellation checkpoint: re-reads the durable `cancel_requested_at` flag
    * and throws [[atomicflow.WorkflowCancelledException]] when set, unless the
    * `Workflow.uncancellable` depth of the current call site (the
    * `uncancellableDepth` carried by the `WorkflowContext` there) is non-zero.
    * Called right before any new work (a Step body about to execute, or an await
    * about to be evaluated); cached replays never call it, so they never
    * deliver.
    */
  def throwIfCancelled(uncancellableDepth: Int): Unit
```

- [ ] **Step 3: Update `Workflow.scala`**

1. `startAsChild` (line ~161): change `ctx.execution.currentScope` to `ctx.currentScope`.
2. `restartable` (~277-282) — the body parameter gains the context-function result type (call sites are textually unchanged):

```scala
  def restartable[S: Cacheable, R](id: String, initialState: => S)(
      body: (S, RestartableScope[S]) => WorkflowContext ?=> R
  )(using
      ctx: WorkflowContext
  ): R =
    regionLoop(id, initialState) { (state, scope) =>
      body(state, scope)
    }
```

3. `loop` (~293-303):

```scala
  def loop[S: Cacheable, R](id: String, initialState: => S)(
      body: (S, LoopScope[R]) => WorkflowContext ?=> S
  )(using
      ctx: WorkflowContext
  ): R =
    regionLoop(id, initialState) { (state, scope) =>
      val loopScope = new LoopScope[R] {
        override def restartCount: Long = scope.restartCount
        override def break(result: R): Nothing = throw new RegionBreakException(result)
      }
      val next = body(state, loopScope)(using summon[WorkflowContext])
      scope.restart(next)
    }
```

(The `summon[WorkflowContext]` resolves to the region context `regionLoop` applies the lambda with — the lambda's body is typed as a `WorkflowContext ?=> R`.)

4. `regionLoop` (~315-362) — replace entirely with (no push/pop, no try/finally; each body application gets its own derived context):

```scala
  private def regionLoop[S: Cacheable, R](id: String, initialState: => S)(
      body: (S, RestartableScope[S]) => WorkflowContext ?=> R
  )(using
      ctx: WorkflowContext
  ): R = {
    val execution = ctx.execution
    val cacheable = summon[Cacheable[S]]
    val parentScopePath = ctx.scopePath
    val parentScope = ctx.currentScope
    val row = execution.readRegionState(id, parentScope)
    val startCount = row.map(_._2).getOrElse(0L)
    val firstState: S = row match {
      case Some((state, _)) => cacheable.read(state)
      case None =>
        val seed = initialState
        execution.createRegion(id, parentScope, cacheable.write(seed))
        seed
    }

    var state: S = firstState
    var count: Long = startCount
    var result: R = null.asInstanceOf[R]
    var done = false
    while (!done) {
      val regionCtx = ctx.derive(scopePath = parentScopePath :+ regionSegment(id, count))
      val scope = new RestartableScope[S] {
        override def restartCount: Long = count
        override def restart(nextState: S): Nothing =
          throw new RegionRestartException(cacheable.write(nextState))
      }
      try {
        result = body(state, scope)(using regionCtx)
        done = true
      } catch {
        case e: RegionRestartException =>
          execution.restartRegion(id, parentScope, count, e.serializedState)
          count += 1
          state = cacheable.read(e.serializedState)
        case e: RegionBreakException[R] =>
          result = e.result
          done = true
      }
    }
    result
  }
```

5. `uncancellable` (~376-380) — in its doc comment replace "It is per-execution transient state" with "It is transient per-run state carried by the context"; implementation:

```scala
  def uncancellable[R](f: WorkflowContext ?=> R)(using ctx: WorkflowContext): R =
    f(using ctx.derive(uncancellableDepth = ctx.uncancellableDepth + 1))
```

6. `withScope` (~400-405):

```scala
  private def withScope[R](escapedSegment: String)(body: WorkflowContext ?=> R)(using ctx: WorkflowContext): R = {
    require(escapedSegment.nonEmpty, "Workflow.scoped requires a non-empty scope")
    body(using ctx.derive(scopePath = ctx.scopePath :+ escapedSegment))
  }
```

7. `parallel` (~441-458) — branches become context functions; the snapshot/restore dance is deleted. In the doc comment, replace "The enclosing scope stack and `uncancellable` depth are propagated into each branch thread, so work inside a branch carries the enclosing `scoped`/region identity." with "Each branch runs with the context of the `parallel` call site: its scope path and `uncancellable` depth travel with the context value, so work inside a branch carries the enclosing `scoped`/region identity on any thread." Implementation:

```scala
  def parallel[R](branches: Seq[WorkflowContext ?=> R])(using ctx: WorkflowContext): Seq[R] = {
    val outcomes: Seq[BranchOutcome[R]] = ox.par(
      branches.map(branch => () => runBranch(branch))
    )
    val controlFlows = outcomes.collect { case BranchControlFlow(c) => c }
    if (controlFlows.nonEmpty) throw controlFlows.head
    val failures = outcomes.collect { case BranchFailed(t) => t }
    if (failures.nonEmpty) throw failures.head
    val suspensions = outcomes.collect { case BranchSuspended(s) => s }
    if (suspensions.nonEmpty) throw new WorkflowSuspendedException(suspensions)
    outcomes.collect { case BranchResult(r) => r }
  }
```

8. The vararg overload (~460-462):

```scala
  /** Vararg form of [[parallel]]. */
  @targetName("parallelVararg")
  def parallel[R](branches: (WorkflowContext ?=> R)*)(using ctx: WorkflowContext): Seq[R] =
    parallel(branches.toVector)
```

9. `runBranch` (~464-470) — takes the context function; the ambient `ctx` applies it:

```scala
  private def runBranch[R](branch: WorkflowContext ?=> R)(using ctx: WorkflowContext): BranchOutcome[R] =
    try BranchResult(branch(using ctx))
    catch {
      case e: WorkflowSuspendedException => BranchSuspended(e)
      case e: WorkflowControlException   => BranchControlFlow(e)
      case e if NonFatal(e)              => BranchFailed(e)
    }
```

- [ ] **Step 4: Update `Step.scala`**

Mechanical replacements (each method already has `(using ctx: WorkflowContext)` in scope):

1. `runStep` (~line 226): `val stepId = StepId(key, execution.currentScope)` → `val stepId = StepId(key, ctx.currentScope)`
2. `execute()` (~294) and `attempt()` (~330): `execution.throwIfCancelled()` → `execution.throwIfCancelled(ctx.uncancellableDepth)`
3. `getExecutionState` (~438): `StepId(key, ctx.execution.currentScope)` → `StepId(key, ctx.currentScope)`
4. `awaitSignal` (~772): `StepId(stepKey, execution.currentScope)` → `StepId(stepKey, ctx.currentScope)`; its `evaluate()` (~799): `execution.throwIfCancelled()` → `execution.throwIfCancelled(ctx.uncancellableDepth)`
5. `awaitUpdate0` (~853) and its `evaluate()` (~888): same two replacements.
6. `awaitTimer` (~933) and its `evaluate()` (~942): same two replacements.
7. `awaitRace0` (~1001) and its `evaluate()` (~1020): same two replacements.
8. `firstToRunWithoutSuspension` Seq overload (~643-648) and vararg overload (~651-658): change the branches parameter type from `Seq[() => R]` / `(() => R)*` to `Seq[WorkflowContext ?=> R]` / `(WorkflowContext ?=> R)*` (keep the `@experimental` / `@targetName` annotations and defaults exactly as they are).
9. `firstToRun0` (~660-665): `branches: Vector[() => R]` → `branches: Vector[WorkflowContext ?=> R]`; `val stepIdv = StepId(stepId, execution.currentScope)` → `StepId(stepId, ctx.currentScope)`.
10. `firstToRun0`'s `evaluate()` (~691-707) — replace the snapshot/restore/push/pop dance with derived contexts:

```scala
    def evaluate(): R = {
      execution.throwIfCancelled(ctx.uncancellableDepth)
      val baseScope = ctx.currentScope
      val baseScopePath = ctx.scopePath
      val outcomes: Seq[Either[WorkflowSuspendedException, R]] =
        ox.par(branches.indices.map { i =>
          () =>
            val branchCtx = ctx.derive(scopePath = baseScopePath :+ branchSegment(i))
            try Right(branches(i)(using branchCtx))
            catch { case e: WorkflowSuspendedException => Left(e) }
        })
      val suspensions = outcomes.collect { case Left(s) => s }
      // ... the rest of evaluate() (winner selection, resolveFirstToRun) is unchanged
```

(Keep the remainder of `evaluate()` exactly as it is — only the header lines above change.)

- [ ] **Step 5: Update `PostgresWorkflowRuntime.scala` (`PostgresExecution`, ~1909-1980)**

1. Delete the two ThreadLocal fields (`uncancellableDepth`, `scopeStack`), `inUncancellableRegion`, `enterUncancellable`, `exitUncancellable`, `currentScope`, `pushScope`, `popScope`, `snapshotBranchContext`, `restoreBranchContext` (lines ~1921-1951).
2. Replace `throwIfCancelled` with:

```scala
    override def throwIfCancelled(uncancellableDepth: Int): Unit = {
      if (uncancellableDepth == 0) {
        val requestedAt = runSync {
          sql"""SELECT cancel_requested_at FROM workflow_instances
                WHERE workflow_id = $workflowId AND key = $key AND scope = $instanceScope""".query[
              Option[java.time.Instant]
            ].unique
        }
        if (requestedAt.isDefined) throw WorkflowCancelledException()
      }
    }
```

Nothing else in the runtime changes: the anonymous `WorkflowContext` created in the run loop (~line 1016) inherits the new members' defaults and the concrete `derive`.

- [ ] **Step 6: Update the example call site**

`example/src/main/scala/example/DocumentProcessingAtomicflow.scala` (~112-115) — drop the `() =>`:

```scala
    Workflow.parallel(
      virusCheck(document.content, "virus-check-1", virusCheckService.checkForVirus1),
      virusCheck(document.content, "virus-check-2", virusCheckService.checkForVirus2)
    )
```

- [ ] **Step 7: Compile main sources**

Run: `sbt "core/compile" "db/compile" "example/compile"`
Expected: success. (If a multi-statement block argument in a later step fails to adapt to the context-function expected type, extract that branch into a local `def` — do not reintroduce `() =>`.)

- [ ] **Step 8: Fix test compile errors (mechanical `() =>` drops)**

Run: `sbt "db/Test/compile"`
Expected errors of the form `() => R does not conform to atomicflow.WorkflowContext ?=> R` in `ScopedParallelSuite`, `FirstToRunSuite`, and possibly `ChildrenSuite`, `TerminateSuite`, `RestartableSuite`. Fix each by removing the `() =>` before the branch argument. Example transformation in `ScopedParallelSuite`:

```scala
      // before
      val results = Workflow.parallel(
        () => Step.atLeastOnce[Int]("a") { 1 },
        () => Step.atLeastOnce[Int]("b") { 2 },
        () => Step.atLeastOnce[Int]("c") { 3 }
      )
      // after
      val results = Workflow.parallel(
        Step.atLeastOnce[Int]("a") { 1 },
        Step.atLeastOnce[Int]("b") { 2 },
        Step.atLeastOnce[Int]("c") { 3 }
      )
```

Multi-statement branches `() => { ... }` become brace blocks `{ ... }` in argument position; if the compiler rejects one, extract it to a local `def` before the call and pass the def's application. The empty-varargs call `Step.firstToRunWithoutSuspension[Int]("race")()` stays valid unchanged. `Workflow.uncancellable` call sites need NO change (signature unchanged).

Also verify no test file references `BranchContextSnapshot`, `snapshotBranchContext`, `pushScope`, or `enterUncancellable` (they are `private[atomicflow]`; tests are in package `test`, so this must already hold).

- [ ] **Step 9: Run the affected suites (including Task 1's tests — they must now be green)**

Run: `sbt "db/testOnly test.ScopedParallelSuite test.FirstToRunSuite test.RestartableSuite test.ChildrenSuite test.TerminateSuite"`
Expected: ALL PASS, including `a step applied from a captured context on a foreign thread records the enclosing scope` and `uncancellable suppression travels with the context onto a foreign thread`.

- [ ] **Step 10: Commit**

```bash
git add core db example
git commit -m "Carry scope and uncancellable state on the immutable WorkflowContext

Region functions (scoped, uncancellable, restartable/loop, parallel,
firstToRunWithoutSuspension) now pass derived contexts to their bodies
instead of mutating per-thread state; PostgresExecution's ThreadLocals
and the branch snapshot/restore protocol are gone. Branch parameters
become context functions; call sites drop the () =>."
```

---

### Task 3: DEVIATIONS.md entry + full verification

**Files:**
- Modify: `DEVIATIONS.md` (append section + entry 76)

**Interfaces:**
- Consumes: the completed refactor from Task 2.
- Produces: documentation of the deviation from the spec's `Seq[() => R]` signatures.

- [ ] **Step 1: Append the deviation entry**

Append to `DEVIATIONS.md` (entry numbers continue from 75):

```markdown
## Phase 10 (context-carried scope state)

76. **`WorkflowContext` carries the transient scope/uncancellable state;
    `PostgresExecution` no longer uses ThreadLocals.** The spec does not
    detail how `Workflow.scoped`/`Workflow.uncancellable` state reaches Step
    registrations; the implementation used per-thread ThreadLocals in
    `PostgresExecution` plus a snapshot/restore protocol for parallel-branch
    threads. Per the design intent (the `local` of a reader monad), the state
    now lives immutably on `WorkflowContext` (`scopePath`,
    `uncancellableDepth`, both `private[atomicflow]`), and every region
    function passes a derived context to its body. Consequence: the branch
    parameters of `Workflow.parallel` and `Step.firstToRunWithoutSuspension`
    and the bodies of `Workflow.restartable`/`Workflow.loop` are context
    functions (`WorkflowContext ?=> R`) — a public signature change from the
    spec's `Seq[() => R]` (sub-workflows-iteration.md,
    restartable-regions-loops.md): call sites drop the `() =>` (behavior
    is otherwise identical; branch bodies are still evaluated at application
    time). Benefit: Step IDs and cancellation suppression travel with the
    context value, so code that hops threads and carries the context (e.g.
    applies a captured `WorkflowContext ?=> R` on another thread) records
    the correct scope instead of silently corrupting Step IDs; branch fork
    sites no longer need the snapshot/restore choreography.
```

- [ ] **Step 2: Full verification**

Run: `sbt test`
Expected: all suites pass (core, db — all 24 suites with testcontainers, example). If anything unrelated to this change flakes (testcontainers startup), re-run once and note it.

- [ ] **Step 3: Verify no ThreadLocals remain**

Run: `grep -rn "ThreadLocal" core/src/main db/src/main`
Expected: no matches.

- [ ] **Step 4: Commit**

```bash
git add DEVIATIONS.md
git commit -m "Record deviation 76: context-carried scope state"
```
