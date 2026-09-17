package atomicflow

import atomicflow.impl.Sha256Fingerprinter
import atomicflow.internal.ScopePath

import java.time.Instant
import scala.annotation.targetName
import scala.concurrent.duration.FiniteDuration
import scala.util.control.NonFatal

/** The typed handle of a workflow instance, obtained only from the runtime. It
  * captures the [[Workflow]] definition it came from plus the instance's stable
  * identity. Methods requiring execution take a contextual `(using
  * WorkflowRuntime)`.
  */
final class WorkflowInstance[In, Out] private[atomicflow] (
    val workflow: Workflow[In, Out],
    val id: WorkflowInstanceId
) {

  /** Run the instance on the caller thread (no input parameter — the input is
    * already persisted at creation). Terminal instances return/throw their stored
    * outcome without re-executing the body.
    */
  @throws[WorkflowNotFoundException]
  def run()(using
      runtime: WorkflowRuntime,
      cacheableThrowable: Cacheable[Throwable]
  ): WorkflowRunResult[Out] =
    runtime.runWorkflowInstance(workflow, id)

  /** Passive waiter: blocks until the instance reaches a terminal state or
    * `timeout` elapses, then throws a timeout exception. Never executes the
    * workflow.
    */
  @throws[WorkflowNotFoundException]
  @throws[java.util.concurrent.TimeoutException]
  def awaitResult(timeout: FiniteDuration)(using
      runtime: WorkflowRuntime,
      cacheableThrowable: Cacheable[Throwable]
  ): WorkflowRunResult[Out] =
    runtime.awaitResult(this, timeout)

  /** The persisted data view of this instance, fresh from the database. */
  def getInfo()(using runtime: WorkflowRuntime): WorkflowInstance.Info =
    runtime.getWorkflowInstanceInfo(this)

  /** Append a `Signal` event addressed to this instance. Forwarder for the
    * runtime's `sendSignal`, resolving the signal's own [[Cacheable]].
    */
  @throws[WorkflowNotFoundException]
  def sendSignal[A](signal: Signal[A], value: A)(using runtime: WorkflowRuntime): SignalSendResult =
    runtime.sendSignal(id, signal.key, value)(using signal.cacheable)

  /** Send a synchronous [[Update]] addressed to this instance. Forwarder for the
    * runtime's `sendUpdate`, resolving the update's own [[Cacheable]]s. This
    * handle already carries the [[Workflow]] definition.
    */
  @throws[WorkflowNotFoundException]
  def sendUpdate[I, R](
      update: Update[I, R],
      input: I,
      idempotencyKey: String = "",
      persistUnhandledUpdates: Boolean = false
  )(using
      runtime: WorkflowRuntime,
      cacheableThrowable: Cacheable[Throwable]
  ): UpdateSendResult[R] =
    runtime.sendUpdate(workflow, id, update.key, input, idempotencyKey, persistUnhandledUpdates)(
      using update,
      cacheableThrowable
    )

  /** An awaitable that yields this instance's terminal outcome as a
    * `WorkflowCompletionResult[Out]`, to be raced or awaited from another
    * workflow via `Step.await`/`Step.awaitRace`. Takes the application-global
    * throwable codec contextually to build the composite outcome codec.
    */
  def completion(using ct: Cacheable[Throwable]): Awaitable.WorkflowCompletion[Out] = {
    given Cacheable[Out] = workflow.outputCacheable
    Awaitable.WorkflowCompletion[Out](id)
  }
}

object WorkflowInstance {

  /** Persisted instance state: the data view of an instance ("a row"), returned
    * by key-based queries that don't have the workflow code, and by
    * `getInfo()`. Fresh from the database.
    */
  final case class Info(
      id: WorkflowInstanceId,
      parentId: Option[WorkflowInstanceId],
      generation: Long,
      terminalState: Option[WorkflowTerminalState],
      workflowVersionAtCreation: Long,
      createdAt: Instant,
      lastRunAt: Option[Instant],
      timesExecuted: Int
  )
}

/** A workflow definition: the identifying/descriptive fields plus the executable
  * `body` and its codecs. There is deliberately no separate meta wrapper; every
  * consumer that needs the definition has the executable object (see
  * `spec/core-types.md`).
  *
  * `Cacheable[In]` and `Cacheable[Out]` are captured at construction (the
  * companion's `[In: Cacheable, Out: Cacheable]` context bounds) and are the
  * definition's codecs; only `Cacheable[Throwable]` is resolved at run call
  * sites.
  */
final class Workflow[In, Out] private[atomicflow] (
    val id: WorkflowId,
    val version: Long,
    val name: String,
    val description: Option[String],
    val body: In => WorkflowContext ?=> Out,
    val onUnconsumedSignals: Map[SignalKey, Seq[String]] => Unit,
    val inputCacheable: Cacheable[In],
    val outputCacheable: Cacheable[Out]
) {

  /** Register the instance for later explicit runs (idempotent). */
  def create(instanceKey: WorkflowInstanceKey, in: In)(using
      runtime: WorkflowRuntime
  ): WorkflowInstance[In, Out] =
    runtime.createWorkflowInstance(this, instanceKey, in)(using inputCacheable)

  /** Register the instance and schedule it for a job runner. */
  def createAndSchedule(instanceKey: WorkflowInstanceKey, in: In)(using
      runtime: WorkflowRuntime
  ): WorkflowInstance[In, Out] =
    runtime.createAndSchedule(this, instanceKey, in)

  /** Start a child workflow from within this workflow's body (see
    * `spec/sub-workflows-iteration.md`, "startAsChild"). Create-if-absent a
    * child under a scope derived from this parent's identity, generation, and
    * enclosing `Workflow.scoped` path; record the parent relationship and
    * inheritance configuration; and schedule the child's first wakeup. The
    * child body is never executed inline on this parent's thread; run it via a
    * job runner or `run` separately. Returns a handle whose `completion` can be
    * awaited with `Step.await`.
    */
  def startAsChild(
      childKey: WorkflowInstanceKey,
      input: In,
      parentClosePolicy: ParentClosePolicy = ParentClosePolicy.Cancel,
      inheritSignals: SignalInheritance = SignalInheritance.none,
      inheritPastEvents: Boolean = false
  )(using ctx: WorkflowContext): WorkflowInstance[In, Out] =
    ctx.runtime.startChild(
      this,
      childKey,
      input,
      parentClosePolicy,
      inheritSignals,
      inheritPastEvents,
      ctx.currentExecution,
      ctx.currentScope
    )(using inputCacheable)

  /** Run a previously created instance on the caller thread (no input param). */
  def run(instanceKey: WorkflowInstanceKey)(using
      runtime: WorkflowRuntime,
      cacheableThrowable: Cacheable[Throwable]
  ): WorkflowRunResult[Out] =
    runtime.runWorkflowInstance(this, WorkflowInstanceId(id, instanceKey))

  /** Atomically create-if-absent and run on the caller thread. */
  def createAndRun(instanceKey: WorkflowInstanceKey, in: In)(using
      runtime: WorkflowRuntime,
      cacheableThrowable: Cacheable[Throwable]
  ): WorkflowRunResult[Out] =
    runtime.createAndRun(this, instanceKey, in)
}

/** The handle passed to a `Workflow.restartable` body. `restartCount` is the
  * number of committed restart transitions (0 before any). `restart(nextState)`
  * starts the next generation of the region and never returns (it is runtime
  * control flow); the normal path is to return the region's result `R`.
  */
trait RestartableScope[S] {
  def restartCount: Long
  def restart(nextState: S): Nothing
}

/** The handle passed to a `Workflow.loop` body. `restartCount` is the number of
  * committed restart transitions. `break(result)` completes the region with
  * `result` and never returns (runtime control flow); the normal path is to
  * return the next state `S` to continue the loop.
  */
trait LoopScope[R] {
  def restartCount: Long
  def break(result: R): Nothing
}

object Workflow {
  /** Passive waiter for an instance's terminal outcome, addressed by key; see
    * `WorkflowInstance.awaitResult`.
    */
  @throws[java.util.concurrent.TimeoutException]
  def awaitResult[In, Out](
      workflow: Workflow[In, Out],
      instanceKey: WorkflowInstanceKey,
      timeout: FiniteDuration
  )(using
      runtime: WorkflowRuntime,
      cacheableThrowable: Cacheable[Throwable]
  ): WorkflowRunResult[Out] =
    runtime.awaitResult(WorkflowInstance(workflow, WorkflowInstanceId(workflow.id, instanceKey)), timeout)

  /** Renews the execution lease of the instance executing on the current thread,
    * extending its `lease_expires_at` by the runtime's `leaseDuration`. The
    * runtime calls this automatically at every checkpoint; call it explicitly
    * inside long-running Step bodies, between checkpoints.
    *
    * Not a cancellation checkpoint, and available only inside an executing
    * workflow (the `(using WorkflowContext)` requirement makes external or
    * off-thread calls unrepresentable).
    *
    * @throws LeaseLostException when the lease was taken over or the instance is terminal
    */
  def heartbeat()(using ctx: WorkflowContext): Unit =
    ctx.runtime.renewLease(ctx.currentExecution)

  /** The `Workflow.version` recorded when the instance was created. The current
    * body may branch on it internally to adapt to the definition version that
    * created the instance.
    */
  def versionAtCreation(using ctx: WorkflowContext): Long = ctx.versionAtCreation

  /** The recursive tail transition: ends the current execution and restarts the
    * same logical workflow with `nextInput` and a fresh history, in place. The
    * generation is incremented and the previous generation's execution records
    * (Step rows, subscriptions, wakeups) are erased; exact-key signal cursors
    * are kept. Children are closed per their `ParentClosePolicy`; the
    * continuation does not wait for cooperative child cancellation. If this
    * instance is itself a child it stays attached to its own parent with the
    * same signal-inheritance configuration.
    *
    * Returns `Nothing` and is implemented as runtime control flow, so the body
    * does not continue after the call. The run ends with a
    * `WorkflowRunResult.ContinueAsNew` outcome and the successor is scheduled;
    * a subsequent `run` (or the job runner) executes it from the top.
    *
    * The next input is encoded with the local `Cacheable` and decoded by the
    * runtime with this workflow's input codec, which the type system ties to
    * `nextInput` by convention; a mismatch fails the run.
    */
  def continueAsNew[A: Cacheable](nextInput: A)(using ctx: WorkflowContext): Nothing = {
    val encoded = summon[Cacheable[A]].write(nextInput)
    throw new ContinueAsNewException(encoded)
  }

  /** Runs a restartable region: a subscope (like [[scoped]]) whose interior
    * Step/Await records are reset on each restart, so an eternal loop such as a
    * poller does not accumulate history. `initialState` is a by-name pure seed,
    * evaluated and persisted only on first creation and ignored on replay.
    *
    * The body receives the current state `S` and a [[RestartableScope]].
    * Calling `scope.restart(nextState)` starts the next generation: the region
    * row's state and restart count are replaced, the previous generation's
    * nested Step rows and subscriptions are discarded, and children created in
    * it are closed per their `ParentClosePolicy`; the body then re-enters
    * locally in the same run (the outer workflow is not replayed to begin the
    * next generation). Returning normally completes the region with `R`; the
    * final looping is kept, so on an outer replay the body re-runs and reuses
    * its cached records rather than caching the whole region value.
    *
    * `restartCount` counts committed restarts, not crash replays. This is the
    * dual of [[loop]]: use [[restartable]] when continuing is exceptional and
    * returning the result is the normal path. Both expose the same durable
    * transitions and the same `restartCount` metadata.
    */
  def restartable[S: Cacheable, R](id: String, initialState: => S)(
      body: (S, RestartableScope[S]) => WorkflowContext ?=> R
  )(using
      ctx: WorkflowContext
  ): R =
    regionLoop(id, initialState) { (state, scope) =>
      body(state, scope)
    }

  /** Runs a looping region: the dual of [[restartable]] — use [[loop]] when
    * continuing is the normal path and breaking with the result is exceptional.
    * The body returns the next state `S` to continue, or calls
    * `loop.break(result)` to complete the region with `R`. It has exactly the
    * same durable transitions, region semantics, and `restartCount` metadata as
    * [[restartable]]; a normal return starts the next generation and `break`
    * completes the region. `initialState` is a by-name pure seed evaluated only
    * on first creation.
    */
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

  private def regionSegment(id: String, count: Long): String =
    ScopePath.escapeScopeSegment(id) + "@" + count

  /** The single shared implementation behind [[restartable]] and [[loop]]. The
    * body uses a [[RestartableScope]]: a normal return completes the region with
    * `R`; `scope.restart` throws [[RegionRestartException]] to start the next
    * generation. [[loop]] adapts its own body onto this shape (a normal return
    * becomes a restart, `break` throws [[RegionBreakException]]). Both control
    * exceptions are caught here and unwound to the region boundary.
    */
  private def regionLoop[S: Cacheable, R](id: String, initialState: => S)(
      body: (S, RestartableScope[S]) => WorkflowContext ?=> R
  )(using
      ctx: WorkflowContext
  ): R = {
    val rt: ctx.runtime.type = ctx.runtime
    val run = ctx.currentExecution
    val cacheable = summon[Cacheable[S]]
    val parentScopePath = ctx.scopePath
    val parentScope = ctx.currentScope
    val row = rt.readRegionState(run, id, parentScope)
    val startCount = row.map(_._2).getOrElse(0L)
    val firstState: S = row match {
      case Some((state, _)) => cacheable.read(state)
      case None =>
        val seed = initialState
        rt.createRegion(run, id, parentScope, cacheable.write(seed))
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
          rt.restartRegion(run, id, parentScope, count, e.serializedState)
          count += 1
          state = cacheable.read(e.serializedState)
        case e: RegionBreakException[R] =>
          result = e.result
          done = true
      }
    }
    result
  }

  /** A lexical region inside which cancellation delivery is suppressed: while
    * `f` runs, checkpoints do not throw [[WorkflowCancelledException]], so Step
    * bodies execute and awaits resolve normally. This is the mechanism for
    * durable compensation (the Saga pattern) and for finishing a unit of work
    * before stopping.
    *
    * The region is lexical and re-entrant. It does not clear `cancel_requested_at`;
    * after it exits, the next new-work checkpoint throws again. Heartbeats are
    * not suppressed inside it. It is transient per-run state carried by the context, so replay
    * re-enters the region as ordinary user code and already-completed
    * compensation Steps are returned from the cache.
    */
  def uncancellable[R](f: WorkflowContext ?=> R)(using ctx: WorkflowContext): R =
    f(using ctx.derive(uncancellableDepth = ctx.uncancellableDepth + 1))

  /** Wraps a body in an ID namespace: every Step/Await ID inside is prefixed
    * with `scopeKey` (see `spec/sub-workflows-iteration.md`, "Primitives"), so
    * the same step definitions can execute independently per element. Scopes
    * nest arbitrarily; the full prefix is the path of enclosing keys joined by
    * `/`.
    */
  def scoped[R](scopeKey: String)(body: WorkflowContext ?=> R)(using ctx: WorkflowContext): R =
    withScope(ScopePath.escapeScopeSegment(scopeKey))(body)

  /** Like [[scoped]] but the scope key is derived deterministically from the
    * element's fingerprint, so equal elements share the same scope (reusing
    * cached Steps) and distinct elements get distinct scopes.
    */
  def scoped[A: Fingerprintable, R](elem: A)(body: WorkflowContext ?=> R)(using ctx: WorkflowContext): R =
    withScope(
      ScopePath.escapeScopeSegment(Fingerprintable[A].fingerprint(elem, Sha256Fingerprinter).toString)
    )(body)

  private def withScope[R](escapedSegment: String)(body: WorkflowContext ?=> R)(using ctx: WorkflowContext): R = {
    require(escapedSegment.nonEmpty, "Workflow.scoped requires a non-empty scope")
    body(using ctx.derive(scopePath = ctx.scopePath :+ escapedSegment))
  }

  /** Runs one by-name block, catches its suspension instead of propagating it,
    * and returns `Either[WorkflowSuspendedException, R]`. The opt-in primitive
    * for local suspension handling; most code should let suspensions propagate.
    */
  def runToSuspension[R](
      body: WorkflowContext ?=> R
  )(using ctx: WorkflowContext): Either[WorkflowSuspendedException, R] =
    try Right(body(using ctx))
    catch { case e: WorkflowSuspendedException => Left(e) }

  /** The terminal outcome of one parallel branch, collected so that ALL branches
    * can be joined to completion before `parallel` decides what to propagate.
    * Because every branch outcome is returned as a value (never thrown), Ox never
    * interrupts a sibling mid-branch, so a DB-working branch is never cut off by
    * another branch's control-flow exception.
    */
  private sealed trait BranchOutcome[+R]
  private final case class BranchResult[R](value: R) extends BranchOutcome[R]
  private final case class BranchSuspended(suspension: WorkflowSuspendedException) extends BranchOutcome[Nothing]
  private final case class BranchControlFlow(c: WorkflowControlException) extends BranchOutcome[Nothing]
  private final case class BranchFailed(t: Throwable) extends BranchOutcome[Nothing]

  /** Runs several branches concurrently (via Ox `par`), waiting for ALL of
    * them. When some complete and others suspend, all results are collected
    * first and then one combined [[WorkflowSuspendedException]] is thrown
    * carrying each branch's suspension in `causes`. Returns `Seq[R]` in order
    * when every branch completes. A non-suspension failure propagates.
    *
    * Control-flow exceptions (restart, break, continue-as-new) never interrupt a
    * sibling: every branch is joined to completion first, then the winning
    * control-flow exception is rethrown at the join point. Each branch runs
    * with the context of the `parallel` call site: its scope path and
    * `uncancellable` depth travel with the context value, so work inside a
    * branch carries the enclosing `scoped`/region identity on any thread.
    */
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

  /** Vararg form of [[parallel]]. */
  @targetName("parallelVararg")
  def parallel[R](branches: (WorkflowContext ?=> R)*)(using ctx: WorkflowContext): Seq[R] =
    parallel(branches.toVector)

  private def runBranch[R](branch: WorkflowContext ?=> R)(using ctx: WorkflowContext): BranchOutcome[R] =
    try BranchResult(branch(using ctx))
    catch {
      case e: WorkflowSuspendedException => BranchSuspended(e)
      case e: WorkflowControlException   => BranchControlFlow(e)
      case e if NonFatal(e)              => BranchFailed(e)
    }

  def apply[In: Cacheable, Out: Cacheable](
      id: WorkflowId,
      version: Long = 1L,
      name: String = "",
      description: Option[String] = None
  )(
      body: In => WorkflowContext ?=> Out,
      onUnconsumedSignals: Map[SignalKey, Seq[String]] => Unit = _ => ()
  ): Workflow[In, Out] =
    new Workflow[In, Out](
      id,
      version,
      name,
      description,
      body,
      onUnconsumedSignals,
      summon[Cacheable[In]],
      summon[Cacheable[Out]]
    )
}
