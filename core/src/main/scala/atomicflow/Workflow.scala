package atomicflow

import atomicflow.impl.Sha256Fingerprinter
import atomicflow.internal.ScopePath

import java.time.Instant
import scala.annotation.targetName
import scala.concurrent.duration.FiniteDuration

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
    ctx.execution.renewLease()

  /** The `Workflow.version` recorded when the instance was created. The current
    * body may branch on it internally to adapt to the definition version that
    * created the instance.
    */
  def versionAtCreation(using ctx: WorkflowContext): Long = ctx.versionAtCreation

  /** A lexical region inside which cancellation delivery is suppressed: while
    * `f` runs, checkpoints do not throw [[WorkflowCancelledException]], so Step
    * bodies execute and awaits resolve normally. This is the mechanism for
    * durable compensation (the Saga pattern) and for finishing a unit of work
    * before stopping.
    *
    * The region is lexical and re-entrant. It does not clear `cancel_requested_at`;
    * after it exits, the next new-work checkpoint throws again. Heartbeats are
    * not suppressed inside it. It is per-execution transient state, so replay
    * re-enters the region as ordinary user code and already-completed
    * compensation Steps are returned from the cache.
    */
  def uncancellable[R](f: WorkflowContext ?=> R)(using ctx: WorkflowContext): R = {
    ctx.execution.enterUncancellable()
    try f(using ctx)
    finally ctx.execution.exitUncancellable()
  }

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
    ctx.execution.pushScope(escapedSegment)
    try body(using ctx)
    finally ctx.execution.popScope()
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

  /** Runs several branches concurrently (via Ox `par`), waiting for ALL of
    * them. When some complete and others suspend, all results are collected
    * first and then one combined [[WorkflowSuspendedException]] is thrown
    * carrying each branch's suspension in `causes`. Returns `Seq[R]` in order
    * when every branch completes. A non-suspension failure propagates (after
    * the branches settle).
    */
  def parallel[R](branches: Seq[() => R]): Seq[R] = {
    val outcomes: Seq[Either[WorkflowSuspendedException, R]] =
      ox.par(branches.map(branch => () => captureSuspension(branch())))
    val suspensions = outcomes.collect { case Left(s) => s }
    if (suspensions.nonEmpty) throw new WorkflowSuspendedException(suspensions)
    else outcomes.collect { case Right(r) => r }
  }

  /** Vararg form of [[parallel]]. */
  @targetName("parallelVararg")
  def parallel[R](branches: (() => R)*): Seq[R] = parallel(branches.toVector)

  private def captureSuspension[R](run: => R): Either[WorkflowSuspendedException, R] =
    try Right(run)
    catch { case e: WorkflowSuspendedException => Left(e) }

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
