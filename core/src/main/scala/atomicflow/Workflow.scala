package atomicflow

import java.time.Instant
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
    val onUnconsumedSignals: Map[SignalKey, Seq[Any]] => Unit,
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

  def apply[In: Cacheable, Out: Cacheable](
      id: WorkflowId,
      version: Long = 1L,
      name: String = "",
      description: Option[String] = None
  )(
      body: In => WorkflowContext ?=> Out,
      onUnconsumedSignals: Map[SignalKey, Seq[Any]] => Unit = _ => ()
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
