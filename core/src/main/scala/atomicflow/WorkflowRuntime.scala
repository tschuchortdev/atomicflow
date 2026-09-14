package atomicflow

import scala.annotation.implicitNotFound
import scala.concurrent.duration.{Duration, FiniteDuration}
import java.time.Clock

/** The canonical home of all single-instance workflow operations. Implemented per
  * backend (in-memory, Postgres, ...). Convenience methods are `final`, built
  * from a small set of abstract primitives.
  */
@implicitNotFound("No WorkflowRuntime available. Add a using clause (using WorkflowRuntime).")
trait WorkflowRuntime {

  /** The single time source for timer due-ness, retry thresholds, sweep
    * predicates, and all persisted `:now` parameters. The database server's
    * clock is never consulted for logic. Exposed so apps and tests share one
    * clock with workflow code.
    */
  def clock: Clock

  /** Append a `Signal` event addressed to `workflowInstanceId`. The sender
    * briefly row-locks the instance and checks `is_accepting_signals`, then
    * appends the event (with the signal's `Cacheable`) and upserts a wakeup for
    * the instance when a matching pending subscription exists. Does not acquire
    * the execution lease.
    */
  @throws[WorkflowNotFoundException]
  def sendSignal[A: Cacheable](
      workflowInstanceId: WorkflowInstanceId,
      key: SignalKey,
      value: A
  ): SignalSendResult

  /** Register an instance, idempotent for equal (serialized) input; throws
    * [[WorkflowInputConflictException]] if the input differs.
    */
  @throws[WorkflowInputConflictException]
  def createWorkflowInstance[In, Out](
      workflow: Workflow[In, Out],
      instanceKey: WorkflowInstanceKey,
      in: In
  )(using Cacheable[In]): WorkflowInstance[In, Out]

  /** Delete any existing instance under the identity and re-create it; returns
    * whether an existing instance was discarded.
    */
  def createWorkflowInstanceDiscardExisting[In, Out](
      workflow: Workflow[In, Out],
      instanceKey: WorkflowInstanceKey,
      in: In
  )(using Cacheable[In]): Boolean

  /** Request cooperative cancellation of an instance. Durably sets
    * `cancel_requested_at` once and never resets it. If the instance has never
    * started and has no execution state, it is finalized `CANCELLED` immediately
    * (the body never runs). Otherwise the flag is delivered at the next new-work
    * checkpoint (a Step body about to execute, or an await about to be
    * evaluated), where the runtime throws [[WorkflowCancelledException]]. A
    * terminal instance is a no-op.
    *
    * Does not acquire the execution lease; delivery is by checkpoint, never by
    * thread interruption.
    */
  @throws[WorkflowNotFoundException]
  def cancel(instanceId: WorkflowInstanceId): Unit

  /** Execute a created instance on the caller thread. */
  @throws[WorkflowNotFoundException]
  def runWorkflowInstance[In, Out](
      workflow: Workflow[In, Out],
      instanceId: WorkflowInstanceId
  )(using Cacheable[Throwable]): WorkflowRunResult[Out]

  /** Upsert a coalesced wakeup row for an instance, `ON CONFLICT DO NOTHING` so an
    * existing row's timestamps are never reset. The backend derives `scheduled_at`
    * from its own clock (`clock.now + delay`).
    */
  private[atomicflow] def upsertWakeup(instanceId: WorkflowInstanceId, delay: FiniteDuration): Unit

  /** Read a step's durable facts without acquiring a lease or fence (a pure
    * lookup of durable state, used by `Step.getExecutionState`).
    */
  private[atomicflow] def readStep(
      instanceId: WorkflowInstanceId,
      stepId: StepId,
      stepVersion: Long
  ): Option[atomicflow.internal.StoredStep]

  /** Register and schedule the instance's first wakeup (due immediately). */
  final def createAndSchedule[In, Out](
      workflow: Workflow[In, Out],
      instanceKey: WorkflowInstanceKey,
      in: In
  ): WorkflowInstance[In, Out] = {
    val instance = createWorkflowInstance(workflow, instanceKey, in)(using workflow.inputCacheable)
    upsertWakeup(instance.id, Duration.Zero)
    instance
  }

  /** Atomically create-if-absent and run on the caller thread. */
  final def createAndRun[In, Out](
      workflow: Workflow[In, Out],
      instanceKey: WorkflowInstanceKey,
      in: In
  )(using cacheableThrowable: Cacheable[Throwable]): WorkflowRunResult[Out] = {
    createWorkflowInstance(workflow, instanceKey, in)(using workflow.inputCacheable)
    runWorkflowInstance(workflow, WorkflowInstanceId(workflow.id, instanceKey))
  }

  /** Upgrade a bare identity to a typed handle. Validates that the workflow id
    * in the identity matches the provided definition (a programming error if it
    * does not). Missing instances are not verified here; handles obtained from
    * the runtime always refer to existing instances.
    */
  final def getWorkflowInstance[In, Out](
      workflow: Workflow[In, Out],
      instanceId: WorkflowInstanceId
  ): WorkflowInstance[In, Out] = {
    if (instanceId.workflowId != workflow.id)
      throw new IllegalArgumentException(
        s"Cannot build a handle for workflow '${workflow.id}' from an instanceId referring to workflow '${instanceId.workflowId}'"
      )
    WorkflowInstance(workflow, instanceId)
  }

  /** Key-prefix query over instances of one workflow definition, scoped exactly
    * to `scope` (empty = top-level only). The prefix is matched literally: `%`
    * and `_` in the user prefix are escaped, not treated as wildcards.
    */
  def getWorkflowInstancesByPrefix(
      workflowId: WorkflowId,
      keyPrefix: WorkflowInstanceKey,
      scope: String = ""
  ): Vector[WorkflowInstance.Info]

  /** Instances of one workflow definition that have not reached a terminal
    * state. `includeWaiting = false` (default) returns only instances that have
    * never started (`times_executed = 0`); `true` returns all non-terminal
    * instances. `limit <= 0` means unlimited.
    */
  def getUnfinishedWorkflowInstances(
      workflowId: WorkflowId,
      includeWaiting: Boolean = false,
      limit: Int = -1
  ): Vector[WorkflowInstance.Info]

  /** Deletes all instances of one workflow definition whose key matches `scope`
    * and `keyPrefix` (escaped as in [[getWorkflowInstancesByPrefix]]), plus
    * their cascaded rows and their event rows. Returns the number of instances
    * deleted.
    */
  def deleteWorkflowInstancesByPrefix(
      workflowId: WorkflowId,
      keyPrefix: WorkflowInstanceKey,
      scope: String = ""
  ): Long

  /** Passive waiter for an instance's terminal outcome: polls the stored
    * terminal projection until a terminal state is reached or `timeout`
    * elapses, then throws [[java.util.concurrent.TimeoutException]]. Never
    * executes the workflow. Terminal outcomes decode exactly as `run` on a
    * terminal instance.
    */
  @throws[java.util.concurrent.TimeoutException]
  def awaitResult[Out](
      instance: WorkflowInstance[?, Out],
      timeout: FiniteDuration
  )(using Cacheable[Throwable]): WorkflowRunResult[Out]

  /** The persisted data view of an instance, fresh from the database. */
  private[atomicflow] def getWorkflowInstanceInfo[In, Out](
      instance: WorkflowInstance[In, Out]
  ): WorkflowInstance.Info
}

object WorkflowRuntime {
  def apply(using runtime: WorkflowRuntime): WorkflowRuntime = runtime
}
