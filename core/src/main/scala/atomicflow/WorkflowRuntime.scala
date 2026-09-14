package atomicflow

import scala.annotation.implicitNotFound

import java.time.Instant

/** The canonical home of all single-instance workflow operations. Implemented per
  * backend (in-memory, Postgres, ...). Convenience methods are `final`, built
  * from a small set of abstract primitives.
  */
@implicitNotFound("No WorkflowRuntime available. Add a using clause (using WorkflowRuntime).")
trait WorkflowRuntime {

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

  /** Execute a created instance on the caller thread. */
  @throws[WorkflowNotFoundException]
  def runWorkflowInstance[In, Out](
      workflow: Workflow[In, Out],
      instanceId: WorkflowInstanceId
  )(using Cacheable[In], Cacheable[Out], Cacheable[Throwable]): WorkflowRunResult[Out]

  /** Upsert a coalesced wakeup row for an instance, `ON CONFLICT DO NOTHING` so an
    * existing row's timestamps are never reset.
    */
  private[atomicflow] def upsertWakeup(instanceId: WorkflowInstanceId, scheduledAt: Instant): Unit

  /** Register and schedule the instance's first wakeup. */
  final def createAndSchedule[In, Out](
      workflow: Workflow[In, Out],
      instanceKey: WorkflowInstanceKey,
      in: In
  )(using Cacheable[In]): WorkflowInstance[In, Out] = {
    val instance = createWorkflowInstance(workflow, instanceKey, in)
    upsertWakeup(instance.id, Instant.now())
    instance
  }

  /** Atomically create-if-absent and run on the caller thread. */
  final def createAndRun[In, Out](
      workflow: Workflow[In, Out],
      instanceKey: WorkflowInstanceKey,
      in: In
  )(using
      Cacheable[In],
      Cacheable[Out],
      Cacheable[Throwable]
  ): WorkflowRunResult[Out] = {
    createWorkflowInstance(workflow, instanceKey, in)
    runWorkflowInstance(workflow, WorkflowInstanceId(workflow.id, instanceKey))
  }

  /** Upgrade a bare identity to a typed handle. Validation of the workflowId
    * against the instance row is completed in a later task.
    */
  def getWorkflowInstance[In, Out](
      workflow: Workflow[In, Out],
      instanceId: WorkflowInstanceId
  ): WorkflowInstance[In, Out] =
    WorkflowInstance(workflow, instanceId)
}

object WorkflowRuntime {
  def apply(using runtime: WorkflowRuntime): WorkflowRuntime = runtime
}
