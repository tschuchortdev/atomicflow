package atomicflow.internal

import atomicflow.StepId

import java.time.Instant

/** The durable facts of one workflow_steps row, as read without any lease or
  * fence. `stateKind` is one of `started`, `succeeded`, `failed`; `statePayload`
  * is the raw, encoded outcome; `inputFingerprints` is the deterministic
  * encoding of the step's named inputs.
  */
private[atomicflow] final case class StoredStep(
    stateKind: String,
    statePayload: String,
    inputFingerprints: String,
    expiresAt: Option[Instant]
)

/** The per-run engine seam, materialized only during execution and funneled to
  * workflow code through [[atomicflow.WorkflowContext.execution]]. It carries
  * the fencing identity of the current run so the engine can implement fenced
  * writes and lease-loss detection, and the step/await storage operations the
  * running body needs. The Postgres runtime implements it with a private class.
  */
private[atomicflow] trait WorkflowExecution {

  /** Identity of the worker holding the execution lease, e.g.
    * `"processUuid:threadId"`.
    */
  def workerId: String

  /** Monotonic fencing token of the current lease; bumped on every acquisition.
    * Stale writers are fenced out by it.
    */
  def fencingToken: Long

  /** The current step scope path (the enclosing `Workflow.scoped` path). `""`
    * for top-level steps; `Workflow.scoped` sets it in a later task.
    */
  def currentScope: String

  /** The runtime's notion of the current instant, from the injected clock. */
  def now: Instant

  /** Renews the execution lease of this run, extending `lease_expires_at` by the
    * runtime's `leaseDuration`. A fenced write that does not bump the fencing
    * token; throws [[atomicflow.LeaseLostException]] if the lease no longer
    * belongs to this run.
    */
  def renewLease(): Unit

  /** Read a step's durable facts (no lease/fence needed), or `None` if absent.
    * Reports the stored row even if it has expired.
    */
  def lookupStep(stepId: StepId, stepVersion: Long): Option[StoredStep]

  /** Replace (or create) the `started` row for the step, fenced. */
  def writeStepStarted(stepId: StepId, stepVersion: Long, stepKind: String, inputFingerprints: String): Unit

  /** Replace (or create) the `succeeded` row for the step, fenced. */
  def writeStepSucceeded(
      stepId: StepId,
      stepVersion: Long,
      stepKind: String,
      inputFingerprints: String,
      payload: String,
      expiresAt: Option[Instant]
  ): Unit

  /** Replace (or create) the `failed` row for the step, fenced. */
  def writeStepFailed(
      stepId: StepId,
      stepVersion: Long,
      stepKind: String,
      inputFingerprints: String,
      payload: String,
      expiresAt: Option[Instant]
  ): Unit

  /** Delete the step row, fenced. */
  def deleteStep(stepId: StepId, stepVersion: Long): Unit
}
