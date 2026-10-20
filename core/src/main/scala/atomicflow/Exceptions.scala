package atomicflow

/** Public exceptions of the atomicflow runtime. */

/** A durable step failure, thrown when a step's persisted failure row is
  * reached: on the run that persisted it and on every replay. The `cause` is
  * the decoded original failure.
  *
  * Thrown only by throwable codecs that present failures this way (the
  * recommended `Cacheable.forThrowable.genericStringMessageSerializer` does);
  * other codecs rethrow the decoded exception unchanged, preserving its
  * original class.
  */
final class StepFailed(val step: StepId, cause: Throwable)
    extends RuntimeException(if cause == null then null else cause.toString, cause)

object StepFailed {

  /** Matches a failing step and binds its message. The step is addressed with
    * the `stepId` interpolator:
    * `case StepFailed(stepId"orders/$id/charge", message) => ...`.
    */
  def unapply(t: Throwable): Option[(StepId, String)] = t match {
    case failure: StepFailed => Some((failure.step, failure.getMessage))
    case _                   => None
  }
}

/** A failure decoded from its persisted portable form: the original exception's
  * class name, message, and cause chain. Renders as the original exception, so
  * stack traces read `Caused by: <original class>: <message>` and never name
  * this class.
  */
private[atomicflow] final class SerializedException(
    private[atomicflow] val originalClassName: String,
    private[atomicflow] val originalMessage: String,
    cause: SerializedException = null
) extends RuntimeException(originalMessage, cause) {
  override def toString: String =
    if originalMessage == null then originalClassName else s"$originalClassName: $originalMessage"
}

/** Thrown before an uncached step or wait while `cancel_requested_at` is
  * set. A plain public [[RuntimeException]] so ordinary cleanup (`NonFatal`, broad
  * catches) can run; catching it is legitimate user behavior (compensation), not a
  * library control-flow violation.
  */
class WorkflowCancelledException(message: String) extends RuntimeException(message)

object WorkflowCancelledException {
  def apply(): WorkflowCancelledException = new WorkflowCancelledException("The workflow instance was cancelled")
}

/** Thrown by the create variants when an instance already exists under the same
  * identity but its persisted (serialized) input differs from the new one. Input
  * equality is byte-equality of the serialized input.
  */
class WorkflowInputConflictException(message: String) extends RuntimeException(message)

object WorkflowInputConflictException {
  def apply(instanceId: WorkflowInstanceId): WorkflowInputConflictException =
    new WorkflowInputConflictException(s"Workflow instance already exists with different input: $instanceId")
}

/** Thrown by operations that address a workflow instance that does not exist. */
class WorkflowNotFoundException(message: String) extends RuntimeException(message)

/** Thrown by `forkWorkflow`/`resetWorkflow` when the step selector matches no
  * already-executed step of the source instance (an empty match, or a selector
  * that does not fit the workflow's current shape).
  */
class NoMatchingStepException(message: String) extends RuntimeException(message)

/** Thrown by an external `run` on an instance whose execution lease is held by
  * another worker and does not become available within the runtime's
  * `leaseAcquireTimeout`. The runtime never steals a live lease.
  */
class LeaseUnavailableException(message: String) extends RuntimeException(message)

object LeaseUnavailableException {
  def apply(instanceId: WorkflowInstanceId): LeaseUnavailableException =
    new LeaseUnavailableException(
      s"Workflow instance lease is held by another worker and did not become available in time: $instanceId"
    )
}

/** Raised when the execution lease was lost mid-run — taken over by another
  * worker or expired — so a fenced write affected zero rows. The run aborts
  * without durable effect; the new owner is responsible for the instance.
  */
class LeaseLostException(message: String) extends RuntimeException(message)

object LeaseLostException {
  def apply(instanceId: WorkflowInstanceId): LeaseLostException =
    new LeaseLostException(s"Workflow instance execution lease was lost: $instanceId")
}

/** Thrown by a step replay when a value pinned with `ensureUnchanged` changed
  * between runs. The workflow is blocked and requires manual intervention; the
  * step body is not re-executed.
  */
class StepInputConflictException(message: String) extends RuntimeException(message)

/** Thrown when a step key is reused for a different step kind. A step key (id,
  * scope, and version) belongs to exactly one step kind — `Step`, `Await`,
  * `FirstToRun`, or `RestartableRegion` — and writing a row of another kind at
  * the same key fails the run instead of overwriting the existing row.
  */
class StepKeyConflictException(message: String) extends RuntimeException(message)

/** Thrown when a step, signal, or terminal outcome cannot be encoded or decoded
  * with the configured codec.
  */
class StepSerializationFailed(message: String) extends RuntimeException(message)
