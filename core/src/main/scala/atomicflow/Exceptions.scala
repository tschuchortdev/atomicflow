package atomicflow

/** Public exceptions of the atomicflow runtime. */

/** A durable step or workflow failure as replayed from persisted state.
  *
  * `Cacheable.forThrowable.genericStringMessageSerializer` decodes failures into
  * this class, carrying the original exception's class name and message as portable
  * diagnostics.
  */
class StepFailed(message: String, cause: Throwable = null) extends RuntimeException(message, cause)

/** Delivered by the runtime at frontier checkpoints while `cancel_requested_at` is
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

/** Thrown by `forkWorkflow`/`resetWorkflow` when `restartFromStep` does not
  * identify an already-executed step of the source instance (an unknown step id,
  * or a step that has no cached row because it never executed).
  */
class InvalidRestartStepException(message: String) extends RuntimeException(message)

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

/** Thrown when a stored terminal outcome cannot be decoded with the configured
  * codecs (mirrors `StepSerializationFailed` for step payloads). A deterministic
  * runtime-owned failure surfaced instead of recursing into a broken codec.
  */
class StepSerializationFailed(message: String) extends StepFailed(message)
