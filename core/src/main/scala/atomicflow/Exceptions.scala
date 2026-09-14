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
