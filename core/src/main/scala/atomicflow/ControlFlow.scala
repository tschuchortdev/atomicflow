package atomicflow

import scala.util.control.ControlThrowable

/** Marker for the library's internal control-flow exceptions (suspension,
  * continue-as-new, restartable-region restart). They are thrown by the runtime to
  * implement non-local control transfer and are always caught at the workflow
  * boundary; they must never escape the workflow body.
  *
  * Extends [[ControlThrowable]], so both [[scala.util.control.NonFatal]] and
  * [[WorkflowNonFatal]] exclude them.
  */
sealed trait WorkflowControlException extends ControlThrowable

/** Thrown when an await cannot be satisfied and the instance durably suspends.
  * The pending subscriptions are committed by the await evaluation before this
  * exception is thrown; it carries no payload. Users may also throw it manually
  * to suspend; the runtime catches it at the workflow boundary and returns
  * `WorkflowSuspended`.
  */
final class WorkflowSuspendedException() extends WorkflowControlException {
  override def toString: String = "WorkflowSuspendedException"
}

/** Thrown by `Workflow.continueAsNew` to abort the current body and restart the
  * instance with new input. The runtime serializes [[newInput]] with the workflow
  * definition's `Cacheable` when committing the transition.
  */
final class ContinueAsNewException private[atomicflow] (val newInput: Any) extends WorkflowControlException {
  override def toString: String = "ContinueAsNewException"
}

/** A `NonFatal`-like extractor that excludes the library's control-flow exceptions
  * (and fatal JVM errors), so broad catches inside workflow code can handle
  * everything else and still rethrow suspension/reset/continue-as-new.
  *
  * Unlike `scala.util.control.NonFatal`, this extractor matches
  * [[InterruptedException]]: interrupts carry no workflow semantics (see
  * `running-workflows.md`, "Delivery mechanics") and user cleanup code may handle
  * them.
  */
object WorkflowNonFatal {
  def unapply(t: Throwable): Option[Throwable] = t match {
    case _: VirtualMachineError | _: ThreadDeath | _: LinkageError | _: ControlThrowable => None
    case _ => Some(t)
  }
}
