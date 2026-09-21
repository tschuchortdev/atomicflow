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
sealed trait WorkflowControlFlowException extends ControlThrowable

/** Thrown when an await cannot be satisfied and the instance durably suspends.
  * The pending subscriptions are committed by the await evaluation before this
  * exception is thrown; it carries no payload. It is an internal control-flow
  * exception: only the runtime's await evaluation throws it, and it must never
  * escape the workflow body.
  */
final class WorkflowSuspendedException private[atomicflow] (
    val causes: Seq[WorkflowSuspendedException] = Nil
) extends WorkflowControlFlowException {
  override def toString: String = "WorkflowSuspendedException"
}

/** Thrown by `Workflow.continueAsNew` to abort the current body and restart the
  * instance with new input. Carries the [[encoded]] bytes of the next input,
  * serialized with the caller's local `Cacheable` at the forwarder. The runtime
  * decodes them with the current workflow definition's input codec when it
  * commits the transition; the type system ties the two by convention, so a
  * mismatch fails the run (acceptable).
  */
final class ContinueAsNewException private[atomicflow] (val encoded: String) extends WorkflowControlFlowException {
  override def toString: String = "ContinueAsNewException"
}

/** Thrown by `RestartableScope.restart` to start the next generation of a
  * `Workflow.restartable`/`Workflow.loop` region. Carries the [[serializedState]]
  * of the next generation, encoded with the region's `Cacheable[S]` at the
  * forwarder. The runtime's region loop catches it, commits the restart
  * transition (replacing the region row's state and count, discarding the
  * previous looping's nested rows and subscriptions, closing its children), and
  * re-enters the body locally in the same run.
  */
final class RegionRestartException private[atomicflow] (val serializedState: String) extends WorkflowControlFlowException {
  override def toString: String = "RegionRestartException"
}

/** Thrown by `LoopScope.break` to complete a `Workflow.loop` region with a
  * result. Carries the in-memory result `R` (not persisted at the region level);
  * the region's loop catches it and returns the value as the function's result.
  */
final class RegionBreakException[R] private[atomicflow] (val result: R) extends WorkflowControlFlowException {
  override def toString: String = "RegionBreakException"
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
