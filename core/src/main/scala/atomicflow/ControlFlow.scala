package atomicflow

import scala.util.control.ControlThrowable

/** Marker for the library's internal control-flow exceptions (suspension,
  * continue-as-new, restartable-region restart). They are thrown by the runtime to
  * implement non-local control transfer and are always caught at the workflow
  * or region boundary. Workflow code must let them propagate to that boundary.
  *
  * Extends [[ControlThrowable]], so both [[scala.util.control.NonFatal]] and
  * [[WorkflowNonFatal]] exclude them.
  */
sealed trait WorkflowControlFlowException extends ControlThrowable

/** Thrown when an await cannot be satisfied and the instance durably suspends.
  * The pending subscriptions are committed by the await evaluation before this
  * exception is thrown. Parallel operations can collect branch suspensions in
  * `causes`. Workflow code must let it propagate to the runtime.
  */
final class WorkflowSuspendedException private[atomicflow] (
    val causes: Seq[WorkflowSuspendedException] = Nil
) extends WorkflowControlFlowException {
  override def toString: String = "WorkflowSuspendedException"
}

/** Thrown by `Workflow.continueAsNew` to abort the current body and restart the
  * instance with new input. Carries the [[encoded]] string for the next input,
  * serialized with the caller's local `Cacheable`. The runtime
  * decodes them with the current workflow definition's input codec when it
  * commits the transition. The caller must supply the workflow's input type;
  * an incompatible encoding fails the run.
  */
final class ContinueAsNewException private[atomicflow] (val encoded: String) extends WorkflowControlFlowException {
  override def toString: String = "ContinueAsNewException"
}

/** Thrown by `RestartableScope.restart` to start the next generation of a
  * `Workflow.restartable`/`Workflow.loop` region. Carries the [[serializedState]]
  * of the next generation, encoded with the region's `Cacheable[S]` at the
  * forwarder. The runtime's region loop catches it, commits the restart
  * transition (replacing the region row's state and count, discarding the
  * previous iteration's nested rows and subscriptions, closing its children), and
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
