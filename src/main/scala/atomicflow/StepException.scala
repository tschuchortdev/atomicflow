package atomicflow

trait StepException(using stepCtx: StepContext[?]) extends WorkflowException {
  self: Exception =>
  def stepMeta: StepMeta = stepCtx.meta
}

class StepInputConflictException(message: String)
                                (using stepCtx: StepContext[?]) extends WorkflowInputConflictException(message) with StepException {
  def this()(using stepCtx: StepContext[?]) =
    this(s"Cannot re-run step with different input: $stepCtx)")
}
