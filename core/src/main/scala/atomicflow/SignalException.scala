package atomicflow

trait SignalException(signal: Signal[?])
                     (using workflowCtx: SimpleWorkflowContext)
  extends WorkflowException {
  self: Exception =>

  def signalMeta: SignalMeta = signal.meta
}

class SignalEmptyException(signal: Signal[?], message: String)
                          (using SimpleWorkflowContext)
  extends Exception(message) with SignalException(signal) {

  def this(signal: Signal[?])(using workflowCtx: SimpleWorkflowContext) =
    this(signal, s"Empty signal for workflow instance: $workflowCtx")
}

class SignalConflictException(signal: Signal[?], message: String)
                             (using stepCtx: StepContext[?])
  extends Exception(message) with SignalException(signal) {

  def this(signal: Signal[?])(using stepCtx: StepContext[?]) =
    this(signal, s"Cannot change signal value: $stepCtx)")
}
