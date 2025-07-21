package atomicflow

trait SignalException(signal: Signal[?])
                     (using SimpleWorkflowContext)
  extends WorkflowException {
  self: Exception =>

  def signalMeta: SignalMeta = signal.meta
}

class SignalEmptyException(signal: Signal[?], message: String)
                          (using SimpleWorkflowContext)
  extends Exception(message) with SignalException(signal) {

  def this(signal: Signal[?])(using workflowCtx: SimpleWorkflowContext) =
    this(signal, s"Empty signal value: $workflowCtx/$signal")
}

class SignalConflictException(signal: Signal[?], message: String)
                             (using SimpleWorkflowContext)
  extends Exception(message) with SignalException(signal) {

  def this(signal: Signal[?])(using workflowCtx: SimpleWorkflowContext) =
    this(signal, s"Cannot change signal value: $workflowCtx/$signal)")
}
