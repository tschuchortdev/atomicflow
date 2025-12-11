package atomicflow

trait SignalException(signal: Signal[?])
                     (using WorkflowInstanceMeta)
  extends WorkflowException {
  self: Exception =>

  def signalMeta: SignalMeta = signal.meta
}

class SignalEmptyException(signal: Signal[?], message: String)
                          (using WorkflowInstanceMeta)
  extends Exception(message) with SignalException(signal) {

  def this(signal: Signal[?])(using workflowCtx: WorkflowInstanceMeta) =
    this(signal, s"Empty signal value: $workflowCtx/$signal")
}

class SignalConflictException(signal: Signal[?], message: String)
                             (using WorkflowInstanceMeta)
  extends Exception(message) with SignalException(signal) {

  def this(signal: Signal[?])(using workflowCtx: WorkflowInstanceMeta) =
    this(signal, s"Cannot change signal value: $workflowCtx/$signal)")
}
