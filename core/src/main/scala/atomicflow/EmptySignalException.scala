package atomicflow

class EmptySignalException(message: String)
                          (using SimpleWorkflowContext)
  extends Exception(message) with WorkflowException {

  def this()(using workflowCtx: SimpleWorkflowContext) =
    this(s"Empty signal for workflow instance: $workflowCtx")
}
