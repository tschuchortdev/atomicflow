package atomicflow

trait WorkflowException(using workflowCtx: SimpleWorkflowContext) {
  self: Exception =>

  def workflowMeta: WorkflowMeta = workflowCtx.meta

  def workflowInstanceId: WorkflowInstanceId = workflowCtx.instanceId
}

class WorkflowNotFoundException(message: String)
                               (using SimpleWorkflowContext)
  extends Exception(message) with WorkflowException {

  def this()(using workflowCtx: SimpleWorkflowContext) =
    this(s"Cannot find workflow instance: $workflowCtx")
}

class WorkflowLockedException(message: String)
                             (using SimpleWorkflowContext)
  extends Exception(message) with WorkflowException {

  def this()(using workflowCtx: SimpleWorkflowContext) =
    this(s"Cannot execute locked workflow instance: $workflowCtx")
}

class WorkflowInputConflictException(message: String)
                                    (using SimpleWorkflowContext)
  extends RuntimeException(message) with WorkflowException {

  def this()(using workflowCtx: SimpleWorkflowContext) =
    this(s"Cannot re-run workflow instance with different input: $workflowCtx")
}
