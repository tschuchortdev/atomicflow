package atomicflow

trait WorkflowException(using val workflowInstance: WorkflowInstanceMeta) { self: Exception =>
}

class WorkflowNotFoundException(message: String, cause: Option[Throwable] = None)
                               (using WorkflowInstanceMeta)
  extends Exception(message, cause.orNull) with WorkflowException {

  def this(cause: Option[Throwable])(using workflowInstance: WorkflowInstanceMeta) =
    this(s"Cannot find workflow instance: $workflowInstance", cause)

  def this()(using workflowInstance: WorkflowInstanceMeta) = this(cause = None)

}

class WorkflowLockedException(message: String, cause: Option[Throwable] = None)
                             (using WorkflowInstanceMeta)
  extends Exception(message, cause.orNull) with WorkflowException {

  def this(cause: Option[Throwable])(using workflowInstance: WorkflowInstanceMeta) =
    this(s"Cannot execute locked workflow instance: $workflowInstance", cause)

  def this()(using workflowInstance: WorkflowInstanceMeta) = this(cause = None)
}

class WorkflowInputConflictException(message: String, cause: Option[Throwable] = None)
                                    (using WorkflowInstanceMeta)
  extends RuntimeException(message, cause.orNull) with WorkflowException {

  def this(cause: Option[Throwable])(using workflowInstance: WorkflowInstanceMeta) =
    this(s"Cannot re-run workflow instance with different input: $workflowInstance", cause)

  def this()(using workflowInstance: WorkflowInstanceMeta) = this(cause = None)
}
