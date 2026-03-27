package atomicflow

trait WorkflowException { self: Exception =>
}

/** A workflow or workflow instance was not found. */
class WorkflowNotFoundException(workflowId: WorkflowId,
                                instanceKey: Option[WorkflowInstanceKey],
                                message: String,
                                cause: Option[Throwable]
                               ) extends Exception(message, cause.orNull) with WorkflowException {

  def this(workflowId: WorkflowId,
           instanceKey: Option[WorkflowInstanceKey],
           cause: Option[Throwable] = None) =
    this(workflowId, instanceKey, s"Cannot find workflow with id=$workflowId ${instanceKey.map("instanceKey=" + _)}", cause)

  def this(workflowInstanceMeta: WorkflowInstanceMeta) =
    this(workflowInstanceMeta.workflowId, Some(workflowInstanceMeta.workflowInstanceKey), cause = None)

  def this(workflowInstanceMeta: WorkflowInstanceMeta, cause: Throwable) =
    this(workflowInstanceMeta.workflowId, Some(workflowInstanceMeta.workflowInstanceKey), cause = Some(cause))
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
