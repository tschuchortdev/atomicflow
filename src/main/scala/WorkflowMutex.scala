import Workflow.{WorkflowId, WorkflowInstanceId}

trait WorkflowMutex {
  def lock(workflowId: WorkflowId, workflowInstanceId: WorkflowInstanceId): Unit

  def unlock(workflowId: WorkflowId, workflowInstanceId: WorkflowInstanceId): Unit
}