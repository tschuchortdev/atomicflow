import Workflow.{WorkflowInstance, WorkflowInstanceId}

trait WorkflowRuntime {
  def makeWorkflowInstanceId: WorkflowInstanceId

  def createWorkflowInstance[In, Out](workflow: Workflow[In, Out], workflowInstanceId: WorkflowInstanceId, in: In): WorkflowInstance[In, Out]

  def recoverWorkflowInstance[In, Out](workflow: Workflow[In, Out], workflowInstanceId: WorkflowInstanceId): WorkflowInstance[In, Out]
}
