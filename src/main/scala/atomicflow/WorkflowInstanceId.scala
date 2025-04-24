package atomicflow

import io.github.iltotore.iron.*
import io.github.iltotore.iron.constraint.string.*

case class WorkflowInstanceId(id: String :| ValidUUID)

object WorkflowInstanceId {
  def generate(using runtime: WorkflowRuntime): WorkflowInstanceId =
    runtime.generateWorkflowInstanceId
}