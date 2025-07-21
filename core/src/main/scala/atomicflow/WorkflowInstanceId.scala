package atomicflow

import neotype.*

type WorkflowInstanceId = WorkflowInstanceId.Type

object WorkflowInstanceId extends Newtype[String] {
  override inline def validate(input: String): Boolean | String =
    if (input.matches("[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}")) true
    else "Should be a UUID"

  def generate(using runtime: WorkflowRuntime): WorkflowInstanceId =
    runtime.generateWorkflowInstanceId
}