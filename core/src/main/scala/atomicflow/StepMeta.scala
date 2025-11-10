package atomicflow

case class StepMeta(
                     id: StepId,
                     version: Long,
                     name: Option[String],
                     description: Option[String],
                     workflowMeta: WorkflowMeta,
                     workflowInstanceId: WorkflowInstanceKey
                   )
