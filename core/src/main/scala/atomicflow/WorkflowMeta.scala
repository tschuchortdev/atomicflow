package atomicflow

case class WorkflowMeta(
                         id: WorkflowId,
                         name: String,
                         description: Option[String]
                       )
