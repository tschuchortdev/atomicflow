package atomicflow

import scala.concurrent.duration.FiniteDuration

case class WorkflowMeta(
                         id: WorkflowId,
                         name: String,
                         description: Option[String]
                       )
