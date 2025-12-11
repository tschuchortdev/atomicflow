package atomicflow

import java.net.URLEncoder
import java.nio.charset.StandardCharsets
import java.util.Objects

class WorkflowMeta(val workflowId: WorkflowId,
                   val workflowName: String,
                   val workflowDescription: Option[String]) {
  def this(other: WorkflowMeta) =
    this(other.workflowId, other.workflowName, other.workflowDescription)

  override def hashCode(): Int =
    Objects.hash(workflowId, workflowName, workflowDescription)

  override def equals(obj: Any): Boolean = obj match
    case other: WorkflowMeta if
      other.workflowId == this.workflowId
        && other.workflowName == this.workflowName
        && other.workflowDescription == this.workflowDescription => true
    case _ => false

  override def toString: String =
    s"workflow:${workflowId}#${URLEncoder.encode(workflowName, StandardCharsets.UTF_8)}"
}
