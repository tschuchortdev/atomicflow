package atomicflow

import atomicflow.Fingerprintable.Fingerprinter
import atomicflow.internal.{SignalStore, StepCache, StepIdempotencyStore}

import java.net.URLEncoder
import java.nio.charset.StandardCharsets
import java.util.Objects
import scala.annotation.implicitNotFound
import scala.concurrent.duration.FiniteDuration

class WorkflowInstanceMeta(val workflowInstanceKey: WorkflowInstanceKey, private val workflowMeta: WorkflowMeta)
    extends WorkflowMeta(workflowMeta) {

  def this(other: WorkflowInstanceMeta) =
    this(other.workflowInstanceKey, other.workflowMeta)
  
  override def equals(obj: Any): Boolean = obj match
    case other: WorkflowInstanceMeta if
      super.equals(other) && other.workflowInstanceKey == this.workflowInstanceKey => true
    case _ => false

  override def hashCode(): Int = Objects.hash(workflowInstanceKey, workflowMeta)

  override lazy val toString: String = s"${workflowMeta.toString}/$workflowInstanceKey"
}

@implicitNotFound("Cannot be used outside a Workflow definition: `Workflow(...) {  }`\nYou can require a WorkflowContext for the enclosing method by adding a using clause `(using WorkflowContext)` to its definition.")
case class WorkflowContext(
  workflowInstanceMeta: WorkflowInstanceMeta,
  workflowRuntime: WorkflowRuntime,
  defaultCacheTtl: FiniteDuration,
  subworkflowScope: Vector[String] = Vector.empty
) {
  def withSubworkflowScope(scopeKey: String): WorkflowContext =
    this.copy(subworkflowScope = this.subworkflowScope.appended(scopeKey))
}
object WorkflowContext {
  given (ctx: WorkflowContext) => WorkflowInstanceMeta = ctx.workflowInstanceMeta
}
