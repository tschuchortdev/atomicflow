package atomicflow

import atomicflow.Fingerprintable.Fingerprinter
import atomicflow.internal.{SignalStore, StepCache, StepIdempotencyStore}

import java.net.URLEncoder
import java.nio.charset.StandardCharsets
import java.util.Objects
import scala.annotation.implicitNotFound
import scala.concurrent.duration.FiniteDuration


@implicitNotFound("Cannot be used outside a Workflow definition: `Workflow(...) {  }`\nYou can require a WorkflowContext for the enclosing method by adding a using clause `(using WorkflowContext)` to its definition.")
case class WorkflowContext(
  workflowInstanceMeta: WorkflowInstanceMeta,
  workflowRuntime: WorkflowRuntime,
  defaultCacheTtl: Option[FiniteDuration] = None,
  subworkflowScope: Vector[String] = Vector.empty
) {
  def withSubworkflowScope(scopeKey: String): WorkflowContext =
    this.copy(subworkflowScope = this.subworkflowScope.appended(scopeKey))
}
object WorkflowContext {
  // TODO: Muss man manchmal explizit importieren... sehr nervig.
  given (ctx: WorkflowContext) => WorkflowInstanceMeta = ctx.workflowInstanceMeta
}
