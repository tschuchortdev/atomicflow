package atomicflow

import scala.annotation.implicitNotFound

trait SimpleWorkflowContext {
  def meta: WorkflowMeta

  def instanceId: WorkflowInstanceId

  override def toString: String = s"workflow:${meta.id}#${meta.name}/$instanceId"
}

object SimpleWorkflowContext {
  given (stepCtx: StepContext[?]) => SimpleWorkflowContext = stepCtx.workflowCtx
}

@implicitNotFound("Cannot be used outside a Workflow definition: `Workflow(...) {  }`\nYou can require a WorkflowContext for the enclosing method by adding a using clause `(using WorkflowContext)` to its definition.")
trait WorkflowContext[In, Out] extends SimpleWorkflowContext {
  protected[atomicflow] def getStepIdempotencyStore(using StepContext[?]): StepIdempotencyStore

  protected[atomicflow] def getStepCache[StepOut: Cacheable](using StepContext[StepOut]): StepCache[StepOut]
}
