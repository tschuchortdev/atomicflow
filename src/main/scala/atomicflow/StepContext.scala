package atomicflow

import scala.annotation.implicitNotFound

@implicitNotFound("Cannot be used outside a Step definition: `Step(...) {  }`")
trait StepContext[Out] {
  def meta: StepMeta

  def workflowCtx: WorkflowContext[?, ?]

  def onComplete(f: Out => Unit): Unit

  private[atomicflow] def complete(out: Out): Unit
}
