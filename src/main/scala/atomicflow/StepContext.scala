package atomicflow

import scala.annotation.implicitNotFound

@implicitNotFound("Cannot be used outside a Step definition: `Step(...) {  }`\nYou can require a StepContext for the enclosing method by adding a using clause `(using StepContext)` to its definition.")
trait StepContext[Out] {
  def meta: StepMeta

  def workflowCtx: WorkflowContext[?, ?]

  override def toString: String = s"$workflowCtx/step:${meta.id}#${meta.name}"

  def idempotencyStore: StepIdempotencyStore

  def cache(using Cacheable[Out]): StepCache[Out]

  def onComplete(f: Out => Unit): Unit

  def onCompensate(f: => Unit): Unit
}
