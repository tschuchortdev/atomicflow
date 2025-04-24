package atomicflow

import scala.annotation.implicitNotFound

@implicitNotFound("Cannot be used outside a Workflow definition: `Workflow(...) {  }`")
trait WorkflowContext[In, Out] {
  def workflow: Workflow[In, Out]

  def instanceId: WorkflowInstanceId

  def stepCache: StepCache.WithWorkflow

  def stepIdempotencyStore: StepIdempotencyStore.WithWorkflow
}
