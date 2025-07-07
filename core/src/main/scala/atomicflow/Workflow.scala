package atomicflow

import atomicflow.Workflow.*

case class Workflow[In: Cacheable, Out] private(
                                                 meta: WorkflowMeta,
                                                 body: (WorkflowContext[In, Out], In) => Out
                                               ) {
  def instance(instanceId: WorkflowInstanceId): WorkflowInstanceBuilder[In, Out] =
    WorkflowInstanceBuilder(
      this,
      instanceId
    )
}

object Workflow {
  def apply[In: Cacheable, Out](
                                 id: WorkflowId,
                                 name: String,
                                 description: String | Unit = ()
                               )(
                                 body: In => WorkflowContext[In, Out] ?=> Out
                               ): Workflow[In, Out] = {
    val workflowMeta = WorkflowMeta(
      id = id,
      name = name,
      description = description match {
        case () => None
        case string: String => Some(string)
      }
    )

    new Workflow[In, Out](
      meta = workflowMeta,
      body = { (ctx: WorkflowContext[In, Out], in: In) =>
        body(in)(using ctx)
      }
    )
  }

  def meta[In, Out](using ctx: WorkflowContext[In, Out]): WorkflowMeta = ctx.meta

  def instanceId[In, Out](using ctx: WorkflowContext[In, Out]): WorkflowInstanceId = ctx.instanceId
}
