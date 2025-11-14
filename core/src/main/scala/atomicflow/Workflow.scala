package atomicflow

import atomicflow.Workflow.*

import java.time.Instant
import scala.util.control.ControlThrowable

case class Workflow[In: Cacheable, Out] private(
                                                 meta: WorkflowMeta,
                                                 body: (WorkflowContext[In, Out], In) => Out
                                               ) {
  def newInstance(instanceId: String): WorkflowInstanceBuilder[In, Out] =
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

  /*def apply[Out](
                  id: WorkflowId,
                  name: String,
                  description: String | Unit = ()
                )(
                  body: () => WorkflowContext[Unit, Out] ?=> Out
                ): Workflow[Unit, Out] =
    apply[Unit, Out](id, name, description)(_ => body())*/

  def meta[In, Out](using ctx: WorkflowContext[In, Out]): WorkflowMeta = ctx.meta

  def instanceId[In, Out](using ctx: WorkflowContext[In, Out]): WorkflowInstanceKey = ctx.instanceKey
}
