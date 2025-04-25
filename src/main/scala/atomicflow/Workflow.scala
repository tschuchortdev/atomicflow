package atomicflow

import atomicflow.Constants.defaultCacheTtl
import atomicflow.Workflow.*
import io.github.iltotore.iron.*
import io.github.iltotore.iron.constraint.string.*

import scala.concurrent.TimeoutException
import scala.concurrent.duration.*

case class Workflow[In, Out] private(
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
  def apply[In, Out](
                      id: WorkflowId | String :| ValidUUID,
                      name: String,
                      description: String | Unit = ()
                    )(
                      body: In => WorkflowContext[In, Out] ?=> Out
                    ): Workflow[In, Out] = {
    val workflowMeta = WorkflowMeta(
      id = id match {
        case workflowId: WorkflowId => workflowId
        case id: (String :| ValidUUID) @unchecked => WorkflowId(id)
      },
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
