package atomicflow

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
                      description: String | Unit = (),
                      cacheTtl: FiniteDuration = defaultCacheTtl
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
      },
      cacheTtl = cacheTtl
    )

    Workflow[In, Out](
      meta = workflowMeta,
      body = { (ctx: WorkflowContext[In, Out], in: In) =>
        body(in)(using ctx)
      }
    )
  }

  def meta[In, Out](using ctx: WorkflowContext[In, Out]): WorkflowMeta = ctx.workflow.meta

  def instanceId[In, Out](using ctx: WorkflowContext[In, Out]): WorkflowInstanceId = ctx.instanceId
}


case class WorkflowMeta(
                         id: WorkflowId,
                         name: String,
                         description: Option[String],
                         cacheTtl: FiniteDuration
                       )

case class WorkflowInstanceBuilder[In, Out] private[atomicflow](
                                                                 workflow: Workflow[In, Out],
                                                                 instanceId: WorkflowInstanceId,
                                                                 cacheTtl: FiniteDuration = defaultCacheTtl,
                                                                 stepIdempotencyIdOverrides: Map[StepId, StepIdempotencyId] = Map.empty
                                                               ) {
  def withCacheTtl(ttl: FiniteDuration): WorkflowInstanceBuilder[In, Out] =
    copy(cacheTtl = ttl)

  def overrideStepIdempotencyId(stepId: StepId, stepIdempotencyId: StepIdempotencyId): WorkflowInstanceBuilder[In, Out] =
    copy(stepIdempotencyIdOverrides = stepIdempotencyIdOverrides + (stepId -> stepIdempotencyId))

  def create(in: In)(using runtime: WorkflowRuntime): Unit =
    runtime.createWorkflowInstance(this, in)

  def run(in: In)(using runtime: WorkflowRuntime): Out =
    runtime.runWorkflowInstance(this, in)

  @throws[TimeoutException]
  def runWithTimeout(in: In, timeout: FiniteDuration)(using runtime: WorkflowRuntime): Out =
    run(in) // TODO: timeout

  def recover()(using runtime: WorkflowRuntime): Out =
    runtime.recoverWorkflowInstance(this)

  @throws[TimeoutException]
  def recoverWithTimeout(timeout: FiniteDuration)(using runtime: WorkflowRuntime): Out =
    recover() // TODO: timeout
}
