import Step.{StepId, StepIdempotencyId}
import Workflow.{WorkflowCtx, WorkflowId, WorkflowInstanceBuilder, WorkflowInstanceId, WorkflowMeta}
import io.github.iltotore.iron.*
import io.github.iltotore.iron.constraint.string.*

import scala.annotation.implicitNotFound
import scala.concurrent.TimeoutException
import scala.concurrent.duration.*

case class Workflow[In, Out] private(
                                      meta: WorkflowMeta,
                                      body: (WorkflowCtx[In, Out], In) => Out
                                    ) {
  def instance(instanceId: WorkflowInstanceId): WorkflowInstanceBuilder[In, Out] =
    WorkflowInstanceBuilder(
      this,
      instanceId
    )
}

object Workflow {
  val libraryVersion: Long = 0

  val defaultCacheTtl: FiniteDuration = 30.days

  def apply[In, Out](
                      id: WorkflowId | String :| ValidUUID,
                      name: String,
                      description: String | Unit = (),
                      cacheTtl: FiniteDuration = defaultCacheTtl
                    )(
                      body: In => WorkflowCtx[In, Out] ?=> Out
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
      body = { (ctx: WorkflowCtx[In, Out], in: In) =>
        body(in)(using ctx)
      }
    )
  }

  def meta[In, Out](using ctx: WorkflowCtx[In, Out]): WorkflowMeta = ctx.workflow.meta

  def instanceId[In, Out](using ctx: WorkflowCtx[In, Out]): WorkflowInstanceId = ctx.instanceId

  case class WorkflowMeta(
                           id: WorkflowId,
                           name: String,
                           description: Option[String],
                           cacheTtl: FiniteDuration
                         )

  case class WorkflowId(id: String :| ValidUUID)

  @implicitNotFound("Cannot be used outside a Workflow definition: `Workflow(...) {  }`")
  trait WorkflowCtx[In, Out] {
    def workflow: Workflow[In, Out]

    def instanceId: WorkflowInstanceId

    def stepCache: WorkflowStepCache

    def stepIdempotencyStore: WorkflowStepIdempotencyStore
  }

  case class WorkflowInstanceBuilder[In, Out] private[Workflow](
                                                          workflow: Workflow[In, Out],
                                                          instanceId: WorkflowInstanceId,
                                                          timeout: Option[FiniteDuration] = None,
                                                          stepIdempotencyIdOverrides: Map[StepId, StepIdempotencyId] = Map.empty
                                                        ) {
    def withTimeout(timeout: FiniteDuration): WorkflowInstanceBuilder[In, Out] =
      copy(timeout = Some(timeout))

    // TODO: mixing of immutable stuff like this with mutable/side-effecting stuff like createInstance
    def overrideStepIdempotencyId(stepId: StepId, stepIdempotencyId: StepIdempotencyId): WorkflowInstanceBuilder[In, Out] =
      copy(stepIdempotencyIdOverrides = stepIdempotencyIdOverrides + (stepId -> stepIdempotencyId))

    def create(in: In)(using runtime: WorkflowRuntime): Unit =
      runtime.createWorkflowInstance(this, in)
    
    @throws[TimeoutException]
    def run(in: In)(using runtime: WorkflowRuntime): Out =
      runtime.runWorkflowInstance(this, in)

    def recover()(using runtime: WorkflowRuntime): Out =
      runtime.recoverWorkflowInstance(this)
  }

  case class WorkflowInstanceId(id: String :| ValidUUID)

  object WorkflowInstanceId {
    def generate(using runtime: WorkflowRuntime): WorkflowInstanceId =
      runtime.generateWorkflowInstanceId
  }
}


trait StepCache {
  def get[Out](
                workflowId: WorkflowId,
                workflowInstanceId: WorkflowInstanceId,
                stepIdempotencyId: StepIdempotencyId,
                stepVersion: Long,
                stepInputs: Seq[StepInput[?]]
              ): Option[Out]

  def put[Out](
                workflowId: WorkflowId,
                workflowInstanceId: WorkflowInstanceId,
                stepIdempotencyId: StepIdempotencyId,
                stepVersion: Long,
                stepInputs: Seq[StepInput[?]],
                value: Out,
                ttl: FiniteDuration
              ): Unit
}

trait StepIdempotencyStore {
  def getOrCreateStepIdempotencyId(
                                    workflowId: WorkflowId,
                                    libraryVersion: Long,
                                    stepId: StepId,
                                    stepVersion: Option[Long],
                                    workflowInstanceId: WorkflowInstanceId,
                                    stepInputs: Seq[StepInput[?]]
                                  ): StepIdempotencyId

  def overrideStepIdempotencyId(
                                 workflowId: WorkflowId,
                                 libraryVersion: Long,
                                 stepId: StepId,
                                 workflowInstanceId: WorkflowInstanceId,
                                 stepIdempotencyId: StepIdempotencyId
                               ): Unit
}
