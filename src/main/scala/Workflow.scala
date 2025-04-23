import Step.{StepId, StepIdempotencyId}
import Workflow.{WorkflowCtx, WorkflowId, WorkflowInstance, WorkflowInstanceId, WorkflowMeta}
import io.github.iltotore.iron.*
import io.github.iltotore.iron.constraint.string.*

import scala.annotation.implicitNotFound
import scala.concurrent.TimeoutException
import scala.concurrent.duration.*

case class Workflow[In, Out] private(
                                      meta: WorkflowMeta,
                                      body: (WorkflowCtx[In, Out], In) => Out
                                    ) {
  def createInstance(instanceId: WorkflowInstanceId, in: In)(using runtime: WorkflowRuntime): WorkflowInstance[In, Out] =
    runtime.createWorkflowInstance(this, instanceId, in)

  def recoverInstance(instanceId: WorkflowInstanceId)(using runtime: WorkflowRuntime): WorkflowInstance[In, Out] =
    runtime.recoverWorkflowInstance(this, instanceId)
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

  case class WorkflowInstance[In, Out](
                                        workflow: Workflow[In, Out],
                                        instanceId: WorkflowInstanceId,
                                        stepIdempotencyIdOverrides: Map[StepId, StepIdempotencyId]
                                      ) {
    def overrideStepIdempotencyId(stepId: StepId, stepIdempotencyId: StepIdempotencyId): WorkflowInstance[In, Out] =
      copy(stepIdempotencyIdOverrides = stepIdempotencyIdOverrides + (stepId -> stepIdempotencyId))

    def run(using WorkflowMutex)(): Out = ???

    @throws[TimeoutException]
    def runWithTimeout(using WorkflowMutex)(timeout: FiniteDuration): Out = ???
  }

  case class WorkflowInstanceId(id: String :| ValidUUID)

  object WorkflowInstanceId {
    def make(using runtime: WorkflowRuntime): WorkflowInstanceId =
      runtime.makeWorkflowInstanceId
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
