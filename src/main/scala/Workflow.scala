import Step.{StepId, StepIdempotencyId}
import Workflow.{WorkflowCtx, WorkflowId, WorkflowInstance, WorkflowInstanceId}
import io.github.iltotore.iron.*
import io.github.iltotore.iron.constraint.string.*

import scala.concurrent.TimeoutException
import scala.concurrent.duration.*

case class Workflow[In, Out] private(
                                      id: WorkflowId,
                                      name: String,
                                      description: Option[String],
                                      cacheTtl: FiniteDuration,
                                      body: (WorkflowCtx[In, Out], In) => Out
                                    ) {
  def createInstance(instanceId: WorkflowInstanceId, in: In): WorkflowInstance[In, Out] = ???

  def recoverInstance(instanceId: WorkflowInstanceId): WorkflowInstance[In, Out] = ???
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
                    ): Workflow[In, Out] =
    new Workflow[In, Out](
      id = id match {
        case workflowId: WorkflowId => workflowId
        case id: (String :| ValidUUID) => WorkflowId(id)
      },
      name = name,
      description = description match {
        case () => None
        case string: String => Some(string)
      },
      cacheTtl = cacheTtl,
      body = { (ctx: WorkflowCtx[In, Out], in: In) =>
        body(in)(using ctx)
      }
    )

  def meta[In, Out](using ctx: WorkflowCtx[In, Out]): Workflow[In, Out] = ctx.workflow

  def instanceId[In, Out](using ctx: WorkflowCtx[In, Out]): WorkflowInstanceId = ctx.instanceId

  case class WorkflowId(id: String :| ValidUUID)

  trait WorkflowCtx[In, Out] {
    def workflow: Workflow[In, Out]

    def instanceId: WorkflowInstanceId

    def stepCache: WorkflowStepCache

    def stepIdempotencyStore: WorkflowStepIdempotencyStore
  }

  trait WorkflowInstance[In, Out] {
    def workflow: Workflow[In, Out]

    def instanceId: WorkflowInstanceId

    def run(using WorkflowMutex)(): Out

    @throws[TimeoutException]
    def runWithTimeout(using WorkflowMutex)(timeout: FiniteDuration): Out
  }

  case class WorkflowInstanceId(id: String :| ValidUUID)
}

trait WorkflowMutex {
  def lock(workflowId: WorkflowId, workflowInstanceId: WorkflowInstanceId): Unit

  def unlock(workflowId: WorkflowId, workflowInstanceId: WorkflowInstanceId): Unit
}

trait StepCache {
  def get[Out](
                workflowId: WorkflowId,
                workflowInstanceId: WorkflowInstanceId,
                stepIdempotencyId: StepIdempotencyId,
                stepInputs: Seq[StepInput[?]]
              ): Option[Out]

  def put[Out](
                workflowId: WorkflowId,
                workflowInstanceId: WorkflowInstanceId,
                stepIdempotencyId: StepIdempotencyId,
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
