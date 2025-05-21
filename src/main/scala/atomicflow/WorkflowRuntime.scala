package atomicflow

import io.github.iltotore.iron.*

import java.util.UUID
import scala.annotation.implicitNotFound

@implicitNotFound("No WorkflowRuntime available.\nAdd a using clause `(using WorkflowRuntime)` to the definition of the enclosing method.")
trait WorkflowRuntime {
  def generateWorkflowInstanceId: WorkflowInstanceId

  def generateStepIdempotencyId: StepIdempotencyId

  @throws[WorkflowInputConflictException]
  def createWorkflowInstance[In, Out](workflowInstance: WorkflowInstanceBuilder[In, Out], in: In): Unit

  /**
   * - Must lock the workflow while running
   */
  @throws[WorkflowInputConflictException]
  def runWorkflowInstance[In, Out](workflowInstance: WorkflowInstanceBuilder[In, Out], in: In): Out

  /**
   * - Must lock the workflow while running
   * - Must throw a WorkflowNotFoundException
   */
  @throws[WorkflowNotFoundException]
  def recoverWorkflowInstance[In, Out](workflowInstance: WorkflowInstanceBuilder[In, Out]): Out
}

object WorkflowRuntime {
  trait GenerateIds extends WorkflowRuntime {

    override def generateWorkflowInstanceId: WorkflowInstanceId = WorkflowInstanceId(UUID.randomUUID().toString.refineUnsafe)

    override def generateStepIdempotencyId: StepIdempotencyId = StepIdempotencyId(UUID.randomUUID().toString.refineUnsafe)

  }
}
