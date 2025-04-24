package atomicflow

import scala.annotation.implicitNotFound
import scala.concurrent.TimeoutException
import scala.concurrent.duration.FiniteDuration

@implicitNotFound("No WorkflowRuntime available.\nAdd a using clause `(using WorkflowRuntime)` to the definition of the enclosing method.")
trait WorkflowRuntime {
  def generateWorkflowInstanceId: WorkflowInstanceId

  def generateStepIdempotencyId: StepIdempotencyId

  def createWorkflowInstance[In, Out](workflowInstance: WorkflowInstanceBuilder[In, Out], in: In): Unit

  /**
   * - Must lock the workflow while running
   */
  def runWorkflowInstance[In, Out](workflowInstance: WorkflowInstanceBuilder[In, Out], in: In): Out

  /**
   * - Must lock the workflow while running
   * - Must throw a WorkflowNotFoundException
   */
  def recoverWorkflowInstance[In, Out](workflowInstance: WorkflowInstanceBuilder[In, Out]): Out
}
