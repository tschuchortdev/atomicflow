package atomicflow

import java.util.UUID
import scala.annotation.implicitNotFound
import scala.concurrent.duration.FiniteDuration

@implicitNotFound("No WorkflowRuntime available.\nAdd a using clause `(using WorkflowRuntime)` to the definition of the enclosing method.")
trait WorkflowRuntime {
  def generateWorkflowInstanceId: String

  def generateStepIdempotencyId: StepIdempotencyId

  // TODO: Why should anyone ever use this?
  /**
   * Registers this workflow instance but doesn't run it.
   * @throws WorkflowInputConflictException when a workflow instance was previously created with non-equal input
   */
  @throws[WorkflowInputConflictException]
  def createWorkflowInstance[In, Out](
                                       workflowInstance: WorkflowInstanceBuilder[In, Out],
                                       in: In
                                     )(
                                       using Cacheable[In]
                                     ): Unit

  /**
   * Runs a workflow instance. The workflow instance is created if it doesn't already exist.
   *
   * Implementations must lock the workflow while running to prevent concurrent executions of the same workflow instance.
   */
  @throws[WorkflowInputConflictException]
  def runWorkflowInstance[In, Out](
                                    workflowInstance: WorkflowInstanceBuilder[In, Out],
                                    in: In
                                  )(
                                    using Cacheable[In]
                                  ): Out

  /**
   * Runs a workflow instance that was previously created. If the workflow instance ran before and didn't finish,
   * it will run again from the beginning, but cached steps will not be executed again.
   *
   * Implementations must lock the workflow while running to prevent concurrent executions of the same workflow instance.
   * @throws WorkflowNotFoundException when no workflow instance with this ID exists
   */
  @throws[WorkflowNotFoundException]
  def recoverWorkflowInstance[In, Out](
                                        workflowInstance: WorkflowInstanceBuilder[In, Out]
                                      )(
                                        using Cacheable[In]
                                      ): Out

  @throws[WorkflowNotFoundException]
  @throws[SignalConflictException]
  def setSignal[A](
                    signal: Signal[A],
                    value: A,
                    ttl: FiniteDuration
                  )(using SimpleWorkflowContext): Unit
}

object WorkflowRuntime {
  trait GenerateIds extends WorkflowRuntime {

    override def generateWorkflowInstanceId: String = UUID.randomUUID().toString

    override def generateStepIdempotencyId: StepIdempotencyId = StepIdempotencyId.unsafeMake(UUID.randomUUID().toString)

  }
}
