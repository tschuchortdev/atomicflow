package atomicflow

import java.util.UUID
import scala.annotation.implicitNotFound
import scala.concurrent.duration.FiniteDuration

@implicitNotFound("No WorkflowRuntime available.\nAdd a using clause `(using WorkflowRuntime)` to the definition of the enclosing method.")
trait WorkflowRuntime {
  def generateWorkflowInstanceId: WorkflowInstanceId

  def generateStepIdempotencyId: StepIdempotencyId

  @throws[WorkflowInputConflictException]
  def createWorkflowInstance[In, Out](
                                       workflowInstance: WorkflowInstanceBuilder[In, Out],
                                       in: In
                                     )(
                                       using Cacheable[In]
                                     ): Unit

  /**
   * - Must lock the workflow while running
   */
  @throws[WorkflowInputConflictException]
  def runWorkflowInstance[In, Out](
                                    workflowInstance: WorkflowInstanceBuilder[In, Out],
                                    in: In
                                  )(
                                    using Cacheable[In]
                                  ): Out

  /**
   * - Must lock the workflow while running
   * - Must throw a WorkflowNotFoundException
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

    override def generateWorkflowInstanceId: WorkflowInstanceId = WorkflowInstanceId.unsafeMake(UUID.randomUUID().toString)

    override def generateStepIdempotencyId: StepIdempotencyId = StepIdempotencyId.unsafeMake(UUID.randomUUID().toString)

  }
}
