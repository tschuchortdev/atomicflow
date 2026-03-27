package atomicflow.internal

import atomicflow.internal.StepInputFingerprints
import atomicflow.{StepIdempotencyId, StepInputConflictException}

import scala.concurrent.duration.FiniteDuration

/*
trait StepCache {
  self =>

  // TODO: throw conflict exception
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

  final def withWorkflowInstance(
                                  workflowId: WorkflowId,
                                  workflowInstanceId: WorkflowInstanceId,
                                  defaultTtl: FiniteDuration
                                ): WithWorkflow = new WithWorkflow {
    override def get[Out](
                           stepIdempotencyId: StepIdempotencyId,
                           stepVersion: Long,
                           stepInputs: Seq[StepInput[?]]
                         ): Option[Out] =
      self.get[Out](
        workflowId = workflowId,
        workflowInstanceId = workflowInstanceId,
        stepIdempotencyId = stepIdempotencyId,
        stepVersion = stepVersion,
        stepInputs = stepInputs
      )

    override def put[Out](
                           stepIdempotencyId: StepIdempotencyId,
                           stepVersion: Long,
                           stepInputs: Seq[StepInput[?]],
                           value: Out,
                           ttl: Option[FiniteDuration]
                         ): Unit =
      self.put[Out](
        workflowId = workflowId,
        workflowInstanceId = workflowInstanceId,
        stepIdempotencyId = stepIdempotencyId,
        stepVersion = stepVersion,
        stepInputs = stepInputs,
        value = value,
        ttl = ttl.getOrElse(defaultTtl)
      )
  }
}
*/
trait StepCache[Out] {
  /**
   * the output value should be retrieved by the stepIdempotencyId. The stepVersion and stepInputs should be compared on
   * retrieval and should throw a StepInputConflictException if they don't match
   */
  @throws[StepInputConflictException]
  def get(
           stepIdempotencyId: StepIdempotencyId,
           inputFingerprints: StepInputFingerprints
         ): Option[Out]

  /**
   * the output value should be cached by the stepIdempotencyId. The stepVersion and stepInputs should be compared on put
   * and should throw a StepInputConflictException if they don't match
   */
  @throws[StepInputConflictException]
  def put(
           stepIdempotencyId: StepIdempotencyId,
           inputFingerprints: StepInputFingerprints,
           value: Out,
           ttl: Option[FiniteDuration]
         ): Unit
}
