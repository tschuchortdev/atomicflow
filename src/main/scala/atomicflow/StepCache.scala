package atomicflow

import atomicflow.StepCache.WithWorkflow

import scala.concurrent.duration.FiniteDuration

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

object StepCache {
  trait WithWorkflow {
    //@throws[IllegalStateException] TODO: throw conflict exception
    def get[Out](
                  stepIdempotencyId: StepIdempotencyId,
                  stepVersion: Long,
                  stepInputs: Seq[StepInput[?]]
                ): Option[Out]

    def put[Out](
                  stepIdempotencyId: StepIdempotencyId,
                  stepVersion: Long,
                  stepInputs: Seq[StepInput[?]],
                  value: Out,
                  ttl: Option[FiniteDuration]
                ): Unit
  }
}
