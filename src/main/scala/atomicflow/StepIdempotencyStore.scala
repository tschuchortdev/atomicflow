package atomicflow

trait StepIdempotencyStore {
  self =>

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

  final def withWorkflowInstance(
                                  workflowId: WorkflowId,
                                  libraryVersion: Long,
                                  workflowInstanceId: WorkflowInstanceId
                                ): StepIdempotencyStore.WithWorkflow = new StepIdempotencyStore.WithWorkflow {
    override def getOrCreateStepIdempotencyId(
                                               stepId: StepId,
                                               stepVersion: Option[Long],
                                               stepInputs: Seq[StepInput[?]]
                                             ): StepIdempotencyId =
      self.getOrCreateStepIdempotencyId(
        workflowId = workflowId,
        libraryVersion = libraryVersion,
        stepId = stepId,
        stepVersion = stepVersion,
        workflowInstanceId = workflowInstanceId,
        stepInputs = stepInputs
      )

    override def overrideStepIdempotencyId(
                                            stepId: StepId,
                                            stepIdempotencyId: StepIdempotencyId
                                          ): Unit =
      self.overrideStepIdempotencyId(
        workflowId = workflowId,
        libraryVersion = libraryVersion,
        stepId = stepId,
        workflowInstanceId = workflowInstanceId,
        stepIdempotencyId = stepIdempotencyId
      )
  }
}

object StepIdempotencyStore {
  trait WithWorkflow {
    def getOrCreateStepIdempotencyId(
                                      stepId: StepId,
                                      stepVersion: Option[Long],
                                      stepInputs: Seq[StepInput[?]]
                                    ): StepIdempotencyId

    def overrideStepIdempotencyId(
                                   stepId: StepId,
                                   stepIdempotencyId: StepIdempotencyId
                                 ): Unit
  }
}
