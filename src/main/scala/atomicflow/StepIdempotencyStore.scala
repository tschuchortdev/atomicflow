package atomicflow

/*
trait StepIdempotencyStore {
  self =>

  def getOrCreateStepIdempotencyId(

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
*/
trait StepIdempotencyStore {
  /**
   * Get or create a stepIdempotencyId
   * The stepIdempotencyId should be namespaced by the tuple
   * (workflowId, libraryVersion, stepId, stepVersion, workflowInstanceId, stepInputs)
   */
  def acquireStepIdempotencyId(hashedStepInputs: HashedStepInputs): StepIdempotencyId

  /**
   * Get or create a stepIdempotencyId
   * The stepIdempotencyId should be namespaced by the tuple
   * (workflowId, libraryVersion, stepId, workflowInstanceId)
   */
  def acquireOnlyOnceStepIdempotencyId(): StepIdempotencyId

  def overrideOnlyOnceStepIdempotencyId(stepIdempotencyId: StepIdempotencyId): Unit
}
