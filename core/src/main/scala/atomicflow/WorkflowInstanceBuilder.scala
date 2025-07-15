package atomicflow

import atomicflow.Constants.defaultCacheTtl

import scala.concurrent.TimeoutException
import scala.concurrent.duration.FiniteDuration

case class WorkflowInstanceBuilder[In: Cacheable, Out] private[atomicflow](
                                                                            workflow: Workflow[In, Out],
                                                                            instanceId: WorkflowInstanceId,
                                                                            cacheTtl: FiniteDuration = defaultCacheTtl,
                                                                            stepIdempotencyIdOverrides: Map[StepId, StepIdempotencyId] = Map.empty
                                                                          ) {
  private[atomicflow] def simpleWorkflowCtx: SimpleWorkflowContext = SimpleWorkflowContext(
    workflow.meta,
    instanceId
  )

  def withCacheTtl(ttl: FiniteDuration): WorkflowInstanceBuilder[In, Out] =
    copy(cacheTtl = ttl)

  def overrideStepIdempotencyId(stepId: StepId, stepIdempotencyId: StepIdempotencyId): WorkflowInstanceBuilder[In, Out] =
    copy(stepIdempotencyIdOverrides = stepIdempotencyIdOverrides + (stepId -> stepIdempotencyId))

  @throws[WorkflowInputConflictException]
  def create(in: In)(using runtime: WorkflowRuntime): Unit =
    runtime.createWorkflowInstance(this, in)

  @throws[WorkflowInputConflictException]
  inline def create()(using runtime: WorkflowRuntime, ev: Unit =:= In): Unit =
    create(())

  @throws[WorkflowInputConflictException]
  def run(in: In)(using runtime: WorkflowRuntime): Out =
    runtime.runWorkflowInstance(this, in)

  @throws[WorkflowInputConflictException]
  inline def run()(using runtime: WorkflowRuntime, ev: Unit =:= In): Out =
    run(())

  @throws[WorkflowInputConflictException]
  @throws[TimeoutException]
  def runWithTimeout(in: In, timeout: FiniteDuration)(using runtime: WorkflowRuntime): Out =
    ox.timeout(timeout) {
      run(in)
    }

  @throws[WorkflowInputConflictException]
  @throws[TimeoutException]
  inline def runWithTimeout(timeout: FiniteDuration)(using runtime: WorkflowRuntime, ev: Unit =:= In): Out =
    runWithTimeout((), timeout)

  @throws[WorkflowNotFoundException]
  def recover()(using runtime: WorkflowRuntime): Out =
    runtime.recoverWorkflowInstance(this)

  @throws[WorkflowNotFoundException]
  @throws[TimeoutException]
  def recoverWithTimeout(timeout: FiniteDuration)(using runtime: WorkflowRuntime): Out =
    ox.timeout(timeout) {
      recover()
    }

  /*
  A signal value can be set even if the workflow instance doesn't exist yet.
  TODO: should it work like this or should the user create the workflow first? Setting a value of a non-existent workflow could lead to bugs
   */
  def setSignal[A](signal: Signal[A], value: A)(using runtime: WorkflowRuntime): Unit =
    runtime.setSignal(signal, value)(using simpleWorkflowCtx)
}
