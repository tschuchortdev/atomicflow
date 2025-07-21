package atomicflow

import atomicflow.Constants.{defaultCacheTtl, defaultSignalTtl}

import scala.concurrent.TimeoutException
import scala.concurrent.duration.FiniteDuration

case class WorkflowInstanceBuilder[In: Cacheable, Out] private[atomicflow](
                                                                            workflow: Workflow[In, Out],
                                                                            instanceId: WorkflowInstanceId,
                                                                            defaultCacheTtl: FiniteDuration = defaultCacheTtl,
                                                                            defaultSignalTtl: FiniteDuration = defaultSignalTtl,
                                                                            stepIdempotencyIdOverrides: Map[StepId, StepIdempotencyId] = Map.empty
                                                                          ) {
  private[atomicflow] def simpleWorkflowCtx: SimpleWorkflowContext = SimpleWorkflowContext(
    workflow.meta,
    instanceId
  )

  def withDefaultCacheTtl(ttl: FiniteDuration): WorkflowInstanceBuilder[In, Out] =
    copy(defaultCacheTtl = ttl)

  def withDefaultSignalTtl(ttl: FiniteDuration): WorkflowInstanceBuilder[In, Out] =
    copy(defaultSignalTtl = ttl)

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

  @throws[WorkflowNotFoundException]
  @throws[SignalConflictException]
  def setSignalFor[A](ttl: FiniteDuration)(signal: Signal[A], value: A)(using runtime: WorkflowRuntime): Unit =
    runtime.setSignal(signal, value, ttl)(using simpleWorkflowCtx)

  @throws[WorkflowNotFoundException]
  @throws[SignalConflictException]
  def setSignal[A](signal: Signal[A], value: A)(using runtime: WorkflowRuntime): Unit =
    runtime.setSignal(signal, value, defaultSignalTtl)(using simpleWorkflowCtx)
}
