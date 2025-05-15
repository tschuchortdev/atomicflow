package atomicflow

import atomicflow.Constants.defaultCacheTtl

import scala.concurrent.TimeoutException
import scala.concurrent.duration.FiniteDuration

case class WorkflowInstanceBuilder[In, Out] private[atomicflow](
                                                                 workflow: Workflow[In, Out],
                                                                 instanceId: WorkflowInstanceId,
                                                                 cacheTtl: FiniteDuration = defaultCacheTtl,
                                                                 stepIdempotencyIdOverrides: Map[StepId, StepIdempotencyId] = Map.empty
                                                               ) {
  def withCacheTtl(ttl: FiniteDuration): WorkflowInstanceBuilder[In, Out] =
    copy(cacheTtl = ttl)

  def overrideStepIdempotencyId(stepId: StepId, stepIdempotencyId: StepIdempotencyId): WorkflowInstanceBuilder[In, Out] =
    copy(stepIdempotencyIdOverrides = stepIdempotencyIdOverrides + (stepId -> stepIdempotencyId))

  @throws[WorkflowInputConflictException]
  def create(in: In)(using runtime: WorkflowRuntime): Unit =
    runtime.createWorkflowInstance(this, in)

  @throws[WorkflowInputConflictException]
  def run(in: In)(using runtime: WorkflowRuntime): Out =
    runtime.runWorkflowInstance(this, in)

  @throws[WorkflowInputConflictException]
  @throws[TimeoutException]
  def runWithTimeout(in: In, timeout: FiniteDuration)(using runtime: WorkflowRuntime): Out =
    ox.timeout(timeout) {
      run(in)
    }

  @throws[WorkflowNotFoundException]
  def recover()(using runtime: WorkflowRuntime): Out =
    runtime.recoverWorkflowInstance(this)

  @throws[WorkflowNotFoundException]
  @throws[TimeoutException]
  def recoverWithTimeout(timeout: FiniteDuration)(using runtime: WorkflowRuntime): Out =
    ox.timeout(timeout) {
      recover()
    }
}
