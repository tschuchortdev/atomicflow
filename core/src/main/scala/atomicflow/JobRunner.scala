package atomicflow

import scala.concurrent.duration.FiniteDuration

/** A running background worker returned by [[WorkflowRuntime.startJobRunner]].
  * Claims wakeups and executes workflow instances on its worker pool.
  *
  * Runners coordinate through storage, including across processes. Each has
  * independent settings, a workflow registry, and lifecycle.
  */
trait JobRunner {

  /** Stop claiming and await in-flight runs for up to `gracePeriod`. Idempotent.
    * Runs still executing when the grace period elapses are abandoned and later
    * recovered after their lease expires.
    */
  def stop(gracePeriod: FiniteDuration): Unit

  /** Executes one driver-loop iteration (sweeps + claim + dispatch) synchronously,
    * awaiting the dispatched runs, for deterministic scheduler stepping in tests.
    */
  private[atomicflow] def runDriverCycle(): Unit

  /** The instance ids claimed by the most recent [[runDriverCycle]], exposed for
    * package-private test assertions.
    */
  private[atomicflow] def lastClaimedBatch: Vector[WorkflowInstanceId]
}
