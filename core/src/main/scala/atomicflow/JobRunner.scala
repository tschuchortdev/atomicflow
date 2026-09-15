package atomicflow

import scala.concurrent.duration.FiniteDuration

/** A started background job runner returned by
  * [[WorkflowRuntime.startJobRunner]]. It claims due wakeups from the shared
  * tables and executes the corresponding instances on its worker pool, driving
  * unattended workflows to completion.
  *
  * A runner is created already started and cooperates with every other process's
  * runner through storage alone. Lifecycle: calling `startJobRunner` again on the
  * same runtime while this runner is active throws; [[stop]] is idempotent; after
  * `stop` the factory may be called again.
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
