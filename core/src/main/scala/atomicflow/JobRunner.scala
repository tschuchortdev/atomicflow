package atomicflow

import scala.concurrent.duration.FiniteDuration

/** A started background job runner returned by
  * [[WorkflowRuntime.startJobRunner]]. It claims due wakeups from the shared
  * tables and executes the corresponding instances on its worker pool, driving
  * unattended workflows to completion.
  *
  * A runner is created already started and cooperates with every other runner —
  * in this process or another — through storage alone. Lifecycle: any number of
  * runners may be started on the same runtime, each with its own settings and
  * registry, and each stops independently; [[stop]] is idempotent and after
  * `stop` the runner is inert while others keep running.
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
