package atomicflow

import java.util.concurrent.Executor
import scala.concurrent.duration.*

/** Tunables for [[WorkflowRuntime.startJobRunner]]. The runner owns a daemon
  * worker pool sized by [[workerThreads]] unless [[executor]] overrides it with
  * a user-supplied [[java.util.concurrent.Executor]].
  *
  * @param workerThreads
  *   runtime-owned daemon worker pool size; ignored when `executor` is set
  * @param executor
  *   user-supplied pool override; when set, the runner never shuts it down
  * @param maxConcurrentInstances
  *   per-process cap on concurrently executing instances per workflowId
  * @param capacityRetryDelay
  *   in-place wakeup defer when a per-workflow cap is reached
  * @param perWorkflowBatchShare
  *   fairness: max claimed wakeups per workflowId per batch
  * @param pollInterval
  *   wakeup poll cadence; jittered per process
  * @param wakeupBatchSize
  *   max wakeups claimed per cycle
  * @param timerSweepInterval
  *   cadence of the timer-firing sweep
  * @param timerBatchSize
  *   max timers fired per sweep pass
  * @param sweepInterval
  *   cadence of the escalation and lease-recovery sweeps
  * @param leaseDuration
  *   execution lease lifetime; must exceed the longest gap between heartbeats
  * @param leaseAcquireTimeout
  *   bound an external run waits for a leased instance
  * @param cancelTimeout
  *   how long a cancellation may run before escalation to TERMINATED
  * @param throwableCacheable
  *   codec used to encode runner-driven terminal failure outcomes
  * @param workerId
  *   worker identity recorded under `workflow_instances.lease_owner` for this
  *   runner's claims and leases; any stable string, and each concurrently
  *   running runner should have its own so lease holders stay attributable.
  *   `None` (the default) means the runner generates a unique id
  */
final case class JobRunnerSettings(
    workerThreads: Int = 8,
    executor: Option[Executor] = None,
    maxConcurrentInstances: WorkflowId => Int = _ => Int.MaxValue,
    capacityRetryDelay: FiniteDuration = 1.second,
    perWorkflowBatchShare: Int = 8,
    pollInterval: FiniteDuration = 250.millis,
    wakeupBatchSize: Int = 32,
    timerSweepInterval: FiniteDuration = 250.millis,
    timerBatchSize: Int = 128,
    sweepInterval: FiniteDuration = 1.second,
    leaseDuration: FiniteDuration = 5.minutes,
    leaseAcquireTimeout: FiniteDuration = 30.seconds,
    cancelTimeout: FiniteDuration = 5.minutes,
    throwableCacheable: Cacheable[Throwable] = Cacheable.forThrowable.genericStringMessageSerializer,
    workerId: Option[String] = None
)

object JobRunnerSettings {

  /** Production defaults: modest worker count, a few hundred millisecond poll. */
  def default: JobRunnerSettings = JobRunnerSettings()

  /** Test-friendly defaults: one worker for deterministic ordering, tiny
    * intervals for prompt background progress, short lease/cancel timeouts so
    * recovery and escalation tests run quickly.
    */
  def forTests: JobRunnerSettings =
    JobRunnerSettings(
      workerThreads = 1,
      pollInterval = 25.millis,
      timerSweepInterval = 25.millis,
      sweepInterval = 50.millis,
      leaseDuration = 5.seconds,
      leaseAcquireTimeout = 1.second,
      cancelTimeout = 500.millis
    )
}
