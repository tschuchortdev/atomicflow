package atomicflow.impl.db

/** Test seam for injecting faults at the durable boundaries of the Postgres
  * runtime. Hooks named `beforeX` run inside their transaction (via
  * `doobie.free.connection.delay`), after any fence check, so a throw rolls the
  * transaction back; hooks named `afterX` run after their transaction commits.
  * Exceptions to the commit boundary and conditional firings:
  *
  *   - `afterCreateInsert` and `afterWakeupInsert` run inside the
  *     create-and-schedule transaction, not after it. `afterCreateInsert` fires
  *     only when the instance insert actually inserted a new row (a plain
  *     `createWorkflowInstance` fires it too); `afterWakeupInsert` fires after
  *     the upsert attempt, even when `ON CONFLICT DO NOTHING` inserted nothing.
  *   - The step `afterX` hooks are state-specific: `afterStepStartedWrite` fires
  *     for a `Started` row, `afterStepSuccessWrite` for `Succeeded`, and
  *     `afterStepFailureWrite` for `Failed`.
  *   - `afterTimerFireCommit` fires only when a timer transaction appended a new
  *     `TimerFired` event. `afterSubscriptionRegistration` fires only when an
  *     await transaction inserted at least one new subscription and did not
  *     resolve; `afterWaitResolution` fires when it resolved.
  *   - `afterLeaseAcquire` fires after lease acquisition (including the job
  *     runner's claim transaction). `afterLeaseRelease` fires only when the
  *     guarded release actually cleared the lease; `afterRestoreWakeup` fires
  *     only when the non-terminal branch restored the wakeup.
  *   - `afterChildClose` fires only when a sweep or run-start self-check actually
  *     applied a close.
  */
private[atomicflow] trait FaultInjector {
  def afterCreateInsert(): Unit = ()
  def afterWakeupInsert(): Unit = ()
  def afterCreateAndScheduleCommit(): Unit = ()

  def afterStepStartedWrite(): Unit = ()
  def beforeStepSuccessWrite(): Unit = ()
  def afterStepSuccessWrite(): Unit = ()
  def beforeStepFailureWrite(): Unit = ()
  def afterStepFailureWrite(): Unit = ()

  def afterEventAppendCommit(): Unit = ()
  def afterTimerFireCommit(): Unit = ()
  def afterSubscriptionRegistration(): Unit = ()
  def afterWaitResolution(): Unit = ()

  def afterTerminalTransition(): Unit = ()
  def afterContinueAsNew(): Unit = ()
  def afterReset(): Unit = ()
  def afterChildCreate(): Unit = ()
  def afterChildClose(): Unit = ()

  def afterLeaseAcquire(): Unit = ()
  def beforeLeaseRelease(): Unit = ()
  def afterLeaseRelease(): Unit = ()
  def afterRestoreWakeup(): Unit = ()
}

private[atomicflow] object FaultInjector { object Noop extends FaultInjector }
