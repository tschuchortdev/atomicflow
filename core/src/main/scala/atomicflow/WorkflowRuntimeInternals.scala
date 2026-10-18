package atomicflow

import java.time.Clock
import scala.concurrent.duration.FiniteDuration

/** The shape core requires of a backend's per-run execution handle: the
  * instance row the backend re-read while holding the instance's lease. A
  * backend may carry additional run-scoped state (fencing identity, worker
  * id) on the concrete handle; core only consumes the instance row.
  */
trait LeaseHandle {

  /** The instance row re-read under the lease. Visible only to the library. */
  private[atomicflow] def instance: InstanceRecord
}

/** The storage and execution operations a backend implements for the library
  * core. Application code uses [[WorkflowRuntime]]; core reaches this view
  * through [[WorkflowContext]]. Every method that takes a [[Lease]] is fenced to
  * the run that holds it. */
trait WorkflowRuntimeInternals {

  /** Clock used for timer deadlines, retry delays, leases, and stored timestamps.
    * Workflow code and background jobs use this clock rather than database time.
    */
  def clock: Clock

  /** The durable-retry threshold of this runtime: a step retry whose computed
    * delay is at or below this sleeps inline inside the run; one whose delay is
    * above it becomes a durable suspension (an ordinary timer subscription).
    */
  def durableRetryThreshold: FiniteDuration

  /** How long a caller-thread run waits for a lease held by another worker
    * before giving up with [[LeaseUnavailableException]].
    */
  def leaseAcquireTimeout: FiniteDuration

  /** The per-run execution handle: created by the backend when it acquires the
    * lease, identity-stable for the run, and opaque to core. `instance` is the
    * instance row re-read under the lease.
    */
  type Lease <: LeaseHandle

  /** Runs `body` while holding the instance's execution lease and always
    * releases the lease afterwards. Returns `AlreadyTerminal` without running
    * `body` when the instance is already terminal.
    */
  def withLease[A](instance: WorkflowInstanceId, acquireTimeout: FiniteDuration)(
      body: Lease ?=> A
  ): LeasedRunResult[A]

  /** Renews the lock on the workflow instance. */
  def renewLease()(using Lease): Unit

  /** Reads whether cancellation has been requested. [[WorkflowRuntime.cancel]]
    * sets the stored flag once and never clears it. Steps and waits check it
    * before new work; cached results replay without checking it. No execution
    * lease is required.
    */
  def isCancellationRequested(instanceId: WorkflowInstanceId): Boolean

  /** Read a stored step without checking lease ownership, or `None` if absent.
    * Reports the stored row even if it has expired. This is the single read path
    * for steps; it is used both by `Step.runStep` and by `Step.getExecutionState`.
    */
  def readStep(
      instanceId: WorkflowInstanceId,
      stepId: StepId,
      stepVersion: Long
  ): Option[StoredStep]

  /** Replace (or create) the step row, fenced. The state, payload, input
    * fingerprints, and expiry all travel inside `step`: `Started` is an
    * unresolved row with an empty payload, `RetryPending` carries the retry
    * bookkeeping and the deadline at which the body may run again, and
    * `Succeeded`/`Failed` carry the terminal outcome. When `step.state` is
    * `RetryPending`, the site's internal retry timer subscription is replaced
    * with one at the new deadline; for every other state, the site's timer
    * subscriptions are deleted. The step row and its retry subscription are
    * written atomically, so a resolved or superseded retry can never re-fire.
    */
  def writeStep(stepId: StepId, stepVersion: Long, stepKind: StepKind, step: StoredStep)(using Lease): Unit

  /** Delete a step record and all its signal, timer, and completion subscriptions
    * in one transaction, checking execution ownership. Used when inputs change
    * or cached results expire, including pending retries.
    *
    * The caller evaluates the operation again after deletion. A new timer gets
    * a new id, so old firing events cannot satisfy it. If the process crashes
    * between deletion and evaluation, lease recovery or the next manual run
    * evaluates the operation and registers its subscriptions again.
    */
  def deleteStep(stepId: StepId, stepVersion: Long)(using Lease): Unit

  /** Reads the full persisted state of an instance, or `None` when absent. No
    * lease is required; serves both the run path and `getInfo`, and backs the
    * core-owned `blockUntilCompleted` poll.
    */
  def readInstance(instanceId: WorkflowInstanceId): Option[InstanceRecord]

  /** Read stored signals visible to this instance for `signalKey`, after its
    * cursor and in sequence order. No lease check is required and the cursor is not
    * advanced. Serves `Step.peekSignal`, so `subscriberKey` is always `""` on the
    * returned candidates.
    */
  def readAwaitSignalCandidates(signalKey: SignalKey)(using Lease): Vector[SubscriberMatch]

  /** Evaluate a wait-site, fenced, in two committed transactions. First, in its
    * own transaction, fire the site's own due timer subscriptions
    * (`deadline <= now`), row-locked in deadline order
    * (`ORDER BY deadline, timer_id`), appending a `TimerFired` event per
    * due subscription that has none yet under the global append protocol (no
    * wakeup upsert); this commits immediately, so a fired event persists even if
    * the evaluation that follows rolls back (a re-fire on a later evaluation is
    * an idempotent event-exists no-op). Then, in a second transaction, register
    * every subscriber idempotently (`ON CONFLICT DO NOTHING`), preserving an
    * existing timer subscription's timer id and stored deadline; read all
    * subscribers' candidates and apply `decide` to the candidates merged across
    * subscribers and sorted by global `sequenceId`. `decide` runs inside this
    * second transaction while the instance row lock is held, so it must be fast
    * and pure (no I/O, no blocking). If `decide` returns `Some(resolution)`,
    * persist the `succeeded` step row (`site.stepKind`), advance the winning
    * signal key's cursor when the resolution requests it, and delete the site's
    * rows in all three subscription tables — all atomically with the registration
    * and candidate read. If it returns `None`, commit the registrations and
    * advance each signal cursor past its examined, rejected events. Returns the
    * persisted payload when resolved, `None` when suspended. A `decide`
    * exception rolls the evaluation transaction back entirely; the earlier fire
    * transaction already committed.
    */
  def evaluateWait(
      site: WaitSite,
      subscribers: Vector[Subscriber]
  )(decide: Vector[SubscriberMatch] => Option[WaitResolution])(using Lease): Option[SerializedValue]

  /** Delete every subscription row whose `step_scope_within_instance` equals one
    * of `scopePaths` or lies beneath one (a longer array with one of `scopePaths`
    * as a prefix), across all three subscription tables (signal, timer,
    * completion), fenced. Used by
    * `Step.firstToRunWithoutSuspension` to retire every branch's pending
    * subscriptions — including nested subscopes — before the winner's step row
    * is written. The winner row is the stored
    * first-wins marker, so deleting first means a crash in between leaves no
    * marker, and the construct's next run re-evaluates from scratch and
    * re-deletes; a committed winner row therefore never coexists with branch
    * subscriptions.
    */
  def deleteSubscriptionsUnderScopePaths(scopePaths: Seq[ScopePath])(using Lease): Unit

  /** Read the persisted state of a `Workflow.restartable`/`Workflow.loop` region
    * located at `regionId` under `parentScopePath`: its encoded state and its
    * committed `restartCount`. `None` when the region has never been created
    * (first creation, so the by-name seed must be evaluated).
    */
  def readRegionState(regionId: String, parentScopePath: ScopePath)(using Lease): Option[RegionState]

  /** The single writer of a `Workflow.restartable`/`Workflow.loop` region's row:
    * persist exactly `restartCount` and `serializedState`, creating the row if
    * it does not exist (UPSERT, last writer wins). Before writing, discard the
    * nested Step rows and subscriptions owned by generation `restartCount - 1`'s
    * scope subtree (a construct-isolated prefix delete; a no-op when that
    * generation left nothing behind, which is always the case at
    * `restartCount == 0`) and close children created in that generation per
    * their `ParentClosePolicy`. Signal cursors are preserved. Fenced. The caller
    * invokes this only on first creation (`restartCount` 0, with the seed) or
    * on a restart (with the next count); replays read the persisted row instead.
    */
  def upsertRegion(
      regionId: String,
      parentScopePath: ScopePath,
      state: RegionState
  )(using Lease): Unit

  /** Start a child workflow of an executing parent: create-if-absent a child
    * instance under a scope derived from the parent's identity, generation, and
    * enclosing `Workflow.scoped` path; record the parent relationship and
    * inheritance configuration; and schedule the child's first wakeup. Never
    * executes the child body on the parent's thread. Idempotent on parent
    * replay (`startAsChild` returns the existing handle). The parent's identity
    * and generation come from the lease.
    */
  def startChild[In, Out](
      workflow: Workflow[In, Out],
      childKey: WorkflowInstanceKey,
      input: In,
      parentClosePolicy: ParentClosePolicy,
      inheritSignals: SignalInheritance,
      inheritPastEvents: Boolean,
      enclosingScopePath: ScopePath
  )(using Cacheable[In], Lease): WorkflowInstance[In, Out]

  /** Upsert a coalesced wakeup row for an instance, `ON CONFLICT DO NOTHING` so an
    * existing row's timestamps are never reset. The backend derives `scheduled_at`
    * from its own clock (`clock.now + delay`).
    */
  def upsertWakeup(instanceId: WorkflowInstanceId, delay: FiniteDuration): Unit

  /** Records the run's terminal outcome under the lease. Returns `Applied` with
    * the instance's unconsumed signals when this run made the transition, or
    * `AlreadyTerminal` when another writer already did.
    */
  def completeInstance(outcome: TerminalOutcome)(using Lease): CompletionResult

  /** Commits a continue-as-new transition with `nextInput` and returns the
    * instance's unconsumed signals.
    */
  def continueAsNew(nextInput: SerializedValue)(using Lease): Map[SignalKey, Seq[String]]
}
