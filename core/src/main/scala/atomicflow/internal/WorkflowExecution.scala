package atomicflow.internal

import atomicflow.{SignalKey, StepId}

import java.time.Instant

/** The durable facts of one workflow_steps row, as read without any lease or
  * fence. `stateKind` is one of `started`, `succeeded`, `failed`; `statePayload`
  * is the raw, encoded outcome; `inputFingerprints` is the deterministic
  * encoding of the step's named inputs.
  */
private[atomicflow] final case class StoredStep(
    stateKind: String,
    statePayload: String,
    inputFingerprints: String,
    expiresAt: Option[Instant]
)

/** One durable `Signal` event that is currently visible to an awaiting site
  * (i.e. after the instance's shared exact-key cursor).
  */
private[atomicflow] final case class AwaitSignalCandidate(
    sequenceId: Long,
    payload: String,
    createdAt: Instant
)

/** One durable `TimerFired` event matching a pending timer subscription of an
  * awaiting site, keyed by the subscription id.
  */
private[atomicflow] final case class AwaitTimerCandidate(
    sequenceId: Long,
    subscriptionId: java.util.UUID,
    createdAt: java.time.Instant
)

/** The per-run engine seam, materialized only during execution and funneled to
  * workflow code through [[atomicflow.WorkflowContext.execution]]. It carries
  * the fencing identity of the current run so the engine can implement fenced
  * writes and lease-loss detection, and the step/await storage operations the
  * running body needs. The Postgres runtime implements it with a private class.
  */
private[atomicflow] trait WorkflowExecution {

  /** Identity of the worker holding the execution lease, e.g.
    * `"processUuid:threadId"`.
    */
  def workerId: String

  /** Monotonic fencing token of the current lease; bumped on every acquisition.
    * Stale writers are fenced out by it.
    */
  def fencingToken: Long

  /** The current step scope path (the enclosing `Workflow.scoped` path). `""`
    * for top-level steps; `Workflow.scoped` sets it in a later task.
    */
  def currentScope: String

  /** The runtime's notion of the current instant, from the injected clock. */
  def now: Instant

  /** Renews the execution lease of this run, extending `lease_expires_at` by the
    * runtime's `leaseDuration`. A fenced write that does not bump the fencing
    * token; throws [[atomicflow.LeaseLostException]] if the lease no longer
    * belongs to this run.
    */
  def renewLease(): Unit

  /** Read a step's durable facts (no lease/fence needed), or `None` if absent.
    * Reports the stored row even if it has expired.
    */
  def lookupStep(stepId: StepId, stepVersion: Long): Option[StoredStep]

  /** Replace (or create) the `started` row for the step, fenced. */
  def writeStepStarted(stepId: StepId, stepVersion: Long, stepKind: String, inputFingerprints: String): Unit

  /** Replace (or create) the `succeeded` row for the step, fenced. */
  def writeStepSucceeded(
      stepId: StepId,
      stepVersion: Long,
      stepKind: String,
      inputFingerprints: String,
      payload: String,
      expiresAt: Option[Instant]
  ): Unit

  /** Replace (or create) the `failed` row for the step, fenced. */
  def writeStepFailed(
      stepId: StepId,
      stepVersion: Long,
      stepKind: String,
      inputFingerprints: String,
      payload: String,
      expiresAt: Option[Instant]
  ): Unit

  /** Delete the step row, fenced. */
  def deleteStep(stepId: StepId, stepVersion: Long): Unit

  /** Read the durable `Signal` events of `signalKey` that are visible to this
    * instance (after its shared exact-key cursor), in sequence order. A plain
    * read of durable facts; no lease or fence is involved and the cursor is not
    * advanced.
    */
  def readAwaitSignalCandidates(signalKey: SignalKey): Vector[AwaitSignalCandidate]

  /** Resolve a signal await atomically, fenced: persist the `succeeded` step row
    * (`stepKind`), advance the exact-key cursor to `winningSequenceId`, and
    * delete the site's pending subscriptions, all in one transaction.
    */
  def resolveAwaitSignal(
      stepId: StepId,
      stepVersion: Long,
      stepKind: String,
      inputFingerprints: String,
      signalKey: SignalKey,
      winningSequenceId: Long,
      payload: String,
      expiresAt: Option[Instant]
  ): Unit

  /** Suspend a signal await, fenced, in one transaction: register/refresh the
    * pending subscription (idempotent), then re-read the visible events and
    * apply `decide`. If `decide` selects a satisfying candidate, resolve the
    * await (succeeded step row + cursor advance + subscription deletion) and
    * return the persisted payload; otherwise return `None` (suspended, cursor
    * unchanged). The re-read within the transaction closes the lost-wakeup gap
    * between event gathering and the suspension commit.
    */
  def suspendAwaitSignal(
      stepId: StepId,
      stepVersion: Long,
      stepKind: String,
      inputFingerprints: String,
      signalKey: SignalKey,
      expiresAt: Option[Instant]
  )(decide: Vector[AwaitSignalCandidate] => Option[(Long, String)]): Option[String]

  /** Fire this await-site's own due timer subscriptions, fenced, in one
    * transaction: for each subscription with `deadline <= now`, row-lock it,
    * re-check that no `TimerFired` event exists yet, and append one via the
    * global append protocol. The subscription row survives firing; only
    * resolution retires it.
    */
  def fireDueTimers(stepId: StepId, stepVersion: Long): Unit

  /** Read the durable `TimerFired` events matching this await-site's pending
    * timer subscriptions (plain durable read; no lock or fence).
    */
  def readAwaitTimerCandidates(stepId: StepId, stepVersion: Long): Vector[AwaitTimerCandidate]

  /** Resolve a timer await atomically, fenced: persist the `succeeded` step row
    * (`stepKind`) and delete the site's timer subscriptions in one transaction
    * (timers have no cursor).
    */
  def resolveAwaitTimer(
      stepId: StepId,
      stepVersion: Long,
      stepKind: String,
      inputFingerprints: String,
      payload: String,
      expiresAt: Option[Instant]
  ): Unit

  /** Suspend a timer await, fenced: register the pending subscription
    * idempotently. The subscription id is minted on first registration with the
    * given absolute `deadline` and reused on replay (the row persists and its
    * stored deadline is never recomputed).
    */
  def suspendAwaitTimer(
      stepId: StepId,
      stepVersion: Long,
      stepKind: String,
      inputFingerprints: String,
      deadline: java.time.Instant,
      expiresAt: Option[Instant]
  ): Unit

  /** Delete this await-site's timer subscriptions, fenced. Used to retire an
    * invalidated timer before re-registering a fresh incarnation.
    */
  def deleteTimerSubscriptions(stepId: StepId, stepVersion: Long): Unit
}
