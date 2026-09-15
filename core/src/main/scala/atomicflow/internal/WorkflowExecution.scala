package atomicflow.internal

import atomicflow.{SignalKey, StepId, WorkflowId, WorkflowInstanceKey}

import java.time.Instant
import scala.concurrent.duration.FiniteDuration

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

/** One unhandled `Update` record visible to an awaiting site (a row in
  * `workflow_updates` with `handled_at IS NULL`). The row is identified by
  * `createdAt` plus `idempotencyKey`; `encodedInput` is the update's payload.
  */
private[atomicflow] final case class UpdateCandidate(
    createdAt: java.time.Instant,
    idempotencyKey: String,
    encodedInput: String
)

/** The outcome of an `awaitUpdate` that satisfies a candidate: which record was
  * handled, the encoded synchronous `response` to persist on the record (what
  * the blocked sender reads), and the encoded `output` to persist on the step
  * row (what the workflow body receives).
  */
private[atomicflow] final case class AwaitUpdateDecision(
    candidate: UpdateCandidate,
    encodedResponse: String,
    encodedOutput: String
)

/** A single leaf of a `Step.awaitRace` site, describing how its durable
  * subscription is registered in the corresponding table. The leaf index is the
  * awaitable's position within the race (0..n-1).
  */
private[atomicflow] sealed trait AwaitRaceLeaf {
  def leafIdx: Int
}

/** A signal leaf: subscribes in `workflow_signal_subscriptions` under its exact
  * key and reads candidates after this instance's shared exact-key cursor.
  */
private[atomicflow] final case class AwaitRaceSignalLeaf(leafIdx: Int, signalKey: SignalKey) extends AwaitRaceLeaf

/** A timer leaf: subscribes in `workflow_timer_subscriptions` with the absolute
  * `deadline` and reads candidates from fired `TimerFired` events keyed by its
  * subscription id.
  */
private[atomicflow] final case class AwaitRaceTimerLeaf(leafIdx: Int, deadline: java.time.Instant) extends AwaitRaceLeaf

/** A completion leaf: subscribes in `workflow_completion_subscriptions` for the
  * terminal outcome of another instance and reads candidates from that
  * instance's `WorkflowCompleted` event (`event_key = ''`).
  */
private[atomicflow] final case class AwaitRaceCompletionLeaf(
    leafIdx: Int,
    completedWorkflowId: WorkflowId,
    completedKey: WorkflowInstanceKey,
    completedScope: String
) extends AwaitRaceLeaf

/** One durable candidate for a race leaf, selected by global `sequenceId`
  * across all leaves so the earliest satisfying event wins regardless of kind or
  * workflow tree.
  */
private[atomicflow] final case class AwaitRaceCandidate(
    sequenceId: Long,
    leafIdx: Int,
    payload: String,
    createdAt: java.time.Instant
)

/** The outcome of a satisfied race: the winning event's global sequence id, the
  * serialized result persisted to the step row, and (only when the winning leaf
  * is a signal) the exact key whose shared cursor advances.
  */
private[atomicflow] final case class AwaitRaceDecision(
    winningSequenceId: Long,
    payload: String,
    advanceSignalKey: Option[SignalKey]
)

/** A point-in-time capture of a thread's transient execution context (the
  * `Workflow.scoped` scope stack and the `Workflow.uncancellable` depth), used
  * to propagate the enclosing context into forked parallel-branch threads.
  */
private[atomicflow] final case class BranchContextSnapshot(
    scopeStack: Vector[String],
    uncancellableDepth: Int
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
    * for top-level steps.
    */
  def currentScope: String

  /** The instance's current run generation (incremented by `continueAsNew`).
    * Used to derive child instance scopes (the `@generation` marker), so
    * children started before and after a `continueAsNew` do not collide.
    */
  def generation: Long

  /** Pushes one already-escaped `Workflow.scoped` segment onto the current
    * thread's scope stack, so subsequent Step/Await IDs are prefixed with it.
    * Must be balanced by a matching [[popScope]] (paired in a `try/finally`).
    * The stack is per-thread transient state, so parallel branches with
    * different scoped keys do not corrupt each other's Step IDs.
    */
  private[atomicflow] def pushScope(escapedSegment: String): Unit

  /** Pops the innermost `Workflow.scoped` segment pushed by [[pushScope]]. */
  private[atomicflow] def popScope(): Unit

  /** A point-in-time capture of this thread's transient execution context (the
    * `Workflow.scoped` scope stack and the `Workflow.uncancellable` depth),
    * used to propagate the enclosing context into forked parallel-branch
    * threads. See [[snapshotBranchContext]] and [[restoreBranchContext]].
    */
  private[atomicflow] def snapshotBranchContext(): BranchContextSnapshot

  /** Restore the scope stack and `uncancellable` depth captured by
    * [[snapshotBranchContext]] onto the current thread, replacing whatever
    * (usually empty) state a forked branch thread started with.
    */
  private[atomicflow] def restoreBranchContext(snapshot: BranchContextSnapshot): Unit

  /** The runtime's notion of the current instant, from the injected clock. */
  def now: Instant

  /** The durable-retry threshold of this runtime: a step retry whose computed
    * delay is at or below this sleeps inline inside the run; one whose delay is
    * above it becomes a durable suspension (an ordinary timer subscription).
    */
  def durableRetryThreshold: FiniteDuration

  /** Renews the execution lease of this run, extending `lease_expires_at` by the
    * runtime's `leaseDuration`. A fenced write that does not bump the fencing
    * token; throws [[atomicflow.LeaseLostException]] if the lease no longer
    * belongs to this run.
    */
  def renewLease(): Unit

  /** A cancellation checkpoint: re-reads the durable `cancel_requested_at` flag
    * and throws [[atomicflow.WorkflowCancelledException]] when set, unless the
    * execution is inside a [[atomicflow.Workflow.uncancellable]] region. Called
    * right before any new work (a Step body about to execute, or an await about
    * to be evaluated); cached replays never call it, so they never deliver.
    */
  def checkCancellation(): Unit

  /** Enters a `Workflow.uncancellable` region: increments a per-run transient
    * counter so `checkCancellation` suppresses delivery while it is non-zero.
    * Not durable; replay re-enters the region as ordinary user code.
    */
  private[atomicflow] def enterUncancellable(): Unit

  /** Leaves a `Workflow.uncancellable` region, decrementing the per-run counter.
    * Must balance every [[enterUncancellable]] (paired in a `try/finally`).
    */
  private[atomicflow] def exitUncancellable(): Unit

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

  /** Durably suspend an at-least-once step for a retry, fenced, in one
    * transaction: persist (or refresh) the `started` step row carrying the
    * runtime-owned retry bookkeeping in `retryPayload` and its `expiresAt`, and
    * register the retry's timer subscription (deadline = `now + delay`) under a
    * reserved subscription identity that cannot collide with user awaits of the
    * same site. The step body is NOT executed until the subscription is due.
    */
  def suspendStepRetry(
      stepId: StepId,
      stepVersion: Long,
      stepKind: String,
      inputFingerprints: String,
      retryPayload: String,
      deadline: java.time.Instant,
      expiresAt: Option[java.time.Instant]
  ): Unit

  /** Fire this step-site's own due retry timer subscriptions, fenced, in one
    * transaction (the same "two paths, one primitive" as user timer awaits): for
    * each retry subscription with `deadline <= now`, row-lock it, re-check that
    * no `TimerFired` event exists yet, and append one. The subscription row
    * survives firing; only resolution retires it.
    */
  def fireDueStepRetries(stepId: StepId, stepVersion: Long): Unit

  /** Read the durable `TimerFired` events matching this step-site's pending retry
    * timer subscription (plain durable read; no lock or fence).
    */
  def readStepRetryCandidates(stepId: StepId, stepVersion: Long): Vector[AwaitTimerCandidate]

  /** Resolve a retrying step to a terminal state, fenced: persist the
    * `stateKind` (`succeeded` or `failed`) step row and delete the retry timer
    * subscription, in one transaction.
    */
  def resolveStepRetry(
      stepId: StepId,
      stepVersion: Long,
      stepKind: String,
      stateKind: String,
      inputFingerprints: String,
      payload: String,
      expiresAt: Option[java.time.Instant]
  ): Unit

  /** Delete the step row and its retry timer subscription atomically, fenced.
    * Used to invalidate an ongoing retry "as if the step never executed".
    */
  def deleteStepRetry(stepId: StepId, stepVersion: Long): Unit

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

  /** Read the unhandled `Update` records of `updateKey` addressed directly to
    * this instance, oldest first, without handling any of them. A plain durable
    * read; no lease, fence, or write. Updates are never inherited, so only this
    * instance's own rows are consulted.
    */
  def readAwaitUpdateCandidates(updateKey: String): Vector[UpdateCandidate]

  /** Resolve an update await atomically, fenced: persist the `succeeded` step
    * row (`stepKind`) carrying `encodedOutput`, mark the selected candidate's
    * record handled by writing `encodedResponse` and `handled_at`, and delete
    * the site's update subscriptions, all in one transaction. The written
    * response is what the blocked sender reads as `UpdateSendResult.Success`.
    */
  def resolveAwaitUpdate(
      stepId: StepId,
      stepVersion: Long,
      stepKind: String,
      inputFingerprints: String,
      updateKey: String,
      candidate: UpdateCandidate,
      encodedResponse: String,
      encodedOutput: String,
      expiresAt: Option[Instant]
  ): Unit

  /** Suspend an update await, fenced, in one transaction: register the pending
    * update subscription (idempotent), then re-read the unhandled records and
    * apply `decide`. If `decide` selects a candidate, resolve the await
    * (succeeded step row + handled record + subscription deletion) and return
    * the decision; otherwise return `None` (suspended, subscriptions remain).
    * The re-read within the transaction closes the lost-wakeup gap between
    * candidate gathering and the suspension commit, so two awaits cannot
    * double-consume the same record.
    */
  def suspendAwaitUpdate(
      stepId: StepId,
      stepVersion: Long,
      stepKind: String,
      inputFingerprints: String,
      updateKey: String,
      expiresAt: Option[Instant]
  )(decide: Vector[UpdateCandidate] => Option[AwaitUpdateDecision]): Option[AwaitUpdateDecision]

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

  /** Retire an invalidated/expired timer await atomically, fenced: discard the
    * site's `succeeded` step row, delete its old timer subscriptions, and
    * register a fresh subscription (new id, `deadline`) — all in one
    * transaction, so no crash window can leave the old incarnation live. The
    * fresh deadline is recomputed (`now + delay`), so its old `TimerFired`
    * event is structurally inert.
    */
  def invalidateTimer(
      stepId: StepId,
      stepVersion: Long,
      deadline: java.time.Instant
  ): Unit

  /** Fire this race-site's own due timer leaves, fenced, in one transaction: for
    * each leaf's subscription with `deadline <= now`, row-lock it, re-check that
    * no `TimerFired` event exists yet, and append one via the global append
    * protocol. Appends happen in deadline order so the earliest due timer leaf
    * receives the lowest sequence id and wins deterministically among timers.
    */
  def fireDueTimerLeaves(stepId: StepId, stepVersion: Long): Unit

  /** Evaluate a race atomically, fenced: register every leaf's subscription
    * (idempotent), read all leaves' candidates, apply `decide` to select the
    * earliest satisfying candidate by global `sequenceId`, and if satisfied
    * persist the `succeeded` step row (`stepKind`), advance ONLY the winning
    * signal key's cursor, and delete every leaf's subscription — all in one
    * transaction. If no candidate is satisfiable, no cursor advances and the
    * subscriptions remain. Returns the persisted payload when resolved.
    */
  def evaluateAwaitRace(
      stepId: StepId,
      stepVersion: Long,
      stepKind: String,
      inputFingerprints: String,
      leaves: Vector[AwaitRaceLeaf],
      expiresAt: Option[java.time.Instant]
  )(decide: Vector[AwaitRaceCandidate] => Option[AwaitRaceDecision]): Option[String]

  /** Resolve a `Step.firstToRunWithoutSuspension` construct atomically, fenced:
    * persist the `succeeded` step row (`stepKind`) recording the winner, and in
    * the same transaction best-effort delete the pending subscriptions of every
    * losing branch (identified by their branch scope paths), so a losing await
    * cannot wake the workflow later. The winner's own subscriptions were already
    * retired when its branch completed, so only the losers' rows are touched.
    */
  def resolveFirstToRun(
      stepId: StepId,
      stepVersion: Long,
      stepKind: String,
      inputFingerprints: String,
      loserScopePaths: Seq[String],
      payload: String,
      expiresAt: Option[java.time.Instant]
  ): Unit

  /** Read the persisted state of a `Workflow.restartable`/`Workflow.loop` region
    * located at `regionId` under `parentScopePath`: its encoded state and its
    * committed `restartCount`. `None` when the region has never been created
    * (first creation, so the by-name seed must be evaluated).
    */
  private[atomicflow] def readRegionState(regionId: String, parentScopePath: String): Option[(String, Long)]

  /** Persist a `Workflow.restartable`/`Workflow.loop` region's row on its first
    * creation, with `serializedState` and `restartCount` 0, fenced. Only the
    * first creation calls this; replays read the persisted row instead.
    */
  private[atomicflow] def createRegion(regionId: String, parentScopePath: String, serializedState: String): Unit

  /** The one-transaction restart transition of a region at `currentRestartCount`:
    * discard the nested Step rows and subscriptions owned by the previous
    * looping's scope subtree (a construct-isolated prefix delete), close children
    * created in that looping per their `ParentClosePolicy`, and replace the
    * region row's state with `serializedState` and its count with
    * `currentRestartCount + 1`. Signal cursors are preserved. Fenced.
    */
  private[atomicflow] def restartRegion(
      regionId: String,
      parentScopePath: String,
      currentRestartCount: Long,
      serializedState: String
  ): Unit
}
