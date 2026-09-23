package atomicflow

import scala.annotation.implicitNotFound
import scala.concurrent.duration.{Duration, FiniteDuration}
import java.time.Clock
import java.time.Instant

/** The canonical home of all single-instance workflow operations. Implemented per
  * backend (in-memory, Postgres, ...). Convenience methods are `final`, built
  * from a small set of abstract primitives.
  */
@implicitNotFound("No WorkflowRuntime available. Add a using clause (using WorkflowRuntime).")
trait WorkflowRuntime {

  /** The single time source for timer due-ness, retry thresholds, sweep
    * predicates, and all persisted `:now` parameters. The database server's
    * clock is never consulted for logic. Exposed so apps and tests share one
    * clock with workflow code.
    */
  def clock: Clock

  /** The runtime's per-run execution handle. The runtime creates a value of this
    * type when a run starts and hands it to workflow code through
    * [[WorkflowContext.currentExecution]]; the type is opaque to callers — a
    * value is only ever passed back to this runtime's own engine operations.
    *
    * Contract:
    *   - The runtime creates the handle at run start, and its identity is stable
    *     for the run's lifetime.
    *   - Its contents are owned and updatable by the runtime (a lease protocol
    *     may rotate fencing values during the run, for example).
    *   - The same handle may be shared across the parallel-branch threads of one
    *     run, so any internal mutation must be thread-safe.
    *   - It should carry only what the runtime cannot derive from its own
    *     configuration.
    */
  type CurrentExecution

  /** The durable-retry threshold of this runtime: a step retry whose computed
    * delay is at or below this sleeps inline inside the run; one whose delay is
    * above it becomes a durable suspension (an ordinary timer subscription).
    */
  def durableRetryThreshold: FiniteDuration

  /** Renews the lock on the workflow instance. */
  def heartbeat(run: CurrentExecution): Unit

  /** Whether cooperative cancellation has been durably requested for the
    * instance: its `cancel_requested_at` flag is set. The flag is a level —
    * [[cancel]] sets it once and it is never reset. A plain durable read; no
    * lease or fence. Consulted at the new-work checkpoints (a Step body about
    * to execute, or an await about to be evaluated) to deliver
    * [[atomicflow.WorkflowCancelledException]]; cached replays never deliver,
    * so they never read the flag.
    */
  def isCancellationRequested(instanceId: WorkflowInstanceId): Boolean

  /** Read a step's durable facts (no lease/fence needed), or `None` if absent.
    * Reports the stored row even if it has expired.
    */
  def lookupStep(run: CurrentExecution, stepId: StepId, stepVersion: Long): Option[StoredStep]

  /** Replace (or create) the step row, fenced: `state` is `Started` (an
    * unresolved row, its `payload` empty or carrying retry bookkeeping), or the
    * terminal `Succeeded`/`Failed`. The site's retry timer subscription is
    * retired in the same transaction, so a resolved or superseded retry can
    * never be re-fired.
    */
  def writeStepState(
      run: CurrentExecution,
      stepId: StepId,
      stepVersion: Long,
      stepKind: String,
      state: StoredStep.State,
      inputFingerprints: String,
      payload: String,
      expiresAt: Option[Instant]
  ): Unit

  /** Delete the step row and its retry timer subscription atomically, fenced.
    * Used both to invalidate an ongoing retry "as if the step never executed"
    * and to discard an expired or drifted cached result.
    */
  def deleteStep(run: CurrentExecution, stepId: StepId, stepVersion: Long): Unit

  /** Retire an await-site atomically, fenced: delete the site's `succeeded`/
    * `started` step row and all of its subscriptions in the three subscription
    * tables, in one transaction. Used by drift/expiry re-evaluation so no stale
    * registration (in particular a fired timer of the previous incarnation) can
    * satisfy the re-evaluated site. Unlike [[deleteStep]], which removes only
    * the step row and its retry timer subscription, this retires every
    * subscription of the site (signal, timer, and update).
    */
  def retireAwaitSite(run: CurrentExecution, stepId: StepId, stepVersion: Long): Unit

  /** Durably suspend an at-least-once step for a retry, fenced, in one
    * transaction: persist (or refresh) the `started` step row carrying the
    * runtime-owned retry bookkeeping in `retryPayload` and its `expiresAt`, and
    * register the retry's timer subscription (deadline = `now + delay`) under
    * the reserved `"__retry__"` subscriber key, which cannot collide with user
    * awaits of the same site. The step body is NOT executed until the
    * subscription is due.
    */
  def suspendStepRetry(
      run: CurrentExecution,
      stepId: StepId,
      stepVersion: Long,
      stepKind: String,
      inputFingerprints: String,
      retryPayload: String,
      deadline: Instant,
      expiresAt: Option[Instant]
  ): Unit

  /** Fire this step-site's own due retry timer subscriptions, fenced, in one
    * transaction (the same "two paths, one primitive" as user timer awaits): for
    * each retry subscription with `deadline <= now`, row-lock it, re-check that
    * no `TimerFired` event exists yet, and append one. The subscription row
    * survives firing; only resolution retires it. Returns whether a `TimerFired`
    * event now exists for the site, distinguishing a due retry (resume the body)
    * from one still waiting (suspend).
    */
  def fireStepRetryIfDue(run: CurrentExecution, stepId: StepId, stepVersion: Long): Boolean

  /** Read the durable `Signal` events of `signalKey` that are visible to this
    * instance (after its shared exact-key cursor), in sequence order. A plain
    * read of durable facts; no lease or fence is involved and the cursor is not
    * advanced. Serves `Step.peekSignal`, so `subscriberKey` is always `""` on the
    * returned candidates.
    */
  def readAwaitSignalCandidates(run: CurrentExecution, signalKey: SignalKey): Vector[SubscriberMatch]

  /** Read the unhandled `Update` records of `updateKey` addressed directly to
    * this instance, oldest first, without handling any of them. A plain durable
    * read; no lease, fence, or write. Updates are never inherited, so only this
    * instance's own rows are consulted.
    */
  def readAwaitUpdateCandidates(run: CurrentExecution, updateKey: String): Vector[UpdateCandidate]

  /** Resolve an update await atomically, fenced: mark the selected candidate's
    * record handled by writing `encodedResponse` and `handled_at` FIRST, gated
    * on the row still being unhandled; only when the update affected a row
    * (this branch won the record) persist the `succeeded` step row (`stepKind`)
    * carrying `encodedOutput` and delete the site's update subscriptions — all
    * in one transaction. The written response is what the blocked sender reads
    * as `UpdateSendResult.Success`. Returns `true` when this branch handled the
    * record, `false` when a concurrent branch already did (the caller should
    * re-evaluate the await rather than commit a losing step row).
    */
  def resolveAwaitUpdate(
      run: CurrentExecution,
      stepId: StepId,
      stepVersion: Long,
      stepKind: String,
      inputFingerprints: String,
      updateKey: String,
      candidate: UpdateCandidate,
      encodedResponse: String,
      encodedOutput: String,
      expiresAt: Option[Instant]
  ): Boolean

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
      run: CurrentExecution,
      stepId: StepId,
      stepVersion: Long,
      stepKind: String,
      inputFingerprints: String,
      updateKey: String,
      expiresAt: Option[Instant]
  )(decide: Vector[UpdateCandidate] => Option[AwaitUpdateDecision]): Option[AwaitUpdateDecision]

  /** Retire an invalidated/expired timer await atomically, fenced: discard the
    * site's `succeeded` step row, delete its old timer subscriptions, and
    * register a fresh subscription (new timer id, `deadline`) — all in one
    * transaction, so no crash window can leave the old incarnation live. The
    * fresh deadline is recomputed (`now + delay`), so its old `TimerFired`
    * event is structurally inert.
    */
  def invalidateTimer(run: CurrentExecution, stepId: StepId, stepVersion: Long, deadline: Instant): Unit

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
    * and candidate read (this registration + read + resolution atomicity is what
    * makes the await lost-wakeup-free). If it returns `None`, the registrations
    * commit and no cursor moves (the await durably suspends). Returns the
    * persisted payload when resolved, `None` when suspended. A `decide`
    * exception rolls the evaluation transaction back entirely; the earlier fire
    * transaction already committed.
    */
  def evaluateWait(
      run: CurrentExecution,
      site: WaitSite,
      subscribers: Vector[Subscriber]
  )(decide: Vector[SubscriberMatch] => Option[WaitResolution]): Option[String]

  /** Delete every subscription row whose `step_scope_path` is one of `scopePaths`
    * or lies beneath one (any `path/...` descendant), across all four
    * subscription tables (signal, timer, completion, update), fenced. Used by
    * `Step.firstToRunWithoutSuspension` to retire every branch's pending
    * subscriptions — including nested subscopes — before the winner's step row
    * is written. The ordering is deliberate: the winner row is the durable
    * first-wins marker, so deleting first means a crash in between leaves no
    * marker, and the construct's next run re-evaluates from scratch and
    * re-deletes; a committed winner row therefore never coexists with branch
    * subscriptions.
    */
  def deleteSubscriptionsUnderScopePaths(run: CurrentExecution, scopePaths: Seq[String]): Unit

  /** Read the persisted state of a `Workflow.restartable`/`Workflow.loop` region
    * located at `regionId` under `parentScopePath`: its encoded state and its
    * committed `restartCount`. `None` when the region has never been created
    * (first creation, so the by-name seed must be evaluated).
    */
  def readRegionState(run: CurrentExecution, regionId: String, parentScopePath: String): Option[(String, Long)]

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
      run: CurrentExecution,
      regionId: String,
      parentScopePath: String,
      restartCount: Long,
      serializedState: String
  ): Unit

  /** Append a `Signal` event addressed to `workflowInstanceId`. The sender
    * briefly row-locks the instance and checks `is_accepting_signals`, then
    * appends the event (with the signal's `Cacheable`) and upserts a wakeup for
    * the instance when a matching pending subscription exists. Does not acquire
    * the execution lease.
    */
  @throws[WorkflowNotFoundException]
  def sendSignal[A: Cacheable](
      workflowInstanceId: WorkflowInstanceId,
      key: SignalKey,
      value: A
  ): SignalSendResult

  /** Send a synchronous [[Update]] addressed directly to `workflowInstanceId`
    * and block for its outcome.
    *
    * Updates are addressed to exactly one instance and are never inherited. The
    * sender needs the [[Workflow]] definition so that, when the instance is free,
    * it can run the workflow on the sender's own thread to deliver the update.
    *
    * `idempotencyKey`: when non-empty, sending the same (instance, update key,
    * idempotency key) twice reuses the first send's record and returns the same
    * result instead of creating a second record.
    *
    * `persistUnhandledUpdates`: when `true`, a run that finishes without handling
    * the update keeps its record so a later `awaitUpdate` can consume it; when
    * `false` (default) the unhandled record is deleted after the run.
    */
  @throws[WorkflowNotFoundException]
  def sendUpdate[I, R](
      workflow: Workflow[?, ?],
      workflowInstanceId: WorkflowInstanceId,
      updateKey: String,
      input: I,
      idempotencyKey: String = "",
      persistUnhandledUpdates: Boolean = false
  )(using u: Update[I, R], cacheableThrowable: Cacheable[Throwable]): UpdateSendResult[R]

  /** Register an instance, idempotent for equal (serialized) input; throws
    * [[WorkflowInputConflictException]] if the input differs.
    */
  @throws[WorkflowInputConflictException]
  def createWorkflowInstance[In, Out](
      workflow: Workflow[In, Out],
      instanceKey: WorkflowInstanceKey,
      in: In
  )(using Cacheable[In]): WorkflowInstance[In, Out]

  /** Delete any existing instance under the identity and re-create it; returns
    * whether an existing instance was discarded.
    */
  def createWorkflowInstanceDiscardExisting[In, Out](
      workflow: Workflow[In, Out],
      instanceKey: WorkflowInstanceKey,
      in: In
  )(using Cacheable[In]): Boolean

  /** Fork a source instance into a brand-new independent top-level instance.
    *
    * The fork receives a completely new instance ID, generation 0, and the same
    * input as the source; it is an independent top-level workflow (no parent
    * relationship, no inherited signals). `restartFromStep` is an exclusive
    * boundary: the cached history strictly before the selected step is copied
    * (rewritten to the new identity), while the selected step and subsequent
    * history are omitted so they re-execute in the fork. The workflow function
    * still executes from its beginning, replaying the copied rows until the
    * first uncopied operation.
    *
    * The boundary is ordered by the step rows' last-updated timestamp: rows
    * strictly before the selected row are kept, the selected row and everything
    * with an equal or later timestamp are omitted. `restartFromStep` must
    * identify an already-executed step of the source or
    * [[InvalidRestartStepException]] is thrown. Forking is allowed on any source
    * state (suspended or terminal) and does not modify the source.
    *
    * LIMITATION: a single step id is used as the causal boundary, which is
    * insufficient to describe a cut through parallel branches that have no one
    * total order; rows sharing the boundary timestamp are conservatively
    * omitted.
    */
  @throws[WorkflowNotFoundException]
  @throws[InvalidRestartStepException]
  def forkWorkflow[In, Out](
      sourceInstanceId: WorkflowInstanceId,
      newInstanceKey: WorkflowInstanceKey,
      restartFromStep: StepId
  )(using workflow: Workflow[In, Out]): WorkflowInstance[In, Out]

  /** Reset an instance in place from `restartFromStep`: keep the same instance
    * identity, increment the generation, erase the selected step and subsequent
    * history (Step rows and their subscriptions/wakeups), and schedule the
    * instance to re-run from the top. History strictly before the selected step
    * remains cached and is replayed, so the workflow function still executes
    * from its beginning. The instance's input, signal cursors, and directly
    * addressed events are untouched.
    *
    * The boundary is ordered by the step rows' last-updated timestamp: rows
    * strictly before the selected row are kept, the selected row and everything
    * with an equal or later timestamp are erased. `restartFromStep` must
    * identify an already-executed step or [[InvalidRestartStepException]] is
    * thrown. Resetting a terminal instance throws [[IllegalStateException]].
    *
    * LIMITATION: a single step id is used as the causal boundary, which is
    * insufficient to describe a cut through parallel branches that have no one
    * total order; rows sharing the boundary timestamp are conservatively
    * erased.
    */
  @throws[WorkflowNotFoundException]
  @throws[InvalidRestartStepException]
  @throws[IllegalStateException]
  def resetWorkflow[In, Out](
      sourceInstanceId: WorkflowInstanceId,
      restartFromStep: StepId
  )(using workflow: Workflow[In, Out]): Unit

  /** Request cooperative cancellation of an instance. Durably sets
    * `cancel_requested_at` once and never resets it. If the instance has never
    * started and has no execution state, it is finalized `CANCELLED` immediately
    * (the body never runs). Otherwise the flag is delivered at the next new-work
    * checkpoint (a Step body about to execute, or an await about to be
    * evaluated), where the runtime throws [[WorkflowCancelledException]]. A
    * terminal instance is a no-op.
    *
    * Does not acquire the execution lease; delivery is by checkpoint, never by
    * thread interruption.
    */
  @throws[WorkflowNotFoundException]
  def cancel(instanceId: WorkflowInstanceId): Unit

  /** Force-stop an instance. In one transaction: a guarded terminal transition to
    * `TERMINATED` (appending the `WorkflowCompleted` event with a `Terminated`
    * outcome), a lease revocation (fencing-token bump plus clearing the lease
    * owner/expiry, fencing out any running orphan), and terminal cleanup of this
    * instance's wakeup and subscription rows. The instance is never scheduled
    * again, no resume is scheduled, and no [[WorkflowCancelledException]] is
    * delivered — the body gets no chance to run.
    *
    * JVM limitation: `terminate` guarantees the instance is durably stopped and
    * lease-revoked; an orphan thread already executing a Step cannot be forcibly
    * stopped and may run until its next checkpoint or the process ends.
    *
    * A missing instance throws [[WorkflowNotFoundException]]; an already-terminal
    * instance is a no-op.
    */
  @throws[WorkflowNotFoundException]
  def terminate(instanceId: WorkflowInstanceId): Unit

  /** Execute a created instance on the caller thread. */
  @throws[WorkflowNotFoundException]
  def runWorkflowInstance[In, Out](
      workflow: Workflow[In, Out],
      instanceId: WorkflowInstanceId
  )(using Cacheable[Throwable]): WorkflowRunResult[Out]

  /** Upsert a coalesced wakeup row for an instance, `ON CONFLICT DO NOTHING` so an
    * existing row's timestamps are never reset. The backend derives `scheduled_at`
    * from its own clock (`clock.now + delay`).
    */
  def upsertWakeup(instanceId: WorkflowInstanceId, delay: FiniteDuration): Unit

  /** Read a step's durable facts without acquiring a lease or fence (a pure
    * lookup of durable state, used by `Step.getExecutionState`).
    */
  def readStep(
      instanceId: WorkflowInstanceId,
      stepId: StepId,
      stepVersion: Long
  ): Option[StoredStep]

  /** Start a child workflow of an executing parent: create-if-absent a child
    * instance under a scope derived from the parent's identity, generation, and
    * enclosing `Workflow.scoped` path; record the parent relationship and
    * inheritance configuration; and schedule the child's first wakeup. Never
    * executes the child body on the parent's thread. Idempotent on parent
    * replay (`startAsChild` returns the existing handle). The parent's identity
    * and generation come from `run`.
    */
  def startChild[In, Out](
      workflow: Workflow[In, Out],
      childKey: WorkflowInstanceKey,
      input: In,
      parentClosePolicy: ParentClosePolicy,
      inheritSignals: SignalInheritance,
      inheritPastEvents: Boolean,
      run: CurrentExecution,
      enclosingScopePath: String
  )(using Cacheable[In]): WorkflowInstance[In, Out]

  /** The active children of `parentId`: instances whose parent relationship
    * still points at `parentId` (not yet cleared by a parent terminal
    * transition).
    */
  def getChildWorkflowInstances(parentId: WorkflowInstanceId): Vector[WorkflowInstance.Info]

  /** Register and schedule the instance's first wakeup (due immediately). */
  final def createAndSchedule[In, Out](
      workflow: Workflow[In, Out],
      instanceKey: WorkflowInstanceKey,
      in: In
  ): WorkflowInstance[In, Out] = {
    val instance = createWorkflowInstance(workflow, instanceKey, in)(using workflow.inputCacheable)
    upsertWakeup(instance.id, Duration.Zero)
    instance
  }

  /** Atomically create-if-absent and run on the caller thread. */
  final def createAndRun[In, Out](
      workflow: Workflow[In, Out],
      instanceKey: WorkflowInstanceKey,
      in: In
  )(using cacheableThrowable: Cacheable[Throwable]): WorkflowRunResult[Out] = {
    createWorkflowInstance(workflow, instanceKey, in)(using workflow.inputCacheable)
    runWorkflowInstance(workflow, WorkflowInstanceId(workflow.id, instanceKey))
  }

  /** Upgrade a bare identity to a typed handle. Validates that the workflow id
    * in the identity matches the provided definition (a programming error if it
    * does not). Missing instances are not verified here; handles obtained from
    * the runtime always refer to existing instances.
    */
  final def getWorkflowInstance[In, Out](
      workflow: Workflow[In, Out],
      instanceId: WorkflowInstanceId
  ): WorkflowInstance[In, Out] = {
    if (instanceId.workflowId != workflow.id)
      throw new IllegalArgumentException(
        s"Cannot build a handle for workflow '${workflow.id}' from an instanceId referring to workflow '${instanceId.workflowId}'"
      )
    WorkflowInstance(workflow, instanceId)
  }

  /** Key-prefix query over instances of one workflow definition, scoped exactly
    * to `scope` (empty = top-level only). The prefix is matched literally: `%`
    * and `_` in the user prefix are escaped, not treated as wildcards.
    */
  def getWorkflowInstancesByPrefix(
      workflowId: WorkflowId,
      keyPrefix: WorkflowInstanceKey,
      scope: String = ""
  ): Vector[WorkflowInstance.Info]

  /** Instances of one workflow definition that have not reached a terminal
    * state. `includeWaiting = false` (default) returns only instances that have
    * never started (`times_executed = 0`); `true` returns all non-terminal
    * instances. `limit <= 0` means unlimited.
    */
  def getUnfinishedWorkflowInstances(
      workflowId: WorkflowId,
      includeWaiting: Boolean = false,
      limit: Int = -1
  ): Vector[WorkflowInstance.Info]

  /** Deletes all instances of one workflow definition whose key matches `scope`
    * and `keyPrefix` (escaped as in [[getWorkflowInstancesByPrefix]]), plus
    * their cascaded rows and their event rows. Returns the number of instances
    * deleted.
    */
  def deleteWorkflowInstancesByPrefix(
      workflowId: WorkflowId,
      keyPrefix: WorkflowInstanceKey,
      scope: String = ""
  ): Long

  /** Passive waiter for an instance's terminal outcome: polls the stored
    * terminal projection until a terminal state is reached or `timeout`
    * elapses, then throws [[java.util.concurrent.TimeoutException]]. Never
    * executes the workflow. Terminal outcomes decode exactly as `run` on a
    * terminal instance.
    */
  @throws[java.util.concurrent.TimeoutException]
  def awaitResult[Out](
      instance: WorkflowInstance[?, Out],
      timeout: FiniteDuration
  )(using Cacheable[Throwable]): WorkflowRunResult[Out]

  /** The persisted data view of an instance, fresh from the database. */
  def getWorkflowInstanceInfo[In, Out](
      instance: WorkflowInstance[In, Out]
  ): WorkflowInstance.Info

  /** Creates and starts a job runner over the given definition registry.
    * Implemented per backend: the runner executes backend-internal operations
    * (wakeup claiming, conditional lease acquisition, sweeps) and is bound to
    * this runtime — runners and runtimes cannot be mixed and matched.
    *
    * The registry maps each `workflowId` to its one current definition; it is
    * validated once (duplicate ids throw) and immutable for the runner's
    * lifetime. The runner claims only wakeups of workflows in the registry, so
    * several applications with different code can share the same tables.
    *
    * Any number of runners may be started on the same runtime — in this process
    * or another — each with its own settings and registry. They cooperate through
    * storage alone (claim-atomic leases and idempotent sweeps), so duplicate
    * runners cost redundant work but are never incorrect. Each runner stops
    * independently via [[JobRunner.stop]].
    */
  def startJobRunner(
      definitions: Seq[Workflow[?, ?]],
      settings: JobRunnerSettings = JobRunnerSettings.default
  ): JobRunner
}

object WorkflowRuntime {
  def apply(using runtime: WorkflowRuntime): WorkflowRuntime = runtime
}
