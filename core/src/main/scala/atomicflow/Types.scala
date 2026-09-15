package atomicflow

/** Identity types of the workflow model (see `spec/core-types.md`). Ids only; no
  * definition objects, codecs, or runtime services.
  */

type WorkflowId = String
type WorkflowInstanceKey = String
type SignalKey = String

/** The address of a workflow instance: definition id, user-chosen key, and scope.
  *
  * `scope` is `""` for top-level instances; children started via `startAsChild`
  * carry a derived, non-empty scope (see `spec/sub-workflows-iteration.md`).
  * Creation APIs never accept a scope.
  */
final case class WorkflowInstanceId(
    workflowId: WorkflowId,
    workflowInstanceKey: WorkflowInstanceKey,
    scope: String = ""
) {

  /** Append a `Signal` event addressed to this instance. Forwarder for the
    * runtime's `sendSignal`, resolving the signal's own [[Cacheable]].
    */
  @throws[WorkflowNotFoundException]
  def sendSignal[A](signal: Signal[A], value: A)(using runtime: WorkflowRuntime): SignalSendResult =
    runtime.sendSignal(this, signal.key, value)(using signal.cacheable)

  /** Send a synchronous [[Update]] addressed to this instance. Forwarder for the
    * runtime's `sendUpdate`, resolving the update's own [[Cacheable]]s.
    */
  @throws[WorkflowNotFoundException]
  def sendUpdate[I, R](
      workflow: Workflow[?, ?],
      update: Update[I, R],
      input: I,
      idempotencyKey: String = "",
      persistUnhandledUpdates: Boolean = false
  )(using
      runtime: WorkflowRuntime,
      cacheableThrowable: Cacheable[Throwable]
  ): UpdateSendResult[R] =
    runtime.sendUpdate(workflow, this, update.key, input, idempotencyKey, persistUnhandledUpdates)(
      using update,
      cacheableThrowable
    )
}

/** The result of appending a signal event. `InstanceAlreadyCompleted` is a
  * first-class return instead of an exception because the caller must handle it
  * (the addressed instance has stopped accepting signals).
  */
enum SignalSendResult {
  case Success
  case InstanceAlreadyCompleted
}

/** Identity of a step (or await-site) within a workflow instance. `scope` is the
  * enclosing `Workflow.scoped` path.
  */
final case class StepId(key: String, scope: String = "")

/** The only durable, user-visible lifecycle states of an instance. `None` means
  * not terminal yet; CREATED/RUNNING/SUSPENDED are transient bookkeeping, and
  * CANCELLING is derived (`cancel_requested_at` set, no terminal state).
  */
enum WorkflowTerminalState {
  case Completed, Failed, Cancelled, Terminated
}

/** What happens to a child workflow when its parent reaches a terminal state or
  * calls `continueAsNew` (see `spec/sub-workflows-iteration.md`,
  * "ParentClosePolicy"). `Cancel` delivers a cooperative cancellation;
  * `Abandon` detaches the child, which continues independently.
  */
enum ParentClosePolicy {
  case Cancel
  case Abandon
}

/** Which signals addressed to a child's parent are visible to the child (see
  * `spec/sub-workflows-iteration.md`, "Inherited signals"). Defaults to `none`;
  * `some(prefixes)` permits matching key prefixes; `all` permits every key.
  * Inheritance is transitive only when every parent-child edge permits the key.
  * Synchronous `Update`s are never inherited because multiple descendant
  * responses would be ambiguous.
  */
enum SignalInheritance {
  case none
  case all
  case some(prefixes: Seq[SignalKey])
}

/** The public synchronous execution outcome of `run`/`createAndRun`/
  * `awaitResult`. Failures still propagate as exceptions.
  */
enum WorkflowRunResult[+A] {
  case WorkflowSuspended
  case WorkflowCancelled
  case WorkflowTerminated
  case ContinueAsNew
  case Result(value: A)
}
