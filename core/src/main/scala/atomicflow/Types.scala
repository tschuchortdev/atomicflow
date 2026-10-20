package atomicflow

/** String identifiers for workflow definitions, instances, and signals. */

type WorkflowId = String
type WorkflowInstanceKey = String
type SignalKey = String

/** The address of a workflow instance: definition id, user-chosen key, and scope.
  *
  * `scope` is [[ScopePath.empty]] for top-level instances; children started via
  * `startAsChild` carry a scope derived from the parent's identity, generation,
  * and enclosing step scopes. Creation APIs never accept a scope.
  */
final case class WorkflowInstanceId(
    workflowId: WorkflowId,
    workflowInstanceKey: WorkflowInstanceKey,
    scope: ScopePath = ScopePath.empty
) {

  /** Append a `Signal` event addressed to this instance. Forwarder for the
    * runtime's `sendSignal`, resolving the signal's own [[Cacheable]].
    */
  @throws[WorkflowNotFoundException]
  def sendSignal[A](signal: Signal[A], value: A)(using runtime: WorkflowRuntime): SignalSendResult =
    runtime.sendSignal(this, signal.key, value)(using signal.cacheable)
}

/** Whether a signal was stored or rejected because the instance is terminal. */
enum SignalSendResult {
  case Success
  case InstanceAlreadyCompleted
}

/** Identity of a step (or await-site) within a workflow instance. `scope` is the
  * enclosing `Workflow.scoped` path.
  */
final case class StepId(key: String, scope: ScopePath = ScopePath.empty) {

  /** The `/`-joined scope and key, for diagnostics and selector rendering. */
  def pathStr: String =
    if (scope.segments.isEmpty) key else s"${scope.encoded}/$key"
}

/** The only durable, user-visible lifecycle states of an instance. `None` means
  * not terminal yet; CREATED/RUNNING/SUSPENDED are transient bookkeeping, and
  * CANCELLING is derived (`cancel_requested_at` set, no terminal state).
  */
enum WorkflowTerminalState {
  case Completed, Failed, Cancelled, Terminated
}

/** What happens to a child workflow when its parent reaches a terminal state or
  * calls `continueAsNew`. `Cancel` delivers a cooperative cancellation;
  * `Abandon` detaches the child, which continues independently.
  */
enum ParentClosePolicy {
  case Cancel
  case Abandon
}

/** Which signals addressed to a child's parent are visible to the child. Defaults to `none`;
  * `some(prefixes)` permits matching key prefixes; `all` permits every key.
  * Inheritance is transitive only when every parent-child edge permits the key.
  */
enum SignalInheritance {
  case none
  case all
  /** Permit every key matching one of `prefixes`. The prefixes are a `Seq`, so a
    * multi-prefix selection is written `some(Seq("approval/", "order/cancelled"))`
    */
  case some(prefixes: Seq[SignalKey])
}

/** The public synchronous execution outcome of `run`/`createAndRun`/
  * `blockUntilCompleted`. Failures still propagate as exceptions.
  */
enum WorkflowRunResult[+A] {
  case WorkflowSuspended
  case WorkflowCancelled
  case WorkflowTerminated
  case ContinueAsNew
  case Result(value: A)
}
