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
)

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

/** The public synchronous execution outcome of `run`/`createAndRun`/
  * `awaitResult`. Failures still propagate as exceptions.
  */
enum WorkflowRunResult[+A] {
  case WorkflowSuspended
  case WorkflowCancelled
  case WorkflowTerminated
  case Result(value: A)
}
