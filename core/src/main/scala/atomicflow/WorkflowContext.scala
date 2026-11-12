package atomicflow

import java.util.concurrent.atomic.AtomicBoolean

/** The context created for each workflow run. It carries
  * the instance identity, the version of the definition captured at creation, the
  * runtime services, and (via [[currentExecution]]) the runtime's per-run
  * execution handle.
  *
  * A new context is created for every run and is not retained between runs.
  * The context is immutable: the transient lexical state of the run — the
  * enclosing `Workflow.scoped` path ([[scopePath]]), the `Workflow.uncancellable`
  * depth ([[uncancellableDepth]]), and the local cancellation flag used by a
  * `Step.firstCompletedWithoutSuspension` race — lives on the context, and every
  * region function (`Workflow.scoped`, `Workflow.uncancellable`,
  * `Workflow.restartable` / `Workflow.loop`, `Workflow.parallel`,
  * `Step.firstCompletedWithoutSuspension`) passes a copy with updated scope or
  * cancellation settings to its body. Derived contexts form a parent chain: the
  * copy records its creator as [[parent]], so cancellation checks can resolve
  * enclosing local flags in memory and, at the root, the instance's durable flag.
  * The state therefore travels with the context value, not with the executing
  * thread.
  */
final class WorkflowContext(
    val instanceId: WorkflowInstanceId,
    /** The `Workflow.version` recorded when the instance was created. The current
      * body may branch on it internally to adapt to the definition version that
      * created the instance.
      */
    val versionAtCreation: Long,
    /** The runtime executing this run, as application code sees it. */
    val runtime: WorkflowRuntime,
    /** The same runtime viewed through its storage/execution engine operations.
      * Core code reaches engine operations through here; application code never
      * sees it.
      */
    private[atomicflow] val internals: WorkflowRuntimeInternals
)(
    /** The runtime's per-run execution handle: an opaque value the runtime
      * created when the run started, passed back to the runtime's engine
      * operations (see [[WorkflowRuntimeInternals.Lease]]). Lives in a second,
      * dependent parameter list so its type can be prefixed by [[internals]].
      */
    val currentExecution: internals.Lease,
    /** The path of enclosing `Workflow.scoped` (and region / parallel-branch)
      * segments at this context's lexical position; [[ScopePath.empty]] at the
      * top level. Transient per-run state carried immutably through derived
      * contexts; never persisted directly. The exact contents are an
      * implementation detail of the runtime and are not portable across runtime
      * implementations — [[currentScope]] is the same raw segment array the
      * durable records key on.
      */
    private[atomicflow] val scopePath: ScopePath = ScopePath.empty,
    /** The depth of enclosing `Workflow.uncancellable` regions at this context's
      * lexical position; `0` outside any region. Transient per-run state carried
      * immutably through derived contexts; a non-zero depth suppresses
      * cancellation delivery ([[throwIfCancelled]]).
      */
    private[atomicflow] val uncancellableDepth: Int = 0,
    /** The context this one was derived from, or [[None]] at the run's root.
      * Cancellation checks walk this chain to find enclosing local cancellation
      * flags and, at the root, to consult the instance's durable flag.
      */
    private[atomicflow] val parent: Option[WorkflowContext] = None,
    /** The local cancellation flag of an enclosing race scope, shared by every
      * branch of that race; [[None]] outside a race.
      */
    private[atomicflow] val localCancellation: Option[AtomicBoolean] = None
) {

  /** True when cancellation is requested for this context: a local cancellation
    * flag of an enclosing race scope, or — at the run's root context — the
    * instance's durable cancellation flag. Contexts form a parent chain;
    * local flags are in-memory and checked first.
    */
  private[atomicflow] def isCancellationRequested: Boolean =
    localCancellation.exists(_.get()) ||
      (parent match {
        case Some(p) => p.isCancellationRequested
        case None    => internals.isCancellationRequested(instanceId)
      })

  /** The shared cancellation-delivery check behind every delivery point (the
    * new-work checkpoints in `Step` and the explicit `Workflow.heartbeat`):
    * walks the parent chain and, unless the `Workflow.uncancellable` depth of
    * this context is non-zero, throws [[WorkflowCancelledException]] when
    * cancellation is requested. Local cancellation flags set by a
    * `Step.firstCompletedWithoutSuspension` race are checked in memory first;
    * the run's root context (parent-less) consults the instance's durable flag
    * via the runtime. `uncancellableDepth > 0` suppresses both. Cached
    * checkpoint replays never call it, so they never deliver.
    */
  private[atomicflow] def throwIfCancelled(): Unit =
    if (uncancellableDepth == 0 && isCancellationRequested)
      throw WorkflowCancelledException()

  /** The current step scope path: [[scopePath]] itself, the raw segment array
    * ([[ScopePath.empty]] at the top level). Step/Await IDs and derived child
    * scopes key on this array.
    */
  private[atomicflow] def currentScope: ScopePath = scopePath

  private[atomicflow] def copy(
      scopePath: ScopePath = scopePath,
      uncancellableDepth: Int = uncancellableDepth,
      localCancellation: Option[AtomicBoolean] = localCancellation
  ): WorkflowContext =
    new WorkflowContext(instanceId, versionAtCreation, runtime, internals)(
      currentExecution,
      scopePath,
      uncancellableDepth,
      Some(this),
      localCancellation
    )
}
