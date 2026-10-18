package atomicflow

/** The context created for each workflow run. It carries
  * the instance identity, the version of the definition captured at creation, the
  * runtime services, and (via [[currentExecution]]) the runtime's per-run
  * execution handle.
  *
  * A new context is created for every run and is not retained between runs.
  * The context is immutable: the transient lexical state of the run — the
  * enclosing `Workflow.scoped` path ([[scopePath]]) and the `Workflow.uncancellable`
  * depth ([[uncancellableDepth]]) — lives on the context, and every region
  * function (`Workflow.scoped`, `Workflow.uncancellable`, `Workflow.restartable` /
  * `Workflow.loop`, `Workflow.parallel`, `Step.firstToRunWithoutSuspension`)
  * passes a copy with updated scope or cancellation settings to its body. The state
  * therefore travels with the context value, not with the executing thread.
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
      * cancellation delivery at checkpoints (`Step.throwIfCancelled`).
      */
    private[atomicflow] val uncancellableDepth: Int = 0
) {

  /** The current step scope path: [[scopePath]] itself, the raw segment array
    * ([[ScopePath.empty]] at the top level). Step/Await IDs and derived child
    * scopes key on this array.
    */
  private[atomicflow] def currentScope: ScopePath = scopePath

  private[atomicflow] def copy(
      scopePath: ScopePath = scopePath,
      uncancellableDepth: Int = uncancellableDepth
  ): WorkflowContext =
    new WorkflowContext(instanceId, versionAtCreation, runtime, internals)(currentExecution, scopePath, uncancellableDepth)
}
