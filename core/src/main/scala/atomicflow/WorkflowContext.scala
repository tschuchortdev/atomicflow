package atomicflow

/** The runtime context materialized only during workflow execution. It carries
  * the instance identity, the version of the definition captured at creation, the
  * runtime services, and (via [[currentExecution]]) the runtime's per-run
  * execution handle.
  *
  * A new context is materialized for every run and is not retained between runs.
  * The context is immutable: the transient lexical state of the run — the
  * enclosing `Workflow.scoped` path ([[scopePath]]) and the `Workflow.uncancellable`
  * depth ([[uncancellableDepth]]) — lives on the context, and every region
  * function (`Workflow.scoped`, `Workflow.uncancellable`, `Workflow.restartable` /
  * `Workflow.loop`, `Workflow.parallel`, `Step.firstToRunWithoutSuspension`)
  * passes a derived copy to its body (the `local` of a reader monad). The state
  * therefore travels with the context value, not with the executing thread.
  */
final class WorkflowContext(
    val instanceId: WorkflowInstanceId,
    /** The `Workflow.version` recorded when the instance was created. The current
      * body may branch on it internally to adapt to the definition version that
      * created the instance (see `spec/workflow-evolution.md`).
      */
    val versionAtCreation: Long,
    /** The runtime executing this run. Declared as a `val` so that
      * [[currentExecution]]'s dependent type can be prefixed by it.
      */
    val runtime: WorkflowRuntime
)(
    /** The runtime's per-run execution handle: an opaque value this runtime
      * created when the run started, passed back to the runtime's engine
      * operations (see [[WorkflowRuntime.CurrentExecution]]). Lives in a second,
      * dependent parameter list so its type can be prefixed by [[runtime]].
      */
    val currentExecution: runtime.CurrentExecution,
    /** The path of enclosing `Workflow.scoped` (and region / parallel-branch)
      * segments at this context's lexical position; `Vector.empty` at the top
      * level. Transient per-run state carried immutably through derived
      * contexts; never persisted directly. The exact contents are an
      * implementation detail of the runtime and are not portable across runtime
      * implementations — [[currentScope]] is the composed string the durable
      * records key on.
      */
    private[atomicflow] val scopePath: Vector[String] = Vector.empty,
    /** The depth of enclosing `Workflow.uncancellable` regions at this context's
      * lexical position; `0` outside any region. Transient per-run state carried
      * immutably through derived contexts; a non-zero depth suppresses
      * cancellation delivery at checkpoints (`WorkflowRuntime.throwIfCancelled`).
      */
    private[atomicflow] val uncancellableDepth: Int = 0
) {

  /** The current step scope path: [[scopePath]]'s segments joined by `/` (`""`
    * at the top level). Step/Await IDs and derived child scopes key on this
    * string.
    */
  private[atomicflow] def currentScope: String = scopePath.mkString("/")

  /** Derives a copy of this context carrying the given transient lexical state,
    * leaving everything else identical — the `local` of a reader monad. Region
    * functions call this and pass the derived context to their body.
    */
  private[atomicflow] def copy(
      scopePath: Vector[String] = scopePath,
      uncancellableDepth: Int = uncancellableDepth
  ): WorkflowContext =
    new WorkflowContext(instanceId, versionAtCreation, runtime)(currentExecution, scopePath, uncancellableDepth)
}
