package atomicflow.internal

/** The per-run engine seam, materialized only during execution and funneled to
  * workflow code through [[atomicflow.WorkflowContext.execution]]. It carries
  * the fencing identity of the current run so the engine can implement fenced
  * writes and lease-loss detection. It will grow with Step/await needs in later
  * tasks; the Postgres runtime implements it with a private class.
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
}
