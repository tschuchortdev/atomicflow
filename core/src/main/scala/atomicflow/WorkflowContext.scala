package atomicflow

import atomicflow.internal.WorkflowExecution

/** The runtime context materialized only during workflow execution. It carries
  * the instance identity, the version of the definition captured at creation, the
  * runtime services, and (via [[execution]]) the per-run engine seam.
  *
  * A new context is materialized for every run and is not retained between runs.
  */
trait WorkflowContext {
  def instanceId: WorkflowInstanceId

  /** The `Workflow.version` recorded when the instance was created. The current
    * body may branch on it internally to adapt to the definition version that
    * created the instance (see `spec/workflow-evolution.md`).
    */
  def versionAtCreation: Long

  def runtime: WorkflowRuntime

  /** The per-run engine seam: fencing identity and, in later tasks, the services
    * a running body needs from the engine.
    */
  private[atomicflow] def execution: WorkflowExecution
}
