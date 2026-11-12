package atomicflow

/** Test-support helpers for constructing the library's internal control-flow
  * exceptions from outside package `atomicflow` (their constructors are
  * `private[atomicflow]`).
  */
object TestControlFlow {
  def suspend(): Nothing = throw new WorkflowSuspendedException()
}
