package atomicflow

import atomicflow.impl.db.PostgresWorkflowRuntime

import scala.concurrent.duration.*

/** Test support: drives the asynchronous child-close sweep deterministically
  * from suites in other packages. The sweep hook (`runChildCloseSweep`) is
  * `private[atomicflow]`, so suites in package `test` cannot call it directly;
  * this object lives in package `atomicflow` and bridges to it.
  */
object ChildCloseTestSupport {

  /** Runs the child-close sweep once for a throwaway runner registered with the
    * given workflow definitions.
    */
  def sweepChildCloses(rt: PostgresWorkflowRuntime, wfs: Workflow[?, ?]*): Unit = {
    val runner = rt.startJobRunnerForTests(wfs.toSeq, JobRunnerSettings.forTests)
    try runner.runChildCloseSweep()
    finally runner.stop(1.second)
  }
}
