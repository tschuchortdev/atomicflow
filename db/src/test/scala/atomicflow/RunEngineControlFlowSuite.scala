package atomicflow

import atomicflow.*
import atomicflow.Cacheable.Simple.given
import doobie.implicits.*
import doobie.postgres.implicits.*
import test.PostgresWorkflowRuntimeSuite

import java.time.Instant

/** Exercises the runtime's boundary handling of [[WorkflowSuspendedException]],
  * the library's internal control-flow exception. Because its constructor is
  * `private[atomicflow]` (user code must not fabricate suspensions — the durable
  * subscriptions are committed by await evaluation before it is thrown), this
  * suite lives in package `atomicflow` so it can construct the exception and
  * verify the boundary-catch behavior the spec mandates.
  */
class RunEngineControlFlowSuite extends PostgresWorkflowRuntimeSuite {

  given Cacheable[Throwable] = Cacheable.forThrowable.genericStringMessageSerializer

  private def instanceRow(workflowId: WorkflowId, key: WorkflowInstanceKey): Option[(Option[String], Option[String], Int, Option[Instant], Option[String], Option[Instant])] =
    run(
      sql"""SELECT terminal_state, terminal_outcome, times_executed, last_run_at, lease_owner, lease_expires_at
            FROM workflow_instances WHERE workflow_id = $workflowId AND workflow_instance_key = $key AND scope = ''""".query[
          (Option[String], Option[String], Int, Option[Instant], Option[String], Option[Instant])
        ].option
    )

  test("body throwing WorkflowSuspendedException returns WorkflowSuspended; instance not terminal; lease released") {
    val rt = newRuntime
    val wf = Workflow[String, String](id = "suspend") { in =>
      throw new WorkflowSuspendedException()
    }

    assertEquals(rt.createAndRun(wf, "k", "x"), WorkflowRunResult.WorkflowSuspended)
    val row = instanceRow(wf.id, "k").get
    assertEquals(row._1, None)
    assertEquals(row._5, None)
    assertEquals(row._6, None)
  }

  test("times_executed and last_run_at are maintained across runs of a non-terminal instance") {
    val rt = newRuntime
    val wf = Workflow[String, String](id = "counts") { in =>
      throw new WorkflowSuspendedException()
    }
    rt.createWorkflowInstance(wf, "k", "a")
    rt.runWorkflowInstance(wf, WorkflowInstanceId(wf.id, "k"))
    rt.runWorkflowInstance(wf, WorkflowInstanceId(wf.id, "k"))

    val row = instanceRow(wf.id, "k").get
    assertEquals(row._3, 2)
    assert(row._4.isDefined)
  }
}
