package atomicflow

import atomicflow.*
import atomicflow.Cacheable.Simple.given
import doobie.implicits.*
import doobie.postgres.implicits.*
import test.PostgresWorkflowRuntimeSuite

import java.time.Instant

/** Verifies that the runtime catches suspension, releases the execution lease,
  * and leaves the instance non-terminal. This package can access the internal
  * exception constructor without registering a real wait.
  */
class RunEngineControlFlowSuite extends PostgresWorkflowRuntimeSuite {

  given Cacheable[Throwable] = Cacheable.forThrowable.genericStringMessageSerializer

  private def instanceRow(workflowId: WorkflowId, key: WorkflowInstanceKey): Option[(Option[String], Option[String], Int, Option[Instant], Option[String], Option[Instant])] =
    run(
      sql"""
        SELECT terminal_state, terminal_outcome, times_executed, last_run_at, lease_owner, lease_expires_at
        FROM workflow_instances WHERE workflow_id = $workflowId AND workflow_instance_key = $key AND workflow_instance_scope = '{}'
      """.query[
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
