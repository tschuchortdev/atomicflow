package test

import atomicflow.*
import atomicflow.Cacheable.Simple.given
import doobie.implicits.*
import doobie.postgres.implicits.*

import java.util.concurrent.atomic.AtomicInteger
import java.time.Clock
import scala.concurrent.duration.*

class RunEngineSuite extends PostgresWorkflowRuntimeSuite {

  given Cacheable[Throwable] = Cacheable.forThrowable.genericStringMessageSerializer

  private def instanceRow(workflowId: WorkflowId, key: WorkflowInstanceKey): Option[(Option[String], Option[String], Int, Option[java.time.Instant], Option[String], Long, Option[java.time.Instant])] =
    run(
      sql"""SELECT terminal_state, terminal_outcome, times_executed, last_run_at, lease_owner, fencing_token, lease_expires_at
            FROM workflow_instances WHERE workflow_id = $workflowId AND key = $key AND scope = ''""".query[
          (Option[String], Option[String], Int, Option[java.time.Instant], Option[String], Long, Option[java.time.Instant])
        ].option
    )

  private def completedEvent(workflowId: WorkflowId, key: WorkflowInstanceKey): Option[(String, String, String)] =
    run(
      sql"""SELECT event_kind, event_key, payload FROM workflow_events
            WHERE workflow_id = $workflowId AND key = $key AND scope = '' AND event_kind = 'WorkflowCompleted'""".query[
          (String, String, String)
        ].option
    )

  test("run on an unknown instance throws WorkflowNotFoundException") {
    val rt = newRuntime
    val wf = Workflow[String, String](id = "unknown")(in => in)
    intercept[WorkflowNotFoundException] {
      rt.runWorkflowInstance(wf, WorkflowInstanceId(wf.id, "missing"))
    }
  }

  test("createAndRun completes: Result, terminal completed/outcome, WorkflowCompleted event") {
    val rt = newRuntime
    val wf = Workflow[String, String](id = "complete")(in => s"result-$in")

    val result = rt.createAndRun(wf, "k", "hello")

    assertEquals(result, WorkflowRunResult.Result("result-hello"))
    val row = instanceRow(wf.id, "k").get
    assertEquals(row._1, Some("completed"))
    assertEquals(row._2.map(Cacheable[WorkflowCompletionResult[String]].read(_)),
      Some(WorkflowCompletionResult.Completed("result-hello")))
    val event = completedEvent(wf.id, "k").get
    assertEquals(event._1, "WorkflowCompleted")
    assertEquals(event._2, "")
    assertEquals(Cacheable[WorkflowCompletionResult[String]].read(event._3),
      WorkflowCompletionResult.Completed("result-hello"))
  }

  test("re-run of a completed instance returns equal Result without re-executing the body") {
    val rt = newRuntime
    val counter = new AtomicInteger(0)
    val wf = Workflow[String, String](id = "rerun") { in =>
      counter.incrementAndGet()
      s"result-$in"
    }

    assertEquals(rt.createAndRun(wf, "k", "a"), WorkflowRunResult.Result("result-a"))
    assertEquals(counter.get(), 1)
    assertEquals(rt.runWorkflowInstance(wf, WorkflowInstanceId(wf.id, "k")), WorkflowRunResult.Result("result-a"))
    assertEquals(counter.get(), 1)
    assertEquals(instanceRow(wf.id, "k").get._3, 1)
  }

  test("failing body: exception propagates, terminal failed, re-run throws the same decoded failure without executing") {
    val rt = newRuntime
    val counter = new AtomicInteger(0)
    val wf = Workflow[String, String](id = "fail") { in =>
      counter.incrementAndGet()
      throw new RuntimeException(s"boom-$in")
    }

    val first = intercept[StepFailed] {
      rt.createAndRun(wf, "k", "x")
    }
    assert(first.getMessage.contains("boom-x"))
    assertEquals(counter.get(), 1)
    assertEquals(instanceRow(wf.id, "k").get._1, Some("failed"))

    val second = intercept[StepFailed] {
      rt.runWorkflowInstance(wf, WorkflowInstanceId(wf.id, "k"))
    }
    assertEquals(second.getMessage, first.getMessage)
    assertEquals(counter.get(), 1)
  }

  test("body throwing WorkflowSuspendedException returns WorkflowSuspended; instance not terminal; lease released") {
    val rt = newRuntime
    val wf = Workflow[String, String](id = "suspend") { in =>
      throw new WorkflowSuspendedException()
    }

    assertEquals(rt.createAndRun(wf, "k", "x"), WorkflowRunResult.WorkflowSuspended)
    val row = instanceRow(wf.id, "k").get
    assertEquals(row._1, None)
    assertEquals(row._5, None)
    assertEquals(row._7, None)
  }

  test("a run on a leased instance throws LeaseUnavailableException after leaseAcquireTimeout") {
    val rt = newRuntime(Clock.systemUTC(), leaseAcquireTimeout = 300.millis)
    val wf = Workflow[String, String](id = "leased")(in => s"out-$in")
    rt.createWorkflowInstance(wf, "k", "a")
    run(
      sql"""UPDATE workflow_instances SET lease_owner = 'someone-else', lease_expires_at = now() + interval '1 hour'
            WHERE workflow_id = ${wf.id} AND key = 'k' AND scope = ''""".update.run
    )
    intercept[LeaseUnavailableException] {
      rt.runWorkflowInstance(wf, WorkflowInstanceId(wf.id, "k"))
    }
  }

  test("a terminal instance never executes the body (pre-set terminal_state via SQL)") {
    val rt = newRuntime
    val counter = new AtomicInteger(0)
    val wf = Workflow[String, String](id = "preterm") { in =>
      counter.incrementAndGet()
      s"body-$in"
    }
    rt.createWorkflowInstance(wf, "k", "a")
    val payload = Cacheable[WorkflowCompletionResult[String]].write(WorkflowCompletionResult.Completed("from-sql"))
    run(
      sql"""UPDATE workflow_instances SET terminal_state = 'completed', terminal_outcome = $payload
            WHERE workflow_id = ${wf.id} AND key = 'k' AND scope = ''""".update.run
    )

    assertEquals(rt.runWorkflowInstance(wf, WorkflowInstanceId(wf.id, "k")),
      WorkflowRunResult.Result("from-sql"))
    assertEquals(counter.get(), 0)
  }

  test("SQL-set cancelled/terminated outcomes decode to WorkflowCancelled/WorkflowTerminated") {
    val rt = newRuntime
    val wf = Workflow[String, String](id = "cancelled")(in => s"out-$in")
    rt.createWorkflowInstance(wf, "ck", "a")
    run(
      sql"""UPDATE workflow_instances SET terminal_state = 'cancelled', terminal_outcome = ${Cacheable[WorkflowCompletionResult[String]].write(WorkflowCompletionResult.Cancelled)}
            WHERE workflow_id = ${wf.id} AND key = 'ck' AND scope = ''""".update.run
    )
    assertEquals(rt.runWorkflowInstance(wf, WorkflowInstanceId(wf.id, "ck")), WorkflowRunResult.WorkflowCancelled)

    val wf2 = Workflow[String, String](id = "terminated")(in => s"out-$in")
    rt.createWorkflowInstance(wf2, "tk", "a")
    run(
      sql"""UPDATE workflow_instances SET terminal_state = 'terminated', terminal_outcome = ${Cacheable[WorkflowCompletionResult[String]].write(WorkflowCompletionResult.Terminated)}
            WHERE workflow_id = ${wf2.id} AND key = 'tk' AND scope = ''""".update.run
    )
    assertEquals(rt.runWorkflowInstance(wf2, WorkflowInstanceId(wf2.id, "tk")), WorkflowRunResult.WorkflowTerminated)
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

  test("success path releases the lease so a second run starts immediately") {
    val rt = newRuntime
    val wf = Workflow[String, String](id = "release")(in => s"out-$in")
    rt.createWorkflowInstance(wf, "k", "a")

    rt.runWorkflowInstance(wf, WorkflowInstanceId(wf.id, "k"))
    val row = instanceRow(wf.id, "k").get
    assertEquals(row._5, None)
    assertEquals(row._7, None)
  }
}
