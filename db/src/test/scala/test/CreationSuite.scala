package test

import atomicflow.*
import atomicflow.Cacheable.Simple.given
import doobie.implicits.*
import doobie.postgres.implicits.*

import java.time.{Clock, Instant, ZoneOffset}

class CreationSuite extends PostgresWorkflowRuntimeSuite {

  given Cacheable[Throwable] = Cacheable.forThrowable.genericStringMessageSerializer

  private def workflow(id: WorkflowId): Workflow[String, String] =
    Workflow[String, String](id = id)(body = in => in)

  private def inputOf(workflowId: WorkflowId, key: WorkflowInstanceKey): Option[String] =
    run(
      sql"SELECT input FROM workflow_instances WHERE workflow_id = $workflowId AND key = $key AND scope = ''"
        .query[String]
        .option
    )

  private def wakeup(workflowId: WorkflowId, key: WorkflowInstanceKey): Option[(Instant, Instant, Int)] =
    run(
      sql"SELECT created_at, scheduled_at, attempts FROM workflow_wakeups WHERE workflow_id = $workflowId AND key = $key AND scope = ''"
        .query[(Instant, Instant, Int)]
        .option
    )

  test("create inserts the instance row with serialized input, version_at_creation, empty scope, NULL terminal") {
    val rt = newRuntime
    val wf = workflow("wf-1")
    rt.createWorkflowInstance(wf, "key-1", "hello")

    assertEquals(inputOf(wf.id, "key-1"), Some(Cacheable[String].write("hello")))

    val row = run(
      sql"SELECT workflow_version_at_creation, scope, terminal_state FROM workflow_instances WHERE workflow_id = ${wf.id} AND key = 'key-1' AND scope = ''"
        .query[(Long, String, Option[String])]
        .unique
    )
    assertEquals(row._1, wf.version)
    assertEquals(row._2, "")
    assertEquals(row._3, None)
  }

  test("create is idempotent with equal input") {
    val rt = newRuntime
    val wf = workflow("wf-2")
    rt.createWorkflowInstance(wf, "key-2", "hello")
    rt.createWorkflowInstance(wf, "key-2", "hello")

    assertEquals(inputOf(wf.id, "key-2"), Some(Cacheable[String].write("hello")))
  }

  test("create with different input throws WorkflowInputConflictException") {
    val rt = newRuntime
    val wf = workflow("wf-3")
    rt.createWorkflowInstance(wf, "key-3", "hello")
    intercept[WorkflowInputConflictException] {
      rt.createWorkflowInstance(wf, "key-3", "world")
    }
    assertEquals(inputOf(wf.id, "key-3"), Some(Cacheable[String].write("hello")))
  }

  test("createAndSchedule and createAndRun on existing instance with different input throw") {
    val rt = newRuntime
    val wf = workflow("wf-4")
    rt.createWorkflowInstance(wf, "key-4", "hello")

    intercept[WorkflowInputConflictException] {
      rt.createAndSchedule(wf, "key-4", "world")
    }
    intercept[WorkflowInputConflictException] {
      rt.createAndRun(wf, "key-4", "world")
    }
    assertEquals(inputOf(wf.id, "key-4"), Some(Cacheable[String].write("hello")))
  }

  test("discardExisting deletes and re-creates; returns true iff something was discarded") {
    val rt = newRuntime
    val wf = workflow("wf-5")

    assert(!rt.createWorkflowInstanceDiscardExisting(wf, "key-5", "a"))
    rt.createWorkflowInstance(wf, "key-5", "a")
    assert(rt.createWorkflowInstanceDiscardExisting(wf, "key-5", "b"))
    assertEquals(inputOf(wf.id, "key-5"), Some(Cacheable[String].write("b")))
  }

  test("createAndSchedule upserts a wakeup row; second call does not reset created_at") {
    val rt = newRuntime
    val wf = workflow("wf-6")
    rt.createAndSchedule(wf, "key-6", "a")

    val before = wakeup(wf.id, "key-6")
    assertEquals(before.map(_._3), Some(0))

    Thread.sleep(30)
    rt.createAndSchedule(wf, "key-6", "a")
    val after = wakeup(wf.id, "key-6")

    assertEquals(after.map(_._1), before.map(_._1))
    assertEquals(after.map(_._3), Some(0))
  }

  test("createAndSchedule derives scheduled_at from the injected clock") {
    val fixed = Clock.fixed(Instant.parse("2026-01-01T00:00:00Z"), ZoneOffset.UTC)
    val rt = newRuntime(fixed)
    val wf = workflow("wf-8")
    rt.createAndSchedule(wf, "key-8", "a")

    assertEquals(wakeup(wf.id, "key-8").map(_._2), Some(Instant.parse("2026-01-01T00:00:00Z")))
  }

  test("plain create schedules nothing (no wakeup row)") {
    val rt = newRuntime
    val wf = workflow("wf-7")
    rt.createWorkflowInstance(wf, "key-7", "a")
    assertEquals(wakeup(wf.id, "key-7"), None)
  }
}
