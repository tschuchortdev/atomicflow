package test

import atomicflow.*
import atomicflow.Cacheable.Simple.given
import doobie.implicits.*
import doobie.postgres.implicits.*

class UnconsumedSignalsSuite extends PostgresWorkflowRuntimeSuite {

  given Cacheable[Throwable] = Cacheable.forThrowable.genericStringMessageSerializer

  test("unconsumed signals do not affect completion when no await consumes them") {
    val rt = newRuntime
    val signal = Signal[String]("A")
    val wf = Workflow[String, String](id = "uc-default") { in => "done" }
    val id = rt.createWorkflowInstance(wf, "k", "in").id
    signal.send(id, "x")(using rt)

    assertEquals(rt.runWorkflowInstance(wf, id), WorkflowRunResult.Result("done"))
  }

  test("after completion the instance's Signal events are deleted; WorkflowCompleted remains; TimerFired survives") {
    val rt = newRuntime
    val signal = Signal[String]("A")
    val wf = Workflow[String, String](id = "uc-delete") { in => "done" }
    val id = rt.createWorkflowInstance(wf, "k", "in").id
    signal.send(id, "x")(using rt)
    run(
      sql"""
        INSERT INTO workflow_events (sequence_id, event_kind, workflow_id, workflow_instance_key, workflow_instance_scope, event_key, payload, created_at)
        VALUES (nextval('workflow_event_sequence'), 'TimerFired', ${wf.id}, 'k', '{}', 'timer1', '', now())
      """.update.run
    )

    assertEquals(rt.runWorkflowInstance(wf, id), WorkflowRunResult.Result("done"))

    val counts = run(
      sql"""
        SELECT event_kind, count(*) FROM workflow_events
        WHERE workflow_id = ${wf.id} AND workflow_instance_key = 'k' AND workflow_instance_scope = '{}'
        GROUP BY event_kind
      """.query[(String, Int)].to[Vector]
    ).toMap
    assertEquals(counts.getOrElse("Signal", 0), 0)
    assertEquals(counts.getOrElse("WorkflowCompleted", 0), 1)
    assertEquals(counts.getOrElse("TimerFired", 0), 1)
  }

  test("a signal sent to a created-but-never-started instance is deleted when cancel finalizes it without a definition") {
    val rt = newRuntime
    val signal = Signal[String]("A")
    val wf = Workflow[String, String](id = "uc-finalize") { in => "done" }
    val id = rt.createWorkflowInstance(wf, "k", "in").id
    signal.send(id, "x")(using rt)

    rt.cancel(id)

    val counts = run(
      sql"""
        SELECT event_kind, count(*) FROM workflow_events
        WHERE workflow_id = ${wf.id} AND workflow_instance_key = 'k' AND workflow_instance_scope = '{}'
        GROUP BY event_kind
      """.query[(String, Int)].to[Vector]
    ).toMap
    assertEquals(counts.getOrElse("Signal", 0), 0, "the finalized instance's directly addressed Signal events are deleted")
    assertEquals(counts.getOrElse("WorkflowCompleted", 0), 1)
    assertEquals(signal.send(id, "late")(using rt), SignalSendResult.InstanceAlreadyCompleted)
  }
}
