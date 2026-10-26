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

  test("a signal sent to a created-but-never-started instance is deleted when the cancelled instance terminalizes") {
    val rt = newRuntime
    val signal = Signal[String]("A")
    val wf = Workflow[String, String](id = "uc-finalize") { in =>
      Step.atLeastOnce[String]("work") { "done" }
    }
    val id = rt.createWorkflowInstance(wf, "k", "in").id
    signal.send(id, "x")(using rt)

    // Cancel flags the instance; without a terminal transition the signal
    // event is not deleted yet.
    rt.cancel(id)
    val before = run(
      sql"""
        SELECT event_kind, count(*) FROM workflow_events
        WHERE workflow_id = ${wf.id} AND workflow_instance_key = 'k' AND workflow_instance_scope = '{}'
        GROUP BY event_kind
      """.query[(String, Int)].to[Vector]
    ).toMap
    assertEquals(before.getOrElse("Signal", 0), 1, "the cancel flag alone does not delete the signal event")
    assertEquals(before.getOrElse("WorkflowCompleted", 0), 0)

    // The instance's own run delivers the cancellation at its first checkpoint;
    // the terminal transition deletes the directly addressed Signal events.
    assertEquals(rt.runWorkflowInstance(wf, id), WorkflowRunResult.WorkflowCancelled)

    val counts = run(
      sql"""
        SELECT event_kind, count(*) FROM workflow_events
        WHERE workflow_id = ${wf.id} AND workflow_instance_key = 'k' AND workflow_instance_scope = '{}'
        GROUP BY event_kind
      """.query[(String, Int)].to[Vector]
    ).toMap
    assertEquals(counts.getOrElse("Signal", 0), 0, "the cancelled instance's directly addressed Signal events are deleted")
    assertEquals(counts.getOrElse("WorkflowCompleted", 0), 1)
    assertEquals(signal.send(id, "late")(using rt), SignalSendResult.InstanceAlreadyCompleted)
  }
}
