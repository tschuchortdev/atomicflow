package test

import atomicflow.*
import atomicflow.Cacheable.Simple.given
import doobie.implicits.*
import doobie.postgres.implicits.*

import java.time.Instant
import scala.concurrent.duration.*

class SignalSuite extends PostgresWorkflowRuntimeSuite {

  given Cacheable[Throwable] = Cacheable.forThrowable.genericStringMessageSerializer

  private def signalEvents(
      workflowId: WorkflowId,
      key: WorkflowInstanceKey
  ): Vector[(Long, String, String, String, Instant)] =
    run(
      sql"""SELECT sequence_id, event_kind, event_key, payload, created_at
            FROM workflow_events WHERE workflow_id = $workflowId AND key = $key AND scope = ''
            ORDER BY sequence_id""".query[(Long, String, String, String, Instant)].to[Vector]
    )

  test("send appends exactly one Signal event with the correct envelope and clock created_at") {
    val now = Instant.parse("2026-01-02T03:04:05Z")
    val rt = newRuntime(new TestClock(now))
    val wf = Workflow[String, String](id = "sig-env") { in => in }
    val id = rt.createWorkflowInstance(wf, "k", "in").id
    val signal = Signal[String]("greet")

    assertEquals(signal.send(id, "hello")(using rt), SignalSendResult.Success)

    val events = signalEvents(wf.id, "k")
    assertEquals(events.size, 1)
    val (seq, kind, eventKey, payload, createdAt) = events.head
    assert(seq > 0)
    assertEquals(kind, "Signal")
    assertEquals(eventKey, "greet")
    assertEquals(payload, "hello")
    assertEquals(createdAt, now)
  }

  test("sequence ids strictly increase across sends and are globally ordered") {
    val rt = newRuntime
    val wf = Workflow[String, String](id = "sig-seq") { in => in }
    val id = rt.createWorkflowInstance(wf, "k", "in").id
    val signal = Signal[String]("greet")

    signal.send(id, "one")(using rt)
    signal.send(id, "two")(using rt)
    signal.send(id, "three")(using rt)

    val ids = signalEvents(wf.id, "k").map(_._1)
    assertEquals(ids, ids.sorted)
    assertEquals(ids.distinct.size, 3)
    assertEquals(ids.head < ids(1), true)
    assertEquals(ids(1) < ids(2), true)
  }

  test("send to a missing instance throws WorkflowNotFoundException") {
    val rt = newRuntime
    val signal = Signal[String]("greet")
    intercept[WorkflowNotFoundException] {
      signal.send(WorkflowInstanceId("no-wf", "k"), "x")(using rt)
    }
  }

  test("send to a terminal instance returns InstanceAlreadyCompleted and appends no event") {
    val rt = newRuntime
    val wf = Workflow[String, String](id = "sig-term") { in => in }
    rt.createAndRun(wf, "k", "in")
    val signal = Signal[String]("greet")

    assertEquals(signal.send(WorkflowInstanceId(wf.id, "k"), "x")(using rt), SignalSendResult.InstanceAlreadyCompleted)
    assertEquals(signalEvents(wf.id, "k").count(_._2 == "Signal"), 0)
  }

  test("send does not acquire the execution lease; a live lease still accepts the event") {
    val rt = newRuntime
    val wf = Workflow[String, String](id = "sig-lease") { in => in }
    rt.createWorkflowInstance(wf, "k", "in")
    val leaseExpiry = Instant.now().plusSeconds(3600)
    run(
      sql"""UPDATE workflow_instances
            SET lease_owner = 'worker', fencing_token = fencing_token + 1, lease_expires_at = $leaseExpiry
            WHERE workflow_id = ${wf.id} AND key = 'k' AND scope = ''""".update.run
    )

    val signal = Signal[String]("greet")
    assertEquals(signal.send(WorkflowInstanceId(wf.id, "k"), "x")(using rt), SignalSendResult.Success)
    assertEquals(signalEvents(wf.id, "k").size, 1)
  }

  test("send upserts a wakeup row when a matching subscription exists") {
    val rt = newRuntime
    val wf = Workflow[String, String](id = "sig-wakeup") { in => in }
    rt.createWorkflowInstance(wf, "k", "in")
    run(
      sql"""INSERT INTO workflow_signal_subscriptions (workflow_id, key, scope, step_id, step_version, leaf_idx, signal_key)
            VALUES (${wf.id}, 'k', '', 'step', 1, 0, 'greet')""".update.run
    )

    val signal = Signal[String]("greet")
    assertEquals(signal.send(WorkflowInstanceId(wf.id, "k"), "x")(using rt), SignalSendResult.Success)

    val wake = run(
      sql"""SELECT scheduled_at FROM workflow_wakeups WHERE workflow_id = ${wf.id} AND key = 'k' AND scope = ''""".query[
          Instant
        ].option
    )
    assert(wake.isDefined)
  }

  test("send does not create a wakeup without a matching subscription") {
    val rt = newRuntime
    val wf = Workflow[String, String](id = "sig-nowake") { in => in }
    rt.createWorkflowInstance(wf, "k", "in")

    val signal = Signal[String]("greet")
    signal.send(WorkflowInstanceId(wf.id, "k"), "x")(using rt)

    val wake = run(
      sql"""SELECT 1 FROM workflow_wakeups WHERE workflow_id = ${wf.id} AND key = 'k' AND scope = ''""".query[Int].option
    )
    assertEquals(wake, None)
  }

  test("TestClock advanceBy changes the reported instant") {
    val start = Instant.parse("2026-01-02T00:00:00Z")
    val tc = new TestClock(start)
    assertEquals(tc.instant(), start)
    assertEquals(Instant.now(tc), start)
    tc.advanceBy(5.seconds)
    assertEquals(Instant.now(tc), start.plusSeconds(5))
    tc.advanceBy(2.seconds)
    assertEquals(Instant.now(tc), start.plusSeconds(7))
  }

  test("Signal carries its Cacheable and typed send resolves it") {
    val rt = newRuntime
    val wf = Workflow[String, String](id = "sig-typed") { in => in }
    rt.createWorkflowInstance(wf, "k", "in")

    val codec = new Cacheable[String] {
      override def stableSerializedTypeId: String = "custom-string"
      override def write(value: String): String = s"encoded[$value]"
      override def read(serialized: String): String = serialized
    }
    val signal = Signal[String]("typed")(using codec)
    signal.send(WorkflowInstanceId(wf.id, "k"), "abc")(using rt)

    assertEquals(signalEvents(wf.id, "k").head._4, codec.write("abc"))
  }
}
