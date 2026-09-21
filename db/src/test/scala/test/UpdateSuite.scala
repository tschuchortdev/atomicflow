package test

import atomicflow.*
import atomicflow.given
import atomicflow.Cacheable.Simple.given
import doobie.implicits.*
import doobie.postgres.implicits.*

import java.time.Instant
import scala.concurrent.duration.*

class UpdateSuite extends PostgresWorkflowRuntimeSuite {

  given Cacheable[Throwable] = Cacheable.forThrowable.genericStringMessageSerializer

  private def updates(
      workflowId: WorkflowId,
      key: WorkflowInstanceKey,
      scope: String,
      updateKey: String
  ): Vector[(String, Option[String], Option[Instant])] =
    run(
      sql"""SELECT idempotency_key, result, handled_at FROM workflow_updates
            WHERE workflow_id = $workflowId AND workflow_instance_key = $key AND scope = $scope AND update_key = $updateKey
            ORDER BY created_at""".query[(String, Option[String], Option[Instant])].to[Vector]
    )

  test("happy path: send runs the workflow on the sender's thread; Success carries the response and awaitUpdate returns the output") {
    val rt = newRuntime
    val update = Update[String, String]("ask")
    val wf = Workflow[String, String]("happy") { in =>
      val out = Step.awaitUpdate[String, String, String]("upd", update)(input => (s"resp:$input", s"out:$input"))
      s"body:$out"
    }
    val id = rt.createWorkflowInstance(wf, "k", "in").id

    val res = update.send(wf, id, "hello")(using rt)
    assertEquals(res, UpdateSendResult.Success("resp:hello"))
    assertEquals(rt.runWorkflowInstance(wf, id), WorkflowRunResult.Result("body:out:hello"))
  }

  test("suspend-first: a suspended awaitUpdate is fired by the send's sender-thread run") {
    val rt = newRuntime
    val update = Update[String, String]("ask")
    val wf = Workflow[String, String]("suspend-first") { in =>
      Step.awaitUpdate[String, String, String]("upd", update)(input => (s"resp:$input", s"out:$input"))
      "done"
    }
    val id = rt.createWorkflowInstance(wf, "k", "in").id
    assertEquals(rt.runWorkflowInstance(wf, id), WorkflowRunResult.WorkflowSuspended)

    val res = update.send(wf, id, "hi")(using rt)
    assertEquals(res, UpdateSendResult.Success("resp:hi"))
    assertEquals(rt.runWorkflowInstance(wf, id), WorkflowRunResult.Result("done"))
  }

  test("unhandled: a run that never awaits the key returns Unhandled and deletes the record when persistUnhandledUpdates=false") {
    val rt = newRuntime
    val update = Update[String, String]("ask")
    val wf = Workflow[String, String]("no-await")(in => "plain")
    val id = rt.createWorkflowInstance(wf, "k", "in").id

    val res = update.send(wf, id, "x")(using rt)
    assertEquals(res, UpdateSendResult.Unhandled)
    assertEquals(updates(wf.id, "k", "", "ask"), Vector.empty, "the unhandled record is deleted by default")
  }

  test("completed instance returns InstanceAlreadyCompleted; missing instance throws WorkflowNotFoundException") {
    val rt = newRuntime
    val update = Update[String, String]("ask")
    val wf = Workflow[String, String]("completed")(in => "done")
    val id = rt.createWorkflowInstance(wf, "k", "in").id
    rt.runWorkflowInstance(wf, id)

    assertEquals(update.send(wf, id, "x")(using rt), UpdateSendResult.InstanceAlreadyCompleted)
    intercept[WorkflowNotFoundException] {
      update.send(wf, WorkflowInstanceId(wf.id, "nope"), "x")(using rt)
    }
  }

  test("idempotencyKey: the same key twice returns the same Success with a single record; different keys are independent") {
    val rt = newRuntime
    val update = Update[String, String]("ask")
    val wf = Workflow[String, String]("idem") { in =>
      Step.awaitUpdate[String, String, String]("upd1", update)(input => (s"r1:$input", s"o1:$input"))
      Step.awaitUpdate[String, String, String]("upd2", update)(input => (s"r2:$input", s"o2:$input"))
      "done"
    }
    val id = rt.createWorkflowInstance(wf, "k", "in").id

    val r1 = update.send(wf, id, "a", idempotencyKey = "ka")(using rt)
    assertEquals(r1, UpdateSendResult.Success("r1:a"))
    val r2 = update.send(wf, id, "a", idempotencyKey = "ka")(using rt)
    assertEquals(r2, UpdateSendResult.Success("r1:a"), "the second idempotent send returns the stored result")

    val r3 = update.send(wf, id, "b", idempotencyKey = "kb")(using rt)
    assertEquals(r3, UpdateSendResult.Success("r2:b"))
    val rows = updates(wf.id, "k", "", "ask")
    assertEquals(rows.size, 2, "different idempotency keys produce independent records")
    assertEquals(rows.map(_._1), Vector("ka", "kb"))
    assertEquals(rows.map(_._2), Vector(Some("r1:a"), Some("r2:b")))
  }

  test("handled empty-idempotency-key records are deleted; non-empty ones are kept for dedup") {
    val rt = newRuntime
    val update = Update[String, String]("ask")
    val wf = Workflow[String, String]("handled-cleanup") { in =>
      Step.awaitUpdate[String, String, String]("upd", update)(input => (s"resp:$input", s"out:$input"))
      "done"
    }
    val idEmpty = rt.createWorkflowInstance(wf, "k1", "in").id
    val idIdem = rt.createWorkflowInstance(wf, "k2", "in").id

    assertEquals(update.send(wf, idEmpty, "e", idempotencyKey = "")(using rt), UpdateSendResult.Success("resp:e"))
    assertEquals(updates(wf.id, "k1", "", "ask"), Vector.empty, "handled empty-idem record is deleted")

    assertEquals(update.send(wf, idIdem, "n", idempotencyKey = "ka")(using rt), UpdateSendResult.Success("resp:n"))
    val rows = updates(wf.id, "k2", "", "ask")
    assertEquals(rows.size, 1, "handled non-empty-idem record is kept")
    assertEquals(rows.head._1, "ka")
    assertEquals(rows.head._2, Some("resp:n"))
  }

  test("persistUnhandledUpdates=true keeps an unhandled record for a later awaitUpdate to consume") {
    val rt = newRuntime
    val update = Update[String, String]("ask")
    val signal = Signal[String]("go")
    val wf = Workflow[String, String]("persist") { in =>
      Step.await[String]("sig", Awaitable.SignalEvent(signal))
      Step.awaitUpdate[String, String, String]("upd", update)(input => (s"resp:$input", s"out:$input"))
      "done"
    }
    val id = rt.createWorkflowInstance(wf, "k", "in").id
    assertEquals(rt.runWorkflowInstance(wf, id), WorkflowRunResult.WorkflowSuspended)

    val r1 = update.send(wf, id, "hello", persistUnhandledUpdates = true)(using rt)
    assertEquals(r1, UpdateSendResult.Unhandled)
    assertEquals(updates(wf.id, "k", "", "ask").size, 1, "the unhandled record is kept")

    signal.send(id, "go")(using rt)
    assertEquals(rt.runWorkflowInstance(wf, id), WorkflowRunResult.Result("done"))
    val rows = updates(wf.id, "k", "", "ask")
    assertEquals(rows.size, 1)
    assertEquals(rows.head._2, Some("resp:hello"), "the kept record is now handled with the response")
  }

  test("updates are never inherited: an update sent to the parent is invisible to the child's awaitUpdate") {
    val rt = newRuntime
    val update = Update[String, String]("ask")
    val gate = Signal[String]("gate")
    val childWf = Workflow[String, String]("worker") { in =>
      Step.awaitUpdate[String, String, String]("upd", update)(input => (s"resp:$input", s"out:$input"))
      "done"
    }
    val parentWf = Workflow[String, String]("orders") { in =>
      childWf.startAsChild("worker-1", "hi", inheritSignals = SignalInheritance.all)
      Step.await[String]("gate", Awaitable.SignalEvent(gate))
      "parent-done"
    }
    val parentId = rt.createWorkflowInstance(parentWf, "order-42", "in").id
    assertEquals(rt.runWorkflowInstance(parentWf, parentId), WorkflowRunResult.WorkflowSuspended)

    val childScope = "orders/order-42@0"
    val childId = WorkflowInstanceId(childWf.id, "worker-1", childScope)
    assertEquals(rt.runWorkflowInstance(childWf, childId), WorkflowRunResult.WorkflowSuspended)

    assertEquals(update.send(parentWf, parentId, "hello")(using rt), UpdateSendResult.Unhandled)
    assertEquals(rt.runWorkflowInstance(childWf, childId), WorkflowRunResult.WorkflowSuspended, "the child stays suspended")
    assertEquals(
      updates(childWf.id, "worker-1", childScope, "ask"),
      Vector.empty,
      "the parent's update is not a candidate for the child"
    )
  }

  test("send waits for a live lease held by another owner to expire, then proceeds") {
    val rt = newRuntime
    val update = Update[String, String]("ask")
    val wf = Workflow[String, String]("lease") { in =>
      Step.awaitUpdate[String, String, String]("upd", update)(input => (s"resp:$input", s"out:$input"))
      "done"
    }
    val id = rt.createWorkflowInstance(wf, "k", "in").id
    assertEquals(rt.runWorkflowInstance(wf, id), WorkflowRunResult.WorkflowSuspended)

    run(
      sql"""UPDATE workflow_instances
            SET lease_owner = 'someone-else', lease_expires_at = now() + interval '300 milliseconds'
            WHERE workflow_id = ${wf.id} AND workflow_instance_key = 'k' AND scope = ''""".update.run
    )

    val res = update.send(wf, id, "hello", idempotencyKey = "lease-idem")(using rt)
    assertEquals(res, UpdateSendResult.Success("resp:hello"))
    assertEquals(updates(wf.id, "k", "", "ask").size, 1, "no duplicate record after the lease wait")
  }

  test("the response computed by the run's transaction is what the sender receives") {
    val rt = newRuntime
    val update = Update[String, String]("ask")
    var respondCalls = 0
    val wf = Workflow[String, String]("response") { in =>
      val out = Step.awaitUpdate[String, String, String]("upd", update) { input =>
        respondCalls += 1
        (s"computed:$input", s"out:$input")
      }
      s"done:$out"
    }
    val id = rt.createWorkflowInstance(wf, "k", "in").id

    assertEquals(update.send(wf, id, "zzz")(using rt), UpdateSendResult.Success("computed:zzz"))
    assertEquals(respondCalls, 1)
    assertEquals(rt.runWorkflowInstance(wf, id), WorkflowRunResult.Result("done:out:zzz"))
    assertEquals(respondCalls, 1, "the respond body is not re-run on the terminal re-run")
  }
}
