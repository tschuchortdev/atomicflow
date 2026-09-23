package test

import atomicflow.*
import atomicflow.Cacheable.Simple.given
import doobie.implicits.*
import doobie.postgres.implicits.*

import java.time.Instant
import java.util.concurrent.atomic.AtomicInteger
import scala.concurrent.duration.*

class CancelSuite extends PostgresWorkflowRuntimeSuite {

  given Cacheable[Throwable] = Cacheable.forThrowable.genericStringMessageSerializer

  private def cancelRequestedAt(workflowId: WorkflowId, key: WorkflowInstanceKey): Option[Instant] =
    run(
      sql"""SELECT cancel_requested_at FROM workflow_instances
            WHERE workflow_id = $workflowId AND workflow_instance_key = $key AND scope = ''""".query[Option[Instant]].unique
    )

  private def terminalRow(
      workflowId: WorkflowId,
      key: WorkflowInstanceKey
  ): Option[(Option[String], Option[String])] =
    run(
      sql"""SELECT terminal_state, terminal_outcome FROM workflow_instances
            WHERE workflow_id = $workflowId AND workflow_instance_key = $key AND scope = ''""".query[
          (Option[String], Option[String])
        ].option
    )

  private def completedEvent(workflowId: WorkflowId, key: WorkflowInstanceKey): Option[(String, String, String)] =
    run(
      sql"""SELECT event_kind, event_key, payload FROM workflow_events
            WHERE workflow_id = $workflowId AND workflow_instance_key = $key AND scope = '' AND event_kind = 'WorkflowCompleted'""".query[
          (String, String, String)
        ].option
    )

  private def hasWakeup(workflowId: WorkflowId, key: WorkflowInstanceKey): Boolean =
    run(
      sql"""SELECT 1 FROM workflow_wakeups WHERE workflow_id = $workflowId AND workflow_instance_key = $key AND scope = ''""".query[Int].option
    ).isDefined

  test("cancel before first start finalizes CANCELLED immediately and the body never runs") {
    val clock = new TestClock(Instant.parse("2026-01-02T00:00:00Z"))
    val rt = newRuntime(clock)
    val counter = new AtomicInteger(0)
    val wf = Workflow[String, String](id = "created-cancel") { in =>
      counter.incrementAndGet()
      s"out-$in"
    }
    val id = rt.createWorkflowInstance(wf, "k", "a").id

    rt.cancel(id)

    assertEquals(counter.get(), 0, "the body must never run")
    assertEquals(terminalRow(wf.id, "k").get._1, Some("cancelled"))
    assertEquals(
      terminalRow(wf.id, "k").get._2.map(Cacheable[WorkflowCompletionResult[String]].read(_)),
      Some(WorkflowCompletionResult.Cancelled)
    )
    val event = completedEvent(wf.id, "k").get
    assertEquals(event._1, "WorkflowCompleted")
    assertEquals(
      Cacheable[WorkflowCompletionResult[String]].read(event._3),
      WorkflowCompletionResult.Cancelled
    )
    assertEquals(
      rt.runWorkflowInstance(wf, id),
      WorkflowRunResult.WorkflowCancelled,
      "re-running a cancelled-before-start instance returns WorkflowCancelled"
    )
  }

  test("cancel on a terminal instance is a no-op") {
    val rt = newRuntime
    val wf = Workflow[String, String](id = "terminal-noop") { in => s"out-$in" }
    val id = rt.createWorkflowInstance(wf, "k", "a").id
    assertEquals(rt.runWorkflowInstance(wf, id), WorkflowRunResult.Result("out-a"))

    rt.cancel(id)

    assertEquals(terminalRow(wf.id, "k").get._1, Some("completed"))
    assertEquals(cancelRequestedAt(wf.id, "k"), None, "no cancel timestamp is set on a terminal instance")
    assertEquals(completedEvent(wf.id, "k").map(_._1), Some("WorkflowCompleted"))
  }

  test("cancel while suspended delivers at the frontier await; the cached step is not re-executed; boundary returns WorkflowCancelled") {
    val clock = new TestClock(Instant.parse("2026-01-02T00:00:00Z"))
    val rt = newRuntime(clock)
    val counter = new AtomicInteger(0)
    val sig = Signal[String]("approve")
    val wf = Workflow[String, String](id = "suspended-cancel") { in =>
      Step.atLeastOnce[String]("A") { counter.incrementAndGet(); "a" }
      val approved = Step.await[String]("wait", Awaitable.SignalEvent(sig))
      s"$in-$approved"
    }
    val id = rt.createWorkflowInstance(wf, "k", "in").id

    assertEquals(rt.runWorkflowInstance(wf, id), WorkflowRunResult.WorkflowSuspended)
    assertEquals(counter.get(), 1, "step A executed on the first run")

    rt.cancel(id)
    assertEquals(cancelRequestedAt(wf.id, "k").isDefined, true)
    assertEquals(hasWakeup(wf.id, "k"), true, "cancel with no live owner upserts a wakeup to schedule the resume")

    assertEquals(rt.runWorkflowInstance(wf, id), WorkflowRunResult.WorkflowCancelled)
    assertEquals(counter.get(), 1, "step A's cached result must NOT be re-executed")
    assertEquals(terminalRow(wf.id, "k").get._1, Some("cancelled"))
  }

  test("cancel while running (self-cancel from a step body) delivers at the next step checkpoint") {
    val rt = newRuntime
    val counter = new AtomicInteger(0)
    val wfId = "running-cancel"
    val wf = Workflow[String, String](id = wfId) { in =>
      Step.atLeastOnce[String]("A") { rt.cancel(WorkflowInstanceId(wfId, "k")); counter.incrementAndGet(); "a" }
      Step.atLeastOnce[String]("B") { counter.incrementAndGet(); "b" }
      "done"
    }
    val id = rt.createWorkflowInstance(wf, "k", "in").id

    assertEquals(rt.runWorkflowInstance(wf, id), WorkflowRunResult.WorkflowCancelled)
    assertEquals(counter.get(), 1, "step A ran; step B's checkpoint threw before its body executed")
    assertEquals(terminalRow(wf.id, "k").get._1, Some("cancelled"))
  }

  test("sticky redelivery: catching WorkflowCancelledException and continuing throws again at the next new-work checkpoint") {
    val rt = newRuntime
    val compCounter = new AtomicInteger(0)
    val sig = Signal[String]("approve")
    val wf = Workflow[String, String](id = "sticky") { in =>
      try {
        Step.atLeastOnce[String]("A") { "a" }
        val v = Step.await[String]("wait", Awaitable.SignalEvent(sig))
        "not-reached"
      } catch {
        case _: WorkflowCancelledException =>
          Step.atLeastOnce[String]("comp") { compCounter.incrementAndGet(); "compensated" }
          "compensated-done"
      }
    }
    val id = rt.createWorkflowInstance(wf, "k", "in").id

    assertEquals(rt.runWorkflowInstance(wf, id), WorkflowRunResult.WorkflowSuspended)
    rt.cancel(id)

    assertEquals(rt.runWorkflowInstance(wf, id), WorkflowRunResult.WorkflowCancelled)
    assertEquals(compCounter.get(), 0, "the compensation step's body never runs: the sticky checkpoint throws before it")
    assertEquals(terminalRow(wf.id, "k").get._1, Some("cancelled"))
  }

  test("a workflow that completes despite the pending cancel is COMPLETED (first-terminal-event rule)") {
    val clock = new TestClock(Instant.parse("2026-01-02T00:00:00Z"))
    val rt = newRuntime(clock)
    val sig = Signal[String]("approve")
    val wf = Workflow[String, String](id = "complete-anyway") { in =>
      Step.atLeastOnce[String]("A") { "a" }
      try {
        Step.await[String]("wait", Awaitable.SignalEvent(sig))
        "unreachable"
      } catch {
        case _: WorkflowCancelledException => "caught"
      }
    }
    val id = rt.createWorkflowInstance(wf, "k", "in").id

    assertEquals(rt.runWorkflowInstance(wf, id), WorkflowRunResult.WorkflowSuspended)
    rt.cancel(id)

    assertEquals(rt.runWorkflowInstance(wf, id), WorkflowRunResult.Result("caught"))
    val row = terminalRow(wf.id, "k").get
    assertEquals(row._1, Some("completed"))
    assertEquals(cancelRequestedAt(wf.id, "k").isDefined, true, "cancel_requested_at stays set even though the instance completed")
  }

  test("cancel_requested_at is set once and never reset") {
    val clock = new TestClock(Instant.parse("2026-01-02T00:00:00Z"))
    val rt = newRuntime(clock)
    val sig = Signal[String]("approve")
    val wf = Workflow[String, String](id = "set-once") { in =>
      Step.await[String]("wait", Awaitable.SignalEvent(sig))
      "done"
    }
    val id = rt.createWorkflowInstance(wf, "k", "in").id

    assertEquals(rt.runWorkflowInstance(wf, id), WorkflowRunResult.WorkflowSuspended)
    rt.cancel(id)
    val t1 = cancelRequestedAt(wf.id, "k").get

    clock.advanceBy(1.hour)
    rt.cancel(id)
    val t2 = cancelRequestedAt(wf.id, "k").get
    assertEquals(t2, t1, "the timestamp must not change on a second cancel")
  }

  test("cancel on an unknown instance throws WorkflowNotFoundException") {
    val rt = newRuntime
    intercept[WorkflowNotFoundException] {
      rt.cancel(WorkflowInstanceId("no-such-workflow", "k"))
    }
  }
}
