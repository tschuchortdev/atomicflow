package test

import atomicflow.*
import atomicflow.Cacheable.Simple.given
import doobie.implicits.*
import doobie.postgres.implicits.*

import java.time.{Clock, Instant}
import java.util.concurrent.atomic.AtomicInteger
import scala.concurrent.duration.*

class TerminateSuite extends PostgresWorkflowRuntimeSuite {

  given Cacheable[Throwable] = Cacheable.forThrowable.genericStringMessageSerializer

  private def terminalRow(
      workflowId: WorkflowId,
      key: WorkflowInstanceKey
  ): Option[(Option[String], Option[String])] =
    run(
      sql"""SELECT terminal_state, terminal_outcome FROM workflow_instances
            WHERE workflow_id = $workflowId AND key = $key AND scope = ''""".query[
          (Option[String], Option[String])
        ].option
    )

  private def completedEvent(workflowId: WorkflowId, key: WorkflowInstanceKey): Option[(String, String, String)] =
    run(
      sql"""SELECT event_kind, event_key, payload FROM workflow_events
            WHERE workflow_id = $workflowId AND key = $key AND scope = '' AND event_kind = 'WorkflowCompleted'""".query[
          (String, String, String)
        ].option
    )

  private def cancelRequestedAt(workflowId: WorkflowId, key: WorkflowInstanceKey): Option[Instant] =
    run(
      sql"""SELECT cancel_requested_at FROM workflow_instances
            WHERE workflow_id = $workflowId AND key = $key AND scope = ''""".query[Option[Instant]].unique
    )

  private def leaseExpiry(workflowId: WorkflowId, key: WorkflowInstanceKey): Option[Instant] =
    run(
      sql"""SELECT lease_expires_at FROM workflow_instances
            WHERE workflow_id = $workflowId AND key = $key AND scope = ''""".query[Option[Instant]].option
    ).flatten

  private def instanceRow(workflowId: WorkflowId, key: WorkflowInstanceKey): Option[(Option[String], Option[String], Long)] =
    run(
      sql"""SELECT terminal_state, terminal_outcome, fencing_token FROM workflow_instances
            WHERE workflow_id = $workflowId AND key = $key AND scope = ''""".query[
          (Option[String], Option[String], Long)
        ].option
    )

  private def leaseOwner(workflowId: WorkflowId, key: WorkflowInstanceKey): Option[String] =
    run(
      sql"""SELECT lease_owner FROM workflow_instances
            WHERE workflow_id = $workflowId AND key = $key AND scope = ''""".query[Option[String]].option
    ).flatten

  private def countWakeups(workflowId: WorkflowId, key: WorkflowInstanceKey): Int =
    run(
      sql"""SELECT COUNT(*) FROM workflow_wakeups
            WHERE workflow_id = $workflowId AND key = $key AND scope = ''""".query[Int].unique
    )

  private def countSignalSubs(workflowId: WorkflowId, key: WorkflowInstanceKey): Int =
    run(
      sql"""SELECT COUNT(*) FROM workflow_signal_subscriptions
            WHERE workflow_id = $workflowId AND key = $key AND scope = ''""".query[Int].unique
    )

  private def countTimerSubs(workflowId: WorkflowId, key: WorkflowInstanceKey): Int =
    run(
      sql"""SELECT COUNT(*) FROM workflow_timer_subscriptions
            WHERE workflow_id = $workflowId AND key = $key AND scope = ''""".query[Int].unique
    )

  private def countCompletionSubs(workflowId: WorkflowId, key: WorkflowInstanceKey): Int =
    run(
      sql"""SELECT COUNT(*) FROM workflow_completion_subscriptions
            WHERE workflow_id = $workflowId AND key = $key AND scope = ''""".query[Int].unique
    )

  private def countSignalEvents(workflowId: WorkflowId, key: WorkflowInstanceKey): Int =
    run(
      sql"""SELECT COUNT(*) FROM workflow_events
            WHERE workflow_id = $workflowId AND key = $key AND scope = '' AND event_kind = 'Signal'""".query[Int].unique
    )

  private def stepStateKind(workflowId: WorkflowId, key: WorkflowInstanceKey, stepId: String): Option[String] =
    run(
      sql"""SELECT state_kind FROM workflow_steps
            WHERE workflow_id = $workflowId AND key = $key AND scope = '' AND step_id = $stepId AND step_version = 1""".query[
          String
        ].option
    )

  test("WorkflowCancelledException is a plain RuntimeException caught by NonFatal") {
    assert(scala.util.control.NonFatal.unapply(WorkflowCancelledException()).isDefined)
  }

  test("uncancellable: compensation steps execute inside the region, persist, and are not re-executed on re-run") {
    val rt = newRuntime
    val compCounter = new AtomicInteger(0)
    val sig = Signal[String]("approve")
    val wf = Workflow[String, String](id = "uncancellable-comp") { in =>
      Step.atLeastOnce[String]("A") { "a" }
      try {
        Step.await[String]("wait", Awaitable.SignalEvent(sig))
        "not-reached"
      } catch {
        case _: WorkflowCancelledException =>
          Workflow.uncancellable {
            Step.atLeastOnce[String]("comp1") { compCounter.incrementAndGet(); "c1" }
            Step.atLeastOnce[String]("comp2") { compCounter.incrementAndGet(); "c2" }
          }
          throw new WorkflowCancelledException("rethrow")
      }
    }
    val id = rt.createWorkflowInstance(wf, "k", "in").id

    assertEquals(rt.runWorkflowInstance(wf, id), WorkflowRunResult.WorkflowSuspended)
    rt.cancel(id)

    assertEquals(rt.runWorkflowInstance(wf, id), WorkflowRunResult.WorkflowCancelled)
    assertEquals(compCounter.get(), 2, "both compensation steps executed inside the region despite cancellation")
    assertEquals(stepStateKind(wf.id, "k", "comp1"), Some("succeeded"), "comp1's succeeded row persisted")
    assertEquals(stepStateKind(wf.id, "k", "comp2"), Some("succeeded"), "comp2's succeeded row persisted")
    assertEquals(terminalRow(wf.id, "k").get._1, Some("cancelled"))

    assertEquals(rt.runWorkflowInstance(wf, id), WorkflowRunResult.WorkflowCancelled)
    assertEquals(compCounter.get(), 2, "a re-run must not re-execute the cached compensation steps")
  }

  test("uncancellable exit: the next checkpoint after the region throws again (sticky)") {
    val rt = newRuntime
    val counter = new AtomicInteger(0)
    val sig = Signal[String]("approve")
    val wf = Workflow[String, String](id = "sticky-after-uncancellable") { in =>
      Step.atLeastOnce[String]("A") { "a" }
      try {
        Step.await[String]("wait", Awaitable.SignalEvent(sig))
        "not-reached"
      } catch {
        case _: WorkflowCancelledException =>
          Workflow.uncancellable {
            Step.atLeastOnce[String]("comp") { counter.incrementAndGet(); "c" }
          }
          Step.atLeastOnce[String]("after") { counter.incrementAndGet(); "after" }
          "unreachable"
      }
    }
    val id = rt.createWorkflowInstance(wf, "k", "in").id

    assertEquals(rt.runWorkflowInstance(wf, id), WorkflowRunResult.WorkflowSuspended)
    rt.cancel(id)

    assertEquals(rt.runWorkflowInstance(wf, id), WorkflowRunResult.WorkflowCancelled)
    assertEquals(counter.get(), 1, "only the in-region step ran; the post-region step's checkpoint threw before its body")
  }

  test("uncancellable does not clear cancel_requested_at") {
    val rt = newRuntime
    val sig = Signal[String]("approve")
    val wf = Workflow[String, String](id = "no-clear") { in =>
      try {
        Step.await[String]("wait", Awaitable.SignalEvent(sig))
        "unreachable"
      } catch {
        case _: WorkflowCancelledException =>
          Workflow.uncancellable {
            Step.atLeastOnce[String]("comp") { "c" }
          }
          "compensated"
      }
    }
    val id = rt.createWorkflowInstance(wf, "k", "in").id

    assertEquals(rt.runWorkflowInstance(wf, id), WorkflowRunResult.WorkflowSuspended)
    rt.cancel(id)
    assert(cancelRequestedAt(wf.id, "k").isDefined)

    assertEquals(rt.runWorkflowInstance(wf, id), WorkflowRunResult.Result("compensated"))
    assertEquals(terminalRow(wf.id, "k").get._1, Some("completed"))
    assert(cancelRequestedAt(wf.id, "k").isDefined, "cancel_requested_at must remain set after an uncancellable region")
  }

  test("heartbeat continues inside an uncancellable region") {
    val clock = new TestClock(Instant.parse("2026-01-02T00:00:00Z"))
    val rt = newRuntime(clock)
    given Clock = clock
    var before: Option[Instant] = None
    var after: Option[Instant] = None
    val sig = Signal[String]("approve")
    val wf = Workflow[String, String](id = "hb-region") { in =>
      try {
        Step.await[String]("wait", Awaitable.SignalEvent(sig))
        "unreachable"
      } catch {
        case _: WorkflowCancelledException =>
          Workflow.uncancellable {
            Step.atLeastOnce[String]("comp") {
              before = leaseExpiry("hb-region", "k")
              clock.advanceBy(1.minute)
              Workflow.heartbeat()
              after = leaseExpiry("hb-region", "k")
              "c"
            }
          }
          "done"
      }
    }
    val id = rt.createWorkflowInstance(wf, "k", "in").id

    assertEquals(rt.runWorkflowInstance(wf, id), WorkflowRunResult.WorkflowSuspended)
    rt.cancel(id)
    assertEquals(rt.runWorkflowInstance(wf, id), WorkflowRunResult.Result("done"))

    assert(before.isDefined, "the lease should be held when the region step starts")
    assert(after.isDefined, "the lease should remain held after the in-region heartbeat")
    assert(after.get.isAfter(before.get), s"heartbeat inside the region must renew the lease: before=$before after=$after")
  }

  test("an await inside an uncancellable region suspends despite the cancel flag and resolves via a signal") {
    val rt = newRuntime
    val wfId = "await-in-region"
    val sig = Signal[String]("compensate")
    val wf = Workflow[String, String](id = wfId) { in =>
      try {
        Step.atLeastOnce[String]("A") { rt.cancel(WorkflowInstanceId(wfId, "k")); "a" }
        Step.atLeastOnce[String]("B") { "b" }
        "not-reached"
      } catch {
        case _: WorkflowCancelledException =>
          Workflow.uncancellable {
            Step.await[String]("wait2", Awaitable.SignalEvent(sig))
          }
          "compensated"
      }
    }
    val id = rt.createWorkflowInstance(wf, "k", "in").id

    assertEquals(
      rt.runWorkflowInstance(wf, id),
      WorkflowRunResult.WorkflowSuspended,
      "the await inside the region must suspend normally despite cancel_requested_at being set"
    )
    assert(cancelRequestedAt(wf.id, "k").isDefined, "the self-cancel must have set cancel_requested_at")
    assertEquals(countSignalSubs(wf.id, "k"), 1, "the await registered its subscription inside the region")

    sig.send(id, "v")(using rt)
    assertEquals(
      rt.runWorkflowInstance(wf, id),
      WorkflowRunResult.Result("compensated"),
      "the signal resolves the await inside the region; the compensation completes"
    )
    assertEquals(terminalRow(wf.id, "k").get._1, Some("completed"))
  }

  test("terminate on a suspended instance: TERMINATED + event; wakeups/subscriptions deleted; body never runs on re-run") {
    val rt = newRuntime
    val bodyCounter = new AtomicInteger(0)
    val sig = Signal[String]("approve")
    val wfId = "suspend-term"
    val wf = Workflow[String, String](id = wfId) { in =>
      bodyCounter.incrementAndGet()
      Step.await[String]("wait", Awaitable.SignalEvent(sig))
      "done"
    }
    val id = rt.createWorkflowInstance(wf, "k", "in").id

    assertEquals(rt.runWorkflowInstance(wf, id), WorkflowRunResult.WorkflowSuspended)
    assertEquals(bodyCounter.get(), 1)
    assertEquals(countSignalSubs(wf.id, "k"), 1, "a pending signal subscription exists while suspended")
    sig.send(id, "v")(using rt)
    assertEquals(countSignalEvents(wf.id, "k"), 1, "a directly addressed Signal event exists while suspended")
    assertEquals(countWakeups(wf.id, "k"), 1, "the signal send upserts a wakeup for the suspended instance")

    rt.terminate(id)

    assertEquals(terminalRow(wf.id, "k").get._1, Some("terminated"))
    val event = completedEvent(wf.id, "k").get
    assertEquals(event._1, "WorkflowCompleted")
    assertEquals(Cacheable[WorkflowCompletionResult[String]].read(event._3), WorkflowCompletionResult.Terminated)
    assertEquals(countWakeups(wf.id, "k"), 0, "terminate must delete the wakeup row")
    assertEquals(countSignalSubs(wf.id, "k"), 0, "terminate must delete signal subscriptions")
    assertEquals(countTimerSubs(wf.id, "k"), 0, "terminate must delete timer subscriptions")
    assertEquals(countCompletionSubs(wf.id, "k"), 0, "terminate must delete completion subscriptions")
    assertEquals(countSignalEvents(wf.id, "k"), 0, "terminate must delete directly addressed Signal events")

    assertEquals(rt.runWorkflowInstance(wf, id), WorkflowRunResult.WorkflowTerminated)
    assertEquals(bodyCounter.get(), 1, "re-running a terminated instance must never execute the body")
  }

  test("terminate while running revokes the lease via a fencing-token bump; a re-run returns WorkflowTerminated") {
    val rt = newRuntime
    val wfId = "running-term"
    val wf = Workflow[String, String](id = wfId) { in => s"out-$in" }
    val id = rt.createWorkflowInstance(wf, "k", "a").id
    run(
      sql"""UPDATE workflow_instances SET lease_owner = 'some-worker', lease_expires_at = now() + interval '1 hour'
            WHERE workflow_id = $wfId AND key = 'k' AND scope = ''""".update.run
    )
    val before = instanceRow(wf.id, "k").get._3

    rt.terminate(id)

    val row = instanceRow(wf.id, "k").get
    assertEquals(row._1, Some("terminated"))
    assertEquals(row._3, before + 1, "terminate must bump the fencing token to revoke the lease")
    assertEquals(leaseOwner(wf.id, "k"), None, "terminate must clear the lease owner")
    assertEquals(leaseExpiry(wf.id, "k"), None, "terminate must clear the lease expiry")

    assertEquals(rt.runWorkflowInstance(wf, id), WorkflowRunResult.WorkflowTerminated)
    assertEquals(instanceRow(wf.id, "k").get._3, before + 1, "a terminal read short-circuits without re-acquiring")
  }

  test("an orphan fenced write after terminate throws LeaseLostException and does not become terminal") {
    val rt = newRuntime
    val wfId = "fenced"
    val wf = Workflow[String, String](id = wfId) { in =>
      Step.atLeastOnce[String]("step") {
        rt.terminate(WorkflowInstanceId(wfId, "k"))
        "result"
      }
      "done"
    }

    intercept[LeaseLostException] {
      rt.createAndRun(wf, "k", "a")
    }
    assertEquals(terminalRow(wf.id, "k").get._1, Some("terminated"))
  }

  test("terminate on an unknown instance throws WorkflowNotFoundException") {
    val rt = newRuntime
    intercept[WorkflowNotFoundException] {
      rt.terminate(WorkflowInstanceId("no-such-workflow", "k"))
    }
  }

  test("terminate on a terminal instance is a no-op") {
    val rt = newRuntime
    val wf = Workflow[String, String](id = "term-noop") { in => s"out-$in" }
    val id = rt.createWorkflowInstance(wf, "k", "a").id
    assertEquals(rt.runWorkflowInstance(wf, id), WorkflowRunResult.Result("out-a"))

    rt.terminate(id)

    assertEquals(terminalRow(wf.id, "k").get._1, Some("completed"))
    assertEquals(completedEvent(wf.id, "k").map(_._1), Some("WorkflowCompleted"))
  }

  test("interrupt clearing: a body that interrupts itself still completes normally") {
    val rt = newRuntime
    val wf = Workflow[String, String](id = "interrupt") { in =>
      Thread.currentThread().interrupt()
      Step.atLeastOnce[String]("A") { "a" }
      "done"
    }
    assertEquals(rt.createAndRun(wf, "k", "a"), WorkflowRunResult.Result("done"))
    assertEquals(terminalRow(wf.id, "k").get._1, Some("completed"))
  }
}
