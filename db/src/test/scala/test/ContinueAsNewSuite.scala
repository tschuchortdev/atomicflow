package test

import atomicflow.*
import atomicflow.Cacheable.Simple.given
import doobie.implicits.*
import doobie.postgres.implicits.*

import java.time.Instant
import java.util.concurrent.atomic.AtomicInteger
import scala.concurrent.duration.*

class ContinueAsNewSuite extends PostgresWorkflowRuntimeSuite {

  given Cacheable[Throwable] = Cacheable.forThrowable.genericStringMessageSerializer

  private final case class ChildRow(
      terminalState: Option[String],
      timesExecuted: Int,
      cancelRequestedAt: Option[Instant],
      parentWf: Option[String],
      parentKey: Option[String],
      parentScope: Option[String]
  )

  private def childRow(workflowId: WorkflowId, key: WorkflowInstanceKey, scope: String): Option[ChildRow] =
    run(
      sql"""SELECT terminal_state, times_executed, cancel_requested_at, parent_workflow_id, parent_instance_key, parent_scope
            FROM workflow_instances
            WHERE workflow_id = $workflowId AND key = $key AND scope = $scope""".query[
          (Option[String], Int, Option[Instant], Option[String], Option[String], Option[String])
        ].option
    ).map { case (t, te, c, pw, pk, ps) => ChildRow(t, te, c, pw, pk, ps) }

  private def countSteps(workflowId: WorkflowId, key: WorkflowInstanceKey): Int =
    run(
      sql"""SELECT COUNT(*) FROM workflow_steps
            WHERE workflow_id = $workflowId AND key = $key AND scope = ''""".query[Int].unique
    )

  private def countSignalEvents(workflowId: WorkflowId, key: WorkflowInstanceKey): Int =
    run(
      sql"""SELECT COUNT(*) FROM workflow_events
            WHERE workflow_id = $workflowId AND key = $key AND scope = '' AND event_kind = 'Signal'""".query[Int].unique
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

  private def countWakeups(workflowId: WorkflowId, key: WorkflowInstanceKey): Int =
    run(
      sql"""SELECT COUNT(*) FROM workflow_wakeups
            WHERE workflow_id = $workflowId AND key = $key AND scope = ''""".query[Int].unique
    )

  private def storedInput(workflowId: WorkflowId, key: WorkflowInstanceKey): String =
    run(
      sql"""SELECT input FROM workflow_instances
            WHERE workflow_id = $workflowId AND key = $key AND scope = ''""".query[String].unique
    )

  test("continueAsNew returns the ContinueAsNew outcome, increments generation, erases old-gen steps, and the next run starts fresh with the new input") {
    val rt = newRuntime
    val counter = new AtomicInteger(0)
    val wf = Workflow[String, String]("can-basic") { in =>
      Step.atLeastOnce[String]("A") { counter.incrementAndGet(); in }
      if (in == "start") Workflow.continueAsNew("next")
      "final-" + in
    }
    val id = rt.createWorkflowInstance(wf, "k", "start").id
    val handle = rt.getWorkflowInstance(wf, id)

    assertEquals(rt.runWorkflowInstance(wf, id), WorkflowRunResult.ContinueAsNew)
    assertEquals(handle.getInfo()(using rt).generation, 1L, "generation increments on continueAsNew")
    assertEquals(storedInput(wf.id, "k"), "next", "the new input is installed durably")
    assertEquals(countSteps(wf.id, "k"), 0, "old-generation step rows are erased")
    assertEquals(counter.get(), 1, "gen 0 executed step A once")

    assertEquals(rt.runWorkflowInstance(wf, id), WorkflowRunResult.Result("final-next"))
    assertEquals(handle.getInfo()(using rt).generation, 1L)
    assertEquals(counter.get(), 2, "gen 1 re-ran step A from a fresh history")
    assertEquals(handle.getInfo()(using rt).terminalState, Some(WorkflowTerminalState.Completed))
  }

  test("continueAsNew keeps identity: same id/key, a single instance row, no duplicates") {
    val rt = newRuntime
    val wf = Workflow[String, String]("can-identity") { in =>
      if (in == "start") Workflow.continueAsNew("next")
      "done"
    }
    val id = rt.createWorkflowInstance(wf, "k", "start").id
    assertEquals(rt.runWorkflowInstance(wf, id), WorkflowRunResult.ContinueAsNew)
    assertEquals(rt.runWorkflowInstance(wf, id), WorkflowRunResult.Result("done"))

    val rows = run(
      sql"""SELECT COUNT(*) FROM workflow_instances
            WHERE workflow_id = ${wf.id} AND key = 'k' AND scope = ''""".query[Int].unique
    )
    assertEquals(rows, 1, "there is exactly one instance row for the key")
  }

  test("signal cursors survive continueAsNew: gen-0 consumed events stay consumed, a new event sent after the transition is visible in gen 1") {
    val rt = newRuntime
    val sig = Signal[String]("S")
    val wf = Workflow[String, String]("can-cursor") { in =>
      if (in == "gen0") {
        Step.await[String]("wait0", Awaitable.SignalEvent(sig))
        Workflow.continueAsNew("gen1")
      } else {
        Step.await[String]("wait1", Awaitable.SignalEvent(sig))
        "saw:" + in
      }
    }
    val id = rt.createWorkflowInstance(wf, "k", "gen0").id
    assertEquals(rt.runWorkflowInstance(wf, id), WorkflowRunResult.WorkflowSuspended, "gen 0 awaits the signal")
    sig.send(id, "v0")(using rt)
    assertEquals(rt.runWorkflowInstance(wf, id), WorkflowRunResult.ContinueAsNew, "gen 0 consumes the signal and continues as new")
    assertEquals(countSignalEvents(wf.id, "k"), 0, "the gen-0 Signal event row is deleted at the transition")

    assertEquals(rt.runWorkflowInstance(wf, id), WorkflowRunResult.WorkflowSuspended, "gen 1 awaits; the consumed event is not re-visible")
    sig.send(id, "v1")(using rt)
    assertEquals(
      rt.runWorkflowInstance(wf, id),
      WorkflowRunResult.Result("saw:gen1"),
      "a NEW event after the transition has a higher sequenceId than the preserved cursor and is visible"
    )
  }

  test("unconsumed directly-addressed Signal events from gen 0 are deleted and onUnconsumedSignals runs") {
    val rt = newRuntime
    val sig = Signal[String]("S")
    var handlerCalled = false
    var handlerMap: Map[SignalKey, Seq[String]] = Map.empty
    val wf = Workflow[String, String](id = "can-events")(
      in => {
        if (in == "start") Workflow.continueAsNew("next")
        "done"
      },
      onUnconsumedSignals = m => { handlerCalled = true; handlerMap = m }
    )
    val id = rt.createWorkflowInstance(wf, "k", "start").id
    sig.send(id, "u1")(using rt)
    sig.send(id, "u2")(using rt)

    assertEquals(rt.runWorkflowInstance(wf, id), WorkflowRunResult.ContinueAsNew)
    assert(handlerCalled, "onUnconsumedSignals must run before the transition")
    assertEquals(handlerMap.get("S"), Some(Seq("u1", "u2")))
    assertEquals(countSignalEvents(wf.id, "k"), 0, "directly addressed Signal events are deleted")
  }

  test("continueAsNew closes CREATED children per policy: Cancel finalizes CANCELLED, Abandon detaches") {
    val rt = newRuntime
    val childWf = Workflow[String, String]("can-worker4") { in => in }
    val parentWf = Workflow[String, String]("can-parent4") { in =>
      childWf.startAsChild("cancel-child", "hi", parentClosePolicy = ParentClosePolicy.Cancel)
      childWf.startAsChild("abandon-child", "hi", parentClosePolicy = ParentClosePolicy.Abandon)
      Workflow.continueAsNew("next")
    }
    val parentId = rt.createWorkflowInstance(parentWf, "p", "start").id
    assertEquals(rt.runWorkflowInstance(parentWf, parentId), WorkflowRunResult.ContinueAsNew)

    val scope = "can-parent4/p@0"
    assertEquals(childRow(childWf.id, "cancel-child", scope).get.terminalState, Some("cancelled"))
    val ab = childRow(childWf.id, "abandon-child", scope).get
    assertEquals(ab.terminalState, None, "an abandoned child is not cancelled")
    assertEquals(ab.cancelRequestedAt, None)
    assertEquals(ab.parentWf, None, "the abandon child's active-parent pointer is cleared")
  }

  test("continueAsNew does not wait for cooperative cancellation of a SUSPENDED child under Cancel") {
    val rt = newRuntime
    val parentGate = Signal[String]("pgate")
    val childGate = Signal[String]("cgate")
    val childWf = Workflow[String, String]("can-worker3") { in =>
      Step.await[String]("cwait", Awaitable.SignalEvent(childGate))
      "done"
    }
    val parentWf = Workflow[String, String]("can-parent3") { in =>
      childWf.startAsChild("cancel-child", "hi", parentClosePolicy = ParentClosePolicy.Cancel)
      if (in == "first") {
        Step.await[String]("pgate", Awaitable.SignalEvent(parentGate))
        Workflow.continueAsNew("next")
      }
      "done"
    }
    val parentId = rt.createWorkflowInstance(parentWf, "p", "first").id
    assertEquals(rt.runWorkflowInstance(parentWf, parentId), WorkflowRunResult.WorkflowSuspended)

    val scope = "can-parent3/p@0"
    val childId = rt.getWorkflowInstancesByPrefix(childWf.id, "cancel-child", scope).head.id
    assertEquals(rt.runWorkflowInstance(childWf, childId), WorkflowRunResult.WorkflowSuspended)

    parentGate.send(parentId, "go")(using rt)
    assertEquals(rt.runWorkflowInstance(parentWf, parentId), WorkflowRunResult.ContinueAsNew)

    val row = childRow(childWf.id, "cancel-child", scope).get
    assertEquals(row.terminalState, None, "the continuation does not wait for cooperative child cancellation")
    assertEquals(row.cancelRequestedAt.isDefined, true, "the suspended child's cancellation is requested")
    assertEquals(row.parentWf, None, "the parent reference is cleared")
  }

  test("a child that continues as new keeps its parent and inheritance configuration") {
    val rt = newRuntime
    val parentGate = Signal[String]("pgate")
    val childWf = Workflow[String, String]("can-child5") { in =>
      if (in == "start") Workflow.continueAsNew("next")
      "done"
    }
    val parentWf = Workflow[String, String]("can-parent5") { in =>
      childWf.startAsChild("c", "start", inheritSignals = SignalInheritance.all)
      Step.await[String]("pgate", Awaitable.SignalEvent(parentGate))
      "parent-done"
    }
    val parentId = rt.createWorkflowInstance(parentWf, "p", "in").id
    assertEquals(rt.runWorkflowInstance(parentWf, parentId), WorkflowRunResult.WorkflowSuspended)

    val scope = "can-parent5/p@0"
    val childId = rt.getWorkflowInstancesByPrefix(childWf.id, "c", scope).head.id
    assertEquals(rt.runWorkflowInstance(childWf, childId), WorkflowRunResult.ContinueAsNew)

    val row = childRow(childWf.id, "c", scope).get
    assertEquals(row.parentWf, Some("can-parent5"), "the child stays attached to its parent")
    assertEquals(row.parentKey, Some("p"))
    assertEquals(row.parentScope, Some(""))
    val inherit = run(
      sql"""SELECT inherit_signals, inherit_past_events FROM workflow_instances
            WHERE workflow_id = ${childWf.id} AND key = 'c' AND scope = $scope""".query[(String, Boolean)].unique
    )
    assertEquals(inherit._1, "all", "the inheritance configuration is unchanged")
    assertEquals(inherit._2, false)
  }

  test("continueAsNew erases old-generation step, subscription, and wakeup rows atomically and schedules the next run") {
    val rt = newRuntime
    val sig = Signal[String]("S")
    val wf = Workflow[String, String]("can-atomic") { in =>
      Step.await[String]("wait", Awaitable.SignalEvent(sig))
      if (in == "start") Workflow.continueAsNew("next")
      "done"
    }
    val id = rt.createWorkflowInstance(wf, "k", "start").id
    assertEquals(rt.runWorkflowInstance(wf, id), WorkflowRunResult.WorkflowSuspended)
    assertEquals(countSignalSubs(wf.id, "k"), 1)
    sig.send(id, "go")(using rt)
    assertEquals(countWakeups(wf.id, "k"), 1, "the signal send schedules a wakeup")

    assertEquals(rt.runWorkflowInstance(wf, id), WorkflowRunResult.ContinueAsNew)

    assertEquals(countSteps(wf.id, "k"), 0, "no old-generation step rows remain")
    assertEquals(countSignalSubs(wf.id, "k"), 0)
    assertEquals(countTimerSubs(wf.id, "k"), 0)
    assertEquals(countCompletionSubs(wf.id, "k"), 0)
    assertEquals(countWakeups(wf.id, "k"), 1, "a fresh wakeup is upserted so the runner picks up the new generation")

    sig.send(id, "go2")(using rt)
    assertEquals(rt.runWorkflowInstance(wf, id), WorkflowRunResult.Result("done"))
  }

  test("a continueAsNew whose new-input codec fails to decode does not consume pending signals and leaves the instance runnable") {
    val rt = newRuntime
    val sig = Signal[String]("S")
    var handlerCalled = false
    val (wf, id) = {
      val identity = {
        import atomicflow.Cacheable.Simple.given
        summon[Cacheable[String]]
      }
      given boomCodec: Cacheable[String] = new Cacheable[String] {
        override def stableSerializedTypeId: String = "boom-str"
        override def write(value: String): String = identity.write(value)
        override def read(serialized: String): String =
          if (serialized == "boom") throw new IllegalStateException("input codec mismatch: boom")
          else identity.read(serialized)
      }
      val w = Workflow[String, String]("can-mismatch")(
        in => {
          if (in == "start") Workflow.continueAsNew("boom")(using identity)
          "done"
        },
        onUnconsumedSignals = _ => handlerCalled = true
      )
      (w, rt.createWorkflowInstance(w, "k", "start").id)
    }
    sig.send(id, "pending")(using rt)
    assertEquals(countSignalEvents(wf.id, "k"), 1)

    intercept[IllegalStateException](rt.runWorkflowInstance(wf, id))

    assertEquals(handlerCalled, false, "onUnconsumedSignals must not run when the new input fails to decode")
    assertEquals(countSignalEvents(wf.id, "k"), 1, "pending signals are not consumed when the new input fails to decode")

    val repaired = Workflow[String, String]("can-mismatch") { in => "recovered:" + in }
    assertEquals(
      rt.runWorkflowInstance(repaired, id),
      WorkflowRunResult.Result("recovered:start"),
      "the instance is not stranded after the decode failure"
    )
  }
}
