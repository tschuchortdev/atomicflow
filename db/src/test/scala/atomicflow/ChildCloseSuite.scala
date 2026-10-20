package atomicflow

import atomicflow.Cacheable.Simple.given
import doobie.implicits.*
import doobie.postgres.implicits.*
import test.PostgresWorkflowRuntimeSuite

import java.time.Instant
import scala.concurrent.duration.*

/** Asynchronous ParentClosePolicy application: a parent's terminal transition,
  * `continueAsNew`, region restart, and reset record a request row in the
  * dedicated `child_close_requests` table and never touch child rows; the close
  * is delivered (a) at the child's own run start (the self-check) and (b) by
  * the periodic child-close sweep. Lives in package `atomicflow` so it can
  * drive the package-private sweep hook deterministically.
  */
class ChildCloseSuite extends PostgresWorkflowRuntimeSuite {

  given Cacheable[Throwable] = Cacheable.forThrowable.genericStringMessageSerializer

  private lazy val rt = newRuntime

  private final case class ChildRow(
      terminalState: Option[String],
      timesExecuted: Int,
      cancelRequestedAt: Option[Instant],
      parentWf: Option[String],
      parentKey: Option[String],
      parentScope: Option[Vector[String]]
  )

  private def childRow(workflowId: WorkflowId, key: WorkflowInstanceKey, scope: ScopePath): Option[ChildRow] =
    run(
      sql"""
        SELECT terminal_state, times_executed, cancel_requested_at, parent_workflow_id, parent_instance_key, parent_instance_scope
        FROM workflow_instances
        WHERE workflow_id = $workflowId AND workflow_instance_key = $key AND workflow_instance_scope = ${scope.segments}
      """.query[
          (Option[String], Int, Option[Instant], Option[String], Option[String], Option[Vector[String]])
        ].option
    ).map { case (t, te, c, pw, pk, ps) => ChildRow(t, te, c, pw, pk, ps) }

  private def childCloseRowCount(): Int =
    run(sql"""
      SELECT COUNT(*) FROM child_close_requests
    """.query[Int].unique)

  private def hasWakeup(workflowId: WorkflowId, key: WorkflowInstanceKey, scope: ScopePath): Boolean =
    run(
      sql"""
        SELECT 1 FROM workflow_wakeups
        WHERE workflow_id = $workflowId AND workflow_instance_key = $key AND workflow_instance_scope = ${scope.segments}
      """.query[
          Int].option
    ).isDefined

  private def sweep(wfs: Workflow[?, ?]*): Unit = {
    val runner = rt.startJobRunnerForTests(wfs.toSeq, JobRunnerSettings.forTests)
    try runner.runChildCloseSweep()
    finally runner.stop(1.second)
  }

  private def setUpdatedAt(workflowId: WorkflowId, key: WorkflowInstanceKey, stepId: String, ts: Instant): Unit =
    run(
      sql"""UPDATE workflow_steps SET updated_at = $ts
            WHERE workflow_id = $workflowId AND workflow_instance_key = $key AND workflow_instance_scope = '{}' AND step_id = $stepId""".update.run
    )

  test("terminal close of a suspended Cancel child is asynchronous and delivered by the sweep") {
    val parentGate = Signal[String]("pgate")
    val childGate  = Signal[String]("cgate")
    val childWf = Workflow[String, String]("worker") { in =>
      Step.await[String]("cwait", Awaitable.SignalEvent(childGate))
      "done"
    }
    val parentWf = Workflow[String, String]("orders") { in =>
      childWf.startAsChild("worker-1", "hi", parentClosePolicy = ParentClosePolicy.Cancel)
      Step.await[String]("pwait", Awaitable.SignalEvent(parentGate))
      "done"
    }
    val parentId = rt.createWorkflowInstance(parentWf, "order-42", "in").id
    assertEquals(rt.runWorkflowInstance(parentWf, parentId), WorkflowRunResult.WorkflowSuspended)

    val scope    = ScopePath("orders", "order-42@0")
    val childId  = WorkflowInstanceId("worker", "worker-1", scope)
    assertEquals(rt.runWorkflowInstance(childWf, childId), WorkflowRunResult.WorkflowSuspended)

    parentGate.send(parentId, "go")(using rt)
    assertEquals(rt.runWorkflowInstance(parentWf, parentId), WorkflowRunResult.Result("done"))

    // The terminal transition recorded the closure and touched no child rows.
    assertEquals(childCloseRowCount(), 1, "the parent's terminal transition records exactly one close request")
    val before = childRow("worker", "worker-1", scope).get
    assertEquals(before.parentWf, Some("orders"), "the child is still attached immediately after the parent completes")
    assertEquals(before.cancelRequestedAt.isEmpty, true, "the cancel flag is not set synchronously")

    sweep(childWf)

    val after = childRow("worker", "worker-1", scope).get
    assertEquals(after.parentWf, None, "the sweep clears the active-parent pointer")
    assertEquals(after.cancelRequestedAt.isDefined, true, "the sweep requests cancellation")
    assert(hasWakeup("worker", "worker-1", scope), "the sweep wakes the suspended child")

    val cancelFlag = after.cancelRequestedAt.get
    sweep(childWf)
    assertEquals(childRow("worker", "worker-1", scope).get.cancelRequestedAt.get, cancelFlag,
                "a repeated sweep is a no-op")
    assertEquals(childCloseRowCount(), 0, "the sweep removes the exhausted close request")

    assertEquals(rt.runWorkflowInstance(childWf, childId), WorkflowRunResult.WorkflowCancelled)
    assertEquals(childRow("worker", "worker-1", scope).get.terminalState, Some("cancelled"))
  }

  test("terminal close of a never-started Cancel child finalizes it CANCELLED via the sweep") {
    val childWf   = Workflow[String, String]("worker") { in => in.toUpperCase }
    val parentWf  = Workflow[String, String]("orders") { in =>
      childWf.startAsChild("worker-1", "hi", parentClosePolicy = ParentClosePolicy.Cancel)
      "done"
    }
    val parentId  = rt.createWorkflowInstance(parentWf, "order-42", "in").id
    assertEquals(rt.runWorkflowInstance(parentWf, parentId), WorkflowRunResult.Result("done"))

    val scope = ScopePath("orders", "order-42@0")
    assertEquals(childRow("worker", "worker-1", scope).get.timesExecuted, 0, "the child never ran")
    assertEquals(childCloseRowCount(), 1)

    sweep(childWf)

    val row = childRow("worker", "worker-1", scope).get
    assertEquals(row.terminalState, Some("cancelled"), "a never-started child is finalized CANCELLED without running")
    assertEquals(row.timesExecuted, 0)
    assertEquals(row.parentWf, None)
  }

  test("terminal close of an Abandon child detaches it and the child continues independently") {
    val parentGate = Signal[String]("pgate")
    val childGate  = Signal[String]("cgate")
    val childWf = Workflow[String, String]("worker") { in =>
      Step.await[String]("cwait", Awaitable.SignalEvent(childGate))
      "done"
    }
    val parentWf = Workflow[String, String]("orders") { in =>
      childWf.startAsChild("worker-1", "hi", parentClosePolicy = ParentClosePolicy.Abandon)
      Step.await[String]("pwait", Awaitable.SignalEvent(parentGate))
      "done"
    }
    val parentId = rt.createWorkflowInstance(parentWf, "order-42", "in").id
    assertEquals(rt.runWorkflowInstance(parentWf, parentId), WorkflowRunResult.WorkflowSuspended)
    val scope   = ScopePath("orders", "order-42@0")
    val childId = WorkflowInstanceId("worker", "worker-1", scope)
    assertEquals(rt.runWorkflowInstance(childWf, childId), WorkflowRunResult.WorkflowSuspended)

    parentGate.send(parentId, "go")(using rt)
    assertEquals(rt.runWorkflowInstance(parentWf, parentId), WorkflowRunResult.Result("done"))

    sweep(childWf)

    val row = childRow("worker", "worker-1", scope).get
    assertEquals(row.parentWf, None, "the sweep clears the pointer of an abandoned child")
    assertEquals(row.cancelRequestedAt, None, "an abandoned child is never cancelled")
    assertEquals(row.terminalState, None)

    childGate.send(childId, "go")(using rt)
    assertEquals(rt.runWorkflowInstance(childWf, childId), WorkflowRunResult.Result("done"),
                 "the abandoned child still completes independently")
  }

  test("the self-check applies a pending close at the child's own run start") {
    val parentGate = Signal[String]("pgate")
    val childGate  = Signal[String]("cgate")
    val childWf = Workflow[String, String]("worker") { in =>
      Step.await[String]("cwait", Awaitable.SignalEvent(childGate))
      "done"
    }
    val parentWf = Workflow[String, String]("orders") { in =>
      childWf.startAsChild("worker-1", "hi", parentClosePolicy = ParentClosePolicy.Cancel)
      Step.await[String]("pwait", Awaitable.SignalEvent(parentGate))
      "done"
    }
    val parentId = rt.createWorkflowInstance(parentWf, "order-42", "in").id
    assertEquals(rt.runWorkflowInstance(parentWf, parentId), WorkflowRunResult.WorkflowSuspended)
    val scope   = ScopePath("orders", "order-42@0")
    val childId = WorkflowInstanceId("worker", "worker-1", scope)
    assertEquals(rt.runWorkflowInstance(childWf, childId), WorkflowRunResult.WorkflowSuspended)

    parentGate.send(parentId, "go")(using rt)
    assertEquals(rt.runWorkflowInstance(parentWf, parentId), WorkflowRunResult.Result("done"))

    assertEquals(childRow("worker", "worker-1", scope).get.cancelRequestedAt.isEmpty, true,
                "before any run or sweep the close is not yet delivered")

    // No sweep: the child's own run applies the close at start.
    assertEquals(rt.runWorkflowInstance(childWf, childId), WorkflowRunResult.WorkflowCancelled)
    val row = childRow("worker", "worker-1", scope).get
    assertEquals(row.terminalState, Some("cancelled"))
    assertEquals(row.parentWf, None, "the self-check clears the pointer")
  }

  test("continueAsNew closes only the children of the continuing generation") {
    val parentGate = Signal[String]("pgate")
    val childGate  = Signal[String]("cgate")
    val childWf = Workflow[String, String]("worker") { in =>
      Step.await[String]("cwait", Awaitable.SignalEvent(childGate))
      "done"
    }
    val parentWf = Workflow[String, String]("orders") { in =>
      childWf.startAsChild("c", "hi", parentClosePolicy = ParentClosePolicy.Cancel)
      Step.await[String]("pwait", Awaitable.SignalEvent(parentGate))
      if (in == "gen0") Workflow.continueAsNew("gen1") else "done"
    }
    val parentId = rt.createWorkflowInstance(parentWf, "order-42", "gen0").id
    assertEquals(rt.runWorkflowInstance(parentWf, parentId), WorkflowRunResult.WorkflowSuspended)

    val child0 = WorkflowInstanceId("worker", "c", ScopePath("orders", "order-42@0"))
    assertEquals(rt.runWorkflowInstance(childWf, child0), WorkflowRunResult.WorkflowSuspended)

    parentGate.send(parentId, "go")(using rt)
    assertEquals(rt.runWorkflowInstance(parentWf, parentId), WorkflowRunResult.ContinueAsNew)

    // The successor generation starts its own child under a distinct scope.
    assertEquals(rt.runWorkflowInstance(parentWf, parentId), WorkflowRunResult.WorkflowSuspended)
    val child1 = WorkflowInstanceId("worker", "c", ScopePath("orders", "order-42@1"))
    assertEquals(rt.runWorkflowInstance(childWf, child1), WorkflowRunResult.WorkflowSuspended)

    assertEquals(childCloseRowCount(), 1, "continueAsNew records one closure request for the old generation")

    sweep(childWf)

    val row0 = childRow("worker", "c", child0.scope).get
    assertEquals(row0.cancelRequestedAt.isDefined, true, "the old generation's child is cancelled")
    assertEquals(row0.parentWf, None)

    val row1 = childRow("worker", "c", child1.scope).get
    assertEquals(row1.cancelRequestedAt, None, "the new generation's child is untouched")
    assertEquals(row1.parentWf, Some("orders"), "the new generation's child remains attached")

    childGate.send(child1, "go")(using rt)
    assertEquals(rt.runWorkflowInstance(childWf, child1), WorkflowRunResult.Result("done"),
                 "the new generation's child still works")
  }

  test("a loop restart closes only the discarded iteration's children") {
    val parentGate = Signal[String]("pgate")
    val childGate  = Signal[String]("cgate")
    val childWf = Workflow[String, String]("worker") { in =>
      Step.await[String]("cwait", Awaitable.SignalEvent(childGate))
      "done"
    }
    val parentWf = Workflow[String, String]("orders") { in =>
      Workflow.restartable[Int, String]("processing", 0) { (i, scope) =>
        childWf.startAsChild("c", "hi", parentClosePolicy = ParentClosePolicy.Cancel)
        Step.await[String]("pwait", Awaitable.SignalEvent(parentGate))
        if (i == 0) scope.restart(i + 1) else "done"
      }
    }
    val parentId = rt.createWorkflowInstance(parentWf, "order-42", "in").id

    // Iteration 0: child under processing@0, both suspended.
    assertEquals(rt.runWorkflowInstance(parentWf, parentId), WorkflowRunResult.WorkflowSuspended)
    val child0 = WorkflowInstanceId("worker", "c", ScopePath("orders", "order-42@0", "processing@0"))
    assertEquals(rt.runWorkflowInstance(childWf, child0), WorkflowRunResult.WorkflowSuspended)

    // Restart into iteration 1: child under processing@1.
    parentGate.send(parentId, "go")(using rt)
    assertEquals(rt.runWorkflowInstance(parentWf, parentId), WorkflowRunResult.WorkflowSuspended)
    val child1 = WorkflowInstanceId("worker", "c", ScopePath("orders", "order-42@0", "processing@1"))
    assertEquals(rt.runWorkflowInstance(childWf, child1), WorkflowRunResult.WorkflowSuspended)

    assertEquals(childCloseRowCount(), 1, "the restart records one closure request for the discarded interior")

    sweep(childWf)

    val row0 = childRow("worker", "c", child0.scope).get
    assertEquals(row0.cancelRequestedAt.isDefined, true, "the discarded iteration's child is cancelled")
    assertEquals(row0.parentWf, None)

    val row1 = childRow("worker", "c", child1.scope).get
    assertEquals(row1.cancelRequestedAt, None, "the new iteration's child is untouched")
    assertEquals(row1.parentWf, Some("orders"))

    childGate.send(child1, "go")(using rt)
    assertEquals(rt.runWorkflowInstance(childWf, child1), WorkflowRunResult.Result("done"),
                 "the new iteration's child still works")
  }

  test("a child of a hard-deleted parent is detached, not cancelled") {
    val parentGate = Signal[String]("pgate")
    val childGate  = Signal[String]("cgate")
    val childWf = Workflow[String, String]("worker") { in =>
      Step.await[String]("cwait", Awaitable.SignalEvent(childGate))
      "done"
    }
    val parentWf = Workflow[String, String]("orders") { in =>
      childWf.startAsChild("worker-1", "hi", parentClosePolicy = ParentClosePolicy.Cancel)
      Step.await[String]("pwait", Awaitable.SignalEvent(parentGate))
      "done"
    }
    val parentId = rt.createWorkflowInstance(parentWf, "order-42", "in").id
    assertEquals(rt.runWorkflowInstance(parentWf, parentId), WorkflowRunResult.WorkflowSuspended)
    val scope   = ScopePath("orders", "order-42@0")
    val childId = WorkflowInstanceId("worker", "worker-1", scope)
    assertEquals(rt.runWorkflowInstance(childWf, childId), WorkflowRunResult.WorkflowSuspended)

    // Hard-delete the suspended parent (its own rows cascade away).
    run(sql"""
      DELETE FROM workflow_instances
      WHERE workflow_id = 'orders' AND workflow_instance_key = 'order-42' AND workflow_instance_scope = '{}'
    """.update.run)

    sweep(childWf)

    val row = childRow("worker", "worker-1", scope).get
    assertEquals(row.parentWf, None, "the stale pointer to the deleted parent is cleared")
    assertEquals(row.cancelRequestedAt, None, "a deleted parent detaches; it never cancels")
    assertEquals(row.terminalState, None)

    childGate.send(childId, "go")(using rt)
    assertEquals(rt.runWorkflowInstance(childWf, childId), WorkflowRunResult.Result("done"),
                 "the detached child still completes")
  }

  test("inheritance is cut off at a continued-as-new parent") {
    val gate        = Signal[String]("gate")
    val inheritable = Signal[String]("order/approve")
    val grandchildWf = Workflow[String, String]("leaf") { in =>
      Step.await[String]("wait", Awaitable.SignalEvent(inheritable))
      "done"
    }
    val parentWf = Workflow[String, String]("middle") { in =>
      grandchildWf.startAsChild("leaf-1", "hi", parentClosePolicy = ParentClosePolicy.Abandon,
                                inheritSignals = SignalInheritance.all)
      Step.await[String]("pwait", Awaitable.SignalEvent(gate))
      if (in == "gen0") Workflow.continueAsNew("gen1") else "done"
    }
    val grandparentWf = Workflow[String, String]("root") { in =>
      parentWf.startAsChild("middle-1", "gen0", inheritSignals = SignalInheritance.all)
      Step.await[String]("gwait", Awaitable.SignalEvent(gate))
      "done"
    }
    val grandparentId = rt.createWorkflowInstance(grandparentWf, "root-1", "in").id
    assertEquals(rt.runWorkflowInstance(grandparentWf, grandparentId), WorkflowRunResult.WorkflowSuspended)

    val parentId = rt.getChildWorkflowInstances(grandparentId).head.id
    assertEquals(rt.runWorkflowInstance(parentWf, parentId), WorkflowRunResult.WorkflowSuspended)

    val grandchildId = rt.getChildWorkflowInstances(parentId).head.id
    assertEquals(rt.runWorkflowInstance(grandchildWf, grandchildId), WorkflowRunResult.WorkflowSuspended)

    // The parent continues as new; the old generation's child stays attached
    // until delivery, but must already be cut off from inherited events.
    gate.send(parentId, "go")(using rt)
    assertEquals(rt.runWorkflowInstance(parentWf, parentId), WorkflowRunResult.ContinueAsNew)

    inheritable.send(grandparentId, "x")(using rt)

    // No sweep: the grandchild's own run detaches it and must not consume the
    // grandparent's signal through the continued parent.
    assertEquals(rt.runWorkflowInstance(grandchildWf, grandchildId), WorkflowRunResult.WorkflowSuspended,
                 "the grandchild must not resolve its await from the grandparent through a continued parent")

    val row = childRow("leaf", "leaf-1", grandchildId.scope).get
    assertEquals(row.parentWf, None, "the self-check detached the abandoned grandchild")
    assertEquals(row.cancelRequestedAt, None)
  }

  test("a terminal attached child is still delivered to: the pointer is cleared and the record removed") {
    val gate     = Signal[String]("gate")
    val childWf  = Workflow[String, String]("worker") { in => in.toUpperCase }
    val parentWf = Workflow[String, String]("orders") { in =>
      childWf.startAsChild("w", "hi", parentClosePolicy = ParentClosePolicy.Abandon)
      Step.await[String]("pwait", Awaitable.SignalEvent(gate))
      "done"
    }
    val parentId = rt.createWorkflowInstance(parentWf, "order-42", "in").id
    assertEquals(rt.runWorkflowInstance(parentWf, parentId), WorkflowRunResult.WorkflowSuspended)

    val scope   = ScopePath("orders", "order-42@0")
    val childId = WorkflowInstanceId("worker", "w", scope)
    assertEquals(rt.runWorkflowInstance(childWf, childId), WorkflowRunResult.Result("HI"))
    assertEquals(childRow("worker", "w", scope).get.parentWf, Some("orders"),
                 "a completed child keeps its pointer until its parent closes")

    gate.send(parentId, "go")(using rt)
    assertEquals(rt.runWorkflowInstance(parentWf, parentId), WorkflowRunResult.Result("done"))
    assertEquals(childCloseRowCount(), 1, "the terminal close over a terminal attached child records a request")

    sweep(childWf)

    assertEquals(childRow("worker", "w", scope).get.parentWf, None, "the terminal child's pointer is cleared by delivery")
    assertEquals(childCloseRowCount(), 0, "the exhausted request is removed")
  }

  test("startChild is fenced: a stale generation or a terminal parent is refused") {
    val childWf  = Workflow[String, String]("worker") { in => in.toUpperCase }
    val parentWf = Workflow[String, String]("orders") { in => "done" }
    val parentId = rt.createWorkflowInstance(parentWf, "order-42", "in").id
    assertEquals(rt.runWorkflowInstance(parentWf, parentId), WorkflowRunResult.Result("done"))

    def handle(generation: Long): rt.Lease = {
      val instance = InstanceRecord(
        parentId,
        SerializedValue.of("in"),
        1L,
        generation,
        None,
        None,
        None,
        java.time.Instant.EPOCH,
        None,
        0
      )
      new rt.PostgresCurrentExecution("test", 0L, parentId.workflowId, parentId.workflowInstanceKey, parentId.scope, generation, instance)
    }

    // A stale run watches a generation the parent has moved past.
    intercept[LeaseLostException] {
      rt.startChild(childWf, "w-stale", "hi", ParentClosePolicy.Abandon, SignalInheritance.none, false,
                    ScopePath.empty)(using summon[Cacheable[String]], handle(999L))
    }
    // A run under a terminal parent is refused.
    intercept[LeaseLostException] {
      rt.startChild(childWf, "w-term", "hi", ParentClosePolicy.Abandon, SignalInheritance.none, false,
                    ScopePath.empty)(using summon[Cacheable[String]], handle(0L))
    }
  }

  test("the self-check finalizes a never-start-gone child on its first run without executing the body") {
    val parentGate = Signal[String]("pgate")
    val childWf    = Workflow[String, String]("worker") { in => in.toUpperCase }
    val parentWf = Workflow[String, String]("orders") { in =>
      childWf.startAsChild("w", "hi", parentClosePolicy = ParentClosePolicy.Cancel)
      Step.await[String]("pwait", Awaitable.SignalEvent(parentGate))
      "done"
    }
    val parentId = rt.createWorkflowInstance(parentWf, "order-42", "in").id
    assertEquals(rt.runWorkflowInstance(parentWf, parentId), WorkflowRunResult.WorkflowSuspended)
    parentGate.send(parentId, "go")(using rt)
    assertEquals(rt.runWorkflowInstance(parentWf, parentId), WorkflowRunResult.Result("done"))

    val scope   = ScopePath("orders", "order-42@0")
    val childId = WorkflowInstanceId("worker", "w", scope)

    // No sweep: the child's own first run applies the close and never runs its body.
    assertEquals(rt.runWorkflowInstance(childWf, childId), WorkflowRunResult.WorkflowCancelled)
    val row = childRow("worker", "w", scope).get
    assertEquals(row.terminalState, Some("cancelled"))
    assertEquals(row.parentWf, None)
  }

  test("reset closes the children of the old generation; the successor's children are untouched") {
    val parentGate = Signal[String]("pgate")
    val childGate  = Signal[String]("cgate")
    val childWf = Workflow[String, String]("worker") { in =>
      Step.await[String]("cwait", Awaitable.SignalEvent(childGate))
      "done"
    }
    val parentWf = Workflow[String, String]("orders") { in =>
      Step.atLeastOnce[String]("pre") { "x" }
      childWf.startAsChild("c", "hi", parentClosePolicy = ParentClosePolicy.Cancel)
      Step.await[String]("pwait", Awaitable.SignalEvent(parentGate))
      "done"
    }
    val parentId = rt.createWorkflowInstance(parentWf, "order-42", "in").id
    assertEquals(rt.runWorkflowInstance(parentWf, parentId), WorkflowRunResult.WorkflowSuspended)

    val child0 = WorkflowInstanceId("worker", "c", ScopePath("orders", "order-42@0"))
    assertEquals(rt.runWorkflowInstance(childWf, child0), WorkflowRunResult.WorkflowSuspended)

    // Reset from the executed `pre` step: the old generation's child must be closed.
    rt.resetWorkflow[String, String](parentId, StepId("pre"))(using parentWf)
    assertEquals(childCloseRowCount(), 1, "reset records one closure request for the old generation")

    sweep(childWf)

    val row0 = childRow("worker", "c", child0.scope).get
    assertEquals(row0.cancelRequestedAt.isDefined, true, "the old generation's child is cancelled by reset")
    assertEquals(row0.parentWf, None, "the old generation's child is detached by reset")

    // The successor generation starts its own child under a distinct scope.
    assertEquals(rt.runWorkflowInstance(parentWf, parentId), WorkflowRunResult.WorkflowSuspended)
    val child1 = WorkflowInstanceId("worker", "c", ScopePath("orders", "order-42@1"))
    assertEquals(rt.runWorkflowInstance(childWf, child1), WorkflowRunResult.WorkflowSuspended)

    sweep(childWf)

    val row1 = childRow("worker", "c", child1.scope).get
    assertEquals(row1.cancelRequestedAt, None, "the successor generation's child is untouched by reset's closure")
    assertEquals(row1.parentWf, Some("orders"), "the successor generation's child remains attached")

    childGate.send(child1, "go")(using rt)
    assertEquals(rt.runWorkflowInstance(childWf, child1), WorkflowRunResult.Result("done"),
                 "the successor generation's child still works")
  }

  test("reset closes a child whose startAsChild is cached before the boundary and does not recreate it") {
    val parentGate = Signal[String]("pgate")
    val childWf    = Workflow[String, String]("worker") { in => in.toUpperCase }
    val parentWf = Workflow[String, String]("orders") { in =>
      Step.atLeastOnce[String]("start-child") {
        childWf.startAsChild("c", "hi", parentClosePolicy = ParentClosePolicy.Cancel)
        "started"
      }
      Step.atLeastOnce[String]("post") { "y" }
      Step.await[String]("pwait", Awaitable.SignalEvent(parentGate))
      "done"
    }
    val parentId = rt.createWorkflowInstance(parentWf, "order-42", "in").id
    assertEquals(rt.runWorkflowInstance(parentWf, parentId), WorkflowRunResult.WorkflowSuspended)

    val child0 = WorkflowInstanceId("worker", "c", ScopePath("orders", "order-42@0"))
    assertEquals(childRow("worker", "c", child0.scope).get.timesExecuted, 0, "the child is CREATED and never ran")

    // Backdate the start-child step so it is strictly before the `post` boundary;
    // on replay the cached step's body (startAsChild) is never re-executed.
    setUpdatedAt("orders", "order-42", "start-child", Instant.parse("2026-01-01T00:00:00Z"))

    rt.resetWorkflow[String, String](parentId, StepId("post"))(using parentWf)

    sweep(childWf)
    assertEquals(childRow("worker", "c", child0.scope).get.terminalState, Some("cancelled"),
                 "the cached-before-boundary child is finalized CANCELLED by reset's closure")

    // The replay resumes at the await; startAsChild is not re-executed.
    assertEquals(rt.runWorkflowInstance(parentWf, parentId), WorkflowRunResult.WorkflowSuspended)

    val rows = run(
      sql"""
        SELECT workflow_instance_scope FROM workflow_instances
        WHERE workflow_id = 'worker' AND workflow_instance_key = 'c' ORDER BY workflow_instance_scope
      """.query[Vector[String]].to[Vector]
    )
    assertEquals(rows, Vector(Vector("orders", "order-42@0")),
                 "the closed child is not recreated: exactly one instance row, the cancelled one")
  }

  test("a locked child row does not block the parent's terminal transition") {
    val parentGate = Signal[String]("pgate")
    val childWf    = Workflow[String, String]("worker") { in => in.toUpperCase }
    val parentWf = Workflow[String, String]("orders") { in =>
      childWf.startAsChild("c", "hi", parentClosePolicy = ParentClosePolicy.Cancel)
      Step.await[String]("pwait", Awaitable.SignalEvent(parentGate))
      "done"
    }
    val parentId = rt.createWorkflowInstance(parentWf, "order-42", "in").id
    assertEquals(rt.runWorkflowInstance(parentWf, parentId), WorkflowRunResult.WorkflowSuspended)

    val scope   = ScopePath("orders", "order-42@0")
    val childId = WorkflowInstanceId("worker", "c", scope)

    // Hold the child's row locked from a raw connection for the duration of the
    // parent's terminal transition.
    val conn = rawConnection
    conn.setAutoCommit(false)
    val st = conn.prepareStatement(
      "SELECT workflow_id FROM workflow_instances WHERE workflow_id = ? AND workflow_instance_key = ? AND workflow_instance_scope = ? FOR UPDATE"
    )
    st.setString(1, "worker")
    st.setString(2, "c")
    st.setArray(3, conn.createArrayOf("varchar", scope.segments.toArray))
    st.executeQuery()

    try {
      parentGate.send(parentId, "go")(using rt)
      assertEquals(rt.runWorkflowInstance(parentWf, parentId), WorkflowRunResult.Result("done"),
                   "the parent's terminal transition completes promptly despite the child row being locked")
      assertEquals(rt.getWorkflowInstance(parentWf, parentId).getInfo()(using rt).terminalState,
                   Some(WorkflowTerminalState.Completed))
    } finally {
      st.close()
      conn.rollback()
      conn.close()
    }

    sweep(childWf)
    assertEquals(childRow("worker", "c", scope).get.terminalState, Some("cancelled"),
                 "after the lock is released, the sweep closes the child")
  }

  test("Info.parentId is cleared after delivery") {
    val parentGate = Signal[String]("pgate")
    val childWf    = Workflow[String, String]("worker") { in => in.toUpperCase }
    val parentWf = Workflow[String, String]("orders") { in =>
      childWf.startAsChild("c", "hi", parentClosePolicy = ParentClosePolicy.Abandon)
      Step.await[String]("pwait", Awaitable.SignalEvent(parentGate))
      "done"
    }
    val parentId = rt.createWorkflowInstance(parentWf, "order-42", "in").id
    assertEquals(rt.runWorkflowInstance(parentWf, parentId), WorkflowRunResult.WorkflowSuspended)

    val scope       = ScopePath("orders", "order-42@0")
    val childId     = WorkflowInstanceId("worker", "c", scope)
    val childHandle = rt.getWorkflowInstance(childWf, childId)

    parentGate.send(parentId, "go")(using rt)
    assertEquals(rt.runWorkflowInstance(parentWf, parentId), WorkflowRunResult.Result("done"))

    assertEquals(childHandle.getInfo()(using rt).parentId, Some(parentId),
                 "before delivery the parent pointer is still visible (the asynchrony window)")

    sweep(childWf)

    assertEquals(childHandle.getInfo()(using rt).parentId, None,
                 "after delivery the parent pointer is cleared")
  }
}
