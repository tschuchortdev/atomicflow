package test

import atomicflow.*
import atomicflow.Cacheable.Simple.given
import doobie.implicits.*
import doobie.postgres.implicits.*

import java.time.Instant
import java.util.concurrent.atomic.AtomicInteger
import scala.concurrent.duration.*

class ForkResetSuite extends PostgresWorkflowRuntimeSuite {

  given Cacheable[Throwable] = Cacheable.forThrowable.genericStringMessageSerializer

  private val t1 = Instant.parse("2026-01-01T00:00:00Z")
  private val t2 = t1.plusSeconds(10)
  private val t3 = t2.plusSeconds(10)

  private def countSteps(workflowId: WorkflowId, key: WorkflowInstanceKey, scope: String = ""): Int =
    run(
      sql"""SELECT COUNT(*) FROM workflow_steps
            WHERE workflow_id = $workflowId AND key = $key AND scope = $scope""".query[Int].unique
    )

  private def stepIds(workflowId: WorkflowId, key: WorkflowInstanceKey, scope: String = ""): Set[String] =
    run(
      sql"""SELECT step_id FROM workflow_steps
            WHERE workflow_id = $workflowId AND key = $key AND scope = $scope""".query[String].to[Set]
    )

  private def setUpdatedAt(
      workflowId: WorkflowId,
      key: WorkflowInstanceKey,
      stepId: String,
      ts: Instant
  ): Unit =
    run(
      sql"""UPDATE workflow_steps SET updated_at = $ts
            WHERE workflow_id = $workflowId AND key = $key AND scope = '' AND step_id = $stepId""".update.run
    )

  private def storedInput(workflowId: WorkflowId, key: WorkflowInstanceKey): String =
    run(
      sql"""SELECT input FROM workflow_instances
            WHERE workflow_id = $workflowId AND key = $key AND scope = ''""".query[String].unique
    )

  private def countWakeups(workflowId: WorkflowId, key: WorkflowInstanceKey): Int =
    run(
      sql"""SELECT COUNT(*) FROM workflow_wakeups
            WHERE workflow_id = $workflowId AND key = $key AND scope = ''""".query[Int].unique
    )

  private def parentCols(workflowId: WorkflowId, key: WorkflowInstanceKey): (Option[String], Option[String], Option[String]) =
    run(
      sql"""SELECT parent_workflow_id, parent_instance_key, parent_scope FROM workflow_instances
            WHERE workflow_id = $workflowId AND key = $key AND scope = ''""".query[
          (Option[String], Option[String], Option[String])
        ].unique
    )

  private def countCursor(workflowId: WorkflowId, key: WorkflowInstanceKey): Int =
    run(
      sql"""SELECT COUNT(*) FROM signal_cursor
            WHERE workflow_id = $workflowId AND key = $key AND scope = ''""".query[Int].unique
    )

  private def countSignalEvents(workflowId: WorkflowId, key: WorkflowInstanceKey): Int =
    run(
      sql"""SELECT COUNT(*) FROM workflow_events
            WHERE workflow_id = $workflowId AND key = $key AND scope = '' AND event_kind = 'Signal'""".query[Int].unique
    )

  private def counterWf(
      id: WorkflowId,
      cA: AtomicInteger,
      cB: AtomicInteger,
      cC: AtomicInteger
  ): Workflow[String, String] =
    Workflow[String, String](id) { _ =>
      Step.atLeastOnce[String]("A") { cA.incrementAndGet(); "a" }
      Step.atLeastOnce[String]("B") { cB.incrementAndGet(); "b" }
      Step.atLeastOnce[String]("C") { cC.incrementAndGet(); "c" }
      "done"
    }

  test("fork copies only the cached history before the boundary; the selected step and later ones re-execute in the fork") {
    val rt = newRuntime
    val cA = new AtomicInteger(0)
    val cB = new AtomicInteger(0)
    val cC = new AtomicInteger(0)
    val wf = counterWf("frk-basic", cA, cB, cC)
    val sourceId = rt.createWorkflowInstance(wf, "k", "in").id
    assertEquals(rt.runWorkflowInstance(wf, sourceId), WorkflowRunResult.Result("done"))
    assertEquals((cA.get(), cB.get(), cC.get()), (1, 1, 1))

    setUpdatedAt(wf.id, "k", "A", t1)
    setUpdatedAt(wf.id, "k", "B", t2)
    setUpdatedAt(wf.id, "k", "C", t3)

    val fork = rt.forkWorkflow[String, String](sourceId, "fork", StepId("B"))(using wf)
    assertEquals(fork.id.workflowInstanceKey, "fork")
    assertEquals(fork.id.scope, "")
    assertEquals(stepIds(wf.id, "fork"), Set("A"), "only the strictly-before step A is copied")

    assertEquals(rt.runWorkflowInstance(wf, fork.id), WorkflowRunResult.Result("done"))
    assertEquals(cA.get(), 1, "step A replays from the copied cache and does NOT re-run")
    assertEquals(cB.get(), 2, "step B re-executes in the fork")
    assertEquals(cC.get(), 2, "step C re-executes in the fork")
  }

  test("fork creates an independent top-level instance: generation 0, same input, no parent columns, source untouched") {
    val rt = newRuntime
    val cA = new AtomicInteger(0)
    val cB = new AtomicInteger(0)
    val cC = new AtomicInteger(0)
    val wf = counterWf("frk-identity", cA, cB, cC)
    val sourceId = rt.createWorkflowInstance(wf, "k", "in").id
    assertEquals(rt.runWorkflowInstance(wf, sourceId), WorkflowRunResult.Result("done"))

    setUpdatedAt(wf.id, "k", "A", t1)
    setUpdatedAt(wf.id, "k", "B", t2)
    setUpdatedAt(wf.id, "k", "C", t3)

    val fork = rt.forkWorkflow[String, String](sourceId, "fork", StepId("B"))(using wf)
    assertEquals(fork.getInfo()(using rt).generation, 0L, "fork starts at generation 0")
    assertEquals(storedInput(wf.id, "fork"), storedInput(wf.id, "k"), "fork carries the same input as the source")
    val (pw, pk, ps) = parentCols(wf.id, "fork")
    assertEquals((pw, pk, ps), (None, None, None), "fork is an independent top-level instance")

    assertEquals(stepIds(wf.id, "k"), Set("A", "B", "C"), "the source's rows are untouched")
    assertEquals(
      rt.getWorkflowInstance(wf, sourceId).getInfo()(using rt).terminalState,
      Some(WorkflowTerminalState.Completed),
      "the source remains terminal and untouched"
    )
  }

  test("fork errors when restartFromStep does not identify an already-executed step") {
    val rt = newRuntime
    val cA = new AtomicInteger(0)
    val cB = new AtomicInteger(0)
    val cC = new AtomicInteger(0)
    val wf = counterWf("frk-errors", cA, cB, cC)
    val sourceId = rt.createWorkflowInstance(wf, "k", "in").id
    assertEquals(rt.runWorkflowInstance(wf, sourceId), WorkflowRunResult.Result("done"))

    intercept[InvalidRestartStepException] {
      rt.forkWorkflow[String, String](sourceId, "fork", StepId("ZZZ"))(using wf)
    }

    val never = Workflow[String, String]("frk-never") { in =>
      if (in == "go") { Step.atLeastOnce[String]("A") { "a" }; "done" }
      else "noop"
    }
    val nid = rt.createWorkflowInstance(never, "nk", "noop").id
    assertEquals(rt.runWorkflowInstance(never, nid), WorkflowRunResult.Result("noop"))
    intercept[InvalidRestartStepException] {
      rt.forkWorkflow[String, String](nid, "nfork", StepId("A"))(using never)
    }
  }

  test("forking twice with the same newInstanceKey throws the domain conflict error, not a raw PSQL error") {
    val rt = newRuntime
    val cA = new AtomicInteger(0)
    val cB = new AtomicInteger(0)
    val cC = new AtomicInteger(0)
    val wf = counterWf("frk-conflict", cA, cB, cC)
    val sourceId = rt.createWorkflowInstance(wf, "k", "in").id
    assertEquals(rt.runWorkflowInstance(wf, sourceId), WorkflowRunResult.Result("done"))

    setUpdatedAt(wf.id, "k", "A", t1)
    setUpdatedAt(wf.id, "k", "B", t2)
    setUpdatedAt(wf.id, "k", "C", t3)

    val fork = rt.forkWorkflow[String, String](sourceId, "fork", StepId("B"))(using wf)
    assertEquals(stepIds(wf.id, "fork"), Set("A"), "first fork succeeds")

    intercept[WorkflowInputConflictException] {
      rt.forkWorkflow[String, String](sourceId, "fork", StepId("B"))(using wf)
    }
  }

  test("reset keeps the strictly-before history cached, erases the selected step and later, increments generation, and re-runs from the top") {
    val rt = newRuntime
    val sig = Signal[String]("R")
    val cA = new AtomicInteger(0)
    val cB = new AtomicInteger(0)
    val cC = new AtomicInteger(0)
    val wf = Workflow[String, String]("rst-basic") { _ =>
      Step.atLeastOnce[String]("A") { cA.incrementAndGet(); "a" }
      Step.atLeastOnce[String]("B") { cB.incrementAndGet(); "b" }
      Step.atLeastOnce[String]("C") { cC.incrementAndGet(); "c" }
      Step.await[String]("w", Awaitable.SignalEvent(sig))
      "done"
    }
    val id = rt.createWorkflowInstance(wf, "k", "in").id
    val handle = rt.getWorkflowInstance(wf, id)
    assertEquals(rt.runWorkflowInstance(wf, id), WorkflowRunResult.WorkflowSuspended)
    assertEquals((cA.get(), cB.get(), cC.get()), (1, 1, 1))

    setUpdatedAt(wf.id, "k", "A", t1)
    setUpdatedAt(wf.id, "k", "B", t2)
    setUpdatedAt(wf.id, "k", "C", t3)

    rt.resetWorkflow[String, String](id, StepId("B"))(using wf)
    assertEquals(handle.getInfo()(using rt).generation, 1L, "generation increments in place")
    assertEquals(stepIds(wf.id, "k"), Set("A"), "the selected step B and later step C are erased; A is kept")
    assertEquals(countWakeups(wf.id, "k"), 1, "a wakeup is scheduled so the instance re-runs")
    assertEquals(handle.getInfo()(using rt).terminalState, None)

    assertEquals(rt.runWorkflowInstance(wf, id), WorkflowRunResult.WorkflowSuspended)
    assertEquals(cA.get(), 1, "step A replays from cache and does NOT re-run")
    assertEquals(cB.get(), 2, "step B re-executes fresh")
    assertEquals(cC.get(), 2, "step C re-executes fresh")

    sig.send(id, "go")(using rt)
    assertEquals(rt.runWorkflowInstance(wf, id), WorkflowRunResult.Result("done"))
    assertEquals(handle.getInfo()(using rt).terminalState, Some(WorkflowTerminalState.Completed))
  }

  test("reset keeps signal cursors and unconsumed events; an await after the reset boundary still sees them") {
    val rt = newRuntime
    val sig = Signal[String]("S")
    val cB = new AtomicInteger(0)
    val wf = Workflow[String, String]("rst-cursor") { _ =>
      Step.await[String]("wait1", Awaitable.SignalEvent(sig))
      Step.atLeastOnce[String]("B") { cB.incrementAndGet(); "b" }
      Step.await[String]("wait2", Awaitable.SignalEvent(sig))
      "done"
    }
    val id = rt.createWorkflowInstance(wf, "k", "in").id
    assertEquals(rt.runWorkflowInstance(wf, id), WorkflowRunResult.WorkflowSuspended)
    sig.send(id, "v1")(using rt)
    assertEquals(rt.runWorkflowInstance(wf, id), WorkflowRunResult.WorkflowSuspended)
    assertEquals(cB.get(), 1, "B executed before the second await")
    sig.send(id, "v2")(using rt)
    assertEquals(countCursor(wf.id, "k"), 1, "the signal cursor exists before reset")
    assertEquals(countSignalEvents(wf.id, "k"), 2, "v1 and v2 events are present")

    setUpdatedAt(wf.id, "k", "wait1", t1)
    setUpdatedAt(wf.id, "k", "B", t2)

    rt.resetWorkflow[String, String](id, StepId("B"))(using wf)
    assertEquals(countCursor(wf.id, "k"), 1, "the signal cursor survives reset")
    assertEquals(countSignalEvents(wf.id, "k"), 2, "signal events are not deleted by reset")
    assertEquals(stepIds(wf.id, "k"), Set("wait1"), "the before-boundary wait1 is kept; B is erased")

    assertEquals(
      rt.runWorkflowInstance(wf, id),
      WorkflowRunResult.Result("done"),
      "after reset, wait1 replays and wait2 re-awaits, seeing the unconsumed v2 event"
    )
    assertEquals(cB.get(), 2, "B re-executed after reset")
  }

  test("the boundary is ordered by updated_at, not execution order: backdating a later step moves it before the boundary") {
    val rt = newRuntime
    val cA = new AtomicInteger(0)
    val cB = new AtomicInteger(0)
    val cC = new AtomicInteger(0)
    val wf = counterWf("frk-ts", cA, cB, cC)
    val sourceId = rt.createWorkflowInstance(wf, "k", "in").id
    assertEquals(rt.runWorkflowInstance(wf, sourceId), WorkflowRunResult.Result("done"))

    setUpdatedAt(wf.id, "k", "A", t1)
    setUpdatedAt(wf.id, "k", "C", t2)
    setUpdatedAt(wf.id, "k", "B", t3)

    val fork = rt.forkWorkflow[String, String](sourceId, "fork", StepId("B"))(using wf)
    assertEquals(stepIds(wf.id, "fork"), Set("A", "C"), "C is backdated below the B boundary, so it is copied too")

    assertEquals(rt.runWorkflowInstance(wf, fork.id), WorkflowRunResult.Result("done"))
    assertEquals(cA.get(), 1, "A replays")
    assertEquals(cB.get(), 2, "B re-executes (selected step)")
    assertEquals(cC.get(), 1, "C replays because it was backdated below the boundary")
  }

  test("reset's boundary is ordered by updated_at, not execution order: backdating a later step keeps it, and an earlier-erased step split follows timestamps") {
    val rt = newRuntime
    val sig = Signal[String]("R")
    val cA = new AtomicInteger(0)
    val cB = new AtomicInteger(0)
    val cC = new AtomicInteger(0)
    val wf = Workflow[String, String]("rst-ts") { _ =>
      Step.atLeastOnce[String]("A") { cA.incrementAndGet(); "a" }
      Step.atLeastOnce[String]("B") { cB.incrementAndGet(); "b" }
      Step.atLeastOnce[String]("C") { cC.incrementAndGet(); "c" }
      Step.await[String]("w", Awaitable.SignalEvent(sig))
      "done"
    }
    val id = rt.createWorkflowInstance(wf, "k", "in").id
    val handle = rt.getWorkflowInstance(wf, id)
    assertEquals(rt.runWorkflowInstance(wf, id), WorkflowRunResult.WorkflowSuspended)
    assertEquals((cA.get(), cB.get(), cC.get()), (1, 1, 1))

    setUpdatedAt(wf.id, "k", "A", t1)
    setUpdatedAt(wf.id, "k", "C", t2)
    setUpdatedAt(wf.id, "k", "B", t3)

    rt.resetWorkflow[String, String](id, StepId("B"))(using wf)
    assertEquals(
      stepIds(wf.id, "k"),
      Set("A", "C"),
      "C is backdated below the B boundary, so it is kept; only B and later are erased"
    )

    assertEquals(rt.runWorkflowInstance(wf, id), WorkflowRunResult.WorkflowSuspended)
    assertEquals(cA.get(), 1, "A replays")
    assertEquals(cB.get(), 2, "B re-executes (selected step)")
    assertEquals(cC.get(), 1, "C replays because it was backdated below the boundary")

    sig.send(id, "go")(using rt)
    assertEquals(rt.runWorkflowInstance(wf, id), WorkflowRunResult.Result("done"))
    assertEquals(handle.getInfo()(using rt).terminalState, Some(WorkflowTerminalState.Completed))
  }

  test("fork and reset work on a SUSPENDED source; the fork completes independently") {
    val rt = newRuntime
    val sig = Signal[String]("S")
    val cA = new AtomicInteger(0)
    val cB = new AtomicInteger(0)
    val wf = Workflow[String, String]("frk-susp") { _ =>
      Step.atLeastOnce[String]("A") { cA.incrementAndGet(); "a" }
      Step.atLeastOnce[String]("B") { cB.incrementAndGet(); "b" }
      Step.await[String]("w", Awaitable.SignalEvent(sig))
      "done"
    }
    val sourceId = rt.createWorkflowInstance(wf, "k", "in").id
    assertEquals(rt.runWorkflowInstance(wf, sourceId), WorkflowRunResult.WorkflowSuspended)
    assertEquals((cA.get(), cB.get()), (1, 1))

    setUpdatedAt(wf.id, "k", "A", t1)
    setUpdatedAt(wf.id, "k", "B", t2)

    val fork = rt.forkWorkflow[String, String](sourceId, "fork", StepId("B"))(using wf)
    assertEquals(stepIds(wf.id, "fork"), Set("A"))
    assertEquals(rt.runWorkflowInstance(wf, fork.id), WorkflowRunResult.WorkflowSuspended)
    assertEquals(cA.get(), 1, "fork replays the copied A")
    assertEquals(cB.get(), 2, "fork re-executes B")

    rt.resetWorkflow[String, String](sourceId, StepId("B"))(using wf)
    assertEquals(stepIds(wf.id, "k"), Set("A"), "reset on the suspended source keeps A")
    assertEquals(rt.runWorkflowInstance(wf, sourceId), WorkflowRunResult.WorkflowSuspended)
    assertEquals(cA.get(), 1, "the reset source replays A")
    assertEquals(cB.get(), 3, "the reset source re-executes B")
  }

  test("fork is allowed on a TERMINAL source; reset on a TERMINAL source errors") {
    val rt = newRuntime
    val cA = new AtomicInteger(0)
    val cB = new AtomicInteger(0)
    val cC = new AtomicInteger(0)
    val wf = counterWf("frk-term", cA, cB, cC)
    val sourceId = rt.createWorkflowInstance(wf, "k", "in").id
    assertEquals(rt.runWorkflowInstance(wf, sourceId), WorkflowRunResult.Result("done"))

    setUpdatedAt(wf.id, "k", "A", t1)
    setUpdatedAt(wf.id, "k", "B", t2)
    setUpdatedAt(wf.id, "k", "C", t3)

    val fork = rt.forkWorkflow[String, String](sourceId, "fork", StepId("B"))(using wf)
    assertEquals(stepIds(wf.id, "fork"), Set("A"), "fork works on a terminal source")

    intercept[IllegalStateException] {
      rt.resetWorkflow[String, String](sourceId, StepId("B"))(using wf)
    }
    assertEquals(stepIds(wf.id, "k"), Set("A", "B", "C"), "the failed reset left the terminal source untouched")
  }
}
