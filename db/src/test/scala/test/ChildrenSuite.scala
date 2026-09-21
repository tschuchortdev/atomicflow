package test

import atomicflow.*
import atomicflow.Cacheable.Simple.given
import doobie.implicits.*
import doobie.postgres.implicits.*

import java.time.Instant
import java.util.concurrent.atomic.AtomicInteger
import scala.concurrent.duration.*

class ChildrenSuite extends PostgresWorkflowRuntimeSuite {

  given Cacheable[Throwable] = Cacheable.forThrowable.genericStringMessageSerializer

  private final case class ChildRow(
      terminalState: Option[String],
      timesExecuted: Int,
      cancelRequestedAt: Option[Instant],
      parentWf: Option[String],
      parentKey: Option[String],
      parentScope: Option[String],
      parentClosePolicy: Option[String]
  )

  private def childRow(workflowId: WorkflowId, key: WorkflowInstanceKey, scope: String): Option[ChildRow] =
    run(
      sql"""SELECT terminal_state, times_executed, cancel_requested_at, parent_workflow_id, parent_instance_key, parent_scope, parent_close_policy
            FROM workflow_instances
            WHERE workflow_id = $workflowId AND workflow_instance_key = $key AND scope = $scope""".query[
          (Option[String], Int, Option[Instant], Option[String], Option[String], Option[String], Option[String])
        ].option
    ).map { case (t, te, c, pw, pk, ps, pcp) => ChildRow(t, te, c, pw, pk, ps, pcp) }

  private def scopesOf(workflowId: WorkflowId, key: WorkflowInstanceKey): Vector[String] =
    run(
      sql"""SELECT scope FROM workflow_instances WHERE workflow_id = $workflowId AND workflow_instance_key = $key ORDER BY scope""".query[String].to[Vector]
    )

  private def hasWakeup(workflowId: WorkflowId, key: WorkflowInstanceKey, scope: String): Boolean =
    run(
      sql"""SELECT 1 FROM workflow_wakeups WHERE workflow_id = $workflowId AND workflow_instance_key = $key AND scope = $scope""".query[Int].option
    ).isDefined

  private def deleteWakeup(workflowId: WorkflowId, key: WorkflowInstanceKey, scope: String): Unit =
    run(sql"""DELETE FROM workflow_wakeups WHERE workflow_id = $workflowId AND workflow_instance_key = $key AND scope = $scope""".update.run)
    ()

  test("startAsChild creates a child with a derived scope, records parent identity, and never runs it inline") {
    val rt = newRuntime
    val gate = Signal[String]("gate")
    val childBodyCalls = new AtomicInteger(0)
    val childWf = Workflow[String, String]("worker") { in =>
      childBodyCalls.incrementAndGet()
      in.toUpperCase
    }
    val parentWf = Workflow[String, String]("orders") { in =>
      childWf.startAsChild("worker-1", "hi")
      Step.await[String]("gate", Awaitable.SignalEvent(gate))
      "done"
    }
    val parentId = rt.createWorkflowInstance(parentWf, "order-42", "in").id

    assertEquals(rt.runWorkflowInstance(parentWf, parentId), WorkflowRunResult.WorkflowSuspended)

    val scope = "orders/order-42@0"
    val row = childRow(childWf.id, "worker-1", scope).get
    assertEquals(row.terminalState, None)
    assertEquals(row.timesExecuted, 0, "the child must not be run inline on the parent's thread")
    assertEquals(row.parentWf, Some("orders"))
    assertEquals(row.parentKey, Some("order-42"))
    assertEquals(row.parentScope, Some(""))
    assertEquals(childBodyCalls.get(), 0, "the child body must not execute during the parent's run")

    gate.send(parentId, "go")(using rt)
    assertEquals(rt.runWorkflowInstance(parentWf, parentId), WorkflowRunResult.Result("done"))
    assertEquals(childBodyCalls.get(), 0, "the child body must never run inline, even after the parent completes")
  }

  test("getChildWorkflowInstances returns the active children; parentId is visible in Info and cleared at parent close") {
    val rt = newRuntime
    val gate = Signal[String]("gate")
    val childWf = Workflow[String, String]("worker") { in => in.toUpperCase }
    val parentWf = Workflow[String, String]("orders") { in =>
      childWf.startAsChild("worker-1", "hi")
      Step.await[String]("gate", Awaitable.SignalEvent(gate))
      "done"
    }
    val parentId = rt.createWorkflowInstance(parentWf, "order-42", "in").id
    assertEquals(rt.runWorkflowInstance(parentWf, parentId), WorkflowRunResult.WorkflowSuspended)

    val children = rt.getChildWorkflowInstances(parentId)
    assertEquals(children.size, 1)
    val info = children.head
    assertEquals(info.parentId, Some(parentId))
    assertEquals(info.id.workflowInstanceKey, "worker-1")
    assertEquals(info.id.scope, "orders/order-42@0")

    gate.send(parentId, "go")(using rt)
    assertEquals(rt.runWorkflowInstance(parentWf, parentId), WorkflowRunResult.Result("done"))
    assertEquals(rt.getChildWorkflowInstances(parentId), Vector.empty, "the active relationship is cleared at parent close")
  }

  test("startAsChild is create-if-absent on parent replay") {
    val rt = newRuntime
    val gate = Signal[String]("gate")
    val childWf = Workflow[String, String]("worker") { in => in.toUpperCase }
    val parentWf = Workflow[String, String]("orders") { in =>
      childWf.startAsChild("worker-1", "hi")
      Step.await[String]("gate", Awaitable.SignalEvent(gate))
      "done"
    }
    val parentId = rt.createWorkflowInstance(parentWf, "order-42", "in").id

    assertEquals(rt.runWorkflowInstance(parentWf, parentId), WorkflowRunResult.WorkflowSuspended)
    assertEquals(scopesOf(childWf.id, "worker-1").size, 1)

    gate.send(parentId, "go")(using rt)
    assertEquals(rt.runWorkflowInstance(parentWf, parentId), WorkflowRunResult.Result("done"))
    assertEquals(scopesOf(childWf.id, "worker-1").size, 1, "replaying startAsChild must not create a second child")
  }

  test("scope derivation includes the parent generation and the enclosing scoped path") {
    val rt = newRuntime
    val childWf = Workflow[String, String]("worker") { in => in.toUpperCase }
    val parentWf = Workflow[String, String]("orders") { in =>
      Workflow.scoped("items") {
        Workflow.scoped("poll") {
          childWf.startAsChild("worker-1", "hi")
        }
      }
      "done"
    }
    val parentId = rt.createWorkflowInstance(parentWf, "order-42", "in").id
    run(sql"""UPDATE workflow_instances SET generation = 3 WHERE workflow_id = 'orders' AND workflow_instance_key = 'order-42' AND scope = ''""".update.run)

    assertEquals(rt.runWorkflowInstance(parentWf, parentId), WorkflowRunResult.Result("done"))
    assertEquals(scopesOf(childWf.id, "worker-1"), Vector("orders/order-42@3/items/poll"))
  }

  test("key uniqueness: same child key under different parent scopes yields distinct instances") {
    val rt = newRuntime
    val childWf = Workflow[String, String]("worker") { in => in.toUpperCase }
    val parentWf = Workflow[String, String]("orders") { in =>
      Workflow.scoped("A") { childWf.startAsChild("c", "1") }
      Workflow.scoped("B") { childWf.startAsChild("c", "2") }
      "done"
    }
    val parentId = rt.createWorkflowInstance(parentWf, "order-42", "in").id
    assertEquals(rt.runWorkflowInstance(parentWf, parentId), WorkflowRunResult.Result("done"))

    val scopes = scopesOf(childWf.id, "c")
    assertEquals(scopes, Vector("orders/order-42@0/A", "orders/order-42@0/B"))
    assertEquals(scopes.distinct.size, 2)
  }

  test("collision escaping: parent identity segments are backslash-escaped in the child scope") {
    val rt = newRuntime
    val childWf = Workflow[String, String]("worker") { in => in.toUpperCase }
    val parentWf = Workflow[String, String]("orders") { in =>
      childWf.startAsChild("worker-1", "hi")
      "done"
    }
    val parentId = rt.createWorkflowInstance(parentWf, "order@42/foo", "in").id
    assertEquals(rt.runWorkflowInstance(parentWf, parentId), WorkflowRunResult.Result("done"))

    assertEquals(scopesOf(childWf.id, "worker-1"), Vector("orders/order\\@42\\/foo@0"))
  }

  test("ParentClosePolicy.Cancel on a CREATED child finalizes it CANCELLED immediately") {
    val rt = newRuntime
    val gate = Signal[String]("gate")
    val childWf = Workflow[String, String]("worker") { in => in.toUpperCase }
    val parentWf = Workflow[String, String]("orders") { in =>
      childWf.startAsChild("worker-1", "hi", parentClosePolicy = ParentClosePolicy.Cancel)
      Step.await[String]("gate", Awaitable.SignalEvent(gate))
      "done"
    }
    val parentId = rt.createWorkflowInstance(parentWf, "order-42", "in").id
    assertEquals(rt.runWorkflowInstance(parentWf, parentId), WorkflowRunResult.WorkflowSuspended)

    val scope = "orders/order-42@0"
    assertEquals(childRow(childWf.id, "worker-1", scope).get.terminalState, None)

    gate.send(parentId, "go")(using rt)
    assertEquals(rt.runWorkflowInstance(parentWf, parentId), WorkflowRunResult.Result("done"))

    val row = childRow(childWf.id, "worker-1", scope).get
    assertEquals(row.terminalState, Some("cancelled"), "a CREATED child under Cancel is finalized CANCELLED immediately")
    assertEquals(row.timesExecuted, 0)
  }

  test("ParentClosePolicy.Cancel on a SUSPENDED child flags it, wakes it, and cancels on the next run") {
    val rt = newRuntime
    val parentGate = Signal[String]("pgate")
    val childGate = Signal[String]("cgate")
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

    val scope = "orders/order-42@0"
    val childId = rt.getWorkflowInstancesByPrefix(childWf.id, "worker-1", scope).head.id
    val childHandle = rt.getWorkflowInstance(childWf, childId)
    assertEquals(rt.runWorkflowInstance(childWf, childId), WorkflowRunResult.WorkflowSuspended)

    parentGate.send(parentId, "go")(using rt)
    assertEquals(rt.runWorkflowInstance(parentWf, parentId), WorkflowRunResult.Result("done"))

    val row = childRow(childWf.id, "worker-1", scope).get
    assertEquals(row.terminalState, None, "a SUSPENDED child is not finalized immediately")
    assertEquals(row.cancelRequestedAt.isDefined, true, "cancel_requested_at is set on the suspended child")
    assert(hasWakeup(childWf.id, "worker-1", scope), "a wakeup is upserted so the child resumes and cancels")

    assertEquals(rt.runWorkflowInstance(childWf, childId), WorkflowRunResult.WorkflowCancelled)
    assertEquals(childRow(childWf.id, "worker-1", scope).get.terminalState, Some("cancelled"))
  }

  test("ParentClosePolicy.Cancel on a RUNNING child sets the flag for the next checkpoint") {
    val rt = newRuntime
    val gate = Signal[String]("gate")
    val childWf = Workflow[String, String]("worker") { in => in.toUpperCase }
    val parentWf = Workflow[String, String]("orders") { in =>
      childWf.startAsChild("worker-1", "hi", parentClosePolicy = ParentClosePolicy.Cancel)
      Step.await[String]("gate", Awaitable.SignalEvent(gate))
      "done"
    }
    val parentId = rt.createWorkflowInstance(parentWf, "order-42", "in").id
    assertEquals(rt.runWorkflowInstance(parentWf, parentId), WorkflowRunResult.WorkflowSuspended)

    val scope = "orders/order-42@0"
    val future = Instant.now().plusSeconds(3600)
    run(sql"""UPDATE workflow_instances
              SET lease_owner = 'x', lease_expires_at = $future, times_executed = 1
              WHERE workflow_id = 'worker' AND workflow_instance_key = 'worker-1' AND scope = $scope""".update.run)
    deleteWakeup(childWf.id, "worker-1", scope)

    rt.terminate(parentId)

    val row = childRow(childWf.id, "worker-1", scope).get
    assertEquals(row.terminalState, None, "a RUNNING child is not finalized immediately")
    assertEquals(row.cancelRequestedAt.isDefined, true, "cancel_requested_at is set on the running child")
    assert(!hasWakeup(childWf.id, "worker-1", scope), "no wakeup is added while a live lease exists")
  }

  test("ParentClosePolicy.Abandon leaves the child untouched and clears only the active-parent pointer") {
    val rt = newRuntime
    val gate = Signal[String]("gate")
    val childWf = Workflow[String, String]("worker") { in => in.toUpperCase }
    val parentWf = Workflow[String, String]("orders") { in =>
      childWf.startAsChild("worker-1", "hi", parentClosePolicy = ParentClosePolicy.Abandon)
      Step.await[String]("gate", Awaitable.SignalEvent(gate))
      "done"
    }
    val parentId = rt.createWorkflowInstance(parentWf, "order-42", "in").id
    assertEquals(rt.runWorkflowInstance(parentWf, parentId), WorkflowRunResult.WorkflowSuspended)

    val scope = "orders/order-42@0"
    assertEquals(childRow(childWf.id, "worker-1", scope).get.terminalState, None)

    gate.send(parentId, "go")(using rt)
    assertEquals(rt.runWorkflowInstance(parentWf, parentId), WorkflowRunResult.Result("done"))

    val row = childRow(childWf.id, "worker-1", scope).get
    assertEquals(row.terminalState, None, "an abandoned child is not cancelled")
    assertEquals(row.cancelRequestedAt, None)
    assertEquals(row.parentWf, None, "the active-parent pointer is cleared")
    assertEquals(row.parentKey, None)
    assertEquals(row.parentScope, None)
  }

  test("ParentClosePolicy application is idempotent: terminating the parent twice gives the same end state") {
    val rt = newRuntime
    val gate = Signal[String]("gate")
    val childWf = Workflow[String, String]("worker") { in => in.toUpperCase }
    val parentWf = Workflow[String, String]("orders") { in =>
      childWf.startAsChild("worker-1", "hi", parentClosePolicy = ParentClosePolicy.Cancel)
      Step.await[String]("gate", Awaitable.SignalEvent(gate))
      "done"
    }
    val parentId = rt.createWorkflowInstance(parentWf, "order-42", "in").id
    assertEquals(rt.runWorkflowInstance(parentWf, parentId), WorkflowRunResult.WorkflowSuspended)

    val scope = "orders/order-42@0"
    rt.terminate(parentId)
    assertEquals(childRow(childWf.id, "worker-1", scope).get.terminalState, Some("cancelled"))

    rt.terminate(parentId)
    assertEquals(childRow(childWf.id, "worker-1", scope).get.terminalState, Some("cancelled"), "re-applying the policy is a no-op")
  }

  test("child completing does not cancel the parent; the parent awaits and reads the child's result end-to-end") {
    val rt = newRuntime
    val childWf = Workflow[String, String]("worker") { in => in.toUpperCase }
    val parentWf = Workflow[String, String]("orders") { in =>
      val child = childWf.startAsChild("worker-1", "hi")
      val comp = Step.await[WorkflowCompletionResult[String]]("wait-child", child.completion)
      comp match {
        case WorkflowCompletionResult.Completed(v) => v
        case _                                     => "?"
      }
    }
    val parentId = rt.createWorkflowInstance(parentWf, "order-42", "in").id
    val parentHandle = rt.getWorkflowInstance(parentWf, parentId)
    assertEquals(rt.runWorkflowInstance(parentWf, parentId), WorkflowRunResult.WorkflowSuspended)

    val childId = rt.getChildWorkflowInstances(parentId).head.id
    val childHandle = rt.getWorkflowInstance(childWf, childId)
    assertEquals(rt.runWorkflowInstance(childWf, childId), WorkflowRunResult.Result("HI"))

    val parentResult = rt.runWorkflowInstance(parentWf, parentId)
    assertEquals(parentResult, WorkflowRunResult.Result("HI"), "the parent resumes and reads the child's result")
    assertEquals(parentHandle.getInfo()(using rt).terminalState, Some(WorkflowTerminalState.Completed), "the parent completes; it is not cancelled by its child completing")
  }

  test("a terminal child's wakeup is not re-inserted by a later parent replay") {
    val rt = newRuntime
    val childWf = Workflow[String, String]("worker") { in => in.toUpperCase }
    val parentWf = Workflow[String, String]("orders") { in =>
      val child = childWf.startAsChild("worker-1", "hi")
      val comp = Step.await[WorkflowCompletionResult[String]]("wait-child", child.completion)
      comp match {
        case WorkflowCompletionResult.Completed(v) => v
        case _                                     => "?"
      }
    }
    val parentId = rt.createWorkflowInstance(parentWf, "order-42", "in").id
    assertEquals(rt.runWorkflowInstance(parentWf, parentId), WorkflowRunResult.WorkflowSuspended)

    val childId = rt.getChildWorkflowInstances(parentId).head.id
    val childHandle = rt.getWorkflowInstance(childWf, childId)
    assertEquals(rt.runWorkflowInstance(childWf, childId), WorkflowRunResult.Result("HI"))

    val scope = "orders/order-42@0"
    assert(!hasWakeup(childWf.id, "worker-1", scope), "a terminal child has no wakeup after completion")

    assertEquals(rt.runWorkflowInstance(parentWf, parentId), WorkflowRunResult.Result("HI"))
    assert(!hasWakeup(childWf.id, "worker-1", scope), "replaying the parent must not re-insert a terminal child's wakeup")
  }

  test("a conflicting startAsChild replay does not commit a wakeup side effect") {
    val rt = newRuntime
    val gate = Signal[String]("gate")
    val childWf = Workflow[String, String]("worker") { in => in.toUpperCase }
    val parentWf = Workflow[String, String]("orders") { in =>
      childWf.startAsChild("worker-1", "hi")
      Step.await[String]("gate", Awaitable.SignalEvent(gate))
      "done"
    }
    val parentId = rt.createWorkflowInstance(parentWf, "order-42", "in").id
    assertEquals(rt.runWorkflowInstance(parentWf, parentId), WorkflowRunResult.WorkflowSuspended)
    val scope = "orders/order-42@0"

    val childId = rt.getChildWorkflowInstances(parentId).head.id
    val childHandle = rt.getWorkflowInstance(childWf, childId)
    assertEquals(rt.runWorkflowInstance(childWf, childId), WorkflowRunResult.Result("HI"))
    assert(!hasWakeup(childWf.id, "worker-1", scope), "terminal child wakeup is cleaned up")

    run(sql"""UPDATE workflow_instances SET input = 'OTHER'
              WHERE workflow_id = 'worker' AND workflow_instance_key = 'worker-1' AND scope = $scope""".update.run)

    intercept[StepFailed] {
      rt.runWorkflowInstance(parentWf, parentId)
    }
    assert(!hasWakeup(childWf.id, "worker-1", scope), "a conflicting replay must not commit a wakeup side effect")
  }

  test("signal inheritance some(prefixes) is stored as an unambiguous JSON array") {
    val rt = newRuntime
    val childWf = Workflow[String, String]("worker") { in => in.toUpperCase }
    val parentWf = Workflow[String, String]("orders") { in =>
      childWf.startAsChild("worker-1", "hi", inheritSignals = SignalInheritance.some(Seq("a/\nb", "c,d")))
      "done"
    }
    val parentId = rt.createWorkflowInstance(parentWf, "order-42", "in").id
    assertEquals(rt.runWorkflowInstance(parentWf, parentId), WorkflowRunResult.Result("done"))

    val scope = "orders/order-42@0"
    val stored = run(sql"""SELECT inherit_signals FROM workflow_instances
                           WHERE workflow_id = 'worker' AND workflow_instance_key = 'worker-1' AND scope = $scope""".query[String].option).get
    assertEquals(stored, "some:" + upickle.default.write(Seq("a/\nb", "c,d")))
    assert(!stored.contains('\n'), "prefixes containing newlines are JSON-escaped, not stored with literal newlines")
  }
}
