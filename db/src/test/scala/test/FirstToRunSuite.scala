package test

import atomicflow.*
import atomicflow.given
import atomicflow.Cacheable.Simple.given
import doobie.implicits.*
import doobie.postgres.implicits.*

import java.util.concurrent.atomic.AtomicInteger
import scala.annotation.experimental

@experimental
class FirstToRunSuite extends PostgresWorkflowRuntimeSuite {

  given Cacheable[Throwable] = Cacheable.forThrowable.genericStringMessageSerializer

  private def winnerRow(
      workflowId: WorkflowId,
      key: WorkflowInstanceKey
  ): Option[(String, String, String)] =
    run(
      sql"""SELECT step_kind, state_kind, state_payload FROM workflow_steps
            WHERE workflow_id = $workflowId AND workflow_instance_key = $key AND step_id = 'race'""".query[(String, String, String)].option
    )

  private def signalSubscriptions(workflowId: WorkflowId, key: WorkflowInstanceKey): Vector[(String, String)] =
    run(
      sql"""SELECT step_scope_path, signal_key FROM workflow_signal_subscriptions
            WHERE workflow_id = $workflowId AND workflow_instance_key = $key
            ORDER BY step_scope_path, signal_key""".query[(String, String)].to[Vector]
    )

  test("all branches suspend -> combined suspension with one cause per branch, no winner row, all subscriptions remain") {
    val rt = newRuntime
    val s1 = Signal[String]("s1")
    val s2 = Signal[String]("s2")
    val wf = Workflow[String, String](id = "ftr-all-suspend") { in =>
      val caught = Workflow.runToSuspension {
        Step.firstToRunWithoutSuspension[Int]("race")(
          "a" -> { () => Step.await[String]("a1", Awaitable.SignalEvent(s1)); 1 },
          "b" -> { () => Step.await[String]("b1", Awaitable.SignalEvent(s2)); 2 }
        )
      }
      caught.left.toOption.map(_.causes.size).getOrElse(-1).toString
    }
    val id = rt.createWorkflowInstance(wf, "k", "in").id
    assertEquals(rt.runWorkflowInstance(wf, id), WorkflowRunResult.Result("2"))
    assertEquals(winnerRow(wf.id, "k"), None, "no winner row when every branch suspends")
  }

  test("all branches suspend at the boundary -> workflow suspends and every branch's subscription remains unresolved") {
    val rt = newRuntime
    val s1 = Signal[String]("s1")
    val s2 = Signal[String]("s2")
    val wf = Workflow[String, String](id = "ftr-all-suspend-durable") { in =>
      Step.firstToRunWithoutSuspension[Int]("race")(
        "a" -> { () => Step.await[String]("a1", Awaitable.SignalEvent(s1)); 1 },
        "b" -> { () => Step.await[String]("b1", Awaitable.SignalEvent(s2)); 2 }
      )
      "unreachable"
    }
    val id = rt.createWorkflowInstance(wf, "k", "in").id
    assertEquals(rt.runWorkflowInstance(wf, id), WorkflowRunResult.WorkflowSuspended)
    assertEquals(winnerRow(wf.id, "k"), None, "no winner row while every branch is suspended")
    assertEquals(
      signalSubscriptions(wf.id, "k"),
      Vector(("race/a", "s1"), ("race/b", "s2")),
      "each branch's await subscription remains registered while the workflow is suspended"
    )
  }

  test("one branch completes, one suspends -> first result returned, losing subscription deleted, winner row persisted") {
    val rt = newRuntime
    val s2 = Signal[String]("s2")
    val wf = Workflow[String, String](id = "ftr-winner") { in =>
      val result = Step.firstToRunWithoutSuspension[Int]("race")(
        "fast" -> { () => Step.atLeastOnce[Int]("fast") { 42 } },
        "wait" -> { () => Step.await[String]("wait", Awaitable.SignalEvent(s2)); 99 }
      )
      result.toString
    }
    val id = rt.createWorkflowInstance(wf, "k", "in").id
    assertEquals(rt.runWorkflowInstance(wf, id), WorkflowRunResult.Result("42"))
    val Some((kind, state, payload)) = winnerRow(wf.id, "k"): @unchecked
    assertEquals(kind, "FirstToRunWithoutSuspension")
    assertEquals(state, "Succeeded")
    assertEquals(payload, "42", "the winner row persists only the winning branch's result")
    assertEquals(signalSubscriptions(wf.id, "k"), Vector.empty, "the losing branch's subscription is cleaned up")
  }

  test("durable first-wins: after a winner is recorded, replay returns it even if another branch would now unblock first") {
    val rt = newRuntime
    val sig = Signal[String]("gate")
    val counter = new AtomicInteger(0)
    val wf = Workflow[String, String](id = "ftr-durable") { in =>
      val result = Step.firstToRunWithoutSuspension[Int]("race")(
        "fast" -> { () => counter.incrementAndGet(); Step.atLeastOnce[Int]("fast") { 1 } },
        "wait" -> { () => Step.await[String]("wait", Awaitable.SignalEvent(sig)); counter.incrementAndGet(); 2 }
      )
      result.toString
    }
    val id = rt.createWorkflowInstance(wf, "k", "in").id
    assertEquals(rt.runWorkflowInstance(wf, id), WorkflowRunResult.Result("1"))
    val c1 = counter.get()
    assertEquals(c1, 1, "only the winning branch ran on the first execution")
    sig.send(id, "go")(using rt)
    assertEquals(rt.runWorkflowInstance(wf, id), WorkflowRunResult.Result("1"), "the recorded winner is returned on replay")
    assertEquals(counter.get(), c1, "neither branch re-ran on replay: the recorded winner is authoritative")
  }

  test("all-suspend then signal one branch and re-run -> that branch wins and the row is written once") {
    val rt = newRuntime
    val s1 = Signal[String]("s1")
    val s2 = Signal[String]("s2")
    val counter = new AtomicInteger(0)
    val wf = Workflow[String, String](id = "ftr-retry") { in =>
      val result = Step.firstToRunWithoutSuspension[Int]("race")(
        "a" -> { () => Step.await[String]("a1", Awaitable.SignalEvent(s1)); counter.incrementAndGet(); 1 },
        "b" -> { () => Step.await[String]("b1", Awaitable.SignalEvent(s2)); counter.incrementAndGet(); 2 }
      )
      result.toString
    }
    val id = rt.createWorkflowInstance(wf, "k", "in").id
    assertEquals(rt.runWorkflowInstance(wf, id), WorkflowRunResult.WorkflowSuspended)
    assertEquals(winnerRow(wf.id, "k"), None, "no winner row while every branch is suspended")
    s1.send(id, "x")(using rt)
    assertEquals(rt.runWorkflowInstance(wf, id), WorkflowRunResult.Result("1"), "the signaled branch (a) wins")
    val Some((_, _, payload)) = winnerRow(wf.id, "k"): @unchecked
    assertEquals(payload, "1", "the winner row is written once with only the result")
    assertEquals(
      signalSubscriptions(wf.id, "k"),
      Vector.empty,
      "the winning branch's own await subscription (a) is cleaned up when it resolves via the pending await"
    )
    assertEquals(
      run(
        sql"""SELECT count(*) FROM workflow_steps
              WHERE workflow_id = ${wf.id} AND workflow_instance_key = 'k' AND step_id = 'race'""".query[Int].unique
      ),
      1,
      "the winner row is written once"
    )
  }

  test("a non-suspension branch failure propagates and records no winner") {
    val rt = newRuntime
    val wf = Workflow[String, String](id = "ftr-failure") { in =>
      try {
        Step.firstToRunWithoutSuspension[Int]("race")(
          "ok" -> { () => Step.atLeastOnce[Int]("ok") { 1 } },
          "boom" -> { () => throw new RuntimeException("boom"); 2 }
        )
        "no-failure"
      } catch {
        case _: RuntimeException => "caught"
      }
    }
    val id = rt.createWorkflowInstance(wf, "k", "in").id
    assertEquals(rt.runWorkflowInstance(wf, id), WorkflowRunResult.Result("caught"))
    assertEquals(winnerRow(wf.id, "k"), None, "a branch failure records no winner")
  }

  test("two branches awaiting the SAME key register distinct subscriptions; cleanup deletes losers only") {
    val rt = newRuntime
    val s = Signal[String]("shared")
    val wf = Workflow[String, String](id = "ftr-loser-cleanup") { in =>
      val result = Step.firstToRunWithoutSuspension[Int]("race")(
        "l1" -> { () => Step.await[String]("l1", Awaitable.SignalEvent(s)); 1 },
        "l2" -> { () => Step.await[String]("l2", Awaitable.SignalEvent(s)); 2 },
        "fast" -> { () => Step.atLeastOnce[Int]("fast") { 3 } }
      )
      result.toString
    }
    val id = rt.createWorkflowInstance(wf, "k", "in").id
    assertEquals(rt.runWorkflowInstance(wf, id), WorkflowRunResult.Result("3"))
    val Some((_, _, payload)) = winnerRow(wf.id, "k"): @unchecked
    assertEquals(payload, "3", "the completing branch (fast) wins")
    assertEquals(
      signalSubscriptions(wf.id, "k"),
      Vector.empty,
      "both losing branches' subscriptions for the same key are cleaned up"
    )
    assertEquals(
      run(
        sql"""SELECT step_id, step_scope_path, state_kind FROM workflow_steps
              WHERE workflow_id = ${wf.id} AND workflow_instance_key = 'k' AND step_id = 'fast'""".query[(String, String, String)].to[Vector]
      ),
      Vector(("fast", "race/fast", "Succeeded")),
      "the winner branch's own step row survives cleanup"
    )
  }

  test("invalidateOn change discards the cached winner and re-runs from scratch") {
    val rt = newRuntime
    val holder = new AtomicInteger(1)
    val counter = new AtomicInteger(0)
    val wf = Workflow[String, String](id = "ftr-invalidate") { in =>
      Step.firstToRunWithoutSuspension[Int]("race", invalidateOn = Seq(StepInput("v", holder.get())))(
        "only" -> { () => counter.incrementAndGet(); holder.get() * 10 }
      )
      TestControlFlow.suspend()
      "unreachable"
    }
    val id = rt.createWorkflowInstance(wf, "k", "in").id
    assertEquals(rt.runWorkflowInstance(wf, id), WorkflowRunResult.WorkflowSuspended)
    assertEquals(counter.get(), 1, "first run executes the branch")
    assertEquals(rt.runWorkflowInstance(wf, id), WorkflowRunResult.WorkflowSuspended)
    assertEquals(counter.get(), 1, "an unchanged input replays the cached winner without re-running")
    holder.set(2)
    assertEquals(rt.runWorkflowInstance(wf, id), WorkflowRunResult.WorkflowSuspended)
    assertEquals(counter.get(), 2, "a changed invalidateOn input re-runs the construct from scratch")
  }

  test("firstToRunWithoutSuspension with zero branches throws IllegalArgumentException") {
    val rt = newRuntime
    val wf = Workflow[String, String](id = "ftr-empty") { in =>
      try {
        Step.firstToRunWithoutSuspension[Int]("race")()
        "no-error"
      } catch {
        case _: IllegalArgumentException => "empty"
      }
    }
    val id = rt.createWorkflowInstance(wf, "k", "in").id
    assertEquals(rt.runWorkflowInstance(wf, id), WorkflowRunResult.Result("empty"))
  }

  test("firstToRunWithoutSuspension rejects empty, duplicate, and reserved-prefixed branch keys") {
    val rt = newRuntime
    val wf = Workflow[String, String](id = "ftr-invalid-keys") { in =>
      def attempt(branches: (String, () => WorkflowContext ?=> Int)*): String =
        try {
          Step.firstToRunWithoutSuspension[Int]("race")(branches*)
          "valid"
        } catch {
          case _: IllegalArgumentException => "invalid"
        }
val empty = attempt("" -> { () => 1 })
      val duplicate = attempt("dup" -> { () => 1 }, "dup" -> { () => 2 })
      val reserved = attempt("__retry__" -> { () => 1 })
      s"$empty-$duplicate-$reserved"
    }
    val id = rt.createWorkflowInstance(wf, "k", "in").id
    assertEquals(rt.runWorkflowInstance(wf, id), WorkflowRunResult.Result("invalid-invalid-invalid"))
  }

  test("sibling constructs in parallel branches sharing a scope keep each other's branch subscriptions (no over-deletion)") {
    val rt = newRuntime
    val sA = Signal[String]("sA")
    val sB = Signal[String]("sB")
    val sC = Signal[String]("sC")
    val sD = Signal[String]("sD")
    val wf = Workflow[String, String](id = "ftr-sibling") { in =>
      val results = Workflow.parallel[Int](
        Step.firstToRunWithoutSuspension[Int]("raceA")(
          "a0" -> { () => Step.await[String]("a0", Awaitable.SignalEvent(sA)); 1 },
          "a1" -> { () => Step.await[String]("a1", Awaitable.SignalEvent(sB)); 2 }
        ),
        Step.firstToRunWithoutSuspension[Int]("raceB")(
          "b0" -> { () => Step.await[String]("b0", Awaitable.SignalEvent(sC)); 10 },
          "b1" -> { () => Step.await[String]("b1", Awaitable.SignalEvent(sD)); 20 }
        )
      )
      results.mkString(",")
    }
    val id = rt.createWorkflowInstance(wf, "k", "in").id
    assertEquals(rt.runWorkflowInstance(wf, id), WorkflowRunResult.WorkflowSuspended, "both constructs suspend on the first run")
    assertEquals(
      signalSubscriptions(wf.id, "k").map(_._1).distinct.size,
      4,
      "each of the two constructs' branches registers a distinct (construct-qualified) subscription path"
    )
    sA.send(id, "x")(using rt)
    assertEquals(rt.runWorkflowInstance(wf, id), WorkflowRunResult.WorkflowSuspended, "construct A completes, construct B stays suspended")
    sD.send(id, "y")(using rt)
    assertEquals(
      rt.runWorkflowInstance(wf, id),
      WorkflowRunResult.Result("1,20"),
      "construct B's branch (b1) is still woken by its signal even after construct A's loser cleanup ran"
    )
  }

  test("firstToRun branches persist step rows under the enclosing scoped prefix") {
    val rt = newRuntime
    val wf = Workflow[String, String](id = "ftr-scoped") { in =>
      Workflow.scoped("outer") {
        Step.firstToRunWithoutSuspension[Int]("race")(
          "one" -> { () => Step.atLeastOnce[Int]("step") { 1 } },
          "two" -> { () => Step.atLeastOnce[Int]("step2") { 2 } }
        )
      }
      "done"
    }
    val id = rt.createWorkflowInstance(wf, "k", "in").id
    assertEquals(rt.runWorkflowInstance(wf, id), WorkflowRunResult.Result("done"))
    val paths = run(
      sql"""SELECT DISTINCT step_scope_path FROM workflow_steps
            WHERE workflow_id = ${wf.id} AND workflow_instance_key = 'k' ORDER BY step_scope_path""".query[String].to[Vector]
    )
    assert(paths.contains("outer/race/one"), s"the winner branch's step must persist under the enclosing scope, got: $paths")
    assert(!paths.contains("race/one"), s"no branch step may be persisted without the enclosing scope prefix, got: $paths")
    assert(paths.contains("outer/race/two"), s"the completing loser branch's step also carries the enclosing prefix, got: $paths")
  }

  test("loser cleanup deletes subscriptions of nested subscopes, not just the branch's immediate scope") {
    val rt = newRuntime
    val sWinner = Signal[String]("winner")
    val sDeep = Signal[String]("deep")
    val wf = Workflow[String, String](id = "ftr-nested-cleanup") { in =>
      val result = Step.firstToRunWithoutSuspension[Int]("race")(
        "winner" -> { () => Step.await[String]("w", Awaitable.SignalEvent(sWinner)); 1 },
        "loser" -> { () =>
          Workflow.scoped("inner") {
            Step.await[String]("deep", Awaitable.SignalEvent(sDeep))
          }
          2
        }
      )
      result.toString
    }
    val id = rt.createWorkflowInstance(wf, "k", "in").id
    assertEquals(rt.runWorkflowInstance(wf, id), WorkflowRunResult.WorkflowSuspended)
    assertEquals(
      signalSubscriptions(wf.id, "k"),
      Vector(("race/loser/inner", "deep"), ("race/winner", "winner")),
      "the nested await registers under race/loser/inner while the construct is suspended"
    )
    sWinner.send(id, "go")(using rt)
    assertEquals(rt.runWorkflowInstance(wf, id), WorkflowRunResult.Result("1"))
    assertEquals(
      signalSubscriptions(wf.id, "k"),
      Vector.empty,
      "resolution cleans up the loser's nested-subscope subscription via the subtree delete"
    )
  }

  test("firstToRun branches inherit an enclosing uncancellable depth") {
    val rt = newRuntime
    val sA = Signal[String]("sA")
    val sB = Signal[String]("sB")
    val wf = Workflow[String, String](id = "ftr-uncancellable-outer") { in =>
      Workflow.uncancellable {
        Step.firstToRunWithoutSuspension[Int]("race")(
          "a" -> { () => Step.await[String]("a1", Awaitable.SignalEvent(sA)); 1 },
          "b" -> { () => Step.await[String]("b1", Awaitable.SignalEvent(sB)); 2 }
        )
      }
      "done"
    }
    val id = rt.createWorkflowInstance(wf, "k", "in").id
    assertEquals(rt.runWorkflowInstance(wf, id), WorkflowRunResult.WorkflowSuspended)
    rt.cancel(id)
    sA.send(id, "go")(using rt)
    assertEquals(
      rt.runWorkflowInstance(wf, id),
      WorkflowRunResult.Result("done"),
      "a pending cancel must not interrupt the awaiting branches inside an enclosing uncancellable region"
    )
  }

  test("ensureUnchanged conflict raises StepInputConflictException") {
    val rt = newRuntime
    val holder = new AtomicInteger(1)
    val counter = new AtomicInteger(0)
    var conflict = false
    val wf = Workflow[String, String](id = "ftr-ensure") { in =>
      try {
        Step.firstToRunWithoutSuspension[Int]("race", ensureUnchanged = Seq(StepInput("v", holder.get())))(
          "only" -> { () => counter.incrementAndGet(); holder.get() * 10 }
        )
      } catch {
        case _: StepInputConflictException => conflict = true
      }
      TestControlFlow.suspend()
      "unreachable"
    }
    val id = rt.createWorkflowInstance(wf, "k", "in").id
    assertEquals(rt.runWorkflowInstance(wf, id), WorkflowRunResult.WorkflowSuspended)
    assertEquals(counter.get(), 1)
    holder.set(2)
    assertEquals(rt.runWorkflowInstance(wf, id), WorkflowRunResult.WorkflowSuspended)
    assert(conflict, "a changed ensureUnchanged input must raise StepInputConflictException")
  }
}
