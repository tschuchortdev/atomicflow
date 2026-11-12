package test

import atomicflow.*
import atomicflow.given
import atomicflow.Cacheable.Simple.given
import doobie.implicits.*
import doobie.postgres.implicits.*

import java.util.concurrent.atomic.AtomicInteger
import scala.annotation.experimental

@experimental
class FirstCompletedSuite extends PostgresWorkflowRuntimeSuite {

  given Cacheable[Throwable] = Cacheable.forThrowable.genericStringMessageSerializer

  private def winnerRow(
      workflowId: WorkflowId,
      key: WorkflowInstanceKey
  ): Option[(String, String, String)] =
    run(
      sql"""
        SELECT step_kind, state_kind, state_payload FROM workflow_steps
        WHERE workflow_id = $workflowId AND workflow_instance_key = $key AND step_id = 'race'
      """.query[(String, String, String)].option
    )

  private def winnerFields(payload: String): (String, String, Boolean) = {
    val hasUnfinishedBranches = payload.charAt(0) == '1'
    val colon = payload.indexOf(':', 1)
    val length = payload.substring(1, colon).toInt
    (
      payload.substring(colon + 1, colon + 1 + length),
      payload.substring(colon + 1 + length),
      hasUnfinishedBranches
    )
  }

  private def signalSubscriptions(workflowId: WorkflowId, key: WorkflowInstanceKey): Vector[(Vector[String], String)] =
    run(
      sql"""
        SELECT step_scope_within_instance, signal_key FROM workflow_signal_subscriptions
        WHERE workflow_id = $workflowId AND workflow_instance_key = $key
        ORDER BY step_scope_within_instance, signal_key
      """.query[(Vector[String], String)].to[Vector]
    )

  test("all branches suspend -> combined suspension with one cause per branch, no winner row, all subscriptions remain") {
    val rt = newRuntime
    val s1 = Signal[String]("s1")
    val s2 = Signal[String]("s2")
    val wf = Workflow[String, String](id = "ftr-all-suspend") { in =>
      val caught = Workflow.runToSuspension {
        Step.firstCompletedWithoutSuspension[Int]("race")(
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
      Step.firstCompletedWithoutSuspension[Int]("race")(
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
      Vector((Vector("race", "a"), "s1"), (Vector("race", "b"), "s2")),
      "each branch's await subscription remains registered while the workflow is suspended"
    )
  }

  test("one branch completes, one suspends -> first result returned, losing subscription deleted, winner row persisted") {
    val rt = newRuntime
    val s2 = Signal[String]("s2")
    val wf = Workflow[String, String](id = "ftr-winner") { in =>
      val result = Step.firstCompletedWithoutSuspension[Int]("race")(
        "fast" -> { () => Step.atLeastOnce[Int]("fast") { 42 } },
        "wait" -> { () => Step.await[String]("wait", Awaitable.SignalEvent(s2)); 99 }
      )
      result.toString
    }
    val id = rt.createWorkflowInstance(wf, "k", "in").id
    assertEquals(rt.runWorkflowInstance(wf, id), WorkflowRunResult.Result("42"))
    val Some((kind, state, payload)) = winnerRow(wf.id, "k"): @unchecked
    assertEquals(kind, "FirstCompleted")
    assertEquals(state, "Succeeded")
    assertEquals(winnerFields(payload)._2, "42", "the winner row persists only the winning branch's result")
    assertEquals(signalSubscriptions(wf.id, "k"), Vector.empty, "the losing branch's subscription is cleaned up")
  }

  test("durable first-wins: after a winner is recorded, replay returns it even if another branch would now unblock first") {
    val rt = newRuntime
    val sig = Signal[String]("gate")
    val counter = new AtomicInteger(0)
    val wf = Workflow[String, String](id = "ftr-durable") { in =>
      val result = Step.firstCompletedWithoutSuspension[Int]("race")(
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
      val result = Step.firstCompletedWithoutSuspension[Int]("race")(
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
    assertEquals(winnerFields(payload)._2, "1", "the winner row is written once with only the result")
    assertEquals(
      signalSubscriptions(wf.id, "k"),
      Vector.empty,
      "the winning branch's own await subscription (a) is cleaned up when it resolves via the pending await"
    )
    assertEquals(
      run(
        sql"""
          SELECT count(*) FROM workflow_steps
          WHERE workflow_id = ${wf.id} AND workflow_instance_key = 'k' AND step_id = 'race'
        """.query[Int].unique
      ),
      1,
      "the winner row is written once"
    )
  }

  test("a non-suspension branch failure propagates and records no winner") {
    val rt = newRuntime
    val s = Signal[String]("gate")
    val wf = Workflow[String, String](id = "ftr-failure") { in =>
      try {
        Step.firstCompletedWithoutSuspension[Int]("race")(
          "ok" -> { () => Step.await[String]("ok-wait", Awaitable.SignalEvent(s)); 1 },
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
      val result = Step.firstCompletedWithoutSuspension[Int]("race")(
        "l1" -> { () => Step.await[String]("l1", Awaitable.SignalEvent(s)); 1 },
        "l2" -> { () => Step.await[String]("l2", Awaitable.SignalEvent(s)); 2 },
        "fast" -> { () => Step.atLeastOnce[Int]("fast") { 3 } }
      )
      result.toString
    }
    val id = rt.createWorkflowInstance(wf, "k", "in").id
    assertEquals(rt.runWorkflowInstance(wf, id), WorkflowRunResult.Result("3"))
    val Some((_, _, payload)) = winnerRow(wf.id, "k"): @unchecked
    assertEquals(winnerFields(payload)._2, "3", "the completing branch (fast) wins")
    assertEquals(
      signalSubscriptions(wf.id, "k"),
      Vector.empty,
      "both losing branches' subscriptions for the same key are cleaned up"
    )
    assertEquals(
      run(
        sql"""
          SELECT step_id, step_scope_within_instance, state_kind FROM workflow_steps
          WHERE workflow_id = ${wf.id} AND workflow_instance_key = 'k' AND step_id = 'fast'
        """.query[(String, Vector[String], String)].to[Vector]
      ),
      Vector(("fast", Vector("race", "fast"), "Succeeded")),
      "the winner branch's own step row survives cleanup"
    )
  }

  test("invalidateOn change discards the cached winner and re-runs from scratch") {
    val rt = newRuntime
    val holder = new AtomicInteger(1)
    val counter = new AtomicInteger(0)
    val wf = Workflow[String, String](id = "ftr-invalidate") { in =>
      Step.firstCompletedWithoutSuspension[Int]("race", invalidateOn = Seq(StepInput("v", holder.get())))(
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
    assertEquals(counter.get(), 2, "a changed invalidateOn input re-runs the first-completed from scratch")
  }

  test("firstCompletedWithoutSuspension with zero branches throws IllegalArgumentException") {
    val rt = newRuntime
    val wf = Workflow[String, String](id = "ftr-empty") { in =>
      try {
        Step.firstCompletedWithoutSuspension[Int]("race")()
        "no-error"
      } catch {
        case _: IllegalArgumentException => "empty"
      }
    }
    val id = rt.createWorkflowInstance(wf, "k", "in").id
    assertEquals(rt.runWorkflowInstance(wf, id), WorkflowRunResult.Result("empty"))
  }

  test("firstCompletedWithoutSuspension rejects empty, duplicate, and reserved-prefixed branch keys") {
    val rt = newRuntime
    val wf = Workflow[String, String](id = "ftr-invalid-keys") { in =>
      def attempt(branches: (String, () => WorkflowContext ?=> Int)*): String =
        try {
          Step.firstCompletedWithoutSuspension[Int]("race")(branches*)
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

  test("sibling first-completed races in parallel branches sharing a scope keep each other's branch subscriptions (no over-deletion)") {
    val rt = newRuntime
    val sA = Signal[String]("sA")
    val sB = Signal[String]("sB")
    val sC = Signal[String]("sC")
    val sD = Signal[String]("sD")
    val wf = Workflow[String, String](id = "ftr-sibling") { in =>
      val results = Workflow.parallel[Int](
        Step.firstCompletedWithoutSuspension[Int]("raceA")(
          "a0" -> { () => Step.await[String]("a0", Awaitable.SignalEvent(sA)); 1 },
          "a1" -> { () => Step.await[String]("a1", Awaitable.SignalEvent(sB)); 2 }
        ),
        Step.firstCompletedWithoutSuspension[Int]("raceB")(
          "b0" -> { () => Step.await[String]("b0", Awaitable.SignalEvent(sC)); 10 },
          "b1" -> { () => Step.await[String]("b1", Awaitable.SignalEvent(sD)); 20 }
        )
      )
      results.mkString(",")
    }
    val id = rt.createWorkflowInstance(wf, "k", "in").id
    assertEquals(rt.runWorkflowInstance(wf, id), WorkflowRunResult.WorkflowSuspended, "both races suspend on the first run")
    assertEquals(
      signalSubscriptions(wf.id, "k").map(_._1).distinct.size,
      4,
      "each of the two races' branches registers a distinct (race-qualified) subscription path"
    )
    sA.send(id, "x")(using rt)
    assertEquals(rt.runWorkflowInstance(wf, id), WorkflowRunResult.WorkflowSuspended, "race A completes, race B stays suspended")
    sD.send(id, "y")(using rt)
    assertEquals(
      rt.runWorkflowInstance(wf, id),
      WorkflowRunResult.Result("1,20"),
      "race B's branch (b1) is still woken by its signal even after race A's loser cleanup ran"
    )
  }

  test("firstCompleted branches persist step rows under the enclosing scoped prefix") {
    val rt = newRuntime
    val bothPastCheckpoint = new java.util.concurrent.CyclicBarrier(2)
    val wf = Workflow[String, String](id = "ftr-scoped") { in =>
      Workflow.scoped("outer") {
        Step.firstCompletedWithoutSuspension[Int]("race")(
          "one" -> { () => Step.atLeastOnce[Int]("step") { bothPastCheckpoint.await(); 1 } },
          "two" -> { () => Step.atLeastOnce[Int]("step2") { bothPastCheckpoint.await(); 2 } }
        )
      }
      "done"
    }
    val id = rt.createWorkflowInstance(wf, "k", "in").id
    assertEquals(rt.runWorkflowInstance(wf, id), WorkflowRunResult.Result("done"))
    val paths = run(
      sql"""
        SELECT DISTINCT step_scope_within_instance FROM workflow_steps
        WHERE workflow_id = ${wf.id} AND workflow_instance_key = 'k' ORDER BY step_scope_within_instance
      """.query[Vector[String]].to[Vector]
    )
    assert(paths.contains(Vector("outer", "race", "one")), s"the winner branch's step must persist under the enclosing scope, got: $paths")
    assert(!paths.contains(Vector("race", "one")), s"no branch step may be persisted without the enclosing scope prefix, got: $paths")
    assert(paths.contains(Vector("outer", "race", "two")), s"the completing loser branch's step also carries the enclosing prefix, got: $paths")
  }

  test("loser cleanup deletes subscriptions of nested subscopes, not just the branch's immediate scope") {
    val rt = newRuntime
    val sWinner = Signal[String]("winner")
    val sDeep = Signal[String]("deep")
    val wf = Workflow[String, String](id = "ftr-nested-cleanup") { in =>
      val result = Step.firstCompletedWithoutSuspension[Int]("race")(
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
      Vector((Vector("race", "loser", "inner"), "deep"), (Vector("race", "winner"), "winner")),
      "the nested await registers under race/loser/inner (three segments) while the race is suspended"
    )
    sWinner.send(id, "go")(using rt)
    assertEquals(rt.runWorkflowInstance(wf, id), WorkflowRunResult.Result("1"))
    assertEquals(
      signalSubscriptions(wf.id, "k"),
      Vector.empty,
      "resolution cleans up the loser's nested-subscope subscription via the subtree delete"
    )
  }

  test("firstCompleted branches inherit an enclosing uncancellable depth") {
    val rt = newRuntime
    val sA = Signal[String]("sA")
    val sB = Signal[String]("sB")
    val wf = Workflow[String, String](id = "ftr-uncancellable-outer") { in =>
      Workflow.uncancellable {
        Step.firstCompletedWithoutSuspension[Int]("race")(
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
      WorkflowRunResult.WorkflowSuspended,
      "the race waits for the still-suspended losing branch"
    )
    val Some((_, _, pendingPayload)) = winnerRow(wf.id, "k"): @unchecked
    assertEquals(winnerFields(pendingPayload)._3, true)
    sB.send(id, "go")(using rt)
    assertEquals(
      rt.runWorkflowInstance(wf, id),
      WorkflowRunResult.Result("done"),
      "a pending cancel must not interrupt the awaiting branches inside an enclosing uncancellable region"
    )
    val Some((_, _, finishedPayload)) = winnerRow(wf.id, "k"): @unchecked
    assertEquals(winnerFields(finishedPayload)._3, false)
  }

  test("a losing branch's uncancellable cleanup that suspends keeps the race pending and resumes") {
    val rt = newRuntime
    val never = Signal[String]("never")
    val confirm = Signal[String]("confirm")
    val cleanupRan = new AtomicInteger(0)
    val wf = Workflow[String, String](id = "ftr-cleanup-suspend") { in =>
      val result = Step.firstCompletedWithoutSuspension[Int]("race")(
        "winner" -> { () => Step.atLeastOnce[Int]("winner-step") { 1 } },
        "loser" -> { () =>
          try Step.await[String]("loser-wait", Awaitable.SignalEvent(never))
          finally {
            Workflow.uncancellable {
              Step.atLeastOnce[Unit]("cleanup-effect") { cleanupRan.incrementAndGet() }
              Step.await[String]("cleanup-confirm", Awaitable.SignalEvent(confirm))
            }
          }
          2
        }
      )
      result.toString
    }
    val id = rt.createWorkflowInstance(wf, "k", "in").id
    assertEquals(rt.runWorkflowInstance(wf, id), WorkflowRunResult.WorkflowSuspended)
    val Some((_, _, payload)) = winnerRow(wf.id, "k"): @unchecked
    assertEquals(winnerFields(payload)._3, true, "the winner row records unfinished losing branches")
    assertEquals(cleanupRan.get(), 1)
    assert(signalSubscriptions(wf.id, "k").exists(_._2 == "confirm"), "the cleanup await keeps the race pending")
    confirm.send(id, "go")(using rt)
    assertEquals(rt.runWorkflowInstance(wf, id), WorkflowRunResult.Result("1"))
    val Some((_, _, payload2)) = winnerRow(wf.id, "k"): @unchecked
    assertEquals(winnerFields(payload2)._3, false, "the winner row records finished losing branches")
    assertEquals(cleanupRan.get(), 1)
    assertEquals(signalSubscriptions(wf.id, "k"), Vector.empty)
  }

  test("a pre-suspended losing branch receives cancellation and runs its cleanup in the same run") {
    val rt = newRuntime
    val win = Signal[String]("win")
    val never = Signal[String]("never")
    val cleanupRan = new AtomicInteger(0)
    val wf = Workflow[String, String](id = "ftr-cleanup-same-run") { _ =>
      val result = Step.firstCompletedWithoutSuspension[Int]("race")(
        "winner" -> { () =>
          Step.await[String]("winner-wait", Awaitable.SignalEvent(win))
          1
        },
        "loser" -> { () =>
          try {
            Step.await[String]("loser-wait", Awaitable.SignalEvent(never))
            2
          } catch {
            case _: WorkflowCancelledException =>
              Workflow.uncancellable {
                Step.atLeastOnce[Unit]("cleanup-effect") { cleanupRan.incrementAndGet() }
              }
              2
          }
        }
      )
      result.toString
    }
    val id = rt.createWorkflowInstance(wf, "k", "in").id
    assertEquals(rt.runWorkflowInstance(wf, id), WorkflowRunResult.WorkflowSuspended)
    assertEquals(cleanupRan.get(), 0, "cleanup only runs once cancellation is delivered")
    assertEquals(winnerRow(wf.id, "k"), None)
    win.send(id, "go")(using rt)
    assertEquals(rt.runWorkflowInstance(wf, id), WorkflowRunResult.Result("1"))
    assertEquals(cleanupRan.get(), 1)
    assertEquals(signalSubscriptions(wf.id, "k"), Vector.empty)
    val Some((_, _, payload)) = winnerRow(wf.id, "k"): @unchecked
    assertEquals(winnerFields(payload)._3, false)
  }

  test("a losing branch's cleanup failure does not change the race result") {
    val rt = newRuntime
    val never = Signal[String]("never")
    val confirm = Signal[String]("confirm")
    val wf = Workflow[String, String](id = "ftr-cleanup-failure") { _ =>
      val result = Step.firstCompletedWithoutSuspension[Int]("race")(
        "winner" -> { () => Step.atLeastOnce[Int]("winner-step") { 1 } },
        "loser" -> { () =>
          try Step.await[String]("loser-wait", Awaitable.SignalEvent(never))
          finally {
            Workflow.uncancellable {
              Step.await[String]("cleanup-gate", Awaitable.SignalEvent(confirm))
              Step.atLeastOnce[Unit]("cleanup-fails") { throw new RuntimeException("cleanup") }
            }
          }
          2
        }
      )
      result.toString
    }
    val id = rt.createWorkflowInstance(wf, "k", "in").id
    assertEquals(rt.runWorkflowInstance(wf, id), WorkflowRunResult.WorkflowSuspended)
    val Some((_, _, payload)) = winnerRow(wf.id, "k"): @unchecked
    assertEquals(winnerFields(payload)._3, true)
    val cleanupState =
      sql"""SELECT state_kind FROM workflow_steps WHERE workflow_id = ${wf.id} AND workflow_instance_key = 'k' AND step_id = 'cleanup-fails'"""
        .query[String]
        .option
    assertEquals(run(cleanupState), None, "cleanup has not run yet")
    confirm.send(id, "go")(using rt)
    assertEquals(rt.runWorkflowInstance(wf, id), WorkflowRunResult.Result("1"))
    assertEquals(
      run(cleanupState),
      Some("Failed"),
      "the cleanup failure is visible as a step row but does not fail the race"
    )
    val Some((_, _, payload2)) = winnerRow(wf.id, "k"): @unchecked
    assertEquals(winnerFields(payload2)._3, false)
  }

  test("the race does not re-run losing branches after its cleanup finished") {
    val rt = newRuntime
    val never = Signal[String]("never")
    val wake = Signal[String]("wake")
    val loserRuns = new AtomicInteger(0)
    val wf = Workflow[String, String](id = "ftr-cleanup-fast-path") { _ =>
      val result = Step.firstCompletedWithoutSuspension[Int]("race")(
        "winner" -> { () => Step.atLeastOnce[Int]("winner-step") { 1 } },
        "loser" -> { () =>
          loserRuns.incrementAndGet()
          try Step.await[String]("loser-wait", Awaitable.SignalEvent(never))
          finally Workflow.uncancellable { () }
          2
        }
      )
      Step.await[String]("after", Awaitable.SignalEvent(wake))
      result.toString
    }
    val id = rt.createWorkflowInstance(wf, "k", "in").id
    assertEquals(rt.runWorkflowInstance(wf, id), WorkflowRunResult.WorkflowSuspended)
    assertEquals(loserRuns.get(), 2, "the first run executes the loser once in the race and once in the loser pass")
    wake.send(id, "go")(using rt)
    assertEquals(rt.runWorkflowInstance(wf, id), WorkflowRunResult.Result("1"))
    assertEquals(loserRuns.get(), 2, "a replay with finished losing branches does not re-run them")
  }

  test("assertUnchanged conflict raises StepInputConflictException") {
    val rt = newRuntime
    val holder = new AtomicInteger(1)
    val counter = new AtomicInteger(0)
    var conflict = false
    val wf = Workflow[String, String](id = "ftr-ensure") { in =>
      try {
        Step.firstCompletedWithoutSuspension[Int]("race", assertUnchanged = Seq(StepInput("v", holder.get())))(
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
    assert(conflict, "a changed assertUnchanged input must raise StepInputConflictException")
  }
}
