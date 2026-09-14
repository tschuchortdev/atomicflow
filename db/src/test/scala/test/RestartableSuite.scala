package test

import atomicflow.*
import atomicflow.Cacheable.Simple.given
import doobie.implicits.*
import doobie.postgres.implicits.*

import java.time.{Clock, Instant}
import java.util.concurrent.atomic.AtomicInteger
import scala.collection.mutable.ListBuffer
import scala.concurrent.duration.*

class RestartableSuite extends PostgresWorkflowRuntimeSuite {

  given Cacheable[Throwable] = Cacheable.forThrowable.genericStringMessageSerializer

  private def countSteps(workflowId: WorkflowId, key: WorkflowInstanceKey): Int =
    run(
      sql"""SELECT COUNT(*) FROM workflow_steps
            WHERE workflow_id = $workflowId AND key = $key AND scope = ''""".query[Int].unique
    )

  private def countNonRegionSteps(workflowId: WorkflowId, key: WorkflowInstanceKey): Int =
    run(
      sql"""SELECT COUNT(*) FROM workflow_steps
            WHERE workflow_id = $workflowId AND key = $key AND scope = ''
              AND step_kind <> 'RestartableRegion'""".query[Int].unique
    )

  private def regionPayload(workflowId: WorkflowId, key: WorkflowInstanceKey, regionId: String): Option[(Long, String)] =
    run(
      sql"""SELECT state_payload FROM workflow_steps
            WHERE workflow_id = $workflowId AND key = $key AND scope = ''
              AND step_id = $regionId AND step_kind = 'RestartableRegion'""".query[String].option
    ).map { p =>
      val o = upickle.default.read[ujson.Value](p)
      (o("count").num.toLong, o("state").str)
    }

  private def childRows(workflowId: WorkflowId, childKey: WorkflowInstanceKey): Vector[(String, Option[String])] =
    run(
      sql"""SELECT scope, terminal_state FROM workflow_instances
            WHERE workflow_id = $workflowId AND key = $childKey
            ORDER BY scope""".query[(String, Option[String])].to[Vector]
    )

  private def countTimerSubs(workflowId: WorkflowId, key: WorkflowInstanceKey): Int =
    run(
      sql"""SELECT COUNT(*) FROM workflow_timer_subscriptions
            WHERE workflow_id = $workflowId AND key = $key AND scope = ''""".query[Int].unique
    )

  private def countTimerEvents(workflowId: WorkflowId, key: WorkflowInstanceKey): Int =
    run(
      sql"""SELECT COUNT(*) FROM workflow_events
            WHERE workflow_id = $workflowId AND key = $key AND scope = '' AND event_kind = 'TimerFired'""".query[Int].unique
    )

  test("restartable restarts until finished, threads state, and exposes restartCount") {
    val rt = newRuntime
    val observed = new ListBuffer[(Int, Long)]
    val wf = Workflow[String, String]("rl-basic") { in =>
      Workflow.restartable[Int, String]("R", 0) { (state, scope) =>
        observed += ((state, scope.restartCount))
        if (state < 3) scope.restart(state + 1)
        else "done-" + state
      }
    }
    val id = rt.createWorkflowInstance(wf, "k", "in").id
    assertEquals(rt.runWorkflowInstance(wf, id), WorkflowRunResult.Result("done-3"))
    assertEquals(observed.toList, List((0, 0L), (1, 1L), (2, 2L), (3, 3L)))
  }

  test("loop continues by returning the next state and completes via break(result)") {
    val rt = newRuntime
    val observed = new ListBuffer[(Int, Long)]
    val wf = Workflow[String, String]("lp-basic") { in =>
      Workflow.loop[Int, String]("R", 0) { (state, loop) =>
        observed += ((state, loop.restartCount))
        if (state < 3) state + 1
        else loop.break("done-" + state)
      }
    }
    val id = rt.createWorkflowInstance(wf, "k", "in").id
    assertEquals(rt.runWorkflowInstance(wf, id), WorkflowRunResult.Result("done-3"))
    assertEquals(observed.toList, List((0, 0L), (1, 1L), (2, 2L), (3, 3L)))
  }

  test("initialState by-name seed is evaluated exactly once even when re-entered on outer replay") {
    val rt = newRuntime
    val seeds = new AtomicInteger(0)
    val gate = Signal[String]("g")
    val wf = Workflow[String, String]("rl-seed") { in =>
      val r = Workflow.restartable[Int, String]("R", { seeds.incrementAndGet(); 0 }) { (state, scope) =>
        if (state < 2) scope.restart(state + 1)
        else "done"
      }
      Step.await[String]("after", Awaitable.SignalEvent(gate))
      r + "-outer"
    }
    val id = rt.createWorkflowInstance(wf, "k", "in").id
    assertEquals(rt.runWorkflowInstance(wf, id), WorkflowRunResult.WorkflowSuspended)
    assertEquals(seeds.get(), 1, "seed evaluated on first creation")
    gate.send(id, "x")(using rt)
    assertEquals(rt.runWorkflowInstance(wf, id), WorkflowRunResult.Result("done-outer"))
    assertEquals(seeds.get(), 1, "seed not re-evaluated when the completed region is re-entered on outer replay")
  }

  test("restart discards nested step rows (bounded) and reusing a step ID executes it freshly each looping") {
    val rt = newRuntime
    val stepRuns = new AtomicInteger(0)
    val wf = Workflow[String, String]("rl-discard") { in =>
      Workflow.loop[Int, String]("R", 0) { (state, loop) =>
        Step.atLeastOnce[Int]("work") { stepRuns.incrementAndGet(); state * 10 }
        if (state < 5) state + 1
        else loop.break("done")
      }
    }
    val id = rt.createWorkflowInstance(wf, "k", "in").id
    assertEquals(rt.runWorkflowInstance(wf, id), WorkflowRunResult.Result("done"))
    assertEquals(stepRuns.get(), 6, "the step re-executes freshly in each of the 6 loopings")
    assertEquals(countNonRegionSteps(wf.id, "k"), 1, "only the final looping's step row remains")
    assertEquals(countSteps(wf.id, "k"), 2, "region row + final looping's step row")
  }

  test("reusing a step/timer ID in a successor looping creates fresh work: the timer fires again each looping") {
    val clock = new TestClock(Instant.parse("2026-01-02T00:00:00Z"))
    val rt = newRuntime(clock)
    given Clock = clock
    val wf = Workflow[String, String]("rl-timer") { in =>
      Workflow.loop[Int, String]("R", 0) { (state, loop) =>
        Step.await[Unit]("poll-interval", Awaitable.Timer(1.minute))
        if (state < 2) state + 1
        else loop.break("done-" + state)
      }
    }
    val id = rt.createWorkflowInstance(wf, "k", "in").id

    assertEquals(rt.runWorkflowInstance(wf, id), WorkflowRunResult.WorkflowSuspended)
    assertEquals(countTimerSubs(wf.id, "k"), 1, "the first looping awaits one timer")

    clock.advanceBy(2.minutes)
    assertEquals(rt.runWorkflowInstance(wf, id), WorkflowRunResult.WorkflowSuspended, "second looping suspends on a fresh timer")
    assertEquals(countTimerSubs(wf.id, "k"), 1, "the discarded looping's timer subscription was replaced, not accumulated")

    clock.advanceBy(2.minutes)
    assertEquals(rt.runWorkflowInstance(wf, id), WorkflowRunResult.WorkflowSuspended, "third looping suspends on a fresh timer")

    clock.advanceBy(2.minutes)
    assertEquals(rt.runWorkflowInstance(wf, id), WorkflowRunResult.Result("done-2"))
    assertEquals(countTimerEvents(wf.id, "k"), 3, "the timer fired once per looping (fresh work each time)")
    assertEquals(countNonRegionSteps(wf.id, "k"), 1, "only the final looping's await row remains")
  }

  test("signal cursors survive restart: a consumed signal is not re-consumable in a later looping") {
    val rt = newRuntime
    val sig = Signal[String]("S")
    val wf = Workflow[String, String]("rl-cursor") { in =>
      Workflow.loop[Int, String]("R", 0) { (state, loop) =>
        Step.await[String]("wait", Awaitable.SignalEvent(sig))
        if (state < 2) state + 1
        else loop.break("done-" + state)
      }
    }
    val id = rt.createWorkflowInstance(wf, "k", "in").id

    assertEquals(rt.runWorkflowInstance(wf, id), WorkflowRunResult.WorkflowSuspended)
    assertEquals(regionPayload(wf.id, "k", "R").map(_._1), Some(0L), "a suspension before restart does not advance restartCount")

    sig.send(id, "v0")(using rt)
    assertEquals(rt.runWorkflowInstance(wf, id), WorkflowRunResult.WorkflowSuspended, "looping 1 consumes v0 and suspends awaiting a fresh signal")
    assertEquals(regionPayload(wf.id, "k", "R").map(_._1), Some(1L), "restartCount now counts the committed restart")

    sig.send(id, "v1")(using rt)
    assertEquals(rt.runWorkflowInstance(wf, id), WorkflowRunResult.WorkflowSuspended, "looping 2 consumes v1, not the already-consumed v0")

    sig.send(id, "v2")(using rt)
    assertEquals(rt.runWorkflowInstance(wf, id), WorkflowRunResult.Result("done-2"), "looping 3 consumes v2 and completes")
  }

  test("children created in a discarded looping are closed per policy and successor loopings use distinct identities") {
    val rt = newRuntime
    val gate = Signal[String]("gate")
    val childWf = Workflow[String, String]("rl-worker") { in => in }
    val wf = Workflow[String, String]("rl-children") { in =>
      val r = Workflow.loop[Int, String]("R", 0) { (state, loop) =>
        childWf.startAsChild("c", "hi-" + state, parentClosePolicy = ParentClosePolicy.Cancel)
        if (state < 2) state + 1
        else loop.break("done")
      }
      Step.await[String]("parentgate", Awaitable.SignalEvent(gate))
      r
    }
    val parentId = rt.createWorkflowInstance(wf, "p", "in").id
    assertEquals(rt.runWorkflowInstance(wf, parentId), WorkflowRunResult.WorkflowSuspended, "the parent suspends after the region, before terminal close policies apply")

    val rows = childRows(childWf.id, "c")
    assertEquals(rows.size, 3, "one distinct child identity per looping")
    val cancelledScopes = rows.filter(_._2.contains("cancelled")).map(_._1)
    val activeScopes = rows.filter(_._2.isEmpty).map(_._1)
    assertEquals(cancelledScopes.size, 2, "children of the two discarded loopings are cancelled per Cancel policy")
    assert(cancelledScopes.exists(_.contains("R@0")), "looping 0's child scope carries R@0")
    assert(cancelledScopes.exists(_.contains("R@1")), "looping 1's child scope carries R@1")
    assertEquals(activeScopes.size, 1, "the final looping's child stays active")
    assert(activeScopes.head.contains("R@2"), "the final child carries R@2")

    gate.send(parentId, "go")(using rt)
    assertEquals(rt.runWorkflowInstance(wf, parentId), WorkflowRunResult.Result("done"))
  }

  test("restart crossing a parallel boundary cleans up sibling branches and propagates to the region boundary") {
    val rt = newRuntime
    val wf = Workflow[String, String]("rl-par") { in =>
      Workflow.restartable[Int, String]("R", 0) { (state, scope) =>
        val results = Workflow.parallel[Int](
          () => {
            Step.atLeastOnce[Int]("sib") { 1 }
            if (state < 2) scope.restart(state + 1)
            else state
          },
          () => 99
        )
        "ok-" + results(0)
      }
    }
    val id = rt.createWorkflowInstance(wf, "k", "in").id
    assertEquals(rt.runWorkflowInstance(wf, id), WorkflowRunResult.Result("ok-2"))
    assertEquals(regionPayload(wf.id, "k", "R").map(_._1), Some(2L), "the region committed 2 restarts")
  }

  test("break crossing a parallel boundary completes the region with the result") {
    val rt = newRuntime
    val wf = Workflow[String, String]("rl-par-break") { in =>
      Workflow.loop[Int, String]("R", 0) { (state, loop) =>
        val results = Workflow.parallel[Int](
          () => {
            Step.atLeastOnce[Int]("a") { 1 }
            if (state < 1) state + 1
            else loop.break("done-" + state)
          },
          () => 99
        )
        results(0)
      }
    }
    val id = rt.createWorkflowInstance(wf, "k", "in").id
    assertEquals(rt.runWorkflowInstance(wf, id), WorkflowRunResult.Result("done-1"))
  }

  test("bounded state after 50 loopings retains O(1) rows") {
    val rt = newRuntime
    val wf = Workflow[String, String]("rl-bounded") { in =>
      Workflow.loop[Int, String]("R", 0) { (state, loop) =>
        Step.atLeastOnce[Int]("step") { state }
        if (state < 49) state + 1
        else loop.break("done")
      }
    }
    val id = rt.createWorkflowInstance(wf, "k", "in").id
    assertEquals(rt.runWorkflowInstance(wf, id), WorkflowRunResult.Result("done"))
    assertEquals(countNonRegionSteps(wf.id, "k"), 1, "only the final looping's step row remains after 50 loopings")
    assertEquals(countSteps(wf.id, "k"), 2)
  }

  test("normal completion + outer suspension later: replay reuses cached records without re-executing the region's effects") {
    val rt = newRuntime
    val stepRuns = new AtomicInteger(0)
    val gate = Signal[String]("g")
    val wf = Workflow[String, String]("rl-replay") { in =>
      val r = Workflow.restartable[Int, String]("R", 0) { (state, scope) =>
        Step.atLeastOnce[Int]("work") { stepRuns.incrementAndGet(); state }
        if (state < 2) scope.restart(state + 1)
        else "done"
      }
      Step.await[String]("after", Awaitable.SignalEvent(gate))
      r + "-outer"
    }
    val id = rt.createWorkflowInstance(wf, "k", "in").id
    assertEquals(rt.runWorkflowInstance(wf, id), WorkflowRunResult.WorkflowSuspended)
    assertEquals(stepRuns.get(), 3, "region executed its step in 3 loopings before the outer suspension")

    gate.send(id, "go")(using rt)
    assertEquals(rt.runWorkflowInstance(wf, id), WorkflowRunResult.Result("done-outer"))
    assertEquals(stepRuns.get(), 3, "on outer replay the completed region's steps are served from cache, not re-executed")
  }

  test("restart and break are excluded by WorkflowNonFatal (not swallowed by broad catches)") {
    val rt = newRuntime
    val wfR = Workflow[String, String]("rl-nf-restart") { in =>
      Workflow.restartable[Int, String]("R", 0) { (state, scope) =>
        try {
          if (state < 2) scope.restart(state + 1)
          else "done-" + state
        } catch {
          case WorkflowNonFatal(t) => "swallowed"
        }
      }
    }
    val idR = rt.createWorkflowInstance(wfR, "k", "in").id
    assertEquals(rt.runWorkflowInstance(wfR, idR), WorkflowRunResult.Result("done-2"), "restart was not swallowed by WorkflowNonFatal")

    val wfL = Workflow[String, String]("rl-nf-break") { in =>
      Workflow.loop[Int, String]("R", 0) { (state, loop) =>
        try {
          if (state < 2) state + 1
          else loop.break("done-" + state)
        } catch {
          case WorkflowNonFatal(t) => -1
        }
      }
    }
    val idL = rt.createWorkflowInstance(wfL, "k", "in").id
    assertEquals(rt.runWorkflowInstance(wfL, idL), WorkflowRunResult.Result("done-2"), "break was not swallowed by WorkflowNonFatal")
  }

  test("restartable and loop are equivalent: identical durable transitions") {
    val rt = newRuntime
    val wfR = Workflow[String, String]("eq-restartable") { in =>
      Workflow.restartable[Int, String]("R", 0) { (state, scope) =>
        Step.atLeastOnce[Int]("work") { state }
        if (state < 3) scope.restart(state + 1)
        else "done-" + state
      }
    }
    val idR = rt.createWorkflowInstance(wfR, "k", "in").id
    assertEquals(rt.runWorkflowInstance(wfR, idR), WorkflowRunResult.Result("done-3"))

    val wfL = Workflow[String, String]("eq-loop") { in =>
      Workflow.loop[Int, String]("R", 0) { (state, loop) =>
        Step.atLeastOnce[Int]("work") { state }
        if (state < 3) state + 1
        else loop.break("done-" + state)
      }
    }
    val idL = rt.createWorkflowInstance(wfL, "k", "in").id
    assertEquals(rt.runWorkflowInstance(wfL, idL), WorkflowRunResult.Result("done-3"))

    assertEquals(countSteps(wfR.id, "k"), countSteps(wfL.id, "k"), "identical total step-row shapes")
    assertEquals(countNonRegionSteps(wfR.id, "k"), countNonRegionSteps(wfL.id, "k"))
    assertEquals(regionPayload(wfR.id, "k", "R"), regionPayload(wfL.id, "k", "R"), "identical region row (state + restartCount)")
  }

  test("a restart from one parallel branch does not interrupt a sibling mid-DB-write; the run completes (no hang)") {
    val rt = newRuntime
    val stepRuns = new AtomicInteger(0)
    val wf = Workflow[String, String]("rl-par-hang") { in =>
      Workflow.restartable[Int, String]("R", 0) { (state, scope) =>
        Workflow.parallel[Int](
          () => {
            Step.atLeastOnce[Int]("sib") {
              stepRuns.incrementAndGet()
              Thread.sleep(120)
              1
            }
            state
          },
          () => {
            if (state < 2) scope.restart(state + 1) else state
          }
        )
        "ok-" + state
      }
    }
    val id = rt.createWorkflowInstance(wf, "k", "in").id
    assertEquals(rt.runWorkflowInstance(wf, id), WorkflowRunResult.Result("ok-2"))
    assertEquals(regionPayload(wf.id, "k", "R").map(_._1), Some(2L), "the region committed 2 restarts")
    assertEquals(stepRuns.get(), 3, "the DB-working sibling's step completed in all 3 loopings")
  }

  test("a step inside parallel under a region persists with the region scope prefix") {
    val rt = newRuntime
    val wf = Workflow[String, String]("rl-par-scope") { in =>
      Workflow.restartable[Int, String]("R", 0) { (state, scope) =>
        Workflow.parallel[Int](
          () => {
            Step.atLeastOnce[Int]("inner") { 1 }
            state
          },
          () => state
        )
        "done"
      }
    }
    val id = rt.createWorkflowInstance(wf, "k", "in").id
    assertEquals(rt.runWorkflowInstance(wf, id), WorkflowRunResult.Result("done"))
    val paths = run(
      sql"""SELECT DISTINCT step_scope_path FROM workflow_steps
            WHERE workflow_id = ${wf.id} AND key = 'k' AND step_id = 'inner'""".query[String].to[Vector]
    )
    assert(paths.contains("R@0"), s"the parallel step's scope carries the enclosing region segment: $paths")
  }

  test("a step inside parallel re-executes each looping under a region (scope propagates into branch threads)") {
    val rt = newRuntime
    val stepRuns = new AtomicInteger(0)
    val wf = Workflow[String, String]("rl-par-loop") { in =>
      Workflow.restartable[Int, String]("R", 0) { (state, scope) =>
        Workflow.parallel[Int](
          () => {
            Step.atLeastOnce[Int]("inner") { stepRuns.incrementAndGet(); 1 }
            state
          },
          () => state
        )
        if (state < 2) scope.restart(state + 1) else "done"
      }
    }
    val id = rt.createWorkflowInstance(wf, "k", "in").id
    assertEquals(rt.runWorkflowInstance(wf, id), WorkflowRunResult.Result("done"))
    assertEquals(stepRuns.get(), 3, "the parallel step re-executes in each of the 3 loopings (no stale cache)")
    assertEquals(countNonRegionSteps(wf.id, "k"), 1, "only the final looping's parallel step row remains")
  }

  test("children started inside a parallel branch under a region derive the region-scoped identity") {
    val rt = newRuntime
    val childWf = Workflow[String, String]("rl-par-child") { in => in }
    val wf = Workflow[String, String]("rl-par-children") { in =>
      Workflow.loop[Int, String]("R", 0) { (state, loop) =>
        Workflow.parallel[Unit](
          () => {
            childWf.startAsChild("c", "hi-" + state)
            ()
          },
          () => ()
        )
        if (state < 1) state + 1
        else loop.break("done")
      }
    }
    val id = rt.createWorkflowInstance(wf, "p", "in").id
    assertEquals(rt.runWorkflowInstance(wf, id), WorkflowRunResult.Result("done"))
    val rows = childRows(childWf.id, "c")
    assertEquals(rows.size, 2, "one distinct child identity per looping")
    assert(rows.map(_._1).exists(_.contains("R@0")), "looping 0's child scope carries R@0")
    assert(rows.map(_._1).exists(_.contains("R@1")), "looping 1's child scope carries R@1")
  }

  test("a restart of a region does not over-close children of a same-id nested region in a sibling subtree") {
    val rt = newRuntime
    val childWf = Workflow[String, String]("rl-nested-child") { in => in }
    val gate = Signal[String]("gate")
    val wf = Workflow[String, String]("rl-overclose") { in =>
      Workflow.restartable[Int, String]("S", 0) { (_, _) =>
        Workflow.restartable[Int, Unit]("R", 0) { (_, _) =>
          childWf.startAsChild("nestedChild", "x")
          ()
        }
        "s-done"
      }
      Workflow.restartable[Int, String]("R", 0) { (state, scope) =>
        if (state < 1) scope.restart(state + 1) else "done"
      }
      Step.await[String]("gate", Awaitable.SignalEvent(gate))
      "all-done"
    }
    val id = rt.createWorkflowInstance(wf, "k", "in").id
    assertEquals(rt.runWorkflowInstance(wf, id), WorkflowRunResult.WorkflowSuspended)
    val rows = childRows(childWf.id, "nestedChild")
    assertEquals(rows.size, 1, "the sibling subtree's child is created once under S@0/R@0")
    assert(rows.head._1.contains("S@0/R@0"), s"the child scope lies under the sibling region: ${rows.head._1}")
    assertEquals(rows.head._2, None, "outer R's restart must not close the sibling subtree's child")
    gate.send(id, "go")(using rt)
    assertEquals(rt.runWorkflowInstance(wf, id), WorkflowRunResult.Result("all-done"))
  }

  test("a region id containing _ is LIKE-escaped: restart does not delete a sibling region's nested rows") {
    val rt = newRuntime
    val wf = Workflow[String, String]("rl-underscore") { in =>
      Workflow.restartable[Int, String]("R11", 0) { (state, scope) =>
        Workflow.scoped("sub") { Step.atLeastOnce[Int]("b") { 2 } }
        "done2"
      }
      Workflow.restartable[Int, String]("R_1", 0) { (state, scope) =>
        Step.atLeastOnce[Int]("a") { 1 }
        if (state < 1) scope.restart(state + 1) else "done"
      }
      "all-done"
    }
    val id = rt.createWorkflowInstance(wf, "k", "in").id
    assertEquals(rt.runWorkflowInstance(wf, id), WorkflowRunResult.Result("all-done"))
    val bCount = run(
      sql"""SELECT COUNT(*) FROM workflow_steps
            WHERE workflow_id = ${wf.id} AND key = 'k' AND step_id = 'b'""".query[Int].unique
    )
    assertEquals(bCount, 1, "the sibling region's nested step survives the LIKE-wildcard restart")
  }
}
