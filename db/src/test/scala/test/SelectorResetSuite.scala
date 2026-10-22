package test

import atomicflow.*
import atomicflow.given
import atomicflow.Cacheable.Simple.given
import doobie.implicits.*
import doobie.postgres.implicits.*

import java.time.Instant
import java.util.concurrent.atomic.AtomicInteger
import scala.concurrent.duration.*

class SelectorResetSuite extends PostgresWorkflowRuntimeSuite {

  given Cacheable[Throwable] = Cacheable.forThrowable.genericStringMessageSerializer

  private val t0 = Instant.parse("2026-01-01T00:00:00Z")
  private val t1 = t0.plusSeconds(10)
  private val t2 = t1.plusSeconds(10)
  private val t3 = t2.plusSeconds(10)
  private val t4 = t3.plusSeconds(10)
  private val t5 = t4.plusSeconds(10)

  private def stepIds(workflowId: WorkflowId, key: WorkflowInstanceKey): Set[String] =
    run(
      sql"""
        SELECT step_id FROM workflow_steps
        WHERE workflow_id = $workflowId AND workflow_instance_key = $key AND workflow_instance_scope = '{}'
      """.query[String].to[Set]
    )

  private def stepRows(workflowId: WorkflowId, key: WorkflowInstanceKey): Set[(Vector[String], String)] =
    run(
      sql"""
        SELECT step_scope_within_instance, step_id FROM workflow_steps
        WHERE workflow_id = $workflowId AND workflow_instance_key = $key AND workflow_instance_scope = '{}'
      """.query[(Vector[String], String)].to[Set]
    )

  private def setUpdatedAt(workflowId: WorkflowId,
                           key: WorkflowInstanceKey,
                           stepId: String,
                           ts: Instant): Unit =
    run(
      sql"""
        UPDATE workflow_steps SET updated_at = $ts
        WHERE workflow_id = $workflowId AND workflow_instance_key = $key AND workflow_instance_scope = '{}' AND step_id = $stepId
      """.update.run
    )

  private def setUpdatedAtForVersion(
      workflowId: WorkflowId,
      key: WorkflowInstanceKey,
      stepId: String,
      stepVersion: Long,
      ts: Instant
  ): Unit =
    run(
      sql"""
        UPDATE workflow_steps SET updated_at = $ts
        WHERE workflow_id = $workflowId AND workflow_instance_key = $key AND workflow_instance_scope = '{}'
          AND step_id = $stepId AND step_version = $stepVersion
      """.update.run
    )

  private def stepVersions(workflowId: WorkflowId, key: WorkflowInstanceKey, stepId: String): Vector[Long] =
    run(
      sql"""
        SELECT step_version FROM workflow_steps
        WHERE workflow_id = $workflowId AND workflow_instance_key = $key AND workflow_instance_scope = '{}' AND step_id = $stepId
        ORDER BY step_version
      """.query[Long].to[Vector]
    )

  private def setUpdatedAtInScope(
      workflowId: WorkflowId,
      key: WorkflowInstanceKey,
      scopeSegments: Vector[String],
      stepId: String,
      ts: Instant
  ): Unit =
    run(
      sql"""
        UPDATE workflow_steps SET updated_at = $ts
        WHERE workflow_id = $workflowId AND workflow_instance_key = $key
          AND workflow_instance_scope = '{}' AND step_scope_within_instance = $scopeSegments AND step_id = $stepId
      """.update.run
    )

  test("reset with an anchored exact selector erases the selected step and later; earlier history replays from cache") {
    val rt = newRuntime
    val sig = Signal[String]("R")
    val cA = new AtomicInteger(0)
    val cB = new AtomicInteger(0)
    val cC = new AtomicInteger(0)
    val wf = Workflow[String, String]("sel-reset-basic") { _ =>
      Step.atLeastOnce[String]("a") { cA.incrementAndGet(); "a" }
      Step.atLeastOnce[String]("b") { cB.incrementAndGet(); "b" }
      Step.atLeastOnce[String]("c") { cC.incrementAndGet(); "c" }
      Step.await[String]("w", Awaitable.SignalEvent(sig))
      "done"
    }
    val id = rt.createWorkflowInstance(wf, "k", "in").id
    val handle = rt.getWorkflowInstance(wf, id)
    assertEquals(rt.runWorkflowInstance(wf, id), WorkflowRunResult.WorkflowSuspended)
    assertEquals((cA.get(), cB.get(), cC.get()), (1, 1, 1))

    setUpdatedAt(wf.id, "k", "a", t1)
    setUpdatedAt(wf.id, "k", "b", t2)
    setUpdatedAt(wf.id, "k", "c", t3)

    rt.resetWorkflow[String, String](id, Seq.empty, Seq(stepId"/b"))(using wf)
    assertEquals(handle.getInfo()(using rt).generation, 1L, "generation increments in place")
    assertEquals(stepIds(wf.id, "k"), Set("a"), "b and c are erased; a is kept")

    assertEquals(rt.runWorkflowInstance(wf, id), WorkflowRunResult.WorkflowSuspended)
    assertEquals((cA.get(), cB.get(), cC.get()), (1, 2, 2), "a replays; b and c re-execute exactly once more")

    sig.send(id, "go")(using rt)
    assertEquals(rt.runWorkflowInstance(wf, id), WorkflowRunResult.Result("done"))
    assertEquals(handle.getInfo()(using rt).terminalState, Some(WorkflowTerminalState.Completed))
  }

  test("an unanchored selector matching a step under a scope resets that step and everything after it") {
    val rt = newRuntime
    val cCharge = new AtomicInteger(0)
    val cAfter = new AtomicInteger(0)
    val wf = Workflow[String, String]("sel-reset-scoped") { _ =>
      Workflow.scoped("orders") {
        Step.atLeastOnce[String]("charge") { cCharge.incrementAndGet(); "charged" }
      }
      Step.atLeastOnce[String]("after") { cAfter.incrementAndGet(); "after" }
      Step.await[Unit]("wait", Awaitable.Timer(3600.seconds))
      "unreachable"
    }
    val id = rt.createWorkflowInstance(wf, "k", "in").id
    assertEquals(rt.runWorkflowInstance(wf, id), WorkflowRunResult.WorkflowSuspended)
    assertEquals((cCharge.get(), cAfter.get()), (1, 1))

    setUpdatedAtInScope(wf.id, "k", Vector("orders"), "charge", t1)
    setUpdatedAt(wf.id, "k", "after", t2)
    setUpdatedAt(wf.id, "k", "wait", t3)

    rt.resetWorkflow[String, String](id, Seq.empty, Seq(stepId"orders/charge"))(using wf)
    assertEquals(stepRows(wf.id, "k"), Set.empty, "charge and everything after it are erased")

    assertEquals(rt.runWorkflowInstance(wf, id), WorkflowRunResult.WorkflowSuspended)
    assertEquals((cCharge.get(), cAfter.get()), (2, 2), "charge and after re-execute exactly once more")
  }

  test("a selector matching multiple rows uses the earliest matched row as the boundary") {
    val rt = newRuntime
    val cX1 = new AtomicInteger(0)
    val cX2 = new AtomicInteger(0)
    val cTail = new AtomicInteger(0)
    val wf = Workflow[String, String]("sel-reset-multi") { _ =>
      Workflow.scoped("s1") {
        Step.atLeastOnce[String]("x") { cX1.incrementAndGet(); "x1" }
      }
      Step.atLeastOnce[String]("tail") { cTail.incrementAndGet(); "t" }
      Workflow.scoped("s2") {
        Step.atLeastOnce[String]("x") { cX2.incrementAndGet(); "x2" }
      }
      Step.await[Unit]("wait", Awaitable.Timer(3600.seconds))
      "unreachable"
    }
    val id = rt.createWorkflowInstance(wf, "k", "in").id
    assertEquals(rt.runWorkflowInstance(wf, id), WorkflowRunResult.WorkflowSuspended)
    assertEquals((cX1.get(), cTail.get(), cX2.get()), (1, 1, 1))

    setUpdatedAtInScope(wf.id, "k", Vector("s1"), "x", t1)
    setUpdatedAt(wf.id, "k", "tail", t2)
    setUpdatedAtInScope(wf.id, "k", Vector("s2"), "x", t3)
    setUpdatedAt(wf.id, "k", "wait", t4)

    rt.resetWorkflow[String, String](id, Seq.empty, Seq(stepId"*/x"))(using wf)
    assertEquals(
      stepRows(wf.id, "k"),
      Set.empty[(Vector[String], String)],
      "the boundary is the earliest matched row (s1/x), so s2/x and the non-matching tail between them are erased too"
    )

    assertEquals(rt.runWorkflowInstance(wf, id), WorkflowRunResult.WorkflowSuspended)
    assertEquals((cX1.get(), cTail.get(), cX2.get()),
                 (2, 2, 2),
                 "every erased row re-executes exactly once more")
  }

  test(
    "a selector matching a step with multiple stored version rows cuts at the MAX updated_at across versions") {
    val rt = newRuntime
    val sig = Signal[String]("R")
    val cA = new AtomicInteger(0)
    val cB = new AtomicInteger(0)
    val cC = new AtomicInteger(0)
    val wf = Workflow[String, String]("sel-reset-multiver") { _ =>
      Step.atLeastOnce[String]("a") { cA.incrementAndGet(); "a" }
      Step.atLeastOnce[String]("b") { cB.incrementAndGet(); "b" }
      Step.atLeastOnce[String]("c") { cC.incrementAndGet(); "c" }
      Step.await[String]("w", Awaitable.SignalEvent(sig))
      "done"
    }
    val id = rt.createWorkflowInstance(wf, "k", "in").id
    val handle = rt.getWorkflowInstance(wf, id)
    assertEquals(rt.runWorkflowInstance(wf, id), WorkflowRunResult.WorkflowSuspended)
    assertEquals((cA.get(), cB.get(), cC.get()), (1, 1, 1))

    // Hand-INSERT a second stored row for "b" at step_version 2, mirroring the
    // fork copy's INSERT (the same row re-keyed at another version), so "b"
    // carries two version rows like after a version bump.
    run(
      sql"""
        INSERT INTO workflow_steps (workflow_id, workflow_instance_key, workflow_instance_scope, step_id, step_scope_within_instance, step_version, step_kind, state_kind, state_payload, retry_deadline, input_fingerprints, expires_at, created_at, updated_at)
        SELECT ${wf.id}, 'k', '{}', step_id, step_scope_within_instance, 2, step_kind, state_kind, state_payload, retry_deadline, input_fingerprints, expires_at, created_at, updated_at
        FROM workflow_steps
        WHERE workflow_id = ${wf.id} AND workflow_instance_key = 'k' AND workflow_instance_scope = '{}' AND step_id = 'b' AND step_version = 1
      """.update.run
    )
    assertEquals(stepVersions(wf.id, "k", "b"), Vector(1L, 2L), "setup: b has two stored version rows")

    // a is updated before the MAX of b's version rows, c after it.
    setUpdatedAt(wf.id, "k", "a", t1)
    setUpdatedAtForVersion(wf.id, "k", "b", 1, t2)
    setUpdatedAtForVersion(wf.id, "k", "b", 2, t3)
    setUpdatedAt(wf.id, "k", "c", t4)
    setUpdatedAt(wf.id, "k", "w", t5)

    rt.resetWorkflow[String, String](id, Seq.empty, Seq(stepId"/b"))(using wf)
    assertEquals(handle.getInfo()(using rt).generation, 1L, "generation increments in place")
    assertEquals(
      stepIds(wf.id, "k"),
      Set("a", "b"),
      "the cut lands at b's MAX updated_at (t3): c and w are erased; a stays"
    )
    assertEquals(
      stepVersions(wf.id, "k", "b"),
      Vector(1L),
      "b's version-2 row sits at the boundary and is erased; the version-1 row strictly below the MAX survives"
    )
    assertEquals(stepVersions(wf.id, "k", "c"),
                 Vector.empty[Long],
                 "c is updated after the boundary and is erased")

    assertEquals(rt.runWorkflowInstance(wf, id), WorkflowRunResult.WorkflowSuspended)
    assertEquals(
      (cA.get(), cB.get(), cC.get()),
      (1, 1, 2),
      "a replays and b replays from its surviving version-1 row; only c, updated after the boundary, re-executes"
    )

    sig.send(id, "go")(using rt)
    assertEquals(rt.runWorkflowInstance(wf, id), WorkflowRunResult.Result("done"))
    assertEquals(handle.getInfo()(using rt).terminalState, Some(WorkflowTerminalState.Completed))
  }

  test("a selector matching a loop iteration's step resets the live iteration; the region row survives") {
    val rt = newRuntime
    val cInner = new AtomicInteger(0)
    val wf = Workflow[String, String]("sel-reset-loop") { in =>
      Workflow.loop[Int, String]("loopid", 0) { (state, loop) =>
        Step.atLeastOnce[Int]("innerstep") { cInner.incrementAndGet(); state }
        if state == 0 then state + 1
        else
          Step.await[Unit]("wait", Awaitable.Timer(3600.seconds))
          loop.break("done")
      }
      "done"
    }
    val id = rt.createWorkflowInstance(wf, "k", "in").id
    assertEquals(rt.runWorkflowInstance(wf, id), WorkflowRunResult.WorkflowSuspended)
    assertEquals(cInner.get(), 2, "the step executed in iteration 0 and in the suspended iteration 1")

    setUpdatedAt(wf.id, "k", "loopid", t0)
    setUpdatedAtInScope(wf.id, "k", Vector("loopid@1"), "innerstep", t1)
    setUpdatedAtInScope(wf.id, "k", Vector("loopid@1"), "wait", t2)

    rt.resetWorkflow[String, String](id, Seq.empty, Seq(stepId"loopid/innerstep"))(using wf)
    assertEquals(
      stepRows(wf.id, "k"),
      Set((Vector.empty[String], "loopid")),
      "only the live iteration's innerstep is erased (plus the later wait); the region row survives"
    )

    assertEquals(rt.runWorkflowInstance(wf, id), WorkflowRunResult.WorkflowSuspended)
    assertEquals(cInner.get(), 3, "iteration 1 resumes from the region row and re-executes innerstep")
  }

  test("fork with an anchored selector copies the history strictly before the selected step") {
    val rt = newRuntime
    val cA = new AtomicInteger(0)
    val cB = new AtomicInteger(0)
    val cC = new AtomicInteger(0)
    val wf = Workflow[String, String]("sel-fork-basic") { _ =>
      Step.atLeastOnce[String]("a") { cA.incrementAndGet(); "a" }
      Step.atLeastOnce[String]("b") { cB.incrementAndGet(); "b" }
      Step.atLeastOnce[String]("c") { cC.incrementAndGet(); "c" }
      "done"
    }
    val sourceId = rt.createWorkflowInstance(wf, "k", "in").id
    assertEquals(rt.runWorkflowInstance(wf, sourceId), WorkflowRunResult.Result("done"))
    assertEquals((cA.get(), cB.get(), cC.get()), (1, 1, 1))

    setUpdatedAt(wf.id, "k", "a", t1)
    setUpdatedAt(wf.id, "k", "b", t2)
    setUpdatedAt(wf.id, "k", "c", t3)

    val fork = rt.forkWorkflow[String, String](sourceId, "fork", Seq.empty, Seq(stepId"/a"))(using wf)
    assertEquals(fork.id.workflowInstanceKey, "fork")
    assertEquals(fork.id.scope, ScopePath.empty)
    assertEquals(stepIds(wf.id, "fork"), Set.empty, "nothing is strictly before a; a and later are omitted")

    assertEquals(rt.runWorkflowInstance(wf, fork.id), WorkflowRunResult.Result("done"))
    assertEquals((cA.get(), cB.get(), cC.get()), (2, 2, 2), "the fork re-executes a, b, and c from scratch")
  }

  test("a selector matching nothing throws NoMatchingStepException naming the pattern and an existing step") {
    val rt = newRuntime
    val sig = Signal[String]("R")
    val wf = Workflow[String, String]("sel-reset-nomatch") { _ =>
      Step.atLeastOnce[String]("a") { "a" }
      Step.await[String]("w", Awaitable.SignalEvent(sig))
      "unreachable"
    }
    val id = rt.createWorkflowInstance(wf, "k", "in").id
    assertEquals(rt.runWorkflowInstance(wf, id), WorkflowRunResult.WorkflowSuspended)

    val resetError = intercept[NoMatchingStepException] {
      rt.resetWorkflow[String, String](id, Seq.empty, Seq(stepId"/zzz"))(using wf)
    }
    assert(resetError.getMessage.contains("/zzz"), s"unexpected message: ${resetError.getMessage}")
    assert(
      resetError.getMessage.contains("existing steps:"),
      s"the message must list the instance's existing steps: ${resetError.getMessage}"
    )
    assert(
      resetError.getMessage.contains("'a'"),
      s"the message must list the existing step 'a' in the existing-steps list: ${resetError.getMessage}"
    )

    val forkError = intercept[NoMatchingStepException] {
      rt.forkWorkflow[String, String](id, "fork", Seq.empty, Seq(stepId"/zzz"))(using wf)
    }
    assert(forkError.getMessage.contains("/zzz"), s"unexpected message: ${forkError.getMessage}")
    assert(
      forkError.getMessage.contains("existing steps:"),
      s"the message must list the instance's existing steps: ${forkError.getMessage}"
    )
    assert(
      forkError.getMessage.contains("'a'"),
      s"the message must list the existing step 'a' in the existing-steps list: ${forkError.getMessage}"
    )
  }

  test("listSteps returns the matched step ids ordered by last update; non-matching is empty; unknown instances throw") {
    val rt = newRuntime
    val wf = Workflow[String, String]("sel-list-steps") { _ =>
      Step.atLeastOnce[String]("a") { "a" }
      Step.atLeastOnce[String]("b") { "b" }
      Step.atLeastOnce[String]("c") { "c" }
      Step.await[Unit]("wait", Awaitable.Timer(3600.seconds))
      "unreachable"
    }
    val id = rt.createWorkflowInstance(wf, "k", "in").id
    assertEquals(rt.runWorkflowInstance(wf, id), WorkflowRunResult.WorkflowSuspended)

    setUpdatedAt(wf.id, "k", "b", t1)
    setUpdatedAt(wf.id, "k", "c", t2)
    setUpdatedAt(wf.id, "k", "a", t3)

    assertEquals(
      rt.listSteps(id, stepId"*"),
      Vector(StepId("b"), StepId("c"), StepId("a")),
      "matched step ids are ordered by last update, not declaration order or alphabetical order"
    )
    assertEquals(rt.listSteps(id, stepId"c"), Vector(StepId("c")))
    assertEquals(rt.listSteps(id, stepId"**/b"), Vector(StepId("b")))

    val scopedWf = Workflow[String, String]("sel-list-steps-scoped") { _ =>
      Workflow.scoped("orders") {
        Step.atLeastOnce[String]("charge") { "charged" }
      }
      Step.atLeastOnce[String]("after") { "after" }
      Step.await[Unit]("wait", Awaitable.Timer(3600.seconds))
      "unreachable"
    }
    val scopedId = rt.createWorkflowInstance(scopedWf, "sk", "in").id
    assertEquals(rt.runWorkflowInstance(scopedWf, scopedId), WorkflowRunResult.WorkflowSuspended)
    assertEquals(
      rt.listSteps(scopedId, stepId"orders/charge"),
      Vector(StepId("charge", ScopePath("orders"))),
      "the step id carries the step's own scope path"
    )

    assertEquals(rt.listSteps(id, stepId"/zzz"), Vector.empty, "a non-matching selector lists nothing")
    intercept[WorkflowNotFoundException] {
      rt.listSteps(WorkflowInstanceId(wf.id, "missing"), stepId"a")
    }
  }
}
