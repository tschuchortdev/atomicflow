package test

import atomicflow.*
import atomicflow.Cacheable.Simple.given
import doobie.implicits.*
import doobie.postgres.implicits.*

import java.util.concurrent.TimeoutException
import java.util.concurrent.atomic.AtomicInteger
import scala.concurrent.duration.*

class QueriesSuite extends PostgresWorkflowRuntimeSuite {

  given Cacheable[Throwable] = Cacheable.forThrowable.genericStringMessageSerializer

  private def wf(id: WorkflowId, version: Long = 1L): Workflow[String, String] =
    Workflow[String, String](id = id, version = version)(in => s"out-$in")

  test("getWorkflowInstance returns a type-safe handle carrying the id and workflow") {
    val rt = newRuntime
    val w = wf("wf-instance")
    rt.createWorkflowInstance(w, "k1", "a")
    val h = rt.getWorkflowInstance(w, WorkflowInstanceId(w.id, "k1"))
    assertEquals(h.id, WorkflowInstanceId(w.id, "k1"))
    assertEquals(h.workflow, w)
  }

  test("getWorkflowInstance throws IllegalArgumentException on workflowId mismatch") {
    val rt = newRuntime
    val w = wf("wf-instance-2")
    val other = wf("other")
    rt.createWorkflowInstance(w, "k1", "a")
    intercept[IllegalArgumentException] {
      rt.getWorkflowInstance(other, WorkflowInstanceId(w.id, "k1"))
    }
  }

  test("getInfo returns populated Info for a created (never-run) instance") {
    val rt = newRuntime
    given WorkflowRuntime = rt
    val w = wf("wf-info", version = 7L)
    rt.createWorkflowInstance(w, "k1", "a")
    val info = rt.getWorkflowInstance(w, WorkflowInstanceId(w.id, "k1")).getInfo()
    assertEquals(info.id, WorkflowInstanceId(w.id, "k1"))
    assertEquals(info.parentId, None)
    assertEquals(info.generation, 0L)
    assertEquals(info.terminalState, None)
    assertEquals(info.workflowVersionAtCreation, 7L)
    assertEquals(info.timesExecuted, 0)
    assertEquals(info.lastRunAt, None)
  }

  test("getInfo reflects terminal state, run count, and lastRunAt after completion") {
    val rt = newRuntime
    given WorkflowRuntime = rt
    val w = wf("wf-info-terminal")
    rt.createAndRun(w, "k", "a")
    val info = rt.getWorkflowInstance(w, WorkflowInstanceId(w.id, "k")).getInfo()
    assertEquals(info.terminalState, Some(WorkflowTerminalState.Completed))
    assertEquals(info.timesExecuted, 1)
    assert(info.lastRunAt.isDefined)
  }

  test("getWorkflowInstancesByPrefix filters by key prefix and is scoped to top-level") {
    val rt = newRuntime
    val w = wf("wf-prefix")
    rt.createWorkflowInstance(w, "alpha-1", "a")
    rt.createWorkflowInstance(w, "alpha-2", "b")
    rt.createWorkflowInstance(w, "beta-1", "c")
    run(sql"""INSERT INTO workflow_instances (workflow_id, workflow_instance_key, scope, input, workflow_version_at_creation)
             VALUES (${w.id}, 'alpha-3', 'child', 'x', 1)""".update.run)

    val res = rt.getWorkflowInstancesByPrefix(w.id, "alpha")
    assertEquals(res.map(_.id.workflowInstanceKey), Vector("alpha-1", "alpha-2"))
  }

  test("getWorkflowInstancesByPrefix treats % and _ in the prefix as literals") {
    val rt = newRuntime
    val w = wf("wf-escape")
    rt.createWorkflowInstance(w, "a_1", "a")
    rt.createWorkflowInstance(w, "a1", "b")
    rt.createWorkflowInstance(w, "a%1", "c")
    rt.createWorkflowInstance(w, "ax1", "d")

    val underscore = rt.getWorkflowInstancesByPrefix(w.id, "a_")
    assertEquals(underscore.map(_.id.workflowInstanceKey), Vector("a_1"))

    val percent = rt.getWorkflowInstancesByPrefix(w.id, "a%")
    assertEquals(percent.map(_.id.workflowInstanceKey), Vector("a%1"))
  }

  test("getUnfinishedWorkflowInstances default excludes started instances; includeWaiting includes them") {
    val rt = newRuntime
    val w = wf("wf-unfinished")
    rt.createWorkflowInstance(w, "waiting-1", "a")
    rt.createWorkflowInstance(w, "waiting-2", "b")
    rt.createAndRun(w, "done", "c")
    run(sql"""INSERT INTO workflow_instances (workflow_id, workflow_instance_key, scope, input, workflow_version_at_creation, times_executed)
             VALUES (${w.id}, 'started', '', 'x', 1, 1)""".update.run)

    val waiting = rt.getUnfinishedWorkflowInstances(w.id)
    assertEquals(waiting.map(_.id.workflowInstanceKey).toSet, Set("waiting-1", "waiting-2"))

    val all = rt.getUnfinishedWorkflowInstances(w.id, includeWaiting = true)
    assertEquals(all.map(_.id.workflowInstanceKey).toSet, Set("waiting-1", "waiting-2", "started"))
  }

  test("getUnfinishedWorkflowInstances respects limit") {
    val rt = newRuntime
    val w = wf("wf-limit")
    rt.createWorkflowInstance(w, "k1", "a")
    rt.createWorkflowInstance(w, "k2", "b")
    rt.createWorkflowInstance(w, "k3", "c")
    val res = rt.getUnfinishedWorkflowInstances(w.id, limit = 2)
    assertEquals(res.size, 2)
  }

  test("deleteWorkflowInstancesByPrefix deletes instances, steps, cursor, and events; returns count; scoped") {
    val rt = newRuntime
    val w = wf("wf-delete")
    rt.createAndRun(w, "x-1", "a")
    rt.createWorkflowInstance(w, "x-2", "b")
    rt.createWorkflowInstance(w, "y-1", "c")
    run(sql"""INSERT INTO workflow_steps (workflow_id, workflow_instance_key, scope, step_id, step_version, step_kind, state_kind, state_payload, input_fingerprints)
             VALUES (${w.id}, 'x-2', '', 's1', 1, 'k', 'Started', '', '')""".update.run)
    run(sql"""INSERT INTO signal_cursor (workflow_id, workflow_instance_key, scope, signal_key, sequence_id)
             VALUES (${w.id}, 'x-2', '', 'sig', 1)""".update.run)

    val deleted = rt.deleteWorkflowInstancesByPrefix(w.id, "x-")
    assertEquals(deleted, 2L)

    assertEquals(
      run(sql"SELECT count(*) FROM workflow_instances WHERE workflow_id = ${w.id}".query[Int].unique),
      1
    )
    assertEquals(
      run(sql"SELECT count(*) FROM workflow_instances WHERE workflow_id = ${w.id} AND workflow_instance_key = 'y-1'".query[Int].unique),
      1
    )
    assertEquals(run(sql"SELECT count(*) FROM workflow_steps WHERE workflow_id = ${w.id}".query[Int].unique), 0)
    assertEquals(run(sql"SELECT count(*) FROM signal_cursor WHERE workflow_id = ${w.id}".query[Int].unique), 0)
    assertEquals(run(sql"SELECT count(*) FROM workflow_events WHERE workflow_id = ${w.id}".query[Int].unique), 0)
  }

  test("awaitResult returns immediately for a terminal completed instance") {
    val rt = newRuntime
    val w = wf("wf-await-done")
    rt.createAndRun(w, "k", "a")
    val h = rt.getWorkflowInstance(w, WorkflowInstanceId(w.id, "k"))
    assertEquals(rt.awaitResult(h, 5.seconds), WorkflowRunResult.Result("out-a"))
  }

  test("awaitResult throws the decoded failure for a terminal failed instance") {
    val rt = newRuntime
    val failing = Workflow[String, String](id = "wf-await-fail")(in => throw new RuntimeException("boom"))
    val original = intercept[StepFailed] {
      rt.createAndRun(failing, "k", "a")
    }
    val h = rt.getWorkflowInstance(failing, WorkflowInstanceId(failing.id, "k"))
    val second = intercept[StepFailed] {
      rt.awaitResult(h, 5.seconds)
    }
    assertEquals(second.getMessage, original.getMessage)
  }

  test("awaitResult returns WorkflowCancelled for a cancelled terminal instance") {
    val rt = newRuntime
    val w = wf("wf-await-cancel")
    rt.createWorkflowInstance(w, "k", "a")
    run(sql"""UPDATE workflow_instances SET terminal_state = 'cancelled',
             terminal_outcome = ${Cacheable[WorkflowCompletionResult[String]].write(WorkflowCompletionResult.Cancelled)}
             WHERE workflow_id = ${w.id} AND workflow_instance_key = 'k' AND scope = ''""".update.run)
    val h = rt.getWorkflowInstance(w, WorkflowInstanceId(w.id, "k"))
    assertEquals(rt.awaitResult(h, 5.seconds), WorkflowRunResult.WorkflowCancelled)
  }

  test("awaitResult on a non-terminal instance times out and never executes the body") {
    val rt = newRuntime
    val counter = new AtomicInteger(0)
    val w = Workflow[String, String](id = "wf-await-timeout") { in =>
      counter.incrementAndGet()
      s"out-$in"
    }
    rt.createWorkflowInstance(w, "k", "a")
    given WorkflowRuntime = rt
    val h = rt.getWorkflowInstance(w, WorkflowInstanceId(w.id, "k"))
    intercept[TimeoutException] {
      rt.awaitResult(h, 300.millis)
    }
    assertEquals(counter.get(), 0)
    assertEquals(h.getInfo().terminalState, None)
  }

  test("awaitResult with a zero timeout still performs one immediate poll, so a terminal instance is observed") {
    val rt = newRuntime
    val w = wf("wf-await-zero-terminal")
    rt.createAndRun(w, "k", "a")
    val h = rt.getWorkflowInstance(w, WorkflowInstanceId(w.id, "k"))
    assertEquals(rt.awaitResult(h, 0.millis), WorkflowRunResult.Result("out-a"))
  }

  test("awaitResult on a non-terminal instance with zero timeout times out without executing the body") {
    val rt = newRuntime
    val counter = new AtomicInteger(0)
    val w = Workflow[String, String](id = "wf-await-zero-nonterminal") { in =>
      counter.incrementAndGet()
      s"out-$in"
    }
    rt.createWorkflowInstance(w, "k", "a")
    given WorkflowRuntime = rt
    val h = rt.getWorkflowInstance(w, WorkflowInstanceId(w.id, "k"))
    intercept[TimeoutException] {
      rt.awaitResult(h, 0.millis)
    }
    assertEquals(counter.get(), 0, "a zero-timeout await must not execute the body")
    assertEquals(h.getInfo().terminalState, None)
  }

  test("awaitResult on a missing instance throws WorkflowNotFoundException on the first poll") {
    val rt = newRuntime
    val w = wf("wf-await-missing")
    rt.createWorkflowInstance(w, "k", "a")
    given WorkflowRuntime = rt
    val h = rt.getWorkflowInstance(w, WorkflowInstanceId(w.id, "k"))
    run(sql"""DELETE FROM workflow_instances WHERE workflow_id = ${w.id} AND workflow_instance_key = 'k' AND scope = ''""".update.run)
    intercept[WorkflowNotFoundException] {
      rt.awaitResult(h, 5.seconds)
    }
  }

  test("instance.awaitResult and Workflow.awaitResult forward to the runtime") {
    val rt = newRuntime
    val w = wf("wf-forwarders")
    rt.createAndRun(w, "k", "a")
    val h = rt.getWorkflowInstance(w, WorkflowInstanceId(w.id, "k"))
    given WorkflowRuntime = rt
    assertEquals(h.awaitResult(5.seconds), WorkflowRunResult.Result("out-a"))
    assertEquals(Workflow.awaitResult(w, "k", 5.seconds), WorkflowRunResult.Result("out-a"))
  }
}
