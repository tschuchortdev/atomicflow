package test

import atomicflow.*
import atomicflow.Cacheable.Simple.given
import doobie.implicits.*
import doobie.postgres.implicits.*

import java.util.concurrent.atomic.AtomicInteger

class TerminalOutcomeSuite extends PostgresWorkflowRuntimeSuite {

  given Cacheable[Throwable] = Cacheable.forThrowable.genericStringMessageSerializer

  private def setTerminal(
      workflowId: WorkflowId,
      key: WorkflowInstanceKey,
      state: String,
      outcome: Option[String]
  ): Unit =
    run(
      sql"""
        UPDATE workflow_instances SET terminal_state = $state, terminal_outcome = $outcome
        WHERE workflow_id = $workflowId AND workflow_instance_key = $key AND workflow_instance_scope = '{}'
      """.update.run
    )

  private def terminalOutcome(workflowId: WorkflowId, key: WorkflowInstanceKey): Option[String] =
    run(
      sql"""
        SELECT terminal_outcome FROM workflow_instances
        WHERE workflow_id = $workflowId AND workflow_instance_key = $key AND workflow_instance_scope = '{}'
      """.query[Option[String]].unique
    )

  private def completedWorkflow(id: WorkflowId, runs: AtomicInteger): Workflow[String, String] =
    Workflow[String, String](id) { in =>
      runs.incrementAndGet()
      s"out-$in"
    }

  test("re-running a completed instance returns its outcome without executing the body") {
    val rt = newRuntime
    val runs = new AtomicInteger(0)
    val wf = completedWorkflow("term-rerun", runs)
    assertEquals(rt.createAndRun(wf, "k", "a"), WorkflowRunResult.Result("out-a"))

    assertEquals(
      rt.runWorkflowInstance(wf, WorkflowInstanceId(wf.id, "k")),
      WorkflowRunResult.Result("out-a")
    )
    assertEquals(runs.get(), 1, "the body must not execute again")
  }

  test("a terminal instance with no persisted outcome is rejected") {
    val rt = newRuntime
    val runs = new AtomicInteger(0)
    val wf = completedWorkflow("term-no-outcome", runs)
    rt.createAndRun(wf, "k", "a")
    setTerminal(wf.id, "k", "completed", None)

    val e = intercept[StepSerializationFailed] {
      rt.runWorkflowInstance(wf, WorkflowInstanceId(wf.id, "k"))
    }
    assert(e.getMessage.contains("has no terminal outcome"), e.getMessage)
  }

  test("a corrupt persisted terminal outcome is rejected") {
    val rt = newRuntime
    val runs = new AtomicInteger(0)
    val wf = completedWorkflow("term-corrupt", runs)
    rt.createAndRun(wf, "k", "a")
    setTerminal(wf.id, "k", "completed", Some("not-a-valid-completion-payload"))

    val e = intercept[StepSerializationFailed] {
      rt.runWorkflowInstance(wf, WorkflowInstanceId(wf.id, "k"))
    }
    assert(e.getMessage.contains("could not be decoded"), e.getMessage)
  }

  test("a completed instance whose outcome encodes a failure is rejected") {
    val rt = newRuntime
    val runs = new AtomicInteger(0)
    val completed = completedWorkflow("term-mismatch-completed", runs)
    rt.createAndRun(completed, "k", "a")

    val failing = Workflow[String, String](id = "term-mismatch-failure") { _ =>
      throw new RuntimeException("boom")
    }
    intercept[RuntimeException] {
      rt.createAndRun(failing, "f", "x")
    }

    setTerminal(completed.id, "k", "completed", terminalOutcome(failing.id, "f"))
    val e = intercept[StepSerializationFailed] {
      rt.runWorkflowInstance(completed, WorkflowInstanceId(completed.id, "k"))
    }
    assert(e.getMessage.contains("stored a non-completed outcome as completed"), e.getMessage)
  }

  test("a failed instance whose outcome encodes a completion is rejected") {
    val rt = newRuntime
    val runs = new AtomicInteger(0)
    val completed = completedWorkflow("term-mismatch-ok", runs)
    rt.createAndRun(completed, "k", "a")

    val failing = Workflow[String, String](id = "term-mismatch-failed") { _ =>
      throw new RuntimeException("boom")
    }
    intercept[RuntimeException] {
      rt.createAndRun(failing, "f", "x")
    }

    setTerminal(failing.id, "f", "failed", terminalOutcome(completed.id, "k"))
    val e = intercept[StepSerializationFailed] {
      rt.runWorkflowInstance(failing, WorkflowInstanceId(failing.id, "f"))
    }
    assert(e.getMessage.contains("stored a non-failed outcome as failed"), e.getMessage)
  }

  test("an unknown persisted terminal state is rejected") {
    val rt = newRuntime
    val runs = new AtomicInteger(0)
    val wf = completedWorkflow("term-unknown-state", runs)
    rt.createAndRun(wf, "k", "a")
    setTerminal(wf.id, "k", "bogus", terminalOutcome(wf.id, "k"))

    val e = intercept[StepSerializationFailed] {
      rt.runWorkflowInstance(wf, WorkflowInstanceId(wf.id, "k"))
    }
    assert(e.getMessage.contains("Unknown terminal state 'bogus'"), e.getMessage)
  }

  test("getWorkflowInstanceInfo on a missing instance raises WorkflowNotFoundException") {
    val rt = newRuntime
    val wf = completedWorkflow("term-missing", new AtomicInteger(0))
    val handle = rt.getWorkflowInstance(wf, WorkflowInstanceId(wf.id, "missing"))

    intercept[WorkflowNotFoundException] {
      handle.getInfo()(using rt)
    }
  }
}
