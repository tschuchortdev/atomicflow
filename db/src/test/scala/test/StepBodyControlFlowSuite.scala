package test

import atomicflow.*
import atomicflow.given
import atomicflow.Cacheable.Simple.given
import doobie.implicits.*
import doobie.postgres.implicits.*

/** Pins the propagate-vs-cache boundary for exceptions originating inside a
  * Step body: the runtime's own exceptions (`NotAStepOutcome`) abort the run
  * without durable effect, while ordinary failures are the Step's durable
  * outcome.
  */
class StepBodyControlFlowSuite extends PostgresWorkflowRuntimeSuite {

  given Cacheable[Throwable] = Cacheable.forThrowable.genericStringMessageSerializer

  private final class BoundaryBreak extends scala.util.control.ControlThrowable

  private def stepStateKinds(workflowId: WorkflowId, key: WorkflowInstanceKey): Seq[(String, String)] =
    run(
      sql"""
        SELECT step_id, state_kind FROM workflow_steps
        WHERE workflow_id = $workflowId AND workflow_instance_key = $key AND workflow_instance_scope = '{}'
        ORDER BY step_id
      """.query[(String, String)].to[Seq]
    )

  test("a suspension from Step.await inside a step body suspends the run and is never cached as a step outcome") {
    val rt = newRuntime
    val sig = Signal[String]("resume")
    var executions = 0
    val wf = Workflow[String, String](id = "susp-in-body") { in =>
      Step.atLeastOnce[String]("outer") {
        executions += 1
        Step.await[String]("inner", Awaitable.SignalEvent(sig))
        "unreachable"
      }
    }
    val instance = rt.createWorkflowInstance(wf, "k", "a")

    assertEquals(rt.runWorkflowInstance(wf, instance.id), WorkflowRunResult.WorkflowSuspended)
    assertEquals(executions, 1)
    assertEquals(
      stepStateKinds(wf.id, "k"),
      Seq(("outer", "Started")),
      "the suspended body must leave an unresolved record, not a cached failure"
    )

    instance.sendSignal(sig, "go")(using rt)
    assertEquals(rt.runWorkflowInstance(wf, instance.id), WorkflowRunResult.Result("unreachable"))
    assertEquals(executions, 2, "the at-least-once Started row re-executes the body on resume")
  }

  test("continueAsNew inside a step body restarts the instance and is never cached as a step outcome") {
    val rt = newRuntime
    val wf = Workflow[String, String](id = "can-in-body") { in =>
      if in == "a" then {
        Step.atLeastOnce[String]("restart-step") {
          Workflow.continueAsNew("b")
          "unreachable"
        }
        "unreachable"
      } else s"done-$in"
    }
    val instance = rt.createWorkflowInstance(wf, "k", "a")

    assertEquals(rt.runWorkflowInstance(wf, instance.id), WorkflowRunResult.ContinueAsNew)
    assertEquals(
      stepStateKinds(wf.id, "k"),
      Seq.empty,
      "the continue-as-new transition erases the previous generation's step rows"
    )

    assertEquals(rt.runWorkflowInstance(wf, instance.id), WorkflowRunResult.Result("done-b"))
  }

  test("a user-thrown ControlThrowable inside a step body is an ordinary, cached step failure") {
    val rt = newRuntime
    var executions = 0
    val wf = Workflow[String, String](id = "user-ct-in-body") { in =>
      Step.atLeastOnce[String]("boom") {
        executions += 1
        throw new BoundaryBreak
      }
      "unreachable"
    }
    val instance = rt.createWorkflowInstance(wf, "k", "a")

    // The run boundary replays failures through the throwable codec, so both the
    // initial run and replay expose the decoded, StepFailed-wrapped form.
    val failure = intercept[RuntimeException](rt.runWorkflowInstance(wf, instance.id))
    assert(failure.toString.contains("StepFailed") && failure.toString.contains("BoundaryBreak"))
    assertEquals(executions, 1)
    assertEquals(
      stepStateKinds(wf.id, "k"),
      Seq(("boom", "Failed")),
      "a user-thrown ControlThrowable is not library control flow and is cached as the step's failure"
    )

    intercept[RuntimeException](rt.runWorkflowInstance(wf, instance.id))
    assertEquals(executions, 1, "replay must not re-execute the failed step body")
  }
}
