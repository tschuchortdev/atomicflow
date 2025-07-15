package test

import atomicflow.WorkflowRuntime
import munit.*
import atomicflow.{*, given}
import upickle.default.given
import Cacheable.MsgPack.given

import java.util.concurrent.atomic.AtomicInteger

abstract class WorkflowRuntimeSuite extends FunSuite {
  def createWorkflowRuntime: WorkflowRuntime

  given WorkflowRuntime = createWorkflowRuntime

  private lazy val emptyWorkflow = Workflow[String, Int](
    WorkflowId("d9ab1884-6e83-48d7-82c4-cbc89fb32ffc"),
    name = "read and send files"
  ) { (string: String) =>
    if (string == "answer")
      42
    else
      0
  }

  test("Unknown empty workflow should fail with WorkflowNotFoundException") {
    intercept[WorkflowNotFoundException] {
      emptyWorkflow.instance(WorkflowInstanceId.generate).recover()
    }
  }

  test("Empty workflow should run") {
    assertEquals(emptyWorkflow.instance(WorkflowInstanceId.generate).run("answer"), 42)
  }

  test("Locked empty workflow should fail with WorkflowLockedException") {
    val instanceId = WorkflowInstanceId.generate
    lazy val workflow: Workflow[Unit, Any] = Workflow[Unit, Any](
      WorkflowId("d9ab1884-6e83-48d7-82c4-cbc89fb32ffc"),
      name = "recursive workflow"
    ) { _ =>
      intercept[WorkflowLockedException] {
        workflow.instance(instanceId).recover()
      }
    }
    workflow.instance(instanceId).run(())
  }

  test("Empty workflow should fail with WorkflowInputConflictException if its inputs change") {
    val workflowInstanceId = WorkflowInstanceId.generate
    assertEquals(emptyWorkflow.instance(workflowInstanceId).run("answer"), 42)
    intercept[WorkflowInputConflictException] {
      emptyWorkflow.instance(workflowInstanceId).run("hello")
    }
  }

  test("Workflow with step should run") {
    val workflow = Workflow[String, Int](
      WorkflowId("15f8bf6d-a719-4958-b790-b3eb846340c1"),
      name = "workflow with steps"
    ) { (string: String) =>
      Step(StepId("d27142b9-e7db-4b8e-b341-6dd3009655c7"), 0) {
        if (string == "answer")
          42
        else
          0
      }
    }

    val workflowInstanceId = WorkflowInstanceId.generate

    assertEquals(workflow.instance(workflowInstanceId).run("answer"), 42)

    assertEquals(workflow.instance(workflowInstanceId).run("answer"), 42)
  }

  test("Workflow with cached step should run") {
    val answer = AtomicInteger(42)

    val workflow = Workflow[String, Int](
      WorkflowId("15f8bf6d-a719-4958-b790-b3eb846340c1"),
      name = "workflow with cached steps"
    ) { (string: String) =>
      Step(StepId("d27142b9-e7db-4b8e-b341-6dd3009655c7"), 0) {
        Step.cache(
          "input" -> string
        )

        if (string == "answer")
          answer.getAndIncrement()
        else
          0
      }
    }

    val workflowInstanceId = WorkflowInstanceId.generate

    assertEquals(workflow.instance(workflowInstanceId).run("answer"), 42)

    assertEquals(workflow.instance(workflowInstanceId).run("answer"), 42)

    assertEquals(workflow.instance(WorkflowInstanceId.generate).run("answer"), 43)
  }

  test("Workflow with cached step and changed inputs should run") {
    val answer = AtomicInteger(42)

    val workflow = Workflow[String, Int](
      WorkflowId("330be340-1958-427d-8baf-0f7562d53a97"),
      name = "workflow with cached steps, changed inputs"
    ) { (string: String) =>
      val a = Step(StepId("0bc4b603-81ec-4af2-a6c5-700df0084243"), 0) {
        answer.getAndIncrement()
      }

      Step(StepId("d27142b9-e7db-4b8e-b341-6dd3009655c7"), 0) {
        Step.cache(
          "input" -> string,
          "a" -> a
        )

        if (string == "answer")
          a
        else
          0
      }
    }

    val workflowInstanceId = WorkflowInstanceId.generate

    assertEquals(workflow.instance(workflowInstanceId).run("answer"), 42)

    assertEquals(workflow.instance(workflowInstanceId).run("answer"), 43)

    assertEquals(workflow.instance(WorkflowInstanceId.generate).run("answer"), 44)
  }

  test("Workflow with once step should run") {
    val answer = AtomicInteger(42)

    val workflow = Workflow[String, Int](
      WorkflowId("72b29e07-46ad-4b95-b591-cea2a4dbffce"),
      name = "workflow with once steps"
    ) { (string: String) =>
      Step(StepId("d27142b9-e7db-4b8e-b341-6dd3009655c7"), 0) {
        Step.onlyOnce(
          "input" -> string
        )

        if (string == "answer")
          answer.getAndIncrement()
        else
          0
      }
    }

    val workflowInstanceId = WorkflowInstanceId.generate

    assertEquals(workflow.instance(workflowInstanceId).run("answer"), 42)

    assertEquals(workflow.instance(workflowInstanceId).run("answer"), 42)

    assertEquals(workflow.instance(WorkflowInstanceId.generate).run("answer"), 43)
  }

  test("Workflow with once step and changed inputs should run") {
    val answer = AtomicInteger(42)

    val workflow = Workflow[String, Int](
      WorkflowId("a8d67a06-d4e2-4d20-8088-002fca20f789"),
      name = "workflow with once steps, changed inputs"
    ) { (string: String) =>
      val a = Step(StepId("0bc4b603-81ec-4af2-a6c5-700df0084243"), 0) {
        answer.get()
      }

      Step(StepId("d27142b9-e7db-4b8e-b341-6dd3009655c7"), 0) {
        Step.onlyOnce(
          "input" -> string,
          "a" -> a
        )

        if (string == "answer")
          a
        else
          0
      }
    }

    val workflowInstanceId = WorkflowInstanceId.generate

    assertEquals(workflow.instance(workflowInstanceId).run("answer"), 42)

    answer.incrementAndGet()

    intercept[StepInputConflictException] {
      workflow.instance(workflowInstanceId).run("answer")
    }

    assertEquals(
      workflow.instance(workflowInstanceId)
        .overrideStepIdempotencyId(
          StepId("d27142b9-e7db-4b8e-b341-6dd3009655c7"),
          StepIdempotencyId.generate
        )
        .run("answer"),
      43
    )

    assertEquals(workflow.instance(workflowInstanceId).run("answer"), 43)

    answer.incrementAndGet()

    assertEquals(workflow.instance(WorkflowInstanceId.generate).run("answer"), 44)
  }

  test("Signals") {
    val signal = Signal[String](SignalId("fbf1760e-c3d4-4635-a84b-8426f2d521ef"))

    val workflow = Workflow[String, String](
      WorkflowId("9196475c-8973-47d1-be37-dbb080c943b9"),
      name = "workflow with signal"
    ) { (string: String) =>
      val value = signal.value
      value
    }

    val workflowInstanceId = WorkflowInstanceId.generate

    intercept[SignalEmptyException] {
      workflow.instance(workflowInstanceId).run("answer")
    }

    workflow.instance(workflowInstanceId).setSignal(signal, "test")

    assertEquals(workflow.instance(workflowInstanceId).run("answer"), "test")

    workflow.instance(workflowInstanceId).setSignal(signal, "test")

    intercept[SignalConflictException] {
      workflow.instance(workflowInstanceId).setSignal(signal, "test2")
    }
  }
}
