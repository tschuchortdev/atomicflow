package test

import atomicflow.WorkflowRuntime
import munit.*
import atomicflow.{*, given}
import upickle.default.given
import Cacheable.MsgPack.given

import java.util.concurrent.atomic.AtomicInteger
import scala.util.Random

abstract class WorkflowRuntimeSuite extends FunSuite {
  def createWorkflowRuntime: WorkflowRuntime
  
  private def randomInstanceId(): String =
    Random.between(0,99999).toString

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
    given WorkflowRuntime = createWorkflowRuntime
    intercept[WorkflowNotFoundException] {
      emptyWorkflow.newInstance(randomInstanceId()).recover()
    }
  }

  test("Empty workflow should run") {
    given WorkflowRuntime = createWorkflowRuntime
    assertEquals(emptyWorkflow.newInstance(randomInstanceId()).run("answer"), 42)
  }

  test("Locked empty workflow should fail with WorkflowLockedException") {
    given WorkflowRuntime = createWorkflowRuntime
    val instanceId = randomInstanceId()
    lazy val workflow: Workflow[Unit, Any] = Workflow[Unit, Any](
      WorkflowId("d9ab1884-6e83-48d7-82c4-cbc89fb32ffc"),
      name = "recursive workflow"
    ) { _ =>
      intercept[WorkflowLockedException] {
        workflow.newInstance(instanceId).recover()
      }
    }
    workflow.newInstance(instanceId).run(())
  }

  test("Empty workflow should fail with WorkflowInputConflictException if its inputs change") {
    given WorkflowRuntime = createWorkflowRuntime
    val workflowInstanceId = randomInstanceId()
    assertEquals(emptyWorkflow.newInstance(workflowInstanceId).run("answer"), 42)
    intercept[WorkflowInputConflictException] {
      emptyWorkflow.newInstance(workflowInstanceId).run("hello")
    }
  }

  test("Workflow with step should run") {
    given WorkflowRuntime = createWorkflowRuntime
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

    val workflowInstanceId = randomInstanceId()

    assertEquals(workflow.newInstance(workflowInstanceId).run("answer"), 42)

    assertEquals(workflow.newInstance(workflowInstanceId).run("answer"), 42)
  }

  test("Workflow with cached step should run") {
    given WorkflowRuntime = createWorkflowRuntime
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

    val workflowInstanceId = randomInstanceId()

    assertEquals(workflow.newInstance(workflowInstanceId).run("answer"), 42)

    assertEquals(workflow.newInstance(workflowInstanceId).run("answer"), 42)

    assertEquals(workflow.newInstance(randomInstanceId()).run("answer"), 43)
  }

  test("Workflow with cached step and changed inputs should run") {
    given WorkflowRuntime = createWorkflowRuntime
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

    val workflowInstanceId = randomInstanceId()

    assertEquals(workflow.newInstance(workflowInstanceId).run("answer"), 42)

    assertEquals(workflow.newInstance(workflowInstanceId).run("answer"), 43)

    assertEquals(workflow.newInstance(randomInstanceId()).run("answer"), 44)
  }

  test("Workflow with once step should run") {
    given WorkflowRuntime = createWorkflowRuntime
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

    val workflowInstanceId = randomInstanceId()

    assertEquals(workflow.newInstance(workflowInstanceId).run("answer"), 42)

    assertEquals(workflow.newInstance(workflowInstanceId).run("answer"), 42)

    assertEquals(workflow.newInstance(randomInstanceId()).run("answer"), 43)
  }

  test("Workflow with once step and changed inputs should run") {
    given WorkflowRuntime = createWorkflowRuntime
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

    val workflowInstanceId = randomInstanceId()

    assertEquals(workflow.newInstance(workflowInstanceId).run("answer"), 42)

    answer.incrementAndGet()

    intercept[StepInputConflictException] {
      workflow.newInstance(workflowInstanceId).run("answer")
    }

    assertEquals(
      workflow.newInstance(workflowInstanceId)
        .overrideStepIdempotencyId(
          StepId("d27142b9-e7db-4b8e-b341-6dd3009655c7"),
          StepIdempotencyId.generate
        )
        .run("answer"),
      43
    )

    assertEquals(workflow.newInstance(workflowInstanceId).run("answer"), 43)

    answer.incrementAndGet()

    assertEquals(workflow.newInstance(randomInstanceId()).run("answer"), 44)
  }

  test("Signals can be set but not to a different value") {
    given WorkflowRuntime = createWorkflowRuntime
    val signal = Signal[String](SignalId("fbf1760e-c3d4-4635-a84b-8426f2d521ef"))

    val workflow = Workflow[Unit, String](
      WorkflowId("9196475c-8973-47d1-be37-dbb080c943b9"),
      name = "workflow with signal"
    ) { _ =>
      val value = signal.valueOrThrow
      value
    }

    val workflowInstanceId = randomInstanceId()

    intercept[SignalEmptyException] {
      workflow.newInstance(workflowInstanceId).run()
    }

    workflow.newInstance(workflowInstanceId).setSignal(signal, "test")

    assertEquals(workflow.newInstance(workflowInstanceId).run(), "test")

    workflow.newInstance(workflowInstanceId).setSignal(signal, "test")

    intercept[SignalConflictException] {
      workflow.newInstance(workflowInstanceId).setSignal(signal, "test2")
    }
  }

  test("Signals must be set on existing workflows") {
    given WorkflowRuntime = createWorkflowRuntime
    val signal = Signal[String](SignalId("a6d6993a-1d27-43a2-b254-b53d164e2de3"))

    val workflow = Workflow[Unit, String](
      WorkflowId("b62a9a63-b6c6-4c05-bbf4-e994ee195437"),
      name = "workflow with signal"
    ) { _ =>
      val value = signal.valueOrThrow
      value
    }

    val workflowInstanceId = randomInstanceId()

    intercept[WorkflowNotFoundException] {
      workflow.newInstance(workflowInstanceId).setSignal(signal, "test")
    }

    workflow.newInstance(workflowInstanceId).create()

    workflow.newInstance(workflowInstanceId).setSignal(signal, "test")
  }
}
