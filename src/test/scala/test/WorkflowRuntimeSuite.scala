package test

import atomicflow.WorkflowRuntime
import munit.*
import atomicflow.{*, given}
import upickle.default.given
import Cacheable.MsgPack.given

abstract class WorkflowRuntimeSuite extends FunSuite {
  def createWorkflowRuntime: WorkflowRuntime

  given WorkflowRuntime = createWorkflowRuntime

  private lazy val emptyWorkflow = Workflow[String, Int](WorkflowId("d9ab1884-6e83-48d7-82c4-cbc89fb32ffc"), name = "read and send files") { (string: String) =>
    if (string == "answer")
      42
    else
      0
  }

  private lazy val emptyWorkflowInstance = emptyWorkflow.instance(WorkflowInstanceId.generate)

  test("Unknown workflow should fail with WorkflowNotFoundException") {
    intercept[WorkflowNotFoundException] {
      emptyWorkflow.instance(WorkflowInstanceId.generate).recover()
    }
  }

  test("Workflow should run") {
    assertEquals(emptyWorkflow.instance(WorkflowInstanceId.generate).run("answer"), 42)
  }

  test("Locked workflow should fail with WorkflowLockedException") {
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

  test("Workflow should fail with WorkflowInputConflictException if its inputs change") {
    val workflowInstanceId = WorkflowInstanceId.generate
    assertEquals(emptyWorkflow.instance(workflowInstanceId).run("answer"), 42)
    intercept[WorkflowInputConflictException] {
      emptyWorkflow.instance(workflowInstanceId).run("hello")
    }
  }
}
