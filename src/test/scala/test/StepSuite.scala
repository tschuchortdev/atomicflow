package test

import atomicflow.WorkflowRuntime
import atomicflow.impl.memory.InMemoryWorkflowRuntime
import munit.*
import atomicflow.{*, given}
import upickle.default.given 
import Cacheable.MsgPack.given

class StepSuite extends FunSuite {
  given WorkflowRuntime = new InMemoryWorkflowRuntime()

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

  test("Workflow should fail with WorkflowConflictException if its inputs change") {
    val workflowInstanceId = WorkflowInstanceId.generate
    assertEquals(emptyWorkflow.instance(workflowInstanceId).run("answer"), 42)
    intercept[WorkflowInputConflictException] {
      emptyWorkflow.instance(workflowInstanceId).run("hello")
    }
  }
}
