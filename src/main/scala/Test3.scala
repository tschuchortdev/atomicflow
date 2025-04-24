import Step.{StepId, StepIdempotencyId}
import Workflow.WorkflowInstanceId
import io.github.iltotore.iron.*
import io.github.iltotore.iron.constraint.string.*

import java.nio.file.{Files, Paths}

class Test3 {
  def sendFile(bytes: Array[Byte], receiver: String): Unit = ???

  val sendFileStepId = StepId("f4a18269-83a8-4fcf-a62b-3cbb6216ddee")

  val flow1: Workflow[String, Unit] = Workflow(id = "99a2866c-99c5-49b7-b0f5-ad097a3e3a78", name = "read and send files") { (fileName: String) =>
    val fileName = ""

    val fileBytes = Step(id = "533dddc7-d355-43b4-81d8-bd8051808ec5", version = 0, name = "read file") {
      Step.cache(
        "fileName" -> fileName
      )

      Files.readAllBytes(Paths.get(fileName))
    }

    Step(id = sendFileStepId, version = 0, name = "send file") {
      Step.onlyOnce(
        "fileBytes" -> fileBytes
      )

      Step.compensate {
        println("revoke sent file")
      }

      sendFile(fileBytes, "receiver")
    }
  }

  def runFlow1(in: String)(using WorkflowRuntime): Unit = {
    flow1.instance(WorkflowInstanceId.generate)
      .overrideStepIdempotencyId(sendFileStepId, StepIdempotencyId.generate)
      .run(in)
  }

}
