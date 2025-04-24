package test

import atomicflow.*
import io.github.iltotore.iron.*

import java.nio.file.{Files, Paths}
import atomicflow.given
import atomicflow.impl.memory.InMemoryWorkflowRuntime

import scala.util.Random

object Test3 {
  def readFile(fileName: String): Array[Byte] = Array.empty // Files.readAllBytes(Paths.get(fileName))

  def sendFile(bytes: Array[Byte], receiver: String): Unit = ()

  private val sendFileStepId = StepId("f4a18269-83a8-4fcf-a62b-3cbb6216ddee")

  val flow1: Workflow[String, Unit] = Workflow(id = "99a2866c-99c5-49b7-b0f5-ad097a3e3a78", name = "read and send files") { (fileName: String) =>
    val fileName = ""

    val fileBytes = Step(id = "533dddc7-d355-43b4-81d8-bd8051808ec5", version = 0, name = "read file") {
      val random = Random.nextInt()
      Step.cache(
        "fileName" -> fileName,
        "random" -> random
      )

      println("reading file")

      readFile(fileName)
    }

    Step(id = sendFileStepId, version = 0, name = "send file") {
      Step.onlyOnce(
        "fileBytes" -> fileBytes
      )

      /*Step.compensate {
        println("revoke sent file")
      }*/

      println(s"sending file")

      sendFile(fileBytes, "receiver")
    }
  }

  def runFlow1(in: String)(using WorkflowRuntime): Unit = {
    val instance = flow1.instance(WorkflowInstanceId.generate)
    //.overrideStepIdempotencyId(sendFileStepId, StepIdempotencyId.generate)
    instance.run(in)
    instance.recover()
  }

  def main(args: Array[String]): Unit = {
    given WorkflowRuntime = new InMemoryWorkflowRuntime()

    runFlow1("hello world")
  }
}
