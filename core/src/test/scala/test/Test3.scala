package test

import atomicflow.*

import java.nio.file.{Files, Path, Paths}
import atomicflow.given
import atomicflow.impl.memory.InMemoryWorkflowRuntime
import ox.*

import cats.syntax.all.*
import scala.annotation.tailrec
import scala.util.Random
import scala.concurrent.duration.*
import scala.util.control.NonFatal
import Cacheable.MsgPack.given

object Test3 {
  def readFile(fileName: String): Array[Byte] = s"hello world ${fileName}".getBytes // Files.readAllBytes(Paths.get(fileName))

  def sendFile(bytes: Array[Byte], receiver: String): Unit = ()

  private val sendFileStepId = StepId("f4a18269-83a8-4fcf-a62b-3cbb6216ddee")

  val flow1: Workflow[String, Unit] = Workflow(WorkflowId("99a2866c-99c5-49b7-b0f5-ad097a3e3a78"), name = "read and send files") { (fileName: String) =>
    val fileBytes: Array[Byte] = Step(StepId("533dddc7-d355-43b4-81d8-bd8051808ec5"), version = 0, name = "read file") {
      val random = Random.nextInt()
      Step.cache(
        "fileName" -> fileName,
        "random" -> random
      )

      println("reading file")

      readFile(fileName)
    }

    val f: Path = Step(id = sendFileStepId, version = 0, name = "send file") {
      Step.onlyOnce(
        "fileBytes" -> fileBytes
      )

      /*Step.compensate {
        println("revoke sent file")
      }*/

      println(s"sending file")

      //sendFile(fileBytes, "receiver")

      val tmpFile = Files.createTempFile("atomicflow-test", "")

      Files.write(tmpFile, fileBytes)

      tmpFile
    }

    val string: String = Step(StepId("9e94a750-59ba-4400-bbde-7cc25a333646"), version = 0, name = "receive file file") {
      val file2 = f.resolveSibling(f.getFileName.toString + "-")

      Files.readString(file2)
    }

    println(string)
  }

  def runFlow1(in: String)(using WorkflowRuntime): Unit = {
    val instance = flow1.instance(WorkflowInstanceId.generate)
    //.overrideStepIdempotencyId(sendFileStepId, StepIdempotencyId.generate)
    instance.create(in)

    @tailrec
    def retry: Unit =
      try instance.recover()
      catch {
        case NonFatal(e) =>
          e.printStackTrace()
          sleep(5.seconds)
          retry
      }

    retry
  }

  def main(args: Array[String]): Unit = {
    given WorkflowRuntime = new InMemoryWorkflowRuntime()

    runFlow1("hello world")
  }
}
