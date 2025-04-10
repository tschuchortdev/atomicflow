import java.nio.file.{Files, Paths}

class Test3 {
  trait WorkflowCtx

  trait StepCtx[Out]

  trait StepCache

  type ValidUuid = String

  trait Hashable[A] {
    def hash(value: A): String
  }

  given Hashable[String] = ???

  case class StepInput[A: Hashable](name: String, value: A)

  given [A: Hashable] => Conversion[(String, A), StepInput[A]] = { (name: String, value: A) =>
    StepInput(name, value)
  }

  object step {
    def apply[Out](id: ValidUuid, version: Long, name: String | Unit = (), description: String | Unit = ())(body: StepCtx[Out] ?=> Out)(using WorkflowCtx) = ???

    def cached[Out](stepInputs: StepInput[?]*)(using ctx: StepCtx[Out], cache: StepCache): Nothing = {
      ???
    }

    def onlyOnce[Out](stepInputs: StepInput[?]*)(using ctx: StepCtx[Out]): Nothing = {
      ???
    }
  }

  object workflow {
    def apply[Out](id: ValidUuid, name: String, description: String | Unit = ())(body: WorkflowCtx ?=> Out): Out = ???
  }

  def sendFile(bytes: Array[Byte], receiver: String): Unit = ???

  workflow(id = "99a2866c-99c5-49b7-b0f5-ad097a3e3a78", name = "read and send files") {
    val fileName = ""

    val fileBytes = step(id = "533dddc7-d355-43b4-81d8-bd8051808ec5", version = 0, name = "read file") {
      step.cached(
        "fileName" -> fileName
      )

      Files.readAllBytes(Paths.get(fileName))
    }

    step(id = "f4a18269-83a8-4fcf-a62b-3cbb6216ddee", version = 0, name = "send file") {
      step.onlyOnce(
        "fileBytes" -> fileBytes
      )

      sendFile(fileBytes, "receiver")
    }
  }

}
