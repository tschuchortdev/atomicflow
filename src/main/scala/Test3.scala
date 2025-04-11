import java.nio.file.{Files, Paths}
import scala.concurrent.duration.FiniteDuration

class Test3 {
  case class WorkflowId(id: ValidUuid)
  
  trait WorkflowCtx extends StepCache {
    def id: WorkflowId
    
    def name: String

    def cacheTtl: FiniteDuration
  }
  
  case class StepId(id: ValidUuid)

  trait StepCtx[Out] {
    def id: StepId

    def version: Long
    
    def name: Option[String]
    
    def description: Option[String]

    def workflowCtx: WorkflowCtx
  }

  final class StepBreak[Out](val ctx: StepCtx[Out], val value: Out)
    extends RuntimeException(
      /*message*/ null, /*cause*/ null, /*enableSuppression=*/ false, /*writableStackTrace*/ false)

  trait StepCache {
    def get[Out](ctx: StepCtx[Out], stepInputs: Seq[StepInput[?]]): Option[Out]
    
    

    // TODO: use ctx.workflowCtx.cacheTtl
    def put[Out](ctx: StepCtx[Out], stepInputs: Seq[StepInput[?]], value: Out): Unit
  }
  
  trait StepOnceStore {
    def get
  }

  type ValidUuid = String

  trait Hashable[A] {
    def hash(value: A): String
  }

  given Hashable[String] = ???

  given Hashable[Array[Byte]] = ???

  case class StepInput[A: Hashable](name: String, value: A)

  given [A: Hashable] => Conversion[(String, A), StepInput[A]] = { (name: String, value: A) =>
    StepInput(name, value)
  }

  object step {
    def apply[Out](
                    id: ValidUuid,
                    version: Long,
                    name: String | Unit = (),
                    description: String | Unit = ()
                  )(
                    body: StepCtx[Out] ?=> Out
                  )(using WorkflowCtx): Out = {
      given ctx: StepCtx[Out] = new StepCtx[Out] {}

      try {
        body
      } catch {
        case r: StepBreak[Out] if ctx.eq(r.ctx) =>
          r.value
      }
    }

    def cached[Out](stepInputs: StepInput[?]*)(using ctx: StepCtx[Out], cache: StepCache): Unit = {
      cache.get(ctx, stepInputs) match {
        case Some(value) =>
          throw new StepBreak[Out](ctx, value)

        case None =>
          ()
      }
    }

    def onlyOnce[Out](stepInputs: StepInput[?]*)(using ctx: StepCtx[Out], cache: StepCache): Unit = {
      cache.get(ctx) match {
        case Some(value) =>
          val stepInputsMatch: Boolean = true // TODO
          if (stepInputsMatch)
            throw new StepBreak[Out](ctx, value)
          else
            throw new IllegalStateException("conflict on step") // TODO

        case None =>
          ()
      }
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
