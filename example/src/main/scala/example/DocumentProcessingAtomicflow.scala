package example

import atomicflow.*
import atomicflow.impl.memory.InMemoryWorkflowRuntime
import cats.effect.IO

import java.io.FileNotFoundException
import java.nio.file.{Files, Path}
import scala.jdk.CollectionConverters.*

class DocumentProcessingWorkflow(val inputDir: Path) {
/*
  private val workflowRuntime = InMemoryWorkflowRuntime()

  val w1 = Workflow(WorkflowId("99a2866c-99c5-49b7-b0f5-ad097a3e3a78"), name = "document processing") { (fileName: String) =>
    val fileBytes = Step(StepId("533dddc7-d355-43b4-81d8-bd8051808ec5"), version = 0, name = "read file") {
      
    }
  }

  def processWithAtomicFlow(): Nothing = {
    val inputFiles = try Files.list(inputDir).toList.asScala
    catch { case _: FileNotFoundException => List.empty  }

    inputFiles.foreach { inputFile =>
      workflowRuntime.runWorkflowInstance()
    }

    ???
  }

  def processWithWorkflow4s(): IO[Nothing] = {
    ???
  }
  
 */
}