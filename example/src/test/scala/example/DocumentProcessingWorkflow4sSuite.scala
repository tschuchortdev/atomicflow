package example

import munit.*
import org.scalamock.stubs.Stubs
import org.camunda.bpm.model.bpmn.Bpmn
import workflows4s.bpmn.BpmnRenderer

import java.nio.file.{Files, Paths, StandardOpenOption}

class DocumentProcessingWorkflow4sSuite extends FunSuite with Stubs {

  test("Should write BPMN diagram for workflow") {
    val stubWorkflow = new DocumentProcessingWorkflow4s(Paths.get(""), stub, stub, stub, stub)

    Files.writeString(
      Paths.get(System.getProperty("user.dir")).resolve("document_processing_workflow.bpmn"),
      Bpmn.convertToString(BpmnRenderer.renderWorkflow(stubWorkflow.workflow.toProgress.toModel, "workflow"))
    )
  }
}
