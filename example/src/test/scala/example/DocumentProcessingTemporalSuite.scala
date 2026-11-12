package example

import cats.effect.IO
import cats.effect.unsafe.IORuntime
import io.circe.syntax.*
import io.temporal.testing.TestWorkflowEnvironment
import munit.FunSuite

import java.nio.file.{Files, Path}
import java.util.concurrent.ConcurrentHashMap

/** Two smoke tests for the Temporal document-processing workflow, run on the
  * in-memory [[TestWorkflowEnvironment]] (no Temporal server needed; timers
  * are time-skipped). This is an API example, not a correctness suite — the
  * logic is covered in detail by the other example variants' tests.
  */
class DocumentProcessingTemporalSuite extends FunSuite {

  given IORuntime = IORuntime.global

  private class Setup {
    val baseDir    = Files.createTempDirectory("temporal-example")
    val archiveDir = baseDir.resolve("archive")
    val inputFile  = baseDir.resolve("batch-1.json")

    val uploads = new ConcurrentHashMap[String, Array[Byte]]()
    val reports = scala.collection.mutable.ListBuffer.empty[String]

    val virusCheckService: VirusCheckService = new VirusCheckService {
      override def checkForVirus1(content: Array[Byte]): IO[Boolean] = IO.pure(true)
      override def checkForVirus2(content: Array[Byte]): IO[Boolean] = IO.pure(true)
    }
    val encryptionService: EncryptionService = new EncryptionService {
      override def signDocument(document: Array[Byte]): IO[Array[Byte]] = IO.pure(document)
    }
    val documentUploadEndpoint: DocumentUploadEndpoint = new DocumentUploadEndpoint {
      override def uploadDocumentForProcessing(
          documentId: String,
          signedDocument: Array[Byte]
      ): IO[Either[DocumentUploadEndpoint.SignatureError.type, Unit]] = {
        uploads.put(documentId, signedDocument)
        IO.pure(Right(()))
      }
      override def checkUploadProcessingStatus(
          documentId: String
      ): IO[DocumentUploadEndpoint.ProcessingStatus] =
        IO.pure(DocumentUploadEndpoint.ProcessingStatus.ProcessedSuccessfully)
    }
    val resultReporter: ResultReporter = new ResultReporter {
      override def reportResultSuccess(filePath: Path): IO[Unit] = IO { reports += "success"; () }
      override def reportCancellation(filePath: Path): IO[Unit] = IO { reports += "cancelled"; () }
      override def reportResultError(filePath: Path, errorMsg: String): IO[Unit] =
        IO { reports += s"error:$errorMsg"; () }
    }

    def writeInputFile(documents: DocumentFromInputFile*): Unit =
      Files.writeString(inputFile, FileWithDocumentBatch(documents.toVector).asJson.noSpaces)

    def newEnvironment(): TestWorkflowEnvironment = {
      val env    = TestWorkflowEnvironment.newInstance()
      val worker = env.newWorker(DocumentProcessingTemporal.TaskQueue)
      worker.registerWorkflowImplementationTypes(classOf[DocumentProcessingTemporal])
      worker.registerActivitiesImplementations(
        new DocumentProcessingTemporalActivitiesImpl(
          archiveDir,
          virusCheckService,
          encryptionService,
          documentUploadEndpoint,
          resultReporter
        )
      )
      env.start()
      env
    }

    def cleanup(): Unit = deleteRecursively(baseDir)

    private def deleteRecursively(path: Path): Unit = {
      import scala.jdk.CollectionConverters.*
      if Files.isDirectory(path) then
        Files.list(path).iterator().asScala.foreach(deleteRecursively)
      Files.deleteIfExists(path)
    }
  }

  test("processes a small batch end-to-end") {
    val setup = new Setup
    val env   = setup.newEnvironment()
    try {
      setup.writeInputFile(
        DocumentFromInputFile("doc-1", "content-1".getBytes("UTF-8")),
        DocumentFromInputFile("doc-2", "content-2".getBytes("UTF-8"))
      )

      val handle = DocumentProcessingTemporal.startForInputFile(env.getWorkflowClient, setup.inputFile)
      val result = handle.getResult(classOf[String])

      assertEquals(result, "processed 2 document(s)")
      assertEquals(setup.uploads.keySet().size, 2)
      assertEquals(setup.reports.toList, List("success"))
      assert(Files.exists(setup.archiveDir.resolve("batch-1.json")), "the input file must be archived")
      assert(!Files.exists(setup.inputFile), "the input file must be moved away")
    } finally {
      env.close()
      setup.cleanup()
    }
  }

  test("a cancellation signal stops processing before the archive") {
    val setup = new Setup
    val env   = setup.newEnvironment()
    try {
      setup.writeInputFile(DocumentFromInputFile("doc-1", "content-1".getBytes("UTF-8")))

      val handle = DocumentProcessingTemporal.startForInputFile(env.getWorkflowClient, setup.inputFile)
      DocumentProcessingTemporal.requestCancellation(env.getWorkflowClient, setup.inputFile)
      val result = handle.getResult(classOf[String])

      assertEquals(result, "cancelled by user")
      assert(setup.uploads.isEmpty, "nothing must be uploaded")
      assert(Files.exists(setup.inputFile), "the input file must not be archived yet")
      assertEquals(setup.reports.toList, List("cancelled"))
    } finally {
      env.close()
      setup.cleanup()
    }
  }
}
