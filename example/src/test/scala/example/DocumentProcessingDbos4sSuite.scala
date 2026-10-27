package example

import cats.effect.IO
import cats.effect.unsafe.IORuntime
import dbos4s.testing.TestDbos
import io.circe.syntax.*
import munit.FunSuite

import java.nio.file.{Files, Path}
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicInteger
import scala.concurrent.duration.DurationInt

/** Logic tests for the dbos4s document-processing workflow, run on the
  * in-memory [[TestDbos]] double: steps execute inline, `recv` consumes fed
  * messages and `dbos.sleep` is recorded instead of waited out, so the polling
  * loops run instantly. Durability itself is the live transactor's job.
  */
class DocumentProcessingDbos4sSuite extends FunSuite {

  given IORuntime = IORuntime.global

  private class Collaborators {
    val baseDir    = Files.createTempDirectory("dbos4s-example")
    val archiveDir = baseDir.resolve("archive")

    val uploads = new ConcurrentHashMap[String, Array[Byte]]()
    val statusChecks = new AtomicInteger(0)
    val statusResults = scala.collection.mutable.Queue.empty[DocumentUploadEndpoint.ProcessingStatus]
    val reports = scala.collection.mutable.ListBuffer.empty[(String, Path)]
    val virusFound = new ConcurrentHashMap[String, Boolean]()

    /** Hook fired after the first upload-status check, used to inject a
      * cancellation mid-batch (TestDbos runs the whole body synchronously).
      */
    var afterFirstStatusCheck: () => Unit = () => ()

    val virusCheckService: VirusCheckService = new VirusCheckService {
      override def checkForVirus1(content: Array[Byte]): IO[Boolean] =
        IO.pure(!virusFound.containsKey(s"1:${new String(content, "UTF-8")}"))
      override def checkForVirus2(content: Array[Byte]): IO[Boolean] =
        IO.pure(!virusFound.containsKey(s"2:${new String(content, "UTF-8")}"))
    }
    val encryptionService: EncryptionService = new EncryptionService {
      override def signDocument(document: Array[Byte]): IO[Array[Byte]] =
        IO.pure(document)
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
      ): IO[DocumentUploadEndpoint.ProcessingStatus] = {
        statusChecks.incrementAndGet()
        if statusChecks.get() == 1 then afterFirstStatusCheck()
        val status =
          if statusResults.isEmpty then DocumentUploadEndpoint.ProcessingStatus.ProcessedSuccessfully
          else statusResults.dequeue()
        IO.pure(status)
      }
    }
    val resultReporter: ResultReporter = new ResultReporter {
      override def reportResultSuccess(filePath: Path): IO[Unit] =
        IO { reports += (("success", filePath)); () }
      override def reportCancellation(filePath: Path): IO[Unit] =
        IO { reports += (("cancelled", filePath)); () }
      override def reportResultError(filePath: Path, errorMsg: String): IO[Unit] =
        IO { reports += ((s"error:$errorMsg", filePath)); () }
    }

    def writeInputFile(documents: DocumentFromInputFile*): Path = {
      val inputFile = baseDir.resolve("batch-1.json")
      Files.writeString(inputFile, FileWithDocumentBatch(documents.toVector).asJson.noSpaces)
      inputFile
    }

    def cleanup(): Unit = deleteRecursively(baseDir)

    private def deleteRecursively(path: Path): Unit = {
      import scala.jdk.CollectionConverters.*
      if Files.isDirectory(path) then
        Files.list(path).iterator().asScala.foreach(deleteRecursively)
      Files.deleteIfExists(path)
    }
  }

  private def withFlow(test: (TestDbos, DocumentProcessingDbos4s, Collaborators) => Any): Any = {
    val dbos = TestDbos()
    val collaborators = new Collaborators
    val flow = new DocumentProcessingDbos4s(
      collaborators.archiveDir,
      collaborators.virusCheckService,
      collaborators.encryptionService,
      collaborators.documentUploadEndpoint,
      collaborators.resultReporter,
      dbos
    )
    try test(dbos, flow, collaborators)
    finally collaborators.cleanup()
  }

  test("processes a small batch end-to-end") {
    withFlow { (dbos, flow, c) =>
      val inputFile = c.writeInputFile(
        DocumentFromInputFile("doc-1", "content-1".getBytes("UTF-8")),
        DocumentFromInputFile("doc-2", "content-2".getBytes("UTF-8"))
      )
      DocumentProcessingDbos4s.announceInputFileFound(dbos, inputFile)

      val result = flow.batchWorkflow.start("batch-1").result

      assertEquals(result, "processed 2 document(s)")
      assertEquals(c.uploads.keySet().size, 2)
      assertEquals(c.statusChecks.get(), 2)
      assertEquals(c.reports.toList, List(("success", inputFile)))
      assert(Files.exists(c.archiveDir.resolve("batch-1.json")), "the input file must be archived")
      assert(!Files.exists(inputFile), "the input file must be moved away")
      assertEquals(
        dbos.steps,
        List(
          "read-input-file",
          "archive-input-file",
          "virus-check-1-doc-1",
          "virus-check-2-doc-1",
          "sign-document-doc-1",
          "upload-document-doc-1",
          "check-upload-status-doc-1",
          "virus-check-1-doc-2",
          "virus-check-2-doc-2",
          "sign-document-doc-2",
          "upload-document-doc-2",
          "check-upload-status-doc-2",
          "report-result"
        )
      )
    }
  }

  test("polls with a durable wait until the upload is acknowledged") {
    withFlow { (dbos, flow, c) =>
      val inputFile = c.writeInputFile(DocumentFromInputFile("doc-1", "content-1".getBytes("UTF-8")))
      c.statusResults += DocumentUploadEndpoint.ProcessingStatus.NoInfo
      c.statusResults += DocumentUploadEndpoint.ProcessingStatus.NoInfo
      DocumentProcessingDbos4s.announceInputFileFound(dbos, inputFile)

      val result = flow.batchWorkflow.start("batch-1").result

      assertEquals(result, "processed 1 document(s)")
      assertEquals(c.statusChecks.get(), 3)
      // One durable sleep after each "no info" poll.
      assertEquals(dbos.slept, List(15.minutes, 15.minutes))
    }
  }

  test("gives up polling after the maximum number of attempts") {
    withFlow { (dbos, flow, c) =>
      val inputFile = c.writeInputFile(DocumentFromInputFile("doc-1", "content-1".getBytes("UTF-8")))
      (1 to 11).foreach(_ => c.statusResults += DocumentUploadEndpoint.ProcessingStatus.NoInfo)
      DocumentProcessingDbos4s.announceInputFileFound(dbos, inputFile)

      val result = flow.batchWorkflow.start("batch-1").result

      assertEquals(result, "failed: upload status timeout exceeded")
      assertEquals(c.statusChecks.get(), 11)
      assertEquals(dbos.slept.size, 10)
      assertEquals(c.reports.toList, List(("error:upload status timeout exceeded", inputFile)))
    }
  }

  test("reports an upload that was processed with errors") {
    withFlow { (dbos, flow, c) =>
      val inputFile = c.writeInputFile(DocumentFromInputFile("doc-1", "content-1".getBytes("UTF-8")))
      c.statusResults += DocumentUploadEndpoint.ProcessingStatus.ProcessedWithErrors("broken")
      DocumentProcessingDbos4s.announceInputFileFound(dbos, inputFile)

      val result = flow.batchWorkflow.start("batch-1").result

      assertEquals(result, "failed: broken")
      assertEquals(c.reports.toList, List(("error:broken", inputFile)))
    }
  }

  test("a failed virus check fails the batch with an error report") {
    withFlow { (dbos, flow, c) =>
      val inputFile = c.writeInputFile(DocumentFromInputFile("doc-1", "content-1".getBytes("UTF-8")))
      c.virusFound.put("1:content-1", true)
      DocumentProcessingDbos4s.announceInputFileFound(dbos, inputFile)

      val result = flow.batchWorkflow.start("batch-1").result

      assertEquals(result, "failed: virus check failed for doc-1")
      assert(c.uploads.isEmpty, "nothing must be uploaded")
      assertEquals(c.reports.toList, List(("error:virus check failed for doc-1", inputFile)))
    }
  }

  test("an unparsable input file is reported as an error") {
    withFlow { (dbos, flow, c) =>
      val inputFile = c.baseDir.resolve("batch-1.json")
      Files.writeString(inputFile, "not json")
      DocumentProcessingDbos4s.announceInputFileFound(dbos, inputFile)

      val result = flow.batchWorkflow.start("batch-1").result

      assert(result.startsWith("failed: Could not parse json"), result)
      assert(c.uploads.isEmpty)
      assertEquals(c.reports.size, 1)
      assert(c.reports.head._1.startsWith("error:Could not parse json"))
    }
  }

  test("a cancellation message stops processing before the archive") {
    withFlow { (dbos, flow, c) =>
      val inputFile = c.writeInputFile(DocumentFromInputFile("doc-1", "content-1".getBytes("UTF-8")))
      DocumentProcessingDbos4s.announceInputFileFound(dbos, inputFile)
      DocumentProcessingDbos4s.requestCancellation(dbos, inputFile)

      val result = flow.batchWorkflow.start("batch-1").result

      assertEquals(result, "cancelled by user")
      assert(c.uploads.isEmpty, "nothing must be uploaded")
      assert(Files.exists(inputFile), "the input file must not be archived yet")
      assertEquals(c.reports.toList, List(("cancelled", inputFile)))
    }
  }

  test("a cancellation message stops processing between documents") {
    withFlow { (dbos, flow, c) =>
      val inputFile = c.writeInputFile(
        DocumentFromInputFile("doc-1", "content-1".getBytes("UTF-8")),
        DocumentFromInputFile("doc-2", "content-2".getBytes("UTF-8"))
      )
      // TestDbos runs the whole body synchronously, so the cancellation is
      // injected from inside the first document's status check: the workflow
      // then picks it up at the cancel poll before the second document.
      c.afterFirstStatusCheck = () => DocumentProcessingDbos4s.requestCancellation(dbos, inputFile)
      DocumentProcessingDbos4s.announceInputFileFound(dbos, inputFile)

      val result = flow.batchWorkflow.start("batch-1").result

      assertEquals(result, "cancelled by user")
      assertEquals(c.uploads.keySet().size, 1, "only the first document must be uploaded")
      assertEquals(c.reports.toList, List(("cancelled", inputFile)))
    }
  }
}
