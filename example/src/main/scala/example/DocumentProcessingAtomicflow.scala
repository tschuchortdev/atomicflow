package example

import atomicflow.*
import atomicflow.given
import atomicflow.Cacheable.Simple.given
import atomicflow.impl.db.PostgresWorkflowRuntime
import cats.effect.IO
import cats.effect.unsafe.IORuntime

import java.nio.file.{Files, Path}
import java.time.Clock
import javax.sql.DataSource
import scala.concurrent.ExecutionContext
import scala.concurrent.duration.*

/** A document-processing batch workflow ported to the atomicflow API: read and
  * archive an input file, run virus checks and encryption per document, upload
  * each document to a processing endpoint, poll for the processing outcome, and
  * report the batch result.
  *
  * The workflow is defined with the durable step primitives (`atLeastOnce` for
  * retryable work, `atMostOnce` for the external upload effect, `Workflow.loop`
  * for polling with timer awaits, `Workflow.parallel` for the virus checks) and
  * runs on a [[PostgresWorkflowRuntime]] built from the supplied `DataSource`.
  */
class DocumentProcessingAtomicflow(
    archiveDir: Path,
    virusCheckService: VirusCheckService,
    encryptionService: EncryptionService,
    documentUploadEndpoint: DocumentUploadEndpoint,
    resultReporter: ResultReporter,
    dataSource: DataSource,
    clock: Clock = Clock.systemUTC()
)(using ExecutionContext, IORuntime) {
  import DocumentProcessingAtomicflow.*
  import DocumentProcessingAtomicflow.given

  private given Clock = clock

  given Cacheable[Throwable] = Cacheable.forThrowable.genericStringMessageSerializer

  private given Cacheable[Array[Byte]] = new Cacheable[Array[Byte]] {
    override def stableSerializedTypeId: String = "byte-array-base64"
    override def write(value: Array[Byte]): String = java.util.Base64.getEncoder.encodeToString(value)
    override def read(serialized: String): Array[Byte] = java.util.Base64.getDecoder.decode(serialized)
  }

  lazy val runtime: PostgresWorkflowRuntime = PostgresWorkflowRuntime(dataSource, clock)

  /** The batch workflow. Input is the input file path; output is the number of
    * documents successfully processed.
    */
  val batchWorkflow: Workflow[String, Int] =
    Workflow[String, Int](
      id = "document-batch-processing",
      version = 1L,
      name = "document batch processing"
    ) { inputFilePath =>
      val batch = readAndArchiveFile(inputFilePath)
      batch.documents.foreach { document =>
        Workflow.scoped(document.documentId) {
          processIndividualDocument(document)
        }
      }
      resultReporter.reportResultSuccess(Path.of(inputFilePath)).unsafeRunSync()
      batch.documents.size
    }

  private def readAndArchiveFile(inputFilePath: String)(using ctx: WorkflowContext): FileWithDocumentBatch =
    Step.atLeastOnce[FileWithDocumentBatch]("read-and-archive-file") {
      val path = Path.of(inputFilePath)
      val fileContent =
        try Files.readString(path)
        catch { case e: java.io.IOException => throw Error.ReadingFileFailed(e.getMessage) }
      val parsed =
        try FileWithDocumentBatch.fromString(fileContent)
        catch { case e: IllegalArgumentException => throw Error.ParsingFileFailed(e.getMessage) }
      try {
        Files.createDirectories(archiveDir)
        Files.move(path, archiveDir.resolve(path.getFileName))
      } catch { case e: java.io.IOException => throw Error.ArchivingFileFailed(e.getMessage) }
      parsed
    }

  private def processIndividualDocument(document: DocumentFromInputFile)(using ctx: WorkflowContext): Unit = {
    Workflow.parallel(
      () => virusCheck(document.content, "virus-check-1", virusCheckService.checkForVirus1),
      () => virusCheck(document.content, "virus-check-2", virusCheckService.checkForVirus2)
    )

    val signed =
      Step.atLeastOnce[Array[Byte]]("sign-document", ensureUnchanged = Seq("documentId" -> document.documentId)) {
        encryptionService.signDocument(document.content).unsafeRunSync()
      }

    Step.atMostOnce[Unit]("upload-document") {
      documentUploadEndpoint.uploadDocumentForProcessing(document.documentId, signed).unsafeRunSync() match {
        case Right(()) => ()
        case Left(_)   => throw Error.UploadRejected(document.documentId)
      }
    }

    pollUploadStatus(document.documentId)
  }

  private def virusCheck(
      content: Array[Byte],
      stepId: String,
      check: Array[Byte] => IO[Boolean]
  )(using ctx: WorkflowContext): Unit =
    Step.atLeastOnce[Boolean](stepId) {
      val ok = check(content).unsafeRunSync()
      if (!ok) throw Error.VirusCheckFailed(stepId)
      ok
    }

  private def pollUploadStatus(documentId: String)(using ctx: WorkflowContext): Unit =
    Workflow.loop[Int, Unit]("poll-upload-status", 0) { (attempts, loop) =>
      val outcome =
        Step.atLeastOnce[String]("check-upload-status") {
          documentUploadEndpoint.checkUploadProcessingStatus(documentId).unsafeRunSync() match {
            case DocumentUploadEndpoint.ProcessingStatus.ProcessedSuccessfully => "success"
            case DocumentUploadEndpoint.ProcessingStatus.ProcessedWithErrors(msg) => s"error:$msg"
            case DocumentUploadEndpoint.ProcessingStatus.NoInfo => "noinfo"
          }
        }
      outcome match {
        case "success" => loop.break(())
        case o if o.startsWith("error:") => throw Error.UploadProcessedWithErrors(o.drop("error:".length))
        case _ =>
          if (attempts >= MaxPollAttempts) throw Error.UploadStatusTimeoutExceeded
          else {
            Step.await[Unit]("poll-wait", Awaitable.Timer(PollInterval))
            attempts + 1
          }
      }
    }
}

object DocumentProcessingAtomicflow {
  private[example] val PollInterval: FiniteDuration = 15.minutes
  private[example] val MaxPollAttempts: Int = 10

  /** Circe-backed codec for the parsed batch, so the read-and-archive step can
    * persist its result durably.
    */
  given Cacheable[FileWithDocumentBatch] = new Cacheable[FileWithDocumentBatch] {
    import io.circe.parser.*
    import io.circe.syntax.*

    override def stableSerializedTypeId: String = "file-with-document-batch"
    override def write(value: FileWithDocumentBatch): String = value.asJson.noSpaces
    override def read(serialized: String): FileWithDocumentBatch =
      parse(serialized).flatMap(_.as[FileWithDocumentBatch]).toOption.get
  }

  sealed abstract class Error(msg: String) extends RuntimeException(msg)
  object Error {
    case class ReadingFileFailed(msg: String) extends Error(msg)
    case class ParsingFileFailed(msg: String) extends Error(msg)
    case class ArchivingFileFailed(msg: String) extends Error(msg)
    case class VirusCheckFailed(msg: String) extends Error(msg)
    case class UploadRejected(documentId: String) extends Error(s"upload rejected for $documentId")
    case class UploadProcessedWithErrors(msg: String) extends Error(msg)
    case object UploadStatusTimeoutExceeded extends Error("upload status timeout exceeded")
  }
}
