package example

import cats.effect.unsafe.IORuntime
import dbos4s.{Dbos, Workflow}
import io.circe.Codec
import io.circe.parser.*
import io.circe.syntax.*

import java.io.IOException
import java.nio.file.{Files, NoSuchFileException, Path}
import scala.concurrent.duration.{DurationInt, FiniteDuration}
import scala.util.control.NonFatal

/** DBOS document-processing workflow: await a file, archive it, check, sign and
  * upload its documents, then poll for completion and report the batch result.
  *
  * Differences from [[DocumentProcessingWorkflow4s]]:
  *   - No-argument workflows receive the file path via [[announceInputFileFound]],
  *     using the file id as the idempotency key. A one-year receive timeout
  *     approximates an indefinite wait.
  *   - Cancellation is polled between phases and during status polling.
  *   - Ordered steps require sequential virus checks.
  *   - Upload retries are bounded per step; expired signatures trigger re-signing.
  *   - Step results use JSON strings to round-trip through the DBOS serializer.
  *
  * Production wiring:
  * {{{
  * val dbos = Dbos("documents") {
  *   _.withDatabaseUrl("jdbc:postgresql://localhost:5432/documents")
  *     .withDbUser("postgres").withDbPassword("dbos")
  * }
  * val flow = new DocumentProcessingDbos4s(archiveDir, virusChecks, encryption,
  *   uploads, reporter, dbos)
  * flow.batchWorkflow // force registration before launch
  * dbos.launch()
  * DocumentProcessingDbos4s.announceInputFileFound(dbos, inputFile) // start signal
  * flow.batchWorkflow.start(DocumentProcessingDbos4s.fileIdFrom(inputFile)).result
  * }}}
  */
class DocumentProcessingDbos4s(
    archiveDir: Path,
    virusCheckService: VirusCheckService,
    encryptionService: EncryptionService,
    documentUploadEndpoint: DocumentUploadEndpoint,
    resultReporter: ResultReporter,
    dbos: Dbos
)(using IORuntime) {
  import DocumentProcessingDbos4s.*

  /** The batch workflow. Input is the announced input file path; output is a
    * short outcome summary ("processed ...", "cancelled by user", "failed: ...").
    */
  lazy val batchWorkflow: Workflow[String] = dbos.workflow("document-batch-processing") {
    val inputFilePath: Option[Path] =
      dbos.recv[String](Topic.inputFileFound, WaitForInputFile).map(Path.of(_))

    inputFilePath match {
      case None =>
        "failed: no input file was announced"
      case Some(path) =>
        try processInputFile(path)
        catch {
          case Error.CancelledByUser =>
            reportResult(Report.Cancelled(path))
            "cancelled by user"
          case NonFatal(e) =>
            reportResult(Report.Error(path, e.getMessage))
            s"failed: ${e.getMessage}"
        }
    }
  }

  private def processInputFile(inputFilePath: Path): String = {
    // Persist the parsed batch so replay no longer needs the archived file.
    val batchJson = dbos.step("read-input-file") {
      val fileContent =
        try Files.readString(inputFilePath)
        catch { case e: IOException => throw Error.ReadingFileFailed(e.getMessage) }
      try FileWithDocumentBatch.fromString(fileContent).asJson.noSpaces
      catch { case e: IllegalArgumentException => throw Error.ParsingFileFailed(e.getMessage) }
    }
    val batch = decode[FileWithDocumentBatch](batchJson)
      .getOrElse(throw Error.ParsingFileFailed("recorded batch could not be decoded"))

    throwIfCancelledByUser()

    // A crash after the move but before step commit leaves the file already archived.
    dbos.step("archive-input-file", maxAttempts = 2, retryInterval = ArchiveRetryInterval) {
      try {
        Files.createDirectories(archiveDir)
        Files.move(inputFilePath, archiveDir.resolve(inputFilePath.getFileName))
        ()
      } catch {
        case _: NoSuchFileException => ()
        case e: IOException         => throw Error.ArchivingFileFailed(e.getMessage)
      }
    }

    throwIfCancelledByUser()

    batch.documents.foreach { document =>
      throwIfCancelledByUser()
      processIndividualDocument(document)
    }

    reportResult(Report.Success(inputFilePath))
    s"processed ${batch.documents.size} document(s)"
  }

  private def processIndividualDocument(document: DocumentFromInputFile): Unit = {
    val documentId = document.documentId

    dbos.step(s"virus-check-1-$documentId") {
      if !virusCheckService.checkForVirus1(document.content).unsafeRunSync() then
        throw Error.VirusCheckFailed(documentId)
    }
    dbos.step(s"virus-check-2-$documentId") {
      if !virusCheckService.checkForVirus2(document.content).unsafeRunSync() then
        throw Error.VirusCheckFailed(documentId)
    }

    signAndUploadWithinSignatureValidity(document)
    pollUploadStatus(documentId)
  }

  /** Re-sign and upload until the upload completes within signature validity. */
  private def signAndUploadWithinSignatureValidity(document: DocumentFromInputFile): Unit = {
    val validityMs = EncryptionService.signatureValidityPeriod.toMillis
    var uploadedWithinValidity = false
    while !uploadedWithinValidity do {
      val signedJson = dbos.step(s"sign-document-${document.documentId}") {
        val signature = encryptionService.signDocument(document.content).unsafeRunSync()
        SignedDocument(signedAtEpochMs = System.currentTimeMillis(), signature).asJson.noSpaces
      }
      val signed = decode[SignedDocument](signedJson)
        .getOrElse(throw Error.SigningFailed("recorded signature could not be decoded"))

      val uploadedAtEpochMs = dbos.step(
        s"upload-document-${document.documentId}",
        maxAttempts = MaxUploadAttempts,
        retryInterval = UploadRetryInterval
      ) {
        documentUploadEndpoint
          .uploadDocumentForProcessing(document.documentId, signed.signature)
          .unsafeRunSync() match {
          case Right(()) => System.currentTimeMillis()
          case Left(_)   => throw Error.DownstreamRejectedUpload(document.documentId)
        }
      }

      uploadedWithinValidity = uploadedAtEpochMs - signed.signedAtEpochMs < validityMs
    }
  }

  private def pollUploadStatus(documentId: String): Unit = {
    var attempts = 0
    var processed = false
    while !processed do {
      val outcome = dbos.step(s"check-upload-status-$documentId") {
        val status =
          try documentUploadEndpoint.checkUploadProcessingStatus(documentId).unsafeRunSync()
          catch { case NonFatal(_) => DocumentUploadEndpoint.ProcessingStatus.NoInfo }
        status match {
          case DocumentUploadEndpoint.ProcessingStatus.ProcessedSuccessfully   => "success"
          case DocumentUploadEndpoint.ProcessingStatus.ProcessedWithErrors(msg) => s"error:$msg"
          case DocumentUploadEndpoint.ProcessingStatus.NoInfo                  => "noinfo"
        }
      }
      outcome match {
        case "success" => processed = true
        case s"error:$msg" =>
          throw Error.UploadProcessedWithErrors(msg)
        case _ =>
          if attempts >= MaxPollAttempts then throw Error.UploadStatusTimeoutExceeded
          dbos.sleep(PollInterval)
          attempts += 1
          throwIfCancelledByUser()
      }
    }
  }

  private def throwIfCancelledByUser(): Unit =
    if dbos.recv[String](Topic.userCancelsUploadOfInputFile, 0.seconds).isDefined then
      throw Error.CancelledByUser

  private def reportResult(report: Report): Unit = dbos.step("report-result") {
    report match {
      case Report.Success(filePath)    => resultReporter.reportResultSuccess(filePath).unsafeRunSync()
      case Report.Cancelled(filePath)  => resultReporter.reportCancellation(filePath).unsafeRunSync()
      case Report.Error(filePath, msg) => resultReporter.reportResultError(filePath, msg).unsafeRunSync()
    }
  }
}

object DocumentProcessingDbos4s {
  private[example] val WaitForInputFile: FiniteDuration = 365.days
  private[example] val ArchiveRetryInterval: FiniteDuration = 30.seconds
  private[example] val PollInterval: FiniteDuration = 15.minutes
  private[example] val MaxPollAttempts: Int = 10
  private[example] val UploadRetryInterval: FiniteDuration = 15.minutes
  private[example] val MaxUploadAttempts: Int = 5

  /** Messaging topics, named after the workflows4s signal definitions. */
  object Topic {
    val inputFileFound: String = "inputFileFound"
    val userCancelsUploadOfInputFile: String = "userCancelsUploadOfInputFile"
  }

  /** JSON step result with a Base64-encoded signature for DBOS serialization. */
  private[example] case class SignedDocument(signedAtEpochMs: Long, signature: Array[Byte]) derives Codec.AsObject

  sealed trait Report
  object Report {
    case class Success(filePath: Path) extends Report
    case class Cancelled(filePath: Path) extends Report
    case class Error(filePath: Path, errorMsg: String) extends Report
  }

  sealed abstract class Error(msg: String) extends RuntimeException(msg)
  object Error {
    case class ReadingFileFailed(msg: String) extends Error(msg)
    case class ParsingFileFailed(msg: String) extends Error(msg)
    case class ArchivingFileFailed(msg: String) extends Error(msg)
    case class VirusCheckFailed(documentId: String) extends Error(s"virus check failed for $documentId")
    case class SigningFailed(msg: String) extends Error(msg)
    case class DownstreamRejectedUpload(documentId: String) extends Error(s"upload rejected for $documentId")
    case class UploadProcessedWithErrors(msg: String) extends Error(msg)
    case object UploadStatusTimeoutExceeded extends Error("upload status timeout exceeded")
    case object CancelledByUser extends Error("cancelled by user")
  }

  def fileIdFrom(inputFilePath: Path): String =
    FileWithDocumentBatch.idFromFileName(inputFilePath.getFileName.toString)

  /** Deliver the input path to the batch workflow's initial receive. */
  def announceInputFileFound(dbos: Dbos, inputFilePath: Path): Unit =
    dbos.send(fileIdFrom(inputFilePath), inputFilePath.toString, Topic.inputFileFound)

  /** Send a cancellation request, polled by the workflow between phases. */
  def requestCancellation(dbos: Dbos, inputFilePath: Path): Unit =
    dbos.send(fileIdFrom(inputFilePath), "cancel", Topic.userCancelsUploadOfInputFile)
}
