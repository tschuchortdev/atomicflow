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

/** A document-processing batch workflow: wait for an input file to be announced,
  * read and archive it, run virus checks and encryption per document, upload
  * each document to a processing endpoint, poll for the processing outcome, and
  * report the batch result.
  *
  * This is the dbos4s equivalent of [[DocumentProcessingWorkflow4s]] and
  * [[DocumentProcessingAtomicflow]]. It runs on DBOS Transact (via the dbos4s
  * Scala bindings): `dbos.workflow` for the durable body, `dbos.step` for the
  * checkpointed side effects, `dbos.recv`/`dbos.send` for signals and
  * `dbos.sleep` for durable timers.
  *
  * Deviations from the workflows4s variant, forced by the DBOS model:
  *   - DBOS workflows are no-argument, so the `inputFileFound` start signal
  *     becomes a `recv` at the top of the workflow body (the idempotency key is
  *     the file id, and the path is delivered with `send`/[[announceInputFileFound]]).
  *     DBOS `recv` needs a finite timeout; a year stands in for the indefinite
  *     await of the workflows4s signal.
  *   - DBOS has no mid-workflow interruption, so the `userCancelsUploadOfInputFile`
  *     signal is polled with a zero-timeout `recv` between phases and while
  *     polling for upload status (an external `dbos.cancel(workflowId)` remains
  *     available as an operational measure).
  *   - Steps inside one DBOS workflow are strictly ordered, so the two virus
  *     checks run sequentially instead of in parallel.
  *   - Upload retries are bounded per step (`maxAttempts`), where workflows4s
  *     retried the whole sign-and-upload block indefinitely; the signature
  *     validity period is still enforced by restarting sign-and-upload when the
  *     upload missed it.
  *   - Step outputs must round-trip through the DBOS serializer, so steps return
  *     JSON strings that the workflow body decodes.
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
    // The durable equivalent of the workflows4s `inputFileFound` start signal.
    val inputFilePath: Option[Path] =
      dbos.recv[String](Topic.inputFileFound, WaitForInputFile).map(Path.of(_))

    inputFilePath match {
      case None =>
        // The (practically unreachable) start-signal timeout. workflows4s
        // would wait forever; DBOS recv needs a finite deadline.
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

  /** Read and parse the input file, archive it, process every document, and
    * report the batch result. Durably restartable at every step boundary.
    */
  private def processInputFile(inputFilePath: Path): String = {
    // Parse inside the step so the recorded JSON output survives the archive
    // move; replay never needs the original file again.
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

    // On a replay after a crash between move and step commit, the file is
    // already gone; that counts as archived. A failed archive attempt is
    // retried once after 30 seconds, mirroring the workflows4s retryOnce.
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

    // Process documents sequentially, with separate step records per document id.
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

  /** Sign the document and upload it. If the upload did not complete within
    * the signature validity period, sign again (the workflows4s timeout
    * interruption that restarts sign-and-upload, expressed as a loop over
    * recorded step outputs).
    */
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

  /** Poll the processing status of an upload, at most [[MaxPollAttempts]] times
    * with a durable 15-minute wait in between, until it is acknowledged as
    * error free.
    */
  private def pollUploadStatus(documentId: String): Unit = {
    var attempts = 0
    var processed = false
    while !processed do {
      val outcome = dbos.step(s"check-upload-status-$documentId") {
        // A transient status-check failure counts as "no info", as in the
        // workflows4s variant.
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

  /** Poll for the user-cancellation signal with a zero-timeout recv. */
  private def throwIfCancelledByUser(): Unit =
    if dbos.recv[String](Topic.userCancelsUploadOfInputFile, 0.seconds).isDefined then
      throw Error.CancelledByUser

  /** Report the batch outcome as a durable step: on a replay the report is
    * never re-executed.
    */
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

  /** Signed content as recorded by the sign-document step. JSON (Base64 for
    * the signature) so the output round-trips through the DBOS serializer.
    */
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

  /** Announce an input file. Replaces the workflows4s `inputFileFound` start
    * signal: the batch workflow for that file id picks the path up from its
    * first `recv`.
    */
  def announceInputFileFound(dbos: Dbos, inputFilePath: Path): Unit =
    dbos.send(fileIdFrom(inputFilePath), inputFilePath.toString, Topic.inputFileFound)

  /** Ask a running batch workflow to cancel. Replaces the workflows4s
    * `userCancelsUploadOfInputFile` interruption signal with a message the
    * workflow polls for between phases.
    */
  def requestCancellation(dbos: Dbos, inputFilePath: Path): Unit =
    dbos.send(fileIdFrom(inputFilePath), "cancel", Topic.userCancelsUploadOfInputFile)
}
