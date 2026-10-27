package example

import cats.effect.unsafe.IORuntime
import io.circe.parser.*
import io.circe.syntax.*
import io.temporal.activity.{ActivityInterface, ActivityOptions}
import io.temporal.client.{WorkflowClient, WorkflowOptions, WorkflowStub}
import io.temporal.common.RetryOptions
import io.temporal.workflow.{Async, SignalMethod, Workflow, WorkflowInterface, WorkflowMethod}

import java.io.IOException
import java.nio.file.{Files, NoSuchFileException, Path}
import java.util.Optional
import scala.concurrent.duration.{DurationInt, FiniteDuration}
import scala.jdk.DurationConverters.*
import scala.util.control.NonFatal

/** Temporal document-processing workflow: await a file, archive it, check, sign
  * and upload its documents, then poll for completion and report the batch result.
  *
  * Uses the Java SDK with activities for side effects and durable waits/timers.
  * Mapping from [[DocumentProcessingWorkflow4s]]:
  *   - Keeps the file-found start signal for equivalence; see
  *     [[DocumentProcessingTemporal.startForInputFile]].
  *   - Cancellation sets a flag polled between phases and during status polling.
  *   - Virus checks run in parallel via `Async.procedure`.
  *   - Sign-and-upload retries every 15 minutes; expired signatures trigger
  *     re-signing using replay-deterministic `Workflow.currentTimeMillis`.
  *   - Status polling uses durable timers, with at most ten waits.
  *   - Error reports unwrap `ActivityFailure` to the root cause's message.
  *
  * Production wiring:
  * {{{
  * val client = WorkflowClient.newInstance(new WorkflowServiceStubs().start())
  * val worker  = WorkerFactory.newInstance(client).newWorker(TaskQueue)
  * worker.registerWorkflowImplementationTypes(classOf[DocumentProcessingTemporal])
  * worker.registerActivitiesImplementations(
  *   new DocumentProcessingTemporalActivitiesImpl(archiveDir, virusChecks, encryption, uploads, reporter))
  * WorkerFactory.newInstance(client).start()
  * val handle = DocumentProcessingTemporal.startForInputFile(client, inputFile) // start signal
  * handle.getResult(classOf[String])
  * }}}
  */
@WorkflowInterface
trait DocumentProcessingTemporalWorkflow {
  @WorkflowMethod(name = "document-batch-processing")
  def processBatch(): String

  @SignalMethod
  def inputFileFound(path: String): Unit

  @SignalMethod
  def userCancelsUploadOfInputFile(): Unit
}

/** Side effects run as activities to keep the workflow body deterministic. */
@ActivityInterface
trait DocumentProcessingTemporalActivities {
  def readInputFile(path: String): String
  def archiveInputFile(path: String): Unit
  def checkForVirus1(documentId: String, content: Array[Byte]): Unit
  def checkForVirus2(documentId: String, content: Array[Byte]): Unit
  def signDocument(documentId: String, content: Array[Byte]): Array[Byte]
  def uploadDocument(documentId: String, signedDocument: Array[Byte]): Unit
  def checkUploadProcessingStatus(documentId: String): String
  def reportResultSuccess(path: String): Unit
  def reportCancellation(path: String): Unit
  def reportResultError(path: String, errorMsg: String): Unit
}

class DocumentProcessingTemporal extends DocumentProcessingTemporalWorkflow {
  import DocumentProcessingTemporal.*

  private val activities: DocumentProcessingTemporalActivities = Workflow.newActivityStub(
    classOf[DocumentProcessingTemporalActivities],
    ActivityOptions.newBuilder().setStartToCloseTimeout(ActivityStartToCloseTimeout.toJava).build()
  )

  private var inputFilePath: String     = null
  private var cancelRequested: Boolean  = false

  override def inputFileFound(path: String): Unit = {
    inputFilePath = path
  }
  
  override def userCancelsUploadOfInputFile(): Unit = {
    cancelRequested = true
  }

  override def processBatch(): String = {
    Workflow.await(() => inputFilePath != null || cancelRequested)
    if inputFilePath == null then "cancelled by user" // cancelled before the file was announced
    else {
      val path = inputFilePath
      try processInputFile(path)
      catch {
        case Error.CancelledByUser =>
          activities.reportCancellation(path)
          "cancelled by user"
        case NonFatal(e) =>
          val errorMsg = rootCauseMessage(e)
          activities.reportResultError(path, errorMsg)
          s"failed: $errorMsg"
      }
    }
  }

  private def processInputFile(path: String): String = {
    val batchJson = activities.readInputFile(path)
    val batch = decode[FileWithDocumentBatch](batchJson)
      .getOrElse(throw Error.ParsingFileFailed("recorded batch could not be decoded"))

    throwIfCancelled()

    activities.archiveInputFile(path)

    batch.documents.foreach { document =>
      throwIfCancelled()
      processIndividualDocument(document)
    }

    activities.reportResultSuccess(path)
    s"processed ${batch.documents.size} document(s)"
  }

  private def processIndividualDocument(document: DocumentFromInputFile): Unit = {
    val documentId = document.documentId

    val virusCheck1 = Async.procedure(() => activities.checkForVirus1(documentId, document.content))
    val virusCheck2 = Async.procedure(() => activities.checkForVirus2(documentId, document.content))
    virusCheck1.get()
    virusCheck2.get()

    signAndUploadWithinSignatureValidity(document)
    pollUploadStatus(documentId)
  }

  /** Retry sign-and-upload every 15 minutes, re-signing if validity expires. */
  private def signAndUploadWithinSignatureValidity(document: DocumentFromInputFile): Unit = {
    val validityMs = EncryptionService.signatureValidityPeriod.toMillis
    var uploadedWithinValidity = false
    while !uploadedWithinValidity do {
      val (signedAtEpochMs, signature) = Workflow.retry(
        RetryOptions.newBuilder()
          .setInitialInterval(UploadRetryInterval.toJava)
          .setBackoffCoefficient(1.0)
          .setMaximumAttempts(0) // unlimited
          .build(),
        Optional.empty(),
        () => {
          val signature = activities.signDocument(document.documentId, document.content)
          val signedAtEpochMs = Workflow.currentTimeMillis()
          activities.uploadDocument(document.documentId, signature)
          (signedAtEpochMs, signature)
        }
      )
      val uploadedAtEpochMs = Workflow.currentTimeMillis()
      uploadedWithinValidity = uploadedAtEpochMs - signedAtEpochMs < validityMs
    }
  }

  private def pollUploadStatus(documentId: String): Unit = {
    var attempts  = 0
    var processed = false
    while !processed do {
      val outcome = activities.checkUploadProcessingStatus(documentId)
      outcome match {
        case "success" => processed = true
        case s"error:$msg" =>
          throw Error.UploadProcessedWithErrors(msg)
        case _ =>
          if attempts >= MaxPollAttempts then throw Error.UploadStatusTimeoutExceeded
          Workflow.sleep(PollInterval.toJava)
          attempts += 1
          throwIfCancelled()
      }
    }
  }

  private def throwIfCancelled(): Unit =
    if cancelRequested then throw Error.CancelledByUser

  /** Activity failures cross the workflow boundary wrapped in `ActivityFailure`;
    * report the root cause's message.
    */
  private def rootCauseMessage(e: Throwable): String = {
    var cause = e
    while cause.getCause != null do cause = cause.getCause
    cause.getMessage
  }
}

class DocumentProcessingTemporalActivitiesImpl(
    archiveDir: Path,
    virusCheckService: VirusCheckService,
    encryptionService: EncryptionService,
    documentUploadEndpoint: DocumentUploadEndpoint,
    resultReporter: ResultReporter
)(using IORuntime) extends DocumentProcessingTemporalActivities {
  import DocumentProcessingTemporal.*

  override def readInputFile(path: String): String = {
    val fileContent =
      try Files.readString(Path.of(path))
      catch { case e: IOException => throw Error.ReadingFileFailed(e.getMessage) }
    try FileWithDocumentBatch.fromString(fileContent).asJson.noSpaces
    catch { case e: IllegalArgumentException => throw Error.ParsingFileFailed(e.getMessage) }
  }

  override def archiveInputFile(path: String): Unit = {
    val inputFilePath = Path.of(path)
    try {
      Files.createDirectories(archiveDir)
      Files.move(inputFilePath, archiveDir.resolve(inputFilePath.getFileName))
      ()
    } catch {
      // An at-least-once replay may find the file already archived.
      case _: NoSuchFileException => ()
      case e: IOException         => throw Error.ArchivingFileFailed(e.getMessage)
    }
  }

  override def checkForVirus1(documentId: String, content: Array[Byte]): Unit =
    if !virusCheckService.checkForVirus1(content).unsafeRunSync() then
      throw Error.VirusCheckFailed(documentId)

  override def checkForVirus2(documentId: String, content: Array[Byte]): Unit =
    if !virusCheckService.checkForVirus2(content).unsafeRunSync() then
      throw Error.VirusCheckFailed(documentId)

  override def signDocument(documentId: String, content: Array[Byte]): Array[Byte] =
    encryptionService.signDocument(content).unsafeRunSync()

  override def uploadDocument(documentId: String, signedDocument: Array[Byte]): Unit =
    documentUploadEndpoint.uploadDocumentForProcessing(documentId, signedDocument).unsafeRunSync() match {
      case Right(()) => ()
      case Left(_)   => throw Error.DownstreamRejectedUpload(documentId)
    }

  override def checkUploadProcessingStatus(documentId: String): String = {
    val status =
      try documentUploadEndpoint.checkUploadProcessingStatus(documentId).unsafeRunSync()
      catch { case NonFatal(_) => DocumentUploadEndpoint.ProcessingStatus.NoInfo }
    status match {
      case DocumentUploadEndpoint.ProcessingStatus.ProcessedSuccessfully   => "success"
      case DocumentUploadEndpoint.ProcessingStatus.ProcessedWithErrors(msg) => s"error:$msg"
      case DocumentUploadEndpoint.ProcessingStatus.NoInfo                  => "noinfo"
    }
  }

  override def reportResultSuccess(path: String): Unit =
    resultReporter.reportResultSuccess(Path.of(path)).unsafeRunSync()

  override def reportCancellation(path: String): Unit =
    resultReporter.reportCancellation(Path.of(path)).unsafeRunSync()

  override def reportResultError(path: String, errorMsg: String): Unit =
    resultReporter.reportResultError(Path.of(path), errorMsg).unsafeRunSync()
}

object DocumentProcessingTemporal {
  private[example] val TaskQueue: String                   = "document-processing"
  private[example] val ActivityStartToCloseTimeout: FiniteDuration = 5.minutes
  private[example] val PollInterval: FiniteDuration       = 15.minutes
  private[example] val MaxPollAttempts: Int                = 10
  private[example] val UploadRetryInterval: FiniteDuration = 15.minutes

  sealed abstract class Error(msg: String) extends RuntimeException(msg)
  object Error {
    case class ReadingFileFailed(msg: String) extends Error(msg)
    case class ParsingFileFailed(msg: String) extends Error(msg)
    case class ArchivingFileFailed(msg: String) extends Error(msg)
    case class VirusCheckFailed(documentId: String) extends Error(s"virus check failed for $documentId")
    case class DownstreamRejectedUpload(documentId: String) extends Error(s"upload rejected for $documentId")
    case class UploadProcessedWithErrors(msg: String) extends Error(msg)
    case object UploadStatusTimeoutExceeded extends Error("upload status timeout exceeded")
    case object CancelledByUser extends Error("cancelled by user")
  }

  def fileIdFrom(inputFilePath: Path): String =
    FileWithDocumentBatch.idFromFileName(inputFilePath.getFileName.toString)

  /** Start under the file id and signal the input path. Keeps signal-based
    * startup to match workflows4s rather than passing a workflow argument.
    */
  def startForInputFile(client: WorkflowClient, inputFilePath: Path): WorkflowStub = {
    val workflow = newWorkflowStub(client, inputFilePath)
    WorkflowClient.start(() => workflow.processBatch())
    workflow.inputFileFound(inputFilePath.toString)
    WorkflowStub.fromTyped(workflow)
  }

  /** Signal cancellation, polled by the workflow between phases. */
  def requestCancellation(client: WorkflowClient, inputFilePath: Path): Unit =
    client
      .newWorkflowStub(classOf[DocumentProcessingTemporalWorkflow], fileIdFrom(inputFilePath))
      .userCancelsUploadOfInputFile()

  private def newWorkflowStub(
      client: WorkflowClient,
      inputFilePath: Path
  ): DocumentProcessingTemporalWorkflow =
    client.newWorkflowStub(
      classOf[DocumentProcessingTemporalWorkflow],
      WorkflowOptions.newBuilder()
        .setWorkflowId(fileIdFrom(inputFilePath))
        .setTaskQueue(TaskQueue)
        .build()
    )
}
