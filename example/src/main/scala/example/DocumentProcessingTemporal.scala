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

/** A document-processing batch workflow: wait for an input file to be announced,
  * read and archive it, run virus checks and encryption per document, upload
  * each document to a processing endpoint, poll for the processing outcome, and
  * report the batch result.
  *
  * This is the Temporal equivalent of [[DocumentProcessingWorkflow4s]],
  * [[DocumentProcessingAtomicflow]] and [[DocumentProcessingDbos4s]], using the
  * official Temporal Java SDK from Scala: the annotated `@WorkflowInterface`
  * below defines the durable workflow (plus its signals), all side effects are
  * `@ActivityInterface` methods, `Workflow.await`/`Workflow.sleep` are the
  * durable waits and timers, and `Async.function`/`Async.procedure` run the
  * two virus checks in parallel again.
  *
  * How this maps to the workflows4s variant:
  *   - The `inputFileFound` start signal is kept as a `@SignalMethod` awaited
  *     with `Workflow.await` (the more idiomatic Temporal style would pass the
  *     path as a `@WorkflowMethod` argument; the signal is kept for
  *     equivalence, at the cost of a start-then-signal dance, see
  *     [[DocumentProcessingTemporal.startForInputFile]]).
  *   - There is no mid-workflow interruption, so the
  *     `userCancelsUploadOfInputFile` signal sets a flag polled at phase
  *     boundaries and in the status-polling loop (an externally cancelled
  *     workflow plus a `CancellationScope` is the idiomatic Temporal
  *     alternative, and `WorkflowStub.cancel()` remains available).
  *   - The two virus checks run in parallel via `Async.procedure` (the dbos4s
  *     variant had to run them sequentially).
  *   - The sign-and-upload block is retried with a constant 15-minute interval
  *     via `Workflow.retry`, and the signature validity period is enforced by
  *     re-signing when the upload missed it (times read with the
  *     replay-deterministic `Workflow.currentTimeMillis`).
  *   - Status polling uses a `Workflow.sleep` timer, at most ten times.
  *   - Exceptions crossing the workflow/activity boundary are wrapped in
  *     `ActivityFailure`, so the error report uses the root cause's message.
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

  /** The workflows4s `inputFileFound` start signal. */
  @SignalMethod
  def inputFileFound(path: String): Unit

  /** The workflows4s `userCancelsUploadOfInputFile` interruption signal. */
  @SignalMethod
  def userCancelsUploadOfInputFile(): Unit
}

/** All durable side effects of the batch workflow. Activities are where
  * collaborators run; the workflow body stays deterministic.
  */
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
    // Durable wait for the start signal (or an early cancellation).
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

  /** Read and parse the input file, archive it, process every document, and
    * report the batch result.
    */
  private def processInputFile(path: String): String = {
    val batchJson = activities.readInputFile(path)
    val batch = decode[FileWithDocumentBatch](batchJson)
      .getOrElse(throw Error.ParsingFileFailed("recorded batch could not be decoded"))

    throwIfCancelled()

    activities.archiveInputFile(path)

    // Process documents sequentially, with separate activity records per document id.
    batch.documents.foreach { document =>
      throwIfCancelled()
      processIndividualDocument(document)
    }

    activities.reportResultSuccess(path)
    s"processed ${batch.documents.size} document(s)"
  }

  private def processIndividualDocument(document: DocumentFromInputFile): Unit = {
    val documentId = document.documentId

    // The two virus checks run in parallel, as in the workflows4s variant.
    val virusCheck1 = Async.procedure(() => activities.checkForVirus1(documentId, document.content))
    val virusCheck2 = Async.procedure(() => activities.checkForVirus2(documentId, document.content))
    virusCheck1.get()
    virusCheck2.get()

    signAndUploadWithinSignatureValidity(document)
    pollUploadStatus(documentId)
  }

  /** Sign the document and upload it. The whole sign-and-upload block is
    * retried with a constant 15-minute interval (the workflows4s `retryIn`),
    * and if the upload missed the signature validity period it is signed
    * again (the workflows4s timeout interruption that restarts the block).
    */
  private def signAndUploadWithinSignatureValidity(document: DocumentFromInputFile): Unit = {
    val validityMs = EncryptionService.signatureValidityPeriod.toMillis
    var uploadedWithinValidity = false
    while !uploadedWithinValidity do {
      val (signedAtEpochMs, signature) = Workflow.retry(
        RetryOptions.newBuilder()
          .setInitialInterval(UploadRetryInterval.toJava)
          .setBackoffCoefficient(1.0)
          .setMaximumAttempts(0) // unlimited, as in the workflows4s variant
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

  /** Poll the processing status of an upload, at most [[MaxPollAttempts]] times
    * with a durable 15-minute timer in between, until it is acknowledged as
    * error free.
    */
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

  /** Check the cancellation flag set by the `userCancelsUploadOfInputFile`
    * signal.
    */
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

/** Activity implementations: where the collaborators' side effects run. */
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

  /** Start a batch workflow for an input file: starts the workflow under the
    * file id and delivers the `inputFileFound` start signal (Temporal workflows
    * take arguments, so passing the path to `processBatch` would be more
    * idiomatic; the signal is kept to match the workflows4s variant).
    */
  def startForInputFile(client: WorkflowClient, inputFilePath: Path): WorkflowStub = {
    val workflow = newWorkflowStub(client, inputFilePath)
    WorkflowClient.start(() => workflow.processBatch())
    workflow.inputFileFound(inputFilePath.toString)
    WorkflowStub.fromTyped(workflow)
  }

  /** Ask a running batch workflow to cancel: delivers the
    * `userCancelsUploadOfInputFile` signal the workflow polls for between
    * phases.
    */
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
