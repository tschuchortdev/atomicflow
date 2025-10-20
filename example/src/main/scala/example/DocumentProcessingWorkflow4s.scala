package example

import cats.effect.IO
import cats.effect.syntax.all
import cats.implicits.*
import cats.mtl.syntax.all
import cats.syntax.all
import io.circe.*
import io.circe.syntax.*
import example.DocumentProcessingWorkflow4s.Event.{FileArchivingResult, FileReadResult}
import example.DocumentUploadEndpoint.ProcessingStatus.NoInfo
import workflows4s.wio
import workflows4s.wio.internal.WorkflowEmbedding
import workflows4s.wio.{SignalDef, SimpleSignalRouter, WCEvent, WCState, WIO, WorkflowContext}

import java.io.{FileNotFoundException, IOException}
import java.nio.file.{Files, Path}
import java.time.Instant
import scala.concurrent.duration.{Duration, DurationInt, FiniteDuration}
import scala.jdk.DurationConverters.*
import scala.util.control.NonFatal

class DocumentProcessingWorkflow4s(archiveDir: Path,
                                   virusCheckService: VirusCheckService,
                                   encryptionService: EncryptionService,
                                   documentUploadEndpoint: DocumentUploadEndpoint,
                                   resultReporter: ResultReporter) {
  import DocumentProcessingWorkflow4s.*
  import Ctx.WIO

  lazy val workflow: WIO[State.NotStarted.type, Nothing, State.ResultReported] = (
    readFile >>>
      archiveFile >>>
      processEachDocument >>>
      reportResult
  ).interruptWith(
      WIO.interruption
        .throughSignal(Signal.userCancelsUploadOfInputFile)
        .handleSync { (_, _) => Event.CancelledByUser }
        .handleEventWithError { (_, _: Event.CancelledByUser.type) => Error.CancelledByUser.asLeft }
        .voidResponse
        .done
    )
    .handleErrorWith(reportResult)

  private lazy val readFile: WIO[State.NotStarted.type, Error.InputFileError, State.InputFileRead] =
    WIO.handleSignal(Signal.inputFileFound)
      .using[State.NotStarted.type]
      .withSideEffects { case (_, signal: Signal.InputFileFound) =>
        IO(Files.readString(signal.path))
          .map { str =>
            try {
              val fileId = FileWithDocumentBatch.idFromFileName(signal.path.getFileName.toString)
              val parsed = FileWithDocumentBatch.fromString(str)
              Event.FileReadResult.Success(signal.path, fileId, parsed)
            } catch { case e: IllegalArgumentException =>
              Event.FileReadResult.ParsingFileFailed(signal.path, e.toString)
            }
          }
          .recover { case e: IOException => Event.FileReadResult.ReadingFailed(signal.path, e) }
      }
      .handleEventWithError { (_: State.NotStarted.type, evt) =>
        evt match {
          case FileReadResult.Success(path, fileId, file) => State.InputFileRead(path, fileId, file).asRight
          case FileReadResult.ReadingFailed(path, e) => Error.InputFileError.FailedToRead(path, e).asLeft
          case FileReadResult.ParsingFileFailed(path, msg) => Error.InputFileError.FailedToParse(path, msg).asLeft
        }
      }
      .voidResponse
      .autoNamed

  private lazy val archiveFile: WIO[State.InputFileRead, Error.InputFileError, State.InputFileArchived] =
    WIO.runIO[State.InputFileRead] { s =>
        def retryOnce[A](a: IO[A]) = a.recoverWith { case NonFatal(_) =>
          IO.sleep(30.seconds) *> a
        }

        retryOnce(
          IO(Files.createDirectories(archiveDir))
            *> IO(Files.move(s.inputFilePath, archiveDir.resolve(s.inputFilePath.getFileName)))
            .as(Event.FileArchivingResult.Success)
            .recover {
              case _: FileNotFoundException => Event.FileArchivingResult.Success
              case e: IOException => Event.FileArchivingResult.Failed(e)
            }
        )
      }
      .handleEventWithError { (s, evt) =>
        evt match {
          case FileArchivingResult.Success => State.InputFileArchived(s.inputFilePath, s.fileId, s.inputFile).asRight
          case FileArchivingResult.Failed(e) => Error.InputFileError.FailedToArchive(s.inputFilePath, e).asLeft
        }
      }
      .autoNamed()

  private val perDocumentWorkflow =
    new PerDocumentWorkflow(virusCheckService, encryptionService, documentUploadEndpoint).workflow

  // TODO: Compensating actions to delete document uploads when not all uploads were successful

  private lazy val processEachDocument: WIO[State.InputFileArchived, PerDocumentWorkflow.Error, State.AllDocumentsProcessedSuccessfully] =
    WIO.forEach[State.InputFileArchived](_.inputFile.documents.toSet)
      .execute[PerDocumentWorkflow.Ctx.Ctx](perDocumentWorkflow, PerDocumentWorkflow.State.Uninitialized)
      .withEventsEmbeddedThrough(new WorkflowEmbedding.Event {
        override def convertEvent(e: (DocumentFromInputFile, PerDocumentWorkflow.Event)): Event =
          Event.PerDocumentWorkflowEmbeddedEvent(e._1, e._2)

        override def unconvertEvent(e: Event): Option[(DocumentFromInputFile, PerDocumentWorkflow.Event)] = e match {
          case Event.PerDocumentWorkflowEmbeddedEvent(elem, event) => Some((elem, event))
          case _ => None
        }
      })
      .withInterimState[State.ProcessingEachDocument] { (inputState: State.InputFileArchived) =>
        State.ProcessingEachDocument()
      }
      .incorporatingChangesThrough {
        (elem: DocumentFromInputFile, innerState: PerDocumentWorkflow.State, interimState: State.ProcessingEachDocument) =>
          // Don't keep track of per-document states in the parent workflow state because we are only interested in reporting
          // success/failure of the batch as a whole. Assume that the workflow library is keeping track of the per-document states internally.
          interimState
      }
      .withOutputBuiltWith {
        (inputState: State.InputFileArchived,
         elemStates: Map[DocumentFromInputFile, PerDocumentWorkflow.State.UploadProcessedSuccessfully]
        ) => State.AllDocumentsProcessedSuccessfully(inputState.inputFilePath, inputState.fileId)
      }
      .withSignalsWrappedWith(new SimpleSignalRouter[DocumentFromInputFile])
      .autoNamed()

  private lazy val reportResult: WIO[
    State.AllDocumentsProcessedSuccessfully | (State, Error | PerDocumentWorkflow.Error),
    Nothing,
    State.ResultReported
  ] = WIO.runIO {
    case s: State.AllDocumentsProcessedSuccessfully =>
      resultReporter.reportResultSuccess(s.inputFilePath).as(Event.ResultReported)

    case (_, _) => IO(???).as(Event.ResultReported) // TODO Error handling

    /*case (_: State, _: Error.CancelledByUser.type) =>
      resultReporter.reportCancellation().as(Event.ResultReported)

    case (_: State, _: Error) =>
      resultReporter.reportResultError().as(Event.ResultReported)*/
  }
    .handleEvent { (_, _: Event.ResultReported.type) => State.ResultReported() }
    .autoNamed()
}
object DocumentProcessingWorkflow4s {
  object Ctx extends WorkflowContext {
    override type Event = DocumentProcessingWorkflow4s.Event
    override type State = DocumentProcessingWorkflow4s.State
  }

  sealed trait State
  object State {
    object NotStarted extends State
    case class InputFileRead(inputFilePath: Path, fileId: String, inputFile: FileWithDocumentBatch) extends State
    case class InputFileArchived(inputFilePath: Path, fileId: String, inputFile: FileWithDocumentBatch) extends State
    case class ProcessingEachDocument() extends State
    case class AllDocumentsProcessedSuccessfully(inputFilePath: Path, fileId: String) extends State
    case class ResultReported() extends State
  }

  sealed trait Event
  object Event {
    enum FileReadResult extends Event {
      case ReadingFailed(filePath: Path, e: IOException)
      case ParsingFileFailed(filePath: Path, msg: String)
      case Success(filePath: Path, fileId: String, file: FileWithDocumentBatch)
    }

    enum FileArchivingResult extends Event {
      case Success
      case Failed(e: IOException)
    }

    case class PerDocumentWorkflowEmbeddedEvent(elem: DocumentFromInputFile, event: PerDocumentWorkflow.Event) extends Event

    object CancelledByUser extends Event
    object ResultReported extends Event
  }

  sealed trait Error
  object Error {

    enum InputFileError extends Error {
      case FailedToParse(filePath: Path, msg: String = "")
      case FailedToRead(filePath: Path, e: IOException)
      case FailedToArchive(filePath: Path, e: IOException)
    }

    object CancelledByUser extends Error
  }

  object Signal {
    val inputFileFound: SignalDef[InputFileFound, Unit] = SignalDef()
    case class InputFileFound(path: Path)

    val userCancelsUploadOfInputFile: SignalDef[UserCancelsUploadOfInputFile, Unit] = SignalDef()
    case class UserCancelsUploadOfInputFile()
  }
}

class PerDocumentWorkflow(virusCheckService: VirusCheckService,
                          encryptionService: EncryptionService,
                          documentUploadEndpoint: DocumentUploadEndpoint) {
  import PerDocumentWorkflow.Ctx.WIO
  import PerDocumentWorkflow.*

  lazy val workflow: WIO[DocumentFromInputFile, Error, State.UploadProcessedSuccessfully] = {
    doVirusChecksInParallel >>>
      signAndUploadWithRetry >>>
      checkUploadStatusLoop
  }

  private lazy val signAndUploadWithRetry: WIO[State.AllVirusChecksPassed, Error, State.PollingForUploadStatus] = {
    WIO.repeat(
        signFile >>>
          uploadFile.interruptWith(
            // Signature is only valid for x amount of time. Interrupt and restart (signFile >> uploadFile) if the validity period is exceeded.
            WIO.interruption
              .throughTimeout(EncryptionService.signatureValidityPeriod)
              .persistStartThrough(started => Event.UploadAndSignInterruption.AwaitingTimeout(started.at))(_.startedAt)
              .persistReleaseThrough(released => Event.UploadAndSignInterruption.ReachedTimeout(released.at))(_.releasedAt)
              .autoNamed
              .andThen(_ >>> WIO.pure.makeFrom[State].value {
                case s: State.DocumentSigned =>
                  State.InterruptedAfterSignatureValidityExceeded(s.documentId, s.documentWithoutSignature)
                    : State.PollingForUploadStatus | State.InterruptedAfterSignatureValidityExceeded
                case _ => throw AssertionError("impossible")
              }.done)
          )
      ).untilRight {
        case s: State.PollingForUploadStatus =>
          s.asRight

        case s: State.InterruptedAfterSignatureValidityExceeded =>
          State.AllVirusChecksPassed(s.documentId, s.documentWithoutSignature).asLeft
      }
      .onRestartContinue
      .named(
        conditionName = "Did signing and upload succeed?",
        releaseBranchName = "success",
        restartBranchName = "signature validity exceeded"
      )
      .retryIn { case NonFatal(e) => 15.minutes.toJava } // TODO: Only unlimited retries are possible >:O
  }


  private lazy val doVirusChecksInParallel: WIO[DocumentFromInputFile, Error, State.AllVirusChecksPassed] =
    WIO.parallel.taking[DocumentFromInputFile]
      .withInterimState[State.WaitingForVirusChecks](initial =
        stateBefore => State.WaitingForVirusChecks(stateBefore.documentId, stateBefore.content)
      )
      .withElement(virusCheck1, incorporatedWith = (interimState, pathState) => interimState)
      .withElement(virusCheck2, incorporatedWith = (interimState, pathState) => interimState)
      .producingOutputWith {
        case (State.VirusCheckPassed(docId1, content1), State.VirusCheckPassed(docId2, content2)) =>
          assert(docId1 == docId2)
          //noinspection EqualityToSameElements
          assert(content1 == content2)
          State.AllVirusChecksPassed(docId1, content1)
      }

  private lazy val virusCheck1: WIO[DocumentFromInputFile, Error.VirusCheck1Failed, State.VirusCheckPassed] =
    WIO.runIO { (state: DocumentFromInputFile) =>
        virusCheckService.checkForVirus1(state.content).map {
          case true => Event.VirusCheck1Result.Completed
          case false => Event.VirusCheck1Result.Failed
        }
      }
      .handleEventWithError { (s, evt) =>
        evt match {
          case Event.VirusCheck1Result.Completed => State.VirusCheckPassed(s.documentId, s.content).asRight
          case Event.VirusCheck1Result.Failed => Error.VirusCheck1Failed().asLeft
        }
      }
      .autoNamed()

  private lazy val virusCheck2: WIO[DocumentFromInputFile, Error.VirusCheck2Failed, State.VirusCheckPassed] =
    WIO.runIO { (state: DocumentFromInputFile) =>
        virusCheckService.checkForVirus2(state.content).map {
          case true => Event.VirusCheck2Result.Completed
          case false => Event.VirusCheck2Result.Failed
        }
      }
      .handleEventWithError { (s, evt) =>
        evt match {
          case Event.VirusCheck2Result.Completed => State.VirusCheckPassed(s.documentId, s.content).asRight
          case Event.VirusCheck2Result.Failed => Error.VirusCheck2Failed().asLeft
        }
      }
      .autoNamed()

  private lazy val signFile: WIO[State.AllVirusChecksPassed, Error.SigningFailed, State.DocumentSigned] =
    WIO.runIO { (state: State.AllVirusChecksPassed) =>
        encryptionService.signDocument(state.documentContent)
          .map(Event.DocumentSigningResult.Signed(_))
          .recover { case NonFatal(e) => Event.DocumentSigningResult.Failed(e.toString) }
      }
      .handleEventWithError { (state, evt) =>
        evt match {
          case Event.DocumentSigningResult.Signed(docWithSignature) =>
            State.DocumentSigned(state.documentId, state.documentContent, docWithSignature).asRight

          case Event.DocumentSigningResult.Failed(msg) =>
            Error.SigningFailed(msg).asLeft
        }
      }
      .autoNamed()

  private lazy val uploadFile: WIO[State.DocumentSigned, Error.DownstreamRejectedUpload, State.PollingForUploadStatus] =
    WIO.runIO { (state: State.DocumentSigned) =>
        documentUploadEndpoint.uploadDocumentForProcessing(state.documentId, state.documentContentWithSignature)
          .map {
            case Right(()) => Event.UploadResult.UploadAccepted
            case Left(e) => Event.UploadResult.UploadRejected(e.toString)
          }
      }
      .handleEventWithError { (state, evt) =>
        evt match {
          case Event.UploadResult.UploadAccepted => State.PollingForUploadStatus(documentId = state.documentId, timesRetried = 0).asRight
          case Event.UploadResult.UploadRejected(reason) => Error.DownstreamRejectedUpload(reason).asLeft
        }
      }
      .autoNamed()

  private lazy val checkUploadStatusLoop: WIO[
    State.PollingForUploadStatus,
    Error.UploadProcessedWithErrors | Error.UploadStatusTimeoutExceeded.type,
    State.UploadProcessedSuccessfully
  ] =
    WIO.repeat(checkUploadStatus)
      .untilRight {
        // Note: untilSome can not be used here because it makes the type of onRestart not line up
        case s: State.UploadProcessedSuccessfully => Right(s)
        case s: State.PollingForUploadStatus => Left(s)
      }
      .onRestart(WIO.await[State.PollingForUploadStatus](15.minutes)
        .persistStartThrough(s => Event.UploadStatusCheckRetryTimer.Started(s.at))(_.startedAt)
        .persistReleaseThrough(r => Event.UploadStatusCheckRetryTimer.Released(r.at))(_.releasedAt)
        .named("Await retry")
      )
      .named(
        conditionName = "Is upload acknowledged as error free?",
        releaseBranchName = "Yes",
        restartBranchName = "No"
      )

  private lazy val checkUploadStatus: WIO[
    State.PollingForUploadStatus,
    Error.UploadProcessedWithErrors | Error.UploadStatusTimeoutExceeded.type,
    State.PollingForUploadStatus | State.UploadProcessedSuccessfully
  ] = WIO.runIO { (s: State.PollingForUploadStatus) =>
    val maxNumRetries = 10
    if (s.timesRetried >= maxNumRetries)
      IO.pure(Event.UploadStatusPollingResult.MaxRetriesReached)
    else
      documentUploadEndpoint.checkUploadProcessingStatus(s.documentId).map {
        case DocumentUploadEndpoint.ProcessingStatus.NoInfo => Event.UploadStatusPollingResult.NoInfo
        case DocumentUploadEndpoint.ProcessingStatus.ProcessedSuccessfully => Event.UploadStatusPollingResult.UploadProcessedSuccessfully
        case DocumentUploadEndpoint.ProcessingStatus.ProcessedWithErrors => Event.UploadStatusPollingResult.UploadProcessedWithErrors("")
      }.recover {
        case NonFatal(e) => Event.UploadStatusPollingResult.NoInfo
      }
  }.handleEventWithError[
    Error.UploadProcessedWithErrors | Error.UploadStatusTimeoutExceeded.type,
    State.PollingForUploadStatus | State.UploadProcessedSuccessfully
  ] { (state, event) =>
    event match {
      case Event.UploadStatusPollingResult.NoInfo => state.copy(timesRetried = state.timesRetried + 1).asRight
      case Event.UploadStatusPollingResult.MaxRetriesReached => Error.UploadStatusTimeoutExceeded.asLeft
      case Event.UploadStatusPollingResult.UploadProcessedSuccessfully => State.UploadProcessedSuccessfully(state.documentId).asRight
      case Event.UploadStatusPollingResult.UploadProcessedWithErrors(msg) => Error.UploadProcessedWithErrors().asLeft
    }
  }.autoNamed()
}

object PerDocumentWorkflow {
  object Ctx extends WorkflowContext {
    override type State = PerDocumentWorkflow.State
    override type Event = PerDocumentWorkflow.Event
  }

  sealed trait State
  object State {
    object Uninitialized extends State
    case class WaitingForVirusChecks(documentId: String, documentContent: Array[Byte]) extends State
    case class VirusCheckPassed(documentId: String, documentContent: Array[Byte]) extends State
    case class AllVirusChecksPassed(documentId: String, documentContent: Array[Byte]) extends State
    case class DocumentSigned(documentId: String, documentWithoutSignature: Array[Byte], documentContentWithSignature: Array[Byte]) extends State
    case class InterruptedAfterSignatureValidityExceeded(documentId: String, documentWithoutSignature: Array[Byte]) extends State
    case class PollingForUploadStatus(documentId: String, timesRetried: Int) extends State
    case class UploadProcessedSuccessfully(documentId: String) extends State
  }

  sealed trait Event
  object Event {
    enum VirusCheck1Result extends Event {
      case Completed, Failed
    }

    enum VirusCheck2Result extends Event {
      case Completed, Failed
    }

    enum DocumentSigningResult extends Event {
      case Signed(doc: Array[Byte])
      case Failed(msg: String = "")
    }

    // Must be a sealed trait and not enum to make type inference work
    sealed trait UploadAndSignInterruption extends Event
    object UploadAndSignInterruption {
      case class AwaitingTimeout(startedAt: Instant) extends UploadAndSignInterruption
      case class ReachedTimeout(releasedAt: Instant) extends UploadAndSignInterruption
    }

    enum UploadResult extends Event {
      case UploadAccepted
      case UploadRejected(reason: String = "")
    }

    enum UploadStatusPollingResult extends Event {
      case NoInfo
      case MaxRetriesReached
      case UploadProcessedSuccessfully
      case UploadProcessedWithErrors(msg: String)
    }

    // Must be a sealed trait and not enum to make type inference work
    sealed trait UploadStatusCheckRetryTimer extends Event
    object UploadStatusCheckRetryTimer {
      case class Started(startedAt: Instant) extends UploadStatusCheckRetryTimer
      case class Released(releasedAt: Instant) extends UploadStatusCheckRetryTimer
    }
  }

  sealed trait Error
  object Error {
    case class InvalidInputFile(msg: String = "") extends Error
    case class VirusCheck1Failed(msg: String = "") extends Error
    case class VirusCheck2Failed(msg: String = "") extends Error
    case class SigningFailed(msg: String = "") extends Error
    object SignatureValidityPeriodExceeded extends Error
    case class DownstreamRejectedUpload(msg: String = "") extends Error
    case class UploadProcessedWithErrors(msg: String = "") extends Error
    object UploadStatusTimeoutExceeded extends Error
    object CancelledByUser extends Error
  }
}

