package example

import cats.effect.IO
import cats.effect.syntax.all
import cats.implicits.*
import cats.mtl.syntax.all
import cats.syntax.all
import example.DocumentProcessingWorkflow4s.Event.{DocumentSigningResult, UploadStatusPollingResult, VirusCheck1Result}
import example.DocumentUploadEndpoint.ProcessingStatus.NoInfo
import workflows4s.wio
import workflows4s.wio.{SignalDef, WIO, WorkflowContext}

import java.nio.file.Path
import java.time.Instant
import scala.concurrent.duration.{DurationInt, FiniteDuration}
import scala.util.NotGiven
import scala.util.control.NonFatal

class DocumentProcessingWorkflow4s(virusCheckService: VirusCheckService,
                                   encryptionService: EncryptionService,
                                   documentUploadEndpoint: DocumentUploadEndpoint,
                                   resultReporter: ResultReporter) {
  import DocumentProcessingWorkflow4s.*
  import Ctx.WIO

  lazy val workflow = (
    readFile >>>
      doVirusChecksInParallel >>>
      (signFile >>>
      uploadFile)
        .retry(???) >>>
      checkUploadStatusLoop >>>
      reportResult >>>
      WIO.end
  ).handleErrorWith(reportResult)

  private lazy val readFile: WIO[State.NotStarted.type, Error.InvalidInputFile, State.FileRead] = ???

  private lazy val doVirusChecksInParallel: WIO[State.FileRead, Error, State.AllVirusChecksPassed] =
    WIO.parallel.taking[State.FileRead]
      .withInterimState[State.WaitingForVirusChecks](initial = stateBefore => State.WaitingForVirusChecks())
      .withElement(virusCheck1, incorporatedWith = (interimState, pathState) => interimState)
      .withElement(virusCheck2, incorporatedWith = (interimState, pathState) => interimState)
      .producingOutputWith {
        case (State.VirusCheckPassed, State.VirusCheckPassed) => State.AllVirusChecksPassed()
      }

  private lazy val virusCheck1: WIO[State.FileRead, Error.VirusCheck1Failed, State.VirusCheckPassed.type] =
    WIO.runIO { (state: State.FileRead) =>
      virusCheckService.checkForVirus1.map {
        case true => Event.VirusCheck1Result.Completed
        case false => Event.VirusCheck1Result.Failed
      }
    }
      .handleEventWithError { (state, evt) =>
        evt match {
          case Event.VirusCheck1Result.Completed => State.VirusCheckPassed.asRight
          case Event.VirusCheck1Result.Failed => Error.VirusCheck1Failed().asLeft
        }
      }
      .autoNamed()

  private lazy val virusCheck2: WIO[State.FileRead, Error.VirusCheck2Failed, State.VirusCheckPassed.type] =
    WIO.runIO { (state: State.FileRead) =>
        virusCheckService.checkForVirus2.map {
          case true => Event.VirusCheck2Result.Completed
          case false => Event.VirusCheck2Result.Failed
        }
      }
      .handleEventWithError { (state, evt) =>
        evt match {
          case Event.VirusCheck2Result.Completed => State.VirusCheckPassed.asRight
          case Event.VirusCheck2Result.Failed => Error.VirusCheck2Failed().asLeft
        }
      }
      .autoNamed()

  private lazy val signFile: WIO[State.AllVirusChecksPassed, Error.SigningFailed, State.FileSigned] =
    WIO.runIO { (state: State.AllVirusChecksPassed) =>
      encryptionService.signDocument(state.fileContent)
        .map(Event.DocumentSigningResult.Signed(_))
        .recover { case NonFatal(e) => Event.DocumentSigningResult.Failed(e.toString) }
    }
      .handleEventWithError { (state, evt) =>
        evt match {
          case DocumentSigningResult.Signed(doc) => State.FileSigned(state.fileId, doc).asRight
          case DocumentSigningResult.Failed(msg) => Error.SigningFailed(msg).asLeft
        }
      }
      .autoNamed()

  private lazy val uploadFile: WIO[State.FileSigned, Error.DownstreamRejectedUpload, State.PollingForUploadStatus] =
    WIO.runIO { (state: State.FileSigned) =>
      documentUploadEndpoint.uploadDocumentForProcessing(state.fileId, state.fileContentWithSignature)
        .map {
          case Right(()) => Event.UploadResult.UploadAccepted
          case Left(e) => Event.UploadResult.UploadRejected
        }
        //.recover { case NonFatal(e) => ??? }
    }
      .handleEventWithError { (state, evt) => ??? }
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
      documentUploadEndpoint.checkUploadProcessingStatus(s.uploadId).map {
        case DocumentUploadEndpoint.ProcessingStatus.NoInfo => Event.UploadStatusPollingResult.NoInfo
        case DocumentUploadEndpoint.ProcessingStatus.ProcessedSuccessfully => Event.UploadStatusPollingResult.UploadProcessedSuccessfully
        case DocumentUploadEndpoint.ProcessingStatus.ProcessedWithErrors => Event.UploadStatusPollingResult.UploadProcessedWithErrors("")
      }
  }.handleEventWithError[
    Error.UploadProcessedWithErrors | Error.UploadStatusTimeoutExceeded.type,
    State.PollingForUploadStatus | State.UploadProcessedSuccessfully
  ] { (state, event) =>
    event match {
      case UploadStatusPollingResult.NoInfo => state.copy(timesRetried = state.timesRetried + 1).asRight
      case UploadStatusPollingResult.MaxRetriesReached => Error.UploadStatusTimeoutExceeded.asLeft
      case UploadStatusPollingResult.UploadProcessedSuccessfully => State.UploadProcessedSuccessfully().asRight
      case UploadStatusPollingResult.UploadProcessedWithErrors(msg) => Error.UploadProcessedWithErrors().asLeft
    }
  }.autoNamed()

  private lazy val reportResult: WIO[
    State.UploadProcessedSuccessfully | (State, Error),
    Nothing,
    State.ResultReported
  ] = WIO.runIO {
    case s: State.UploadProcessedSuccessfully =>
      resultReporter.reportResultSuccess().as(Event.ResultReported)

    case (s: State, e: Error) =>
      resultReporter.reportResultError().as(Event.ResultReported)
  }
    .handleEvent { (_, _: Event.ResultReported.type) => State.ResultReported() }
    .autoNamed()
}
object DocumentProcessingWorkflow4s {
  object Ctx extends WorkflowContext {
    override type Event = DocumentProcessingWorkflow4s.Event
    override type State = DocumentProcessingWorkflow4s.State
  }



  sealed trait State {
    val retryData: Option[RetryData]
  }

  object State {
    object NotStarted extends State
    case class FileRead(inputFile: InputFile) extends State
    case class WaitingForVirusChecks() extends State
    object VirusCheckPassed extends State
    case class AllVirusChecksPassed(fileId: String, fileContent: Array[Byte]) extends State
    case class FileSigned(fileId: String, fileContentWithSignature: Array[Byte]) extends State
    case class PollingForUploadStatus(uploadId: String, timesRetried: Int) extends State
    case class UploadProcessedSuccessfully() extends State
    case class ResultReported() extends State
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

    enum UploadResult extends Event {
      case UploadAccepted
      case UploadRejected
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

    object ResultReported extends Event
  }

  sealed trait Error

  object Error {
    case class InvalidInputFile(msg: String = "") extends Error
    case class VirusCheck1Failed(msg: String = "") extends Error
    case class VirusCheck2Failed(msg: String = "") extends Error
    case class SigningFailed(msg: String = "") extends Error
    case class DownstreamRejectedUpload(msg: String = "") extends Error
    case class UploadProcessedWithErrors(msg: String = "") extends Error
    object UploadStatusTimeoutExceeded extends Error

  }

  object Signals {
    val inputFileFound: SignalDef[InputFileFound, Unit] = SignalDef()

    case class InputFileFound(path: Path)

    case class UserCancelsUploadOfInputFile(path: Path)
  }
}

trait HasRetryData {
  val retryData: Option[RetryData]
}

case class RetryData(retryCount: Int = 0, lastRetry: Instant)

extension [In <: HasRetryData, Err, Out, Ctx <: WorkflowContext](wio: WIO[In, Err, Out, Ctx])
  def retryWithExponentialBackoff(initialDelay: FiniteDuration)(shouldRetry: Throwable => Boolean)
    : WIO[In, Err, Out, Ctx] = wio.retry { (throwable, state, now) =>
    if (!shouldRetry(throwable))
      None.pure[IO]
    else
      ???
  }