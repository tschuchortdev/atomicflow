package example

import cats.effect.IO
import cats.effect.syntax.all
import cats.implicits.*
import cats.mtl.syntax.all
import cats.syntax.all
import example.DocumentProcessingWorkflow4s.Error.SignatureValidityPeriodExceeded
import example.DocumentProcessingWorkflow4s.Event.{DocumentSigningResult, UploadResult, UploadStatusPollingResult, VirusCheck1Result}
import example.DocumentProcessingWorkflow4s.State
import example.DocumentUploadEndpoint.ProcessingStatus.NoInfo
import workflows4s.wio
import workflows4s.wio.internal.WorkflowEmbedding
import workflows4s.wio.{SignalDef, WCEvent, WCState, WIO, WorkflowContext}

import java.nio.file.Path
import java.time.Instant
import scala.concurrent.duration.{Duration, DurationInt, FiniteDuration}
import scala.jdk.DurationConverters.*
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
      signAndUploadWithRetry >>>
      checkUploadStatusLoop >>>
      reportResult >>>
      WIO.end
  ).handleErrorWith(reportResult)

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
        case s: State.PollingForUploadStatus => s.asRight
        case s: State.InterruptedAfterSignatureValidityExceeded => State.AllVirusChecksPassed(s.documentId, s.documentWithoutSignature).asLeft
      }
      .onRestartContinue
      .named(
        conditionName = "Did signing and upload succeed?",
        releaseBranchName = "success",
        restartBranchName = "signature validity exceeded"
      )
      .retryIn { case NonFatal(e) => 15.minutes.toJava }
  }


  private lazy val readFile: WIO[State.NotStarted.type, Error.InvalidInputFile, State.FileRead] = ???

  private lazy val doVirusChecksInParallel: WIO[State.FileRead, Error, State.AllVirusChecksPassed] =
    WIO.parallel.taking[State.FileRead]
      .withInterimState[State.WaitingForVirusChecks](initial = stateBefore => State.WaitingForVirusChecks(???, ???))
      .withElement(virusCheck1, incorporatedWith = (interimState, pathState) => interimState)
      .withElement(virusCheck2, incorporatedWith = (interimState, pathState) => interimState)
      .producingOutputWith {
        case (State.VirusCheckPassed, State.VirusCheckPassed) => State.AllVirusChecksPassed(???, ???)
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

  private lazy val signFile: WIO[State.AllVirusChecksPassed, Error.SigningFailed, State.DocumentSigned] =
    WIO.runIO { (state: State.AllVirusChecksPassed) =>
      encryptionService.signDocument(state.documentContent)
        .map(Event.DocumentSigningResult.Signed(_))
        .recover { case NonFatal(e) => Event.DocumentSigningResult.Failed(e.toString) }
    }
      .handleEventWithError { (state, evt) =>
        evt match {
          case DocumentSigningResult.Signed(docWithSignature) =>
            State.DocumentSigned(state.documentId, state.documentContent, docWithSignature).asRight

          case DocumentSigningResult.Failed(msg) =>
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
          case UploadResult.UploadAccepted => State.PollingForUploadStatus(documentId = state.documentId, timesRetried = 0).asRight
          case UploadResult.UploadRejected(reason) => Error.DownstreamRejectedUpload(reason).asLeft
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
      case UploadStatusPollingResult.NoInfo => state.copy(timesRetried = state.timesRetried + 1).asRight
      case UploadStatusPollingResult.MaxRetriesReached => Error.UploadStatusTimeoutExceeded.asLeft
      case UploadStatusPollingResult.UploadProcessedSuccessfully => State.UploadProcessedSuccessfully(state.documentId).asRight
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


  /*object SignUploadWorkflow {
    object Ctx extends WorkflowContext {
      override type State = SignUploadWorkflow.State
      override type Event = SignUploadWorkflow.Event
    }

    sealed trait State
    object State {
      case class SigningDocument(documentId: String, documentContent: Array[Byte]) extends State
      case class UploadingDocument(documentId: String, documentContentWithSignature: Array[Byte]) extends State
      case class Finished(documentId: String) extends State
    }

    sealed trait Event
    object Event {
      object DocumentSigned extends Event
      object UploadSucceeded extends Event
    }

    sealed trait Error
    object Error {
      object SignatureValidityPeriodExceeded extends Error
    }

    val embedding = new WorkflowEmbedding[
      SignUploadWorkflow.Ctx.type,
      DocumentProcessingWorkflow4s.Ctx.type,
      DocumentProcessingWorkflow4s.State.AllVirusChecksPassed
    ] {
      override def convertEvent(e: WCEvent[Ctx.type]): WCEvent[DocumentProcessingWorkflow4s.Ctx.type] = ???

      override def unconvertEvent(e: WCEvent[DocumentProcessingWorkflow4s.Ctx.type]): Option[WCEvent[Ctx.type]] = ???

      override def convertState[In <: WCState[Ctx.type]](innerState: In, input: State.AllVirusChecksPassed): OutputState[In] = ???

      override def unconvertState(outerState: WCState[DocumentProcessingWorkflow4s.Ctx.type]): Option[WCState[Ctx.type]] = ???
    }

  }*/

  sealed trait State
  object State {
    object NotStarted extends State
    case class FileRead(inputFile: FileWithDocumentBatch) extends State
    case class WaitingForVirusChecks(documentId: String, documentContent: Array[Byte]) extends State
    object VirusCheckPassed extends State
    case class AllVirusChecksPassed(documentId: String, documentContent: Array[Byte]) extends State
    case class DocumentSigned(documentId: String, documentWithoutSignature: Array[Byte], documentContentWithSignature: Array[Byte]) extends State
    case class InterruptedAfterSignatureValidityExceeded(documentId: String, documentWithoutSignature: Array[Byte]) extends State
    case class PollingForUploadStatus(documentId: String, timesRetried: Int) extends State
    case class UploadProcessedSuccessfully(documentId: String) extends State
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

    object ResultReported extends Event
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
  }

  object Signals {
    val inputFileFound: SignalDef[InputFileFound, Unit] = SignalDef()

    case class InputFileFound(path: Path)

    case class UserCancelsUploadOfInputFile(path: Path)
  }

  /*extension [Ctx <: WorkflowContext, In <: WCState[Ctx], ErrIn, Out <: WCState[Ctx]](wio: WIO[In, ErrIn, Out, Ctx])
    def retryOnError(decideDelay: (WCState[Ctx], ErrIn) => Option[FiniteDuration]) = {
      Ctx.WIO.getState[In].flatMap { stateBefore =>
        wio.handleErrorWith(
          Ctx.WIO.getState[(WCState[Ctx], ErrIn)].flatMap { case (stateAfter, error: ErrIn) =>
            decideDelay(stateAfter, error) match {
              case Some(delay) => Ctx.WIO.await()
            }
            wio.provideInput(stateBefore)
          }
        )
      }
    }*/
}

