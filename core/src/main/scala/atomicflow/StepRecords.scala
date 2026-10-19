package atomicflow

import java.time.Instant

/** One row of `workflow_steps` as read or written by the runtime-internals SPI.
  * A row pairs a [[StepState]] (which records which step kind wrote the
  * row) with
  * the comparison data shared by every row: the encoded fingerprints of the
  * step's named inputs, and the expiry of the cached result.
  */
final case class StepRow(
    state: StepState,
    inputFingerprints: InputFingerprints,
    expiresAt: Option[Instant]
)

/** The durable state stored in a `workflow_steps` row. The state's subgroup is
  * the step kind that wrote the row; it is nothing but the states that
  * step kind actually persists. A state that is not legal for its kind cannot
  * be constructed, and [[StepState.fromPersisted]] rejects such stored data.
  */
sealed trait StepState

object StepState {

  /** The state of an ordinary step row (`StepKind.Step`): both
    * `Step.atLeastOnce` and `Step.atMostOnce` rows.
    */
  sealed trait SimpleStep extends StepState

  object SimpleStep {
    /** The step body started executing but has not yet committed an outcome. */
    case object Started extends SimpleStep

    /** The step body failed with a retry scheduled: `bookkeeping` is the opaque
      * retry state and `deadline` is the earliest instant the body may run
      * again.
      */
    final case class RetryPending(bookkeeping: SerializedValue, deadline: Instant) extends SimpleStep

    /** The step body completed; the payload is the serialized result. */
    final case class Succeeded(payload: SerializedValue) extends SimpleStep

    /** The step body failed without a retry scheduled; the payload is the
      * serialized failure.
      */
    final case class Failed(payload: SerializedValue) extends SimpleStep
  }

  /** The stored state of an await row (`StepKind.Await`, a state resolved by
    * `Step.evaluateWait`): the payload is the serialized await result. A
    * suspended await persists no row at all — its subscriptions carry the
    * waiting — so a resolved row is the only state that occurs.
    */
  final case class Await(payload: SerializedValue) extends StepState

  /** The stored state of a first-to-run row (`StepKind.FirstToRun`, the
    * `Step.firstToRunWithoutSuspension` winner marker): the result is the
    * serialized winning branch value.
    */
  final case class FirstToRun(result: SerializedValue) extends StepState

  /** The stored state of a restartable-region row (`StepKind.RestartableRegion`):
    * the current generation's committed restart count and encoded user state.
    */
  final case class Region(snapshot: RegionState) extends StepState

  /** The stored `state_kind` texts. Exported for SQL that must reference them
    * by name; [[fromPersisted]] and [[StepState.persistedColumns]] defer to the
    * same texts.
    */
  private val StartedText: String        = "Started"
  private val RetryPendingText: String   = "RetryPending"
  private val SucceededText: String      = "Succeeded"
  private val FailedText: String         = "Failed"
  private val RegionStateText: String    = "RestartableRegion"

  /** Rejects a stored state whose kind is not legal for its row's kind. */
  private def illegalRowState(rowKindText: String, stateKindText: String): Nothing =
    throw new IllegalArgumentException(s"state_kind '$stateKindText' is not valid for a '$rowKindText' row")

  /** Single parse point for stored rows: maps the row's stored columns to a
    * [[StepState]]. `rowKindText` selects which state kinds are legal; any
    * other value, and any state kind that is not legal for the row's kind, is a
    * parse error.
    */
  def fromPersisted(
      rowKindText: String,
      stateKindText: String,
      payload: String,
      retryDeadline: Option[Instant]
  ): StepState =
    rowKindText match {
      case StepKind.Step =>
        stateKindText match {
          case StartedText =>
            SimpleStep.Started
          case RetryPendingText =>
            SimpleStep.RetryPending(
              SerializedValue.of(payload),
              retryDeadline.getOrElse(
                throw new IllegalArgumentException("RetryPending step row is missing its retry_deadline")
              )
            )
          case SucceededText => SimpleStep.Succeeded(SerializedValue.of(payload))
          case FailedText    => SimpleStep.Failed(SerializedValue.of(payload))
          case other         => illegalRowState(rowKindText, other)
        }
      case StepKind.Await =>
        stateKindText match {
          case SucceededText => Await(SerializedValue.of(payload))
          case other         => illegalRowState(rowKindText, other)
        }
      case StepKind.FirstToRun =>
        stateKindText match {
          case SucceededText => FirstToRun(SerializedValue.of(payload))
          case other         => illegalRowState(rowKindText, other)
        }
      case StepKind.RestartableRegion =>
        stateKindText match {
          case RegionStateText =>
            Region(RegionState.decode(payload))
          case other =>
            illegalRowState(rowKindText, other)
        }
      case other =>
        throw new IllegalArgumentException(s"Unknown step_kind '$other'")
    }

  extension (state: StepState)

    /** The stored `step_kind` text of the row this state belongs to. */
    def rowKindText: String = state match {
      case _: SimpleStep => StepKind.Step
      case _: Await      => StepKind.Await
      case _: FirstToRun => StepKind.FirstToRun
      case _: Region     => StepKind.RestartableRegion
    }

    /** The stored `(state_kind, state_payload, retry_deadline)` columns of the
      * row this state belongs to.
      */
    def persistedColumns: (String, String, Option[Instant]) = state match {
      case SimpleStep.Started =>
        (StartedText, "", None)
      case SimpleStep.RetryPending(bookkeeping, deadline) =>
        (RetryPendingText, bookkeeping.raw, Some(deadline))
      case SimpleStep.Succeeded(payload) =>
        (SucceededText, payload.raw, None)
      case SimpleStep.Failed(payload) =>
        (FailedText, payload.raw, None)
      case Await(payload) =>
        (SucceededText, payload.raw, None)
      case FirstToRun(result) =>
        (SucceededText, result.raw, None)
      case Region(snapshot) =>
        (RegionStateText, RegionState.encode(snapshot), None)
    }

    /** Some(deadline) only for a state that has armed a retry timer; every
      * other state has no timer at the row's site.
      */
    def pendingRetryDeadline: Option[Instant] = state match {
      case SimpleStep.RetryPending(_, deadline) => Some(deadline)
      case _                                    => None
    }
}

/** One durable `TimerFired` event matching a pending timer subscription of an
  * awaiting site, keyed by the timer id.
  */
final case class AwaitTimerCandidate(
    sequenceId: Long,
    timerId: java.util.UUID,
    createdAt: java.time.Instant
)

/** Identifies the step record where a wait's result will be stored, including
  * its input fingerprints and expiry.
  */
final case class WaitSite(
    stepId: StepId,
    stepVersion: Long,
    inputFingerprints: InputFingerprints,
    expiresAt: Option[Instant]
)

/** One subscriber of a wait-site: how its subscription is registered in the
  * corresponding table. `subscriberKey` names the subscriber within the site
  * (`""` for single-subscriber awaits, the member's user-chosen key for races).
  */
enum Subscriber {
  case Signal(subscriberKey: String, signalKey: SignalKey)
  case Timer(subscriberKey: String, deadline: Instant)
  case Completion(
      subscriberKey: String,
      completedWorkflowId: WorkflowId,
      completedWorkflowInstanceKey: WorkflowInstanceKey,
      completedScope: ScopePath
  )

  /** The subscriber's key within its wait-site. */
  def key: String = this match {
    case Signal(subscriberKey, _)           => subscriberKey
    case Timer(subscriberKey, _)            => subscriberKey
    case Completion(subscriberKey, _, _, _) => subscriberKey
  }
}

/** A stored event available to a wait's subscriber. Events from all subscribers
  * are ordered by `sequenceId`, so the earliest matching event wins.
  */
final case class SubscriberMatch(
    subscriberKey: String,
    sequenceId: Long,
    payload: SerializedValue,
    createdAt: Instant
)

/** The outcome chosen by a wait-site's `decide` function: the serialized
  * result persisted to the site's step row, plus (only when the winning
  * candidate is a signal) the exact key whose shared cursor advances to the
  * winning event's `sequenceId`.
  */
final case class WaitResolution(
    payload: SerializedValue,
    advanceSignalCursor: Option[(SignalKey, Long)]
)

/** The persisted state of a `Workflow.restartable`/`Workflow.loop` region:
  * its committed `restartCount` plus the encoded user state of the current
  * generation.
  */
final case class RegionState(restartCount: Long, serializedState: SerializedValue)

object RegionState {

  /** The stored JSON shape of a region row's `state_payload`. */
  private case class Payload(count: Long, state: String) derives upickle.default.ReadWriter

  /** Encodes a region state as the `state_payload` of a region step row. */
  def encode(state: RegionState): String =
    upickle.default.write(Payload(state.restartCount, state.serializedState.raw))

  /** Decodes a region row's `state_payload`. */
  def decode(payload: String): RegionState = {
    val stored = upickle.default.read[Payload](payload)
    RegionState(stored.count, SerializedValue.of(stored.state))
  }
}
