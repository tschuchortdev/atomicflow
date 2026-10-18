package atomicflow

import java.time.Instant

/** A stored step record, read without acquiring an execution lease.
  * `state` is the persisted state of the step row; `inputFingerprints` is the
  * deterministic encoding of the step's named inputs.
  */
final case class StoredStep(
    state: StoredStep.State,
    inputFingerprints: InputFingerprints,
    expiresAt: Option[Instant]
)

object StoredStep {

  /** The persisted state of a step row. Only `Succeeded` and `Failed` carry a
    * payload; `RetryPending` carries the opaque retry bookkeeping and the
    * deadline at which the step body may run again.
    */
  enum State {
    case Started
    case RetryPending(bookkeeping: SerializedValue, deadline: Instant)
    case Succeeded(payload: SerializedValue)
    case Failed(payload: SerializedValue)
  }

  object State {

    /** Reconstructs a persisted state from the row's `state_kind` name together
      * with its `state_payload` and `retry_deadline` columns. Only the four
      * written `state_kind` values are valid; `RetryPending` requires a non-null
      * deadline.
      */
    def fromString(kind: String, payload: SerializedValue, retryDeadline: Option[Instant]): State =
      kind match {
        case "Started" => Started
        case "RetryPending" =>
          RetryPending(
            payload,
            retryDeadline.getOrElse(
              throw new IllegalArgumentException("RetryPending step row is missing its retry_deadline")
            )
          )
        case "Succeeded" => Succeeded(payload)
        case "Failed"    => Failed(payload)
        case other       => throw new IllegalArgumentException(s"Unknown step state_kind '$other'")
      }
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
  * its kind, input fingerprints, and expiry.
  */
final case class WaitSite(
    stepId: StepId,
    stepVersion: Long,
    stepKind: StepKind,
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
