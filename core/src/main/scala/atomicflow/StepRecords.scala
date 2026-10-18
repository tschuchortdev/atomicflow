package atomicflow

import java.time.Instant

/** A stored step record, read without acquiring an execution lease.
  * `state` is one of `Started`, `Succeeded`, `Failed`; `statePayload`
  * is the raw, encoded outcome; `inputFingerprints` is the deterministic
  * encoding of the step's named inputs.
  */
final case class StoredStep(
    state: StoredStep.State,
    statePayload: SerializedValue,
    inputFingerprints: InputFingerprints,
    expiresAt: Option[Instant]
)

object StoredStep {

  /** The `state_kind` of a `workflow_steps` row; stored as `toString`.
    */
  enum State {
    case Started, Succeeded, Failed
  }

  object State {

    /** Parses the stored DB value. Only the three written values are valid. */
    def fromString(s: String): State =
      values
        .find(_.toString == s)
        .getOrElse(throw new IllegalArgumentException(s"Unknown step state_kind '$s'"))
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
