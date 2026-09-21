package atomicflow

import java.time.Instant

/** The durable facts of one workflow_steps row, as read without any lease or
  * fence. `stateKind` is one of `started`, `succeeded`, `failed`; `statePayload`
  * is the raw, encoded outcome; `inputFingerprints` is the deterministic
  * encoding of the step's named inputs.
  */
final case class StoredStep(
    stateKind: String,
    statePayload: String,
    inputFingerprints: String,
    expiresAt: Option[Instant]
)

/** One durable `TimerFired` event matching a pending timer subscription of an
  * awaiting site, keyed by the timer id.
  */
final case class AwaitTimerCandidate(
    sequenceId: Long,
    timerId: java.util.UUID,
    createdAt: java.time.Instant
)

/** One unhandled `Update` record visible to an awaiting site (a row in
  * `workflow_updates` with `handled_at IS NULL`). The row is identified by
  * `createdAt` plus `idempotencyKey`; `encodedInput` is the update's payload.
  */
final case class UpdateCandidate(
    createdAt: java.time.Instant,
    idempotencyKey: String,
    encodedInput: String
)

/** The outcome of an `awaitUpdate` that satisfies a candidate: which record was
  * handled, the encoded synchronous `response` to persist on the record (what
  * the blocked sender reads), and the encoded `output` to persist on the step
  * row (what the workflow body receives).
  */
final case class AwaitUpdateDecision(
    candidate: UpdateCandidate,
    encodedResponse: String,
    encodedOutput: String
)

/** The durable wait-site a runtime-computed await resolves into: the identity,
  * kind, fingerprints, and TTL of the `workflow_steps` row its result lands in.
  */
final case class WaitSite(
    stepId: StepId,
    stepVersion: Long,
    stepKind: String,
    inputFingerprints: String,
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
      completedScope: String
  )
}

/** One durable fact currently available to one subscriber of a wait-site,
  * selected by global `sequenceId` across all subscribers so the earliest
  * satisfying event wins regardless of kind or workflow tree.
  */
final case class SubscriberMatch(
    subscriberKey: String,
    sequenceId: Long,
    payload: String,
    createdAt: Instant
)

/** The outcome chosen by a wait-site's `decide` function: the serialized
  * result persisted to the site's step row, plus (only when the winning
  * candidate is a signal) the exact key whose shared cursor advances to the
  * winning event's `sequenceId`.
  */
final case class WaitResolution(
    payload: String,
    advanceSignalCursor: Option[(SignalKey, Long)]
)
