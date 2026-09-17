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

/** One durable `Signal` event that is currently visible to an awaiting site
  * (i.e. after the instance's shared exact-key cursor).
  */
final case class AwaitSignalCandidate(
    sequenceId: Long,
    payload: String,
    createdAt: Instant
)

/** One durable `TimerFired` event matching a pending timer subscription of an
  * awaiting site, keyed by the subscription id.
  */
final case class AwaitTimerCandidate(
    sequenceId: Long,
    subscriptionId: java.util.UUID,
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

/** A single leaf of a `Step.awaitRace` site, describing how its durable
  * subscription is registered in the corresponding table. The leaf index is the
  * awaitable's position within the race (0..n-1).
  */
sealed trait AwaitRaceLeaf {
  def leafIdx: Int
}

/** A signal leaf: subscribes in `workflow_signal_subscriptions` under its exact
  * key and reads candidates after this instance's shared exact-key cursor.
  */
final case class AwaitRaceSignalLeaf(leafIdx: Int, signalKey: SignalKey) extends AwaitRaceLeaf

/** A timer leaf: subscribes in `workflow_timer_subscriptions` with the absolute
  * `deadline` and reads candidates from fired `TimerFired` events keyed by its
  * subscription id.
  */
final case class AwaitRaceTimerLeaf(leafIdx: Int, deadline: java.time.Instant) extends AwaitRaceLeaf

/** A completion leaf: subscribes in `workflow_completion_subscriptions` for the
  * terminal outcome of another instance and reads candidates from that
  * instance's `WorkflowCompleted` event (`event_key = ''`).
  */
final case class AwaitRaceCompletionLeaf(
    leafIdx: Int,
    completedWorkflowId: WorkflowId,
    completedKey: WorkflowInstanceKey,
    completedScope: String
) extends AwaitRaceLeaf

/** One durable candidate for a race leaf, selected by global `sequenceId`
  * across all leaves so the earliest satisfying event wins regardless of kind or
  * workflow tree.
  */
final case class AwaitRaceCandidate(
    sequenceId: Long,
    leafIdx: Int,
    payload: String,
    createdAt: java.time.Instant
)

/** The outcome of a satisfied race: the winning event's global sequence id, the
  * serialized result persisted to the step row, and (only when the winning leaf
  * is a signal) the exact key whose shared cursor advances.
  */
final case class AwaitRaceDecision(
    winningSequenceId: Long,
    payload: String,
    advanceSignalKey: Option[SignalKey]
)
