package atomicflow

/** The stored kind of row in `workflow_steps.step_kind`: which step kind
  * wrote the row. A step key (step id, scope, and version) belongs to
  * exactly one step kind, so a key can never hold rows of two kinds.
  *
  * A [[StepKind]] is an opaque subtype of [[String]]. Backends receive the
  * stored text through the runtime-internals methods and persist it, but cannot
  * match on or construct kinds.
  */
opaque type StepKind <: String = String

object StepKind:

  /** An ordinary step row: `Step.atLeastOnce` and `Step.atMostOnce`. */
  val Step: StepKind = "Step"

  /** A runtime-computed wait: `Step.await` and `Step.awaitRace`. */
  val Await: StepKind = "Await"

  /** A `Step.firstToRunWithoutSuspension` winner row. */
  val FirstToRun: StepKind = "FirstToRun"

  /** A `Workflow.restartable` / `Workflow.loop` region row. */
  val RestartableRegion: StepKind = "RestartableRegion"
