package atomicflow

/** The durable kind of a stored step row, written to `workflow_steps.step_kind`.
  * Case names are code-facing; `persistedName` is the stored text. Preserve the
  * stored text values unless a data migration is intended.
  *
  * The runtime-internal restartable-region row is stored with a raw SQL literal
  * (`'RestartableRegion'`) and is deliberately not part of this enum. */
enum StepKind(val persistedName: String):
  case AtLeastOnce extends StepKind("AtLeastOnce")
  case AtMostOnce extends StepKind("AtMostOnce")
  case Await extends StepKind("Await")
  case FirstToRun extends StepKind("FirstToRunWithoutSuspension")
