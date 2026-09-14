package atomicflow

/** The durable, inspection-only view of a step's execution state, as returned by
  * [[Step.getExecutionState]]. It reads only persisted facts (no lease or fence
  * is involved) and reports the stored row even if it has expired.
  */
enum StepExecutionState[+A]:
  case NeverStarted
  case Started
  case Failed(failure: Throwable)
  case Completed(value: A)
