package atomicflow

/** Stored step state returned by [[Step.getExecutionState]], including expired
  * records. Inspection does not acquire an execution lease or run the step.
  */
enum StepExecutionState[+A]:
  case NeverStarted
  case Started
  case Failed(failure: Throwable)
  case Completed(value: A)
