package atomicflow

/** Stored step state returned by `Step.getStepState` and
  * `WorkflowRuntime.getStepState`, including expired records. Inspection does
  * not acquire an execution lease or run the step.
  */
enum StepExecutionState[+A]:
  case NeverStarted
  case Started
  case Failed(failure: Throwable)
  case Completed(value: A)
