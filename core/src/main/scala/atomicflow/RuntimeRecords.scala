package atomicflow

import java.time.Instant

/** The persisted state of a workflow instance, as read by the runtime. */
final case class InstanceRecord(
    instanceId: WorkflowInstanceId,
    input: SerializedValue,
    versionAtCreation: Long,
    generation: Long,
    terminalState: Option[WorkflowTerminalState],
    terminalOutcome: Option[SerializedValue],
    parentId: Option[WorkflowInstanceId],
    createdAt: Instant,
    lastRunAt: Option[Instant],
    timesExecuted: Int
) {

  /** The application-facing data view of this instance. */
  def info: WorkflowInstance.Info =
    WorkflowInstance.Info(
      id = instanceId,
      parentId = parentId,
      generation = generation,
      terminalState = terminalState,
      workflowVersionAtCreation = versionAtCreation,
      createdAt = createdAt,
      lastRunAt = lastRunAt,
      timesExecuted = timesExecuted
    )
}

/** The terminal outcome a run reports to its backend when it finishes. */
enum TerminalOutcome:
  case Completed(outcome: SerializedValue)
  case Failed(outcome: SerializedValue)
  case Cancelled
  case Terminated

/** The result of reporting a terminal outcome: either the guarded terminal
  * transition applied, or a concurrent writer had already made the instance
  * terminal. */
enum CompletionResult:
  case Applied(unconsumedSignals: Map[SignalKey, Seq[String]])
  case AlreadyTerminal(record: InstanceRecord)

/** The result of running a body under an execution lease. */
enum LeasedRunResult[+A]:
  case AlreadyTerminal(record: InstanceRecord)
  case Ran(value: A)
