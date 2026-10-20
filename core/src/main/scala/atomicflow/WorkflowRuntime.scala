package atomicflow

import org.slf4j.LoggerFactory

import java.util.concurrent.{TimeUnit, TimeoutException}
import scala.annotation.implicitNotFound
import scala.concurrent.duration.*

/** The application-facing view of a workflow runtime: creates, runs, and
  * manages workflow instances. Some methods are final and composed here from
  * the storage/execution engine operations; the rest are abstract and
  * implemented by the storage backend. The engine operations themselves live on
  * [[WorkflowRuntimeInternals]], which this trait requires as a self-type (a
  * backend implements both), so application code holding a `WorkflowRuntime`
  * cannot reach them.
  */
@implicitNotFound("No WorkflowRuntime available. Add a using clause (using WorkflowRuntime).")
trait WorkflowRuntime { this: WorkflowRuntimeInternals =>

  private val log = LoggerFactory.getLogger(getClass)

  /** Register an instance, idempotent for equal (serialized) input; throws
    * [[WorkflowInputConflictException]] if the input differs.
    */
  @throws[WorkflowInputConflictException]
  def createWorkflowInstance[In, Out](
      workflow: Workflow[In, Out],
      instanceKey: WorkflowInstanceKey,
      in: In
  )(using Cacheable[In]): WorkflowInstance[In, Out]

  /** Delete any existing instance under the identity and re-create it; returns
    * whether an existing instance was discarded.
    */
  def createWorkflowInstanceDiscardExisting[In, Out](
      workflow: Workflow[In, Out],
      instanceKey: WorkflowInstanceKey,
      in: In
  )(using Cacheable[In]): Boolean

  /** Fork a source instance into a brand-new independent top-level instance.
    *
    * The fork receives a completely new instance ID, generation 0, and the same
    * input as the source; it is an independent top-level workflow (no parent
    * relationship, no inherited signals). `restartFromStep` is an exclusive
    * boundary: the cached history strictly before the selected step is copied
    * (rewritten to the new identity), while the selected step and subsequent
    * history are omitted so they re-execute in the fork. The workflow function
    * still executes from its beginning, replaying the copied rows until the
    * first uncopied operation.
    *
    * The boundary is ordered by the step rows' last-updated timestamp: rows
    * strictly before the selected row are kept, the selected row and everything
    * with an equal or later timestamp are omitted. `restartFromStep` must
    * identify an already-executed step of the source or
    * [[InvalidRestartStepException]] is thrown. Forking is allowed on any source
    * state (suspended or terminal) and does not modify the source.
    *
    * LIMITATION: a single step id is used as the causal boundary, which is
    * insufficient to describe a cut through parallel branches that have no one
    * total order; rows sharing the boundary timestamp are conservatively
    * omitted.
    */
  @throws[WorkflowNotFoundException]
  @throws[InvalidRestartStepException]
  def forkWorkflow[In, Out](
      sourceInstanceId: WorkflowInstanceId,
      newInstanceKey: WorkflowInstanceKey,
      restartFromStep: StepId
  )(using workflow: Workflow[In, Out]): WorkflowInstance[In, Out]

  /** Reset an instance in place from `restartFromStep`: keep the same instance
    * identity, increment the generation, erase the selected step and subsequent
    * history (Step rows and their subscriptions/wakeups), and schedule the
    * instance to re-run from the top. History strictly before the selected step
    * remains cached and is replayed, so the workflow function still executes
    * from its beginning. The instance's input, signal cursors, and directly
    * addressed events are untouched.
    *
    * The boundary is ordered by the step rows' last-updated timestamp: rows
    * strictly before the selected row are kept, the selected row and everything
    * with an equal or later timestamp are erased. `restartFromStep` must
    * identify an already-executed step or [[InvalidRestartStepException]] is
    * thrown. Resetting a terminal instance throws [[IllegalStateException]].
    *
    * LIMITATION: a single step id is used as the causal boundary, which is
    * insufficient to describe a cut through parallel branches that have no one
    * total order; rows sharing the boundary timestamp are conservatively
    * erased.
    */
  @throws[WorkflowNotFoundException]
  @throws[InvalidRestartStepException]
  @throws[IllegalStateException]
  def resetWorkflow[In, Out](
      sourceInstanceId: WorkflowInstanceId,
      restartFromStep: StepId
  )(using workflow: Workflow[In, Out]): Unit

  /** Append a `Signal` event addressed to `workflowInstanceId`. The sender
    * briefly row-locks the instance and rejects the append with
    * [[SignalSendResult.InstanceAlreadyCompleted]] when it has already reached a
    * terminal state, otherwise appends the event (with the signal's
    * `Cacheable`). The sender only appends; wakeups are produced asynchronously
    * by the job runner. Does not acquire the execution lease.
    */
  @throws[WorkflowNotFoundException]
  def sendSignal[A: Cacheable](
      workflowInstanceId: WorkflowInstanceId,
      key: SignalKey,
      value: A
  ): SignalSendResult

  /** Request cooperative cancellation of an instance. Durably sets
    * `cancel_requested_at` once and never resets it. If the instance has never
    * started and has no execution state, it is finalized `CANCELLED` immediately
    * (the body never runs). Otherwise the flag is delivered at the next new-work
    * checkpoint (a Step body about to execute, or an await about to be
    * evaluated), where the runtime throws [[WorkflowCancelledException]]. A
    * terminal instance is a no-op.
    *
    * Does not acquire the execution lease; delivery is by checkpoint, never by
    * thread interruption.
    */
  @throws[WorkflowNotFoundException]
  def cancel(instanceId: WorkflowInstanceId): Unit

  /** Force-stop an instance. In one transaction: a guarded terminal transition to
    * `TERMINATED` (appending the `WorkflowCompleted` event with a `Terminated`
    * outcome), a lease revocation (fencing-token bump plus clearing the lease
    * owner/expiry, fencing out any running orphan), and terminal cleanup of this
    * instance's wakeup and subscription rows. The instance is never scheduled
    * again, no resume is scheduled, and no [[WorkflowCancelledException]] is
    * delivered — the body gets no chance to run.
    *
    * JVM limitation: `terminate` guarantees the instance is durably stopped and
    * lease-revoked; an orphan thread already executing a Step cannot be forcibly
    * stopped and may run until its next checkpoint or the process ends.
    *
    * A missing instance throws [[WorkflowNotFoundException]]; an already-terminal
    * instance is a no-op.
    */
  @throws[WorkflowNotFoundException]
  def terminate(instanceId: WorkflowInstanceId): Unit

  /** Execute a created instance on the caller thread. */
  @throws[WorkflowNotFoundException]
  final def runWorkflowInstance[In, Out](
      workflow: Workflow[In, Out],
      instanceId: WorkflowInstanceId
  )(using cacheableThrowable: Cacheable[Throwable]): WorkflowRunResult[Out] =
    withLease(instanceId, leaseAcquireTimeout) { executeUnderLease(workflow) } match
      case LeasedRunResult.Ran(value)              => value
      case LeasedRunResult.AlreadyTerminal(record) => decodeTerminal(workflow, record)

  /** The shared body-execution path: decode the persisted input with the
    * definition's `Cacheable[In]`, build a [[WorkflowContext]] for the lease,
    * run the body, and translate its outcome into a terminal transition (or a
    * control-flow result). Called by final [[runWorkflowInstance]] and by
    * backends that already hold a lease for a claimed job.
    */
  final private[atomicflow] def executeUnderLease[In, Out](workflow: Workflow[In, Out])(using
      cacheableThrowable: Cacheable[Throwable],
      lease: Lease
  ): WorkflowRunResult[Out] = {
    given Cacheable[Out] = workflow.outputCacheable
    val completionCodec = summon[Cacheable[WorkflowCompletionResult[Out]]]
    val record = lease.currentWfInstance
    val instanceId = record.instanceId
    val input = workflow.inputCacheable.read(record.input.raw)
    val ctx = new WorkflowContext(
      instanceId,
      record.versionAtCreation,
      this,
      this
    )(lease)

    var outValue: Out = null.asInstanceOf[Out]
    var suspended = false
    try {
      outValue = workflow.body(input)(using ctx)
    } catch {
      case _: WorkflowSuspendedException =>
        log.debug(s"Workflow instance $instanceId suspended")
        suspended = true
      case _: LeaseLostException =>
        throw new LeaseLostException(s"Workflow instance lease lost during run: $instanceId")
      case e: ContinueAsNewException =>
        val newInput = workflow.inputCacheable.read(e.encoded.raw)
        val newSerializedInput = workflow.inputCacheable.write(newInput)
        val unconsumed = continueAsNew(SerializedValue.of(newSerializedInput))
        invokeUnconsumedSignalsListener(workflow, instanceId, unconsumed)
        log.info(s"Workflow instance $instanceId continued as new")
        return WorkflowRunResult.ContinueAsNew
      case _: WorkflowCancelledException =>
        completeInstance(TerminalOutcome.Cancelled) match {
          case CompletionResult.Applied(unconsumed) =>
            invokeUnconsumedSignalsListener(workflow, instanceId, unconsumed)
            log.info(s"Workflow instance $instanceId cancelled")
            return WorkflowRunResult.WorkflowCancelled
          case CompletionResult.AlreadyTerminal(record) =>
            return decodeTerminal(workflow, record)
        }
      case WorkflowNonFatal(t) =>
        val payload = completionCodec.write(WorkflowCompletionResult.Failed(t))
        completeInstance(TerminalOutcome.Failed(SerializedValue.of(payload))) match {
          case CompletionResult.Applied(unconsumed) =>
            invokeUnconsumedSignalsListener(workflow, instanceId, unconsumed)
            log.info(s"Workflow instance $instanceId failed", t)
            throw cacheableThrowable.read(cacheableThrowable.write(t))
          case CompletionResult.AlreadyTerminal(record) =>
            return decodeTerminal(workflow, record)
        }
    }

    if (suspended) WorkflowRunResult.WorkflowSuspended
    else {
      val payload = completionCodec.write(WorkflowCompletionResult.Completed(outValue))
      completeInstance(TerminalOutcome.Completed(SerializedValue.of(payload))) match {
        case CompletionResult.Applied(unconsumed) =>
          invokeUnconsumedSignalsListener(workflow, instanceId, unconsumed)
          log.debug(s"Workflow instance $instanceId completed")
          WorkflowRunResult.Result(workflow.outputCacheable.read(workflow.outputCacheable.write(outValue)))
        case CompletionResult.AlreadyTerminal(record) =>
          decodeTerminal(workflow, record)
      }
    }
  }

  /** Decodes a terminal instance's persisted outcome and returns or throws it,
    * without executing the body. Used when a run finds the instance already
    * terminal, or when a concurrent writer won the terminal transition. Only the
    * definition's output codec is needed, so any `Workflow[?, Out]` suffices.
    */
  final private[atomicflow] def decodeTerminal[Out](
      workflow: Workflow[?, Out],
      record: InstanceRecord
  )(using cacheableThrowable: Cacheable[Throwable]): WorkflowRunResult[Out] = {
    given Cacheable[Out] = workflow.outputCacheable
    val completionCodec = summon[Cacheable[WorkflowCompletionResult[Out]]]

    def decode(): WorkflowCompletionResult[Out] = {
      val payload = record.terminalOutcome.getOrElse(
        throw new StepSerializationFailed(s"Terminal instance ${record.instanceId} has no terminal outcome")
      )
      try completionCodec.read(payload.raw)
      catch {
        case _: Throwable =>
          throw new StepSerializationFailed(s"Terminal outcome of ${record.instanceId} could not be decoded")
      }
    }

    record.terminalState match {
      case Some(WorkflowTerminalState.Completed) =>
        decode() match {
          case WorkflowCompletionResult.Completed(v) => WorkflowRunResult.Result(v)

          case _ =>
            throw new StepSerializationFailed(s"Terminal instance ${record.instanceId} stored a non-completed outcome as completed")
        }

      case Some(WorkflowTerminalState.Failed) =>
        decode() match {
          case WorkflowCompletionResult.Failed(t) => throw t

          case _ =>
            throw new StepSerializationFailed(s"Terminal instance ${record.instanceId} stored a non-failed outcome as failed")
        }

      case Some(WorkflowTerminalState.Cancelled)  => WorkflowRunResult.WorkflowCancelled
      case Some(WorkflowTerminalState.Terminated) => WorkflowRunResult.WorkflowTerminated

      case None =>
        throw new LeaseLostException(s"Workflow instance ${record.instanceId} is not terminal after a terminal transition")
    }
  }

  /** Invokes the completing/restarting instance's `onUnconsumedSignals` listener
    * with the map gathered inside the already-committed transition transaction.
    * A throwing listener must never mask the workflow outcome, so any
    * [[WorkflowNonFatal]] it raises is logged and swallowed. At-most-once: a
    * crash between the transition commit and this call loses the delivery
    * (acceptable for a logging/debug aid).
    */
  private def invokeUnconsumedSignalsListener(
      workflow: Workflow[?, ?],
      instanceId: WorkflowInstanceId,
      unconsumed: Map[SignalKey, Seq[String]]
  ): Unit =
    try workflow.onUnconsumedSignals(unconsumed)
    catch {
      case WorkflowNonFatal(t) =>
        log.warn(s"onUnconsumedSignals listener failed for workflow instance $instanceId; the workflow outcome is unaffected", t)
    }

  /** Register and schedule the instance's first wakeup (due immediately). */
  final def createAndScheduleForAsyncRun[In, Out](
      workflow: Workflow[In, Out],
      instanceKey: WorkflowInstanceKey,
      in: In
  ): WorkflowInstance[In, Out] = {
    val instance = createWorkflowInstance(workflow, instanceKey, in)(using workflow.inputCacheable)
    upsertWakeup(instance.id, Duration.Zero)
    instance
  }

  /** Create the instance if absent, then run it on the caller thread. */
  final def createAndRun[In, Out](
      workflow: Workflow[In, Out],
      instanceKey: WorkflowInstanceKey,
      in: In
  )(using cacheableThrowable: Cacheable[Throwable]): WorkflowRunResult[Out] = {
    createWorkflowInstance(workflow, instanceKey, in)(using workflow.inputCacheable)
    runWorkflowInstance(workflow, WorkflowInstanceId(workflow.id, instanceKey))
  }

  /** Upgrade a bare identity to a typed handle. Validates that the workflow id
    * in the identity matches the provided definition (a programming error if it
    * does not). Missing instances are not verified here; handles obtained from
    * the runtime always refer to existing instances.
    */
  final def getWorkflowInstance[In, Out](
      workflow: Workflow[In, Out],
      instanceId: WorkflowInstanceId
  ): WorkflowInstance[In, Out] = {
    if (instanceId.workflowId != workflow.id)
      throw new IllegalArgumentException(
        s"Cannot build a handle for workflow '${workflow.id}' from an instanceId referring to workflow '${instanceId.workflowId}'"
      )
    WorkflowInstance(workflow, instanceId)
  }

  /** Key-prefix query over instances of one workflow definition, scoped exactly
    * to `scope` ([[ScopePath.empty]] = top-level only). The prefix is matched
    * literally: `%` and `_` in the user prefix are escaped, not treated as
    * wildcards.
    */
  def getWorkflowInstancesByPrefix(
      workflowId: WorkflowId,
      keyPrefix: WorkflowInstanceKey,
      scope: ScopePath = ScopePath.empty
  ): Vector[WorkflowInstance.Info]

  /** Instances of one workflow definition that have not reached a terminal
    * state. `includeWaiting = false` (default) returns only instances that have
    * never started (`times_executed = 0`); `true` returns all non-terminal
    * instances. `limit <= 0` means unlimited.
    */
  def getUnfinishedWorkflowInstances(
      workflowId: WorkflowId,
      includeWaiting: Boolean = false,
      limit: Int = -1
  ): Vector[WorkflowInstance.Info]

  /** Deletes all instances of one workflow definition whose key matches `scope`
    * and `keyPrefix` (escaped as in [[getWorkflowInstancesByPrefix]]), plus
    * their cascaded rows and their event rows. Returns the number of instances
    * deleted.
    */
  def deleteWorkflowInstancesByPrefix(
      workflowId: WorkflowId,
      keyPrefix: WorkflowInstanceKey,
      scope: ScopePath = ScopePath.empty
  ): Long

  /** The active children of `parentId`: instances whose parent relationship
    * still points at `parentId` (not yet cleared by a parent terminal
    * transition).
    */
  def getChildWorkflowInstances(parentId: WorkflowInstanceId): Vector[WorkflowInstance.Info]

  /** Passive waiter for an instance's terminal outcome: polls the persisted
    * instance row until a terminal state is reached or `timeout` elapses, then
    * throws [[java.util.concurrent.TimeoutException]]. Never executes the
    * workflow. Terminal outcomes decode exactly as `run` on a terminal instance.
    *
    * Each poll reads the full instance row through [[readInstance]]; a missing
    * instance throws [[WorkflowNotFoundException]] on any poll that finds the
    * row absent. The loop always performs one immediate poll, so a zero timeout
    * still observes an already-terminal instance. `pollInterval` must be
    * positive and is slept between polls with nanosecond precision, so
    * sub-millisecond intervals are honored.
    */
  @throws[WorkflowNotFoundException]
  @throws[TimeoutException]
  final def blockUntilCompleted[Out](
      instance: WorkflowInstance[?, Out],
      timeout: FiniteDuration,
      pollInterval: FiniteDuration = 50.milliseconds
  )(using cacheableThrowable: Cacheable[Throwable]): WorkflowRunResult[Out] = {
    require(pollInterval > Duration.Zero, s"pollInterval must be positive, was $pollInterval")
    val deadlineNanos = System.nanoTime() + timeout.toNanos
    var terminal: Option[InstanceRecord] = None
    var expired = false
    while (terminal.isEmpty && !expired) {
      val record = readInstance(instance.id).getOrElse(
        throw new WorkflowNotFoundException(s"Workflow instance not found: ${instance.id}")
      )
      if (record.terminalState.isDefined) 
        terminal = Some(record)
      else if (System.nanoTime() >= deadlineNanos) 
        expired = true
      else 
        TimeUnit.NANOSECONDS.sleep(pollInterval.toNanos)
    }
    terminal match {
      case Some(record) => decodeTerminal(instance.workflow, record)
      case None =>
        throw new TimeoutException(
          s"Workflow instance did not reach a terminal state within $timeout: ${instance.id}"
        )
    }
  }

  /** The persisted data view of an instance, fresh from the database. */
  @throws[WorkflowNotFoundException]
  final def getWorkflowInstanceInfo[In, Out](
      instance: WorkflowInstance[In, Out]
  ): WorkflowInstance.Info =
    readInstance(instance.id)
      .map(_.info)
      .getOrElse(throw new WorkflowNotFoundException(s"Workflow instance not found: ${instance.id}"))

  /** Creates and starts a job runner over the given definition registry.
    * Implemented per backend: the runner executes backend-internal operations
    * (wakeup claiming, conditional lease acquisition, sweeps) and is bound to
    * this runtime — runners and runtimes cannot be mixed and matched.
    *
    * The registry maps each `workflowId` to its one current definition; it is
    * validated once (duplicate ids throw) and immutable for the runner's
    * lifetime. The runner claims only wakeups of workflows in the registry, so
    * several applications with different code can share the same tables.
    *
    * Any number of runners may be started on the same runtime — in this process
    * or another — each with its own settings and registry. They cooperate through
    * storage alone (claim-atomic leases and idempotent sweeps), so duplicate
    * runners cost redundant work but are never incorrect. Each runner stops
    * independently via [[JobRunner.stop]].
    */
  def startJobRunner(
      definitions: Seq[Workflow[?, ?]],
      settings: JobRunnerSettings = JobRunnerSettings.default
  ): JobRunner
}

object WorkflowRuntime {
  def apply(using runtime: WorkflowRuntime): WorkflowRuntime = runtime
}
