//noinspection ScalaWeakerAccess
package atomicflow

import atomicflow.Fingerprintable.Fingerprinter
import atomicflow.WorkflowContext.given_WorkflowInstanceMeta
import atomicflow.WorkflowRuntime.StoppedWorkflow
import atomicflow.WorkflowRuntime.StoppedWorkflow.ListenerHandle
import atomicflow.internal.{SignalStore, StepCache, StepIdempotencyStore}
import ox.channels.Channel
import ox.mapPar

import java.time.{Clock, Instant}
import java.util.UUID
import scala.annotation.{implicitNotFound, tailrec}
import scala.concurrent.duration.FiniteDuration
import scala.util.{Failure, Success, Try, Using}
import scala.util.control.ControlThrowable

@implicitNotFound("No WorkflowRuntime available.\nAdd a using clause `(using WorkflowRuntime)` to the definition of the enclosing method.")
trait WorkflowRuntime {
  def generateWorkflowInstanceKey: String

  def generateStepIdempotencyId: StepIdempotencyId

  /**
   * Registers this workflow instance key and input but doesn't run it.
   * @throws WorkflowInputConflictException when a workflow instance was previously created with non-equal input
   */
  @throws[WorkflowInputConflictException]
  def createWorkflowInstance[In, Out](workflow: Workflow[In, Out], instanceId: WorkflowInstanceKey, in: In)(using Cacheable[In]): Unit

  def createWorkflowInstanceDiscardExisting[In, Out](workflow: Workflow[In, Out], instanceId: WorkflowInstanceKey, in: In)(using Cacheable[In]): Boolean

  /**
   * Runs a workflow instance. The workflow instance is created if it doesn't already exist.
   *
   * Implementations must lock the workflow while running to prevent concurrent executions of the same workflow instance.
   */
  @throws[WorkflowInputConflictException]
  def createAndRunWorkflowInstance[In, Out](workflow: Workflow[In, Out], instanceId: WorkflowInstanceKey, in: In)(using Cacheable[In], Cacheable[Out], WorkflowRunSettings)
      : Either[StoppedWorkflow[Out], Out]

  /**
   * Runs a workflow instance that was previously created. If the workflow instance ran before and didn't finish,
   * it will run again from the beginning, but cached steps will not be executed again.
   *
   * Implementations must lock the workflow while running to prevent concurrent executions of the same workflow instance.
   * @throws WorkflowNotFoundException when no workflow instance with this ID exists
   */
  @throws[WorkflowNotFoundException]
  def runWorkflowInstance[In, Out](workflow: Workflow[In, Out], instanceId: WorkflowInstanceKey)(using Cacheable[In], Cacheable[Out], WorkflowRunSettings)
      : Either[StoppedWorkflow[Out], Out]

  def getWorkflowInstancesByPrefix(workflowId: WorkflowId, keyPrefix: WorkflowInstanceKey)
      : Vector[WorkflowInstanceKey]

  /**
   * Get all workflow instances that haven't yet run to finish, including those that have yet to start.
   * @param includeWaiting Include workflow instances that have stopped themselves and are waiting for a timer or signal.
   */
  def getUnfinishedWorkflowInstances(workflowId: WorkflowId, includeWaiting: Boolean = false, limit: Int = -1)
      : Vector[WorkflowInstanceKey]

  def deleteWorkflowInstancesByPrefix(workflowId: WorkflowId, instanceKeyPrefix: WorkflowInstanceKey): Long

  @throws[WorkflowNotFoundException]
  def isWorkflowInstanceCompleted(workflowId: WorkflowId, workflowInstanceKey: WorkflowInstanceKey): Boolean

  /** Check if a workflow instance with this key has been created. */
  def isWorkflowInstanceCreated(workflowId: WorkflowId, workflowInstanceKey: WorkflowInstanceKey): Boolean

  @throws[WorkflowNotFoundException]
  def getWorkflowResult[Out](workflow: Workflow[?, Out], workflowInstanceKey: WorkflowInstanceKey)(using Cacheable[Out]): Option[Out]

  /** Instructs the runtime to start/recover the workflow some time after the [[wakeupTime]] is reached.
   *
   * Implementation note: If the workflow is already running at the time when the wakeup time is reached, the runtime should
   * skip this schedule and try again later. If the workflow is finished when the wakeup time is reached, the schedule should be deleted.
   * */
  @throws[WorkflowNotFoundException]
  def scheduleWakeupOnTimer(workflowId: WorkflowId, workflowInstanceKey: WorkflowInstanceKey, wakeupTime: Instant): Unit

  /** Instructs the runtime to start/recover the workflow some time after the [[signal]] occurs.
   *
   * Implementation note: If the workflow is already running at the time when the wakeup time is reached, the runtime should
   * skip this schedule and try again later. If the workflow is finished when the wakeup time is reached, the schedule should be deleted.
   * */
  @throws[WorkflowNotFoundException]
  def scheduleWakeupOnSignal(workflowId: WorkflowId, workflowInstanceKey: WorkflowInstanceKey, signal: Signal[?]): Unit

  /** Instructs the runtime to start/recover the workflow some time after the [[awaitedWorkflow]] has finished.
   *
   * Implementation note: If the workflow is already running at the time when the wakeup time is reached, the runtime should
   * skip this schedule and try again later. If the workflow is finished when the wakeup time is reached, the schedule should be deleted.
   * */
  @throws[WorkflowNotFoundException]
  def scheduleWakeupOnWorkflowCompletion(workflowId: WorkflowId, workflowInstanceKey: WorkflowInstanceKey,
                                         awaitedWorkflowId: WorkflowId, awaitedWorkflowInstanceKey: WorkflowInstanceKey): Unit

  @throws[WorkflowNotFoundException]
  @throws[SignalConflictException]
  def setSignal[A](
                    signal: Signal[A],
                    value: A,
                    ttl: FiniteDuration
                  )(using WorkflowInstanceMeta): Unit

  def getFingerprinter: Fingerprinter

  def getStepIdempotencyStore(using StepContext[?]): StepIdempotencyStore

  def getStepCache[StepOut: Cacheable](using StepContext[StepOut]): StepCache[StepOut]

  def getSignalStore: SignalStore
}

object WorkflowRuntime {
  def apply(using runtime: WorkflowRuntime): WorkflowRuntime = runtime

  trait DefaultGenerateIdsMixin extends WorkflowRuntime {

    override def generateWorkflowInstanceKey: String = UUID.randomUUID().toString

    override def generateStepIdempotencyId: StepIdempotencyId = StepIdempotencyId.unsafeMake(UUID.randomUUID().toString)

  }

  /** Handle to a workflow that has __stopped__ itself because it is waiting on a timer or external signal */
  trait StoppedWorkflow[Out](
        val workflowId: WorkflowId,
        val workflowInstanceKey: WorkflowInstanceKey
  ) {
    /** Blocks the current thread until this workflow has restarted and finished execution. If the workflow restarts and
     * then stops itself a second time, this method will continue to block.
     *
     * This method usually shouldn't be used. Workflows stop themselves when they will wait for a very long time,
     * and it would be wasteful to block a (virtual) thread for this long. */
    @throws[InterruptedException]
    def inefficientBlockUntilFinished(): Out = {
      val resultChannel = Channel.rendezvous[Out]
      Using.resource(addContinueListener { r =>
        try {
          r match {
            case Success(Right(out)) => resultChannel.send(out)
            case Success(Left(stopped)) =>
              // TODO Try uses NonFatal which doesn't catch InterruptedException. Could this become a problem?
              Try(stopped.inefficientBlockUntilFinished()) match {
                case Failure(ex) => resultChannel.error(ex)
                case Success(out) => resultChannel.send(out)
              }
            case Failure(ex) => resultChannel.error(ex)
          }
        } finally { resultChannel.doneOrClosed() }
      }) { _ =>
        resultChannel.receive()
      }
    }

    def addContinueListener(onWorkflowContinued: Try[Either[StoppedWorkflow[Out], Out]] => Unit): ListenerHandle
  }
  object StoppedWorkflow {
    trait ListenerHandle extends AutoCloseable {
      def remove(): Unit
      override final def close(): Unit = remove()
    }
  }

  /** An exception that is used internally by the Workflow runtime when the workflow wants to stop itself because
   * it needs to wait on a timer or signal for a long time.
   *
   * This exception should always be rethrown and not logged. The [[scala.util.control.NonFatal]] extractor will not
   * match this exception because it is a [[ControlThrowable]].
   */
  sealed abstract class WorkflowStoppedToWait extends ControlThrowable {
    // TODO: should this really be a ControlThrowable?
    def isRestartConditionFulfilledNow(using clk: Clock, ctx: WorkflowContext): Boolean
  }

  case class WorkflowStoppedToAwaitTimer(expectedRestartTime: Instant) extends WorkflowStoppedToWait {
    def isRestartConditionFulfilledNow(using clk: Clock, ctx: WorkflowContext): Boolean =
      Instant.now(clk).isAfter(expectedRestartTime)
  }

  case class WorkflowStoppedToAwaitSignal(signal: Signal[?]) extends WorkflowStoppedToWait {
    def isRestartConditionFulfilledNow(using clk: Clock, ctx: WorkflowContext): Boolean =
      ctx.workflowRuntime.getSignalStore.getSignalValue(signal).isDefined
  }

  case class WorkflowStoppedToAwaitWorkflow(awaitedWorkflowId: WorkflowId, awaitedInstanceKey: WorkflowInstanceKey) extends WorkflowStoppedToWait {
    def isRestartConditionFulfilledNow(using clk: Clock, ctx: WorkflowContext): Boolean =
      ctx.workflowRuntime.isWorkflowInstanceCompleted(awaitedWorkflowId, awaitedInstanceKey)
  }

  case class WorkflowStoppedToAwaitManyConditions(stops: Vector[WorkflowStoppedToWait]) extends WorkflowStoppedToWait {
    def isRestartConditionFulfilledNow(using clk: Clock, ctx: WorkflowContext): Boolean =
      stops.forall(_.isRestartConditionFulfilledNow)
  }

  
}
