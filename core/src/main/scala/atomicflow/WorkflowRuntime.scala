//noinspection ScalaWeakerAccess
package atomicflow

import atomicflow.WorkflowRuntime.StoppedWorkflow
import atomicflow.WorkflowRuntime.StoppedWorkflow.ListenerHandle
import ox.channels.Channel

import java.time.Instant
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
   * Registers this workflow instance but doesn't run it.
   * @throws WorkflowInputConflictException when a workflow instance was previously created with non-equal input
   */
  @throws[WorkflowInputConflictException]
  def createWorkflowInstance[In, Out](
                                       workflowInstance: WorkflowInstanceBuilder[In, Out],
                                       in: In
                                     )(
                                       using Cacheable[In]
                                     ): Unit

  def createWorkflowInstanceDiscardExisting[In, Out](
                                                      workflowInstance: WorkflowInstanceBuilder[In, Out],
                                                      in: In
                                                    )(
                                                      using Cacheable[In]
                                                    ): Boolean = ??? // TODO

  /**
   * Runs a workflow instance. The workflow instance is created if it doesn't already exist.
   *
   * Implementations must lock the workflow while running to prevent concurrent executions of the same workflow instance.
   */
  @throws[WorkflowInputConflictException]
  def runWorkflowInstance[In, Out](
                                    workflowInstance: WorkflowInstanceBuilder[In, Out],
                                    in: In
                                  )(
                                    using Cacheable[In]
                                  ): Either[StoppedWorkflow[Out], Out]

  /**
   * Runs a workflow instance that was previously created. If the workflow instance ran before and didn't finish,
   * it will run again from the beginning, but cached steps will not be executed again.
   *
   * Implementations must lock the workflow while running to prevent concurrent executions of the same workflow instance.
   * @throws WorkflowNotFoundException when no workflow instance with this ID exists
   */
  @throws[WorkflowNotFoundException]
  def recoverWorkflowInstance[In, Out](
                                        workflowInstance: WorkflowInstanceBuilder[In, Out]
                                      )(
                                        using Cacheable[In]
                                      ): Either[StoppedWorkflow[Out], Out]

  def getWorkflowInstance[Out](exactKey: WorkflowInstanceKey): Nothing = ??? // TODO

  def getWorkflowInstances[Out](keyPrefix: WorkflowInstanceKey): Vector[Nothing] = ??? // TODO

  /**
   * Get all workflow instances that haven't yet run to finish, including those that yet to start.
   * @param includeWaiting Include workflow instances that have stopped themselves and are waiting for a timer or signal.
   */
  def getUnfinishedWorkflowInstances(includeWaiting: Boolean = false): Vector[Nothing] = ??? // TODO

  def scheduleWakeupOnTimer(workflowInstanceKey: WorkflowInstanceKey, wakeupTime: Instant): Unit = ??? // TODO

  def scheduleWakeupOnSignal(workflowInstanceKey: WorkflowInstanceKey, signal: Signal[?]): Unit = ??? // TODO

  @throws[WorkflowNotFoundException]
  @throws[SignalConflictException]
  def setSignal[A](
                    signal: Signal[A],
                    value: A,
                    ttl: FiniteDuration
                  )(using SimpleWorkflowContext): Unit
}

object WorkflowRuntime {
  trait DefaultGenerateIdsMixin extends WorkflowRuntime {

    override def generateWorkflowInstanceKey: String = UUID.randomUUID().toString

    override def generateStepIdempotencyId: StepIdempotencyId = StepIdempotencyId.unsafeMake(UUID.randomUUID().toString)

  }

  /** Handle to a workflow that has __stopped__ itself because it is waiting on a timer or external signal */
  trait StoppedWorkflow[Out](
        val workflowId: WorkflowId,
        val workflowInstanceKey: WorkflowInstanceKey,
        /** The expected time when this workflow will be restarted (if it is waiting on a timer) or None if it is waiting on a signal. */
        val expectedRestartTime: Option[Instant]
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
   * match this exception because it is a [[ControlThrowable]]
   */
  case class WorkflowStoppedToWait(expectedRestartTime: Option[Instant]) extends ControlThrowable
  // TODO: should this really be a ControlThrowable?
}
