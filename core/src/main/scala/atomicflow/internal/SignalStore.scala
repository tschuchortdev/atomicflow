package atomicflow.internal

import atomicflow.{Signal, SignalConflictException, SimpleWorkflowContext}

import scala.concurrent.duration.FiniteDuration

trait SignalStore {
  //@throws[SignalEmptyException]
  def getSignalValue[A](signal: Signal[A])(using SimpleWorkflowContext): Option[A]

  @throws[SignalConflictException]
  def setSignalValue[A](signal: Signal[A], value: A, ttl: FiniteDuration)(using SimpleWorkflowContext): Unit
}
