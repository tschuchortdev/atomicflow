package atomicflow.internal

import atomicflow.{Signal, SignalConflictException, WorkflowInstanceMeta}

import scala.concurrent.duration.FiniteDuration

trait SignalStore {
  //@throws[SignalEmptyException]
  def getSignalValue[A](signal: Signal[A])(using WorkflowInstanceMeta): Option[A]

  @throws[SignalConflictException]
  def setSignalValue[A](signal: Signal[A], value: A, ttl: FiniteDuration)(using WorkflowInstanceMeta): Unit
}
