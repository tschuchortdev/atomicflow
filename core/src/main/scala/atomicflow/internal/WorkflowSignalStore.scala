package atomicflow.internal

import atomicflow.{Signal, SignalConflictException, SignalEmptyException}

trait WorkflowSignalStore {
  //@throws[SignalEmptyException]
  def getSignalValue[A](signal: Signal[A]): Option[A]

  @throws[SignalConflictException]
  def setSignalValue[A](signal: Signal[A], value: A): Unit
}
