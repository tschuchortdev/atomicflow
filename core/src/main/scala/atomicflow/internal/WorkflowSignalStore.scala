package atomicflow.internal

import atomicflow.Signal

trait WorkflowSignalStore {
  def getSignalValue[A](signal: Signal[A]): Option[A]

  def setSignalValue[A](signal: Signal[A], value: A): Unit
}
