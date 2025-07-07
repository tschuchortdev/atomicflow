package atomicflow

import java.util.UUID

trait Signal[A] {
  def meta: SignalMeta

  def option(using workflowCtx: WorkflowContext[?, ?]): Option[A]

  def isDefined(using workflowCtx: WorkflowContext[?, ?]): Boolean = option.isDefined

  def isEmpty(using workflowCtx: WorkflowContext[?, ?]): Boolean = option.isEmpty

  @throws[EmptySignalException]
  def get(using workflowCtx: WorkflowContext[?, ?]): A =
    option.getOrElse(throw new EmptySignalException())

  // TODO: you set the signal with workflowInstance.setSignal(signal, value)
  // TODO: SignalConflictException
  //@throws[WorkflowInputConflictException]
  //def set(value: A): Unit
}

object Signal {
  def apply[A](
                  id: SignalId,
                  name: String | Unit = (),
                  description: String | Unit = ()
                ): Signal[A] = {
    new Signal[A] {
      override val meta: SignalMeta = SignalMeta(
        id = id,
        name = name match {
          case () => None
          case string: String => Some(string)
        },
        description = description match {
          case () => None
          case string: String => Some(string)
        }
      )

      override def option(using workflowCtx: WorkflowContext[?, ?]): Option[A] =
        workflowCtx.getSignalStore.getSignalValue(this)
    }
  }
}
