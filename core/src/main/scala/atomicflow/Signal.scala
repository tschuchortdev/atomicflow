package atomicflow

import atomicflow.WorkflowContext.given_WorkflowInstanceMeta

import java.net.URLEncoder
import java.nio.charset.StandardCharsets

// TODO multi-event signals
trait Signal[A] {
  def meta: SignalMeta

  def asCacheable: Cacheable[A]

  def option(using WorkflowContext): Option[A]

  def isDefined(using WorkflowContext): Boolean = option.isDefined

  def isEmpty(using WorkflowContext): Boolean = option.isEmpty

  @throws[SignalEmptyException]
  def valueOrThrow(using WorkflowContext): A =
    option.getOrElse(throw new SignalEmptyException(this))

  /*@throws[SignalConflictException]
  def set(value: A)(using WorkflowContext[?, ?]): Unit*/

  override def toString: String = s"signal:${meta.id}${meta.name.fold("")(name => "#" + URLEncoder.encode(name, StandardCharsets.UTF_8))}"
}

object Signal {
  def apply[A: Cacheable as A](
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

      override def asCacheable: Cacheable[A] = A

      override def option(using workflowCtx: WorkflowContext): Option[A] =
        workflowCtx.workflowRuntime.getSignalStore.getSignalValue(this)

      /*override def set(value: A)(using workflowCtx: WorkflowContext[?, ?]): Unit =
        workflowCtx.getSignalStore.setSignalValue(this, value)*/
    }
  }
}
