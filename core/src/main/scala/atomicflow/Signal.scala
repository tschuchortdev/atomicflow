package atomicflow

/** A named signal that workflows can await by type.
  *
  * The key is the durable event identity; `A` is the value type exchanged by
  * the sender and receiver. `Cacheable[A]` is captured at definition time, so
  * senders and receivers agree on the payload format without importing a codec
  * at each call site. The class is invariant in `A`.
  */
final class Signal[A] private (val key: SignalKey)(using val cacheable: Cacheable[A]) {

  /** Append a signal event addressed to the given instance. Forwarder for the
    * runtime's `sendSignal`, resolving this signal's [[Cacheable]].
    */
  @throws[WorkflowNotFoundException]
  def send(workflowInstanceId: WorkflowInstanceId, value: A)(using runtime: WorkflowRuntime): SignalSendResult =
    runtime.sendSignal(workflowInstanceId, key, value)(using cacheable)
}

object Signal {
  def apply[A: Cacheable](key: SignalKey): Signal[A] = new Signal[A](key)
}
