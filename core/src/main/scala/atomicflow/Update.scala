package atomicflow

/** The outcome of sending an [[Update]]. `Success(value)` means the addressed
  * workflow handled the update and returned `value` as its synchronous response.
  * `Unhandled` means the workflow's run finished without consuming the update.
  * `InstanceAlreadyCompleted` means the addressed instance had already reached a
  * terminal state and could not accept the update.
  */
enum UpdateSendResult[+R] {
  case Success(value: R)
  case Unhandled
  case InstanceAlreadyCompleted
}

/** A synchronous, direct-addressed request channel into a workflow instance.
  *
  * An [[Update]] is like a `Signal` with a synchronous response channel: the
  * sender blocks until the addressed workflow runs and either handles the update
  * (returning a `response` via `Step.awaitUpdate`) or finishes without handling
  * it. Updates are addressed directly to one instance and are never inherited by
  * children (a synchronous request cannot have one unambiguous response if
  * several descendants handle it).
  *
  * The key is the durable identity of the update; `I` is the request payload type
  * and `R` the response type. `Cacheable[I]`/`Cacheable[R]` are captured at
  * definition time so senders and receivers agree on the payload format.
  */
final class Update[I, R](val key: String)(using
    val inputCacheable: Cacheable[I],
    val responseCacheable: Cacheable[R]
) {

  /** Send the update to `instanceId` and block for its outcome. Forwarder for
    * the runtime's `sendUpdate`, resolving this update's [[Cacheable]]s.
    *
    * The sender runs the addressed workflow on its own thread when needed, so a
    * [[Workflow]] definition is required (the sender must know which code to
    * run).
    */
  @throws[WorkflowNotFoundException]
  def send(
      workflow: Workflow[?, ?],
      instanceId: WorkflowInstanceId,
      input: I,
      idempotencyKey: String = "",
      persistUnhandledUpdates: Boolean = false
  )(using
      runtime: WorkflowRuntime,
      cacheableThrowable: Cacheable[Throwable]
  ): UpdateSendResult[R] =
    runtime.sendUpdate(workflow, instanceId, key, input, idempotencyKey, persistUnhandledUpdates)(
      using this,
      cacheableThrowable
    )
}

object Update {
  def apply[I: Cacheable, R: Cacheable](key: String): Update[I, R] = new Update[I, R](key)
}
