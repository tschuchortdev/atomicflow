package atomicflow

import scala.concurrent.duration.{Duration, FiniteDuration}
import java.time.Instant

/** A signal, timer, or workflow completion to wait for, with result type `R`.
  * `Step.await` and `Step.awaitRace` require a `Cacheable` for the result they store.
  */
enum Awaitable[R] {

  /** Await the next visible event of the exact signal key that satisfies
    * `filter`, optionally restricted to events accepted within the last
    * `lookBack` (measured by the event's original `created_at` against the
    * runtime clock).
    */
  case SignalEvent[A](
      signal: Signal[A],
      filter: A => Boolean = (_: A) => true,
      lookBack: Duration = Duration.Inf
  ) extends Awaitable[A]

  /** Await until the given absolute deadline. */
  case Timer(deadline: Instant) extends Awaitable[Unit]

  /** Await the terminal outcome of another workflow instance. Carries the
    * composite codec needed to decode the raw `WorkflowCompletionResult[R]`, so
    * it can be mapped onto another result type and raced against other
    * awaitables without losing the ability to decode the completed payload.
    */
  case WorkflowCompletion[R](workflowInstanceId: WorkflowInstanceId)(using
      val completionCacheable: Cacheable[WorkflowCompletionResult[R]]
  ) extends Awaitable[WorkflowCompletionResult[R]]

  /** The result of mapping an underlying awaitable. Used to align heterogeneous
    * raw results onto a common result type for racing and combining.
    */
  case Mapped[A, B](underlying: Awaitable[A], f: A => B) extends Awaitable[B]

  /** Transform the awaited result onto another type. No `Cacheable[B]` is needed
    * here; `Step.awaitRace` requires the mapped result's codec. Direct
    * `Step.await` does not currently support mapped awaitables.
    */
  def map[B](f: R => B): Awaitable[B] = this match {
    case Mapped(u, g) => Mapped(u, g.andThen(f))
    case other        => Mapped(other, f)
  }
}

object Awaitable {
  object Timer {

    /** A timer `delay` from the runtime clock's current instant: resolves the
      * clock through the [[WorkflowContext]] of the calling workflow body, so
      * deadlines are always computed from the executing runtime's single time
      * source — never from an ambient clock of the enclosing scope. Construct
      * the absolute [[Timer]] case directly when building an awaitable outside
      * a workflow body.
      */
    def apply(delay: FiniteDuration)(using ctx: WorkflowContext): Timer =
      Timer(ctx.runtime.clock.instant().plusNanos(delay.toNanos))
  }
}
