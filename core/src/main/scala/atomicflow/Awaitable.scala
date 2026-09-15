package atomicflow

import scala.concurrent.duration.{Duration, FiniteDuration}
import java.time.{Clock, Instant}

/** A durable description of something a workflow can wait on. The result type
  * `R` is what the successful await yields; it is invariant because the cases
  * embed their value-producing witnesses (a `SignalEvent`, `Timer`, or
  * `WorkflowCompletion`, possibly wrapped by `Mapped`), so `Awaitable[A]` and
  * `Awaitable[B]` are never interchangeable.
  *
  * The enum deliberately carries no `Cacheable` context bound: the `Cacheable`
  * needed to persist the awaited result is supplied at the `Step.await` call
  * site (the result's type), not here.
  */
enum Awaitable[R] {

  /** Await the next visible event of the exact signal key that satisfies
    * `filter`, optionally restricted to events accepted within the last
    * `lookBack` (measured by the event's original `created_at` against the
    * runtime clock).
    */
  case SignalEvent[A](
      s: Signal[A],
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
    * here; the `Cacheable` for the mapped result is supplied at the `Step.await`
    * call site.
    */
  def map[B](f: R => B): Awaitable[B] = this match {
    case Mapped(u, g) => Mapped(u, g.andThen(f))
    case other        => Mapped(other, f)
  }
}

object Awaitable {
  object Timer {
    /** A timer `delay` from the supplied clock's current instant. */
    def apply(delay: FiniteDuration)(using clk: Clock): Timer =
      Timer(clk.instant().plusMillis(delay.toMillis))
  }
}
