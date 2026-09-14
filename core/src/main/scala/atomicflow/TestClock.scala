package atomicflow

import java.time.{Clock, Instant, ZoneId}
import scala.concurrent.duration.FiniteDuration

/** A mutable [[Clock]] for tests: its `instant` is fixed at construction and
  * only changes via [[advanceBy]], so tests control time deterministically. The
  * zone is always UTC.
  */
final class TestClock(private var now: Instant) extends Clock {
  override def withZone(zone: ZoneId): Clock = this
  override def getZone: ZoneId = ZoneId.of("UTC")
  override def instant: Instant = now

  /** Advances the clock by `duration`. */
  def advanceBy(duration: FiniteDuration): Unit =
    now = now.plus(java.time.Duration.ofNanos(duration.toNanos))
}
