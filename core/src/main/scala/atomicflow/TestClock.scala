package atomicflow

import java.time.{Clock, Instant, ZoneId}
import java.util.concurrent.atomic.AtomicReference
import scala.concurrent.duration.FiniteDuration

/** A mutable [[Clock]] for tests: its `instant` is fixed at construction and
  * only changes via [[advanceBy]], so tests control time deterministically.
  *
  * [[withZone]] returns a clock that reports the requested zone while sharing
  * this clock's mutable instant, so advancing the original also advances any
  * zone-adjusted view. The default zone is UTC.
  */
final class TestClock private (private val now: AtomicReference[Instant], private val zone: ZoneId)
    extends Clock {
  def this(now: Instant) = this(new AtomicReference[Instant](now), ZoneId.of("UTC"))

  override def withZone(zone: ZoneId): Clock = new TestClock(now, zone)
  override def getZone: ZoneId = zone
  override def instant: Instant = now.get()

  /** Advances the clock by `duration`. */
  def advanceBy(duration: FiniteDuration): Unit =
    now.updateAndGet(_.plus(java.time.Duration.ofNanos(duration.toNanos)))
}
