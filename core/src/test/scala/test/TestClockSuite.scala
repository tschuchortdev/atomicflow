package test

import atomicflow.TestClock
import java.time.{Instant, ZoneId}
import munit.FunSuite
import scala.concurrent.duration.*

class TestClockSuite extends FunSuite {
  test("withZone returns a clock reporting the requested zone while sharing the mutable instant") {
    val base = Instant.parse("2024-01-01T12:00:00Z")
    val tc = new TestClock(base)
    val paris = tc.withZone(ZoneId.of("Europe/Paris"))
    assertEquals(tc.getZone, ZoneId.of("UTC"), "the original clock keeps its default zone")
    assertEquals(paris.getZone, ZoneId.of("Europe/Paris"), "withZone honors the requested zone")
    assertEquals(paris.instant, base)
    tc.advanceBy(1.hour)
    assertEquals(
      paris.instant,
      base.plus(java.time.Duration.ofHours(1)),
      "withZone shares the original's mutable instant"
    )
    assertEquals(tc.instant, base.plus(java.time.Duration.ofHours(1)))
  }

  test("withZone is stable and does not mutate the original clock's zone") {
    val tc = new TestClock(Instant.parse("2024-01-01T12:00:00Z"))
    val tokyo = tc.withZone(ZoneId.of("Asia/Tokyo"))
    assertEquals(tc.getZone, ZoneId.of("UTC"))
    assertEquals(tokyo.getZone, ZoneId.of("Asia/Tokyo"))
    assertEquals(tc.withZone(ZoneId.of("UTC")).getZone, ZoneId.of("UTC"))
  }
}
