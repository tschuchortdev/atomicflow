package test

import atomicflow.*
import atomicflow.Cacheable.{Json, Simple}
import munit.FunSuite

import java.nio.file.Paths

// fixtures live in an object so they have no outer pointer (serializable)
object CacheableSuiteFixtures:
  case class Order(id: Int, items: Seq[String]) derives Cacheable
  case class ReceiptV2(id: String, amount: Int) derives Cacheable
  case class Tagged(x: String) derives Cacheable
  case class PaymentDeclined(reason: String) extends RuntimeException(reason) derives Cacheable
  case class ProviderUnavailable(reason: String) extends RuntimeException(reason) derives Cacheable

class CacheableSuite extends FunSuite:
  import CacheableSuiteFixtures.*

  private def roundTrip[A](value: A)(using c: Cacheable[A]): A =
    c.read(c.write(value))

  test("Simple givens round-trip values") {
    import Simple.given
    assertEquals(roundTrip("hello wörld"), "hello wörld")
    assertEquals(roundTrip(42), 42)
    assertEquals(roundTrip(42L), 42L)
    assertEquals(roundTrip(true), true)
    assertEquals(roundTrip(1.5), 1.5)
    assertEquals(roundTrip(1.5f), 1.5f)
    assertEquals(roundTrip(()), ())
  }

  test("Simple givens have pinned stable serialized type ids") {
    import Simple.given
    assertEquals(summon[Cacheable[String]].stableSerializedTypeId, "string")
    assertEquals(summon[Cacheable[Int]].stableSerializedTypeId, "int")
    assertEquals(summon[Cacheable[Long]].stableSerializedTypeId, "long")
    assertEquals(summon[Cacheable[Boolean]].stableSerializedTypeId, "boolean")
    assertEquals(summon[Cacheable[Double]].stableSerializedTypeId, "double")
    assertEquals(summon[Cacheable[Float]].stableSerializedTypeId, "float")
    assertEquals(summon[Cacheable[Unit]].stableSerializedTypeId, "unit")
    assertEquals(summon[Cacheable[Seq[Int]]].stableSerializedTypeId, "seq")
    assertEquals(summon[Cacheable[Option[Int]]].stableSerializedTypeId, "option")
    assertEquals(summon[Cacheable[Map[String, Int]]].stableSerializedTypeId, "map")
  }

  test("Seq and Option and Map givens round-trip nested values") {
    import Simple.given
    assertEquals(roundTrip(Seq(1, 2, 3)), Seq(1, 2, 3))
    assertEquals(roundTrip(Seq.empty[String]), Seq.empty[String])
    assertEquals(roundTrip(Seq(Seq("a"), Seq())), Seq(Seq("a"), Seq()))
    assertEquals(roundTrip(Some(1): Option[Int]), Some(1))
    assertEquals(roundTrip(None: Option[Int]), None)
    assertEquals(roundTrip(Map("a" -> 1, "b" -> 2)), Map("a" -> 1, "b" -> 2))
    assertEquals(roundTrip(Map.empty[String, Int]), Map.empty[String, Int])
  }

  test("imap adapts types and preserves the serialized type id") {
    import Simple.given
    val pathCacheable = summon[Cacheable[String]].imap(s => Paths.get(s))(_.toString)
    assertEquals(pathCacheable.stableSerializedTypeId, "string")
    assertEquals(roundTrip(Paths.get("/tmp/x"))(using pathCacheable), Paths.get("/tmp/x"))
  }

  test("Json-derived cacheable round-trips case classes and pins its type id") {
    val c = summon[Cacheable[Order]]
    assertEquals(c.stableSerializedTypeId, s"json:${classOf[Order].getName}")
    assertEquals(roundTrip(Order(1, Seq("a", "b"))), Order(1, Seq("a", "b")))
  }


  test("withFallback writes with the current codec and reads both formats") {
    import Simple.given
    val v1 = summon[Cacheable[String]].imap(s => ReceiptV2(s, 0))(r => r.id)
    val v2 = summon[Cacheable[ReceiptV2]]
    val codec = v2.withFallback(v1)

    // newly written data round-trips via the current codec
    assertEquals(codec.read(codec.write(ReceiptV2("new", 5))), ReceiptV2("new", 5))
    // legacy payload (written by v1 directly) is read through the fallback
    val legacyPayload = v1.write(ReceiptV2("old", 0))
    assertEquals(codec.read(legacyPayload), ReceiptV2("old", 0))
  }

  test("withFallback tries the current format first, then fallbacks in order") {
    import Simple.given
    // v2 tags with "str:", v1 tags with "int:" so the winning fallback is observable
    val v1 = summon[Cacheable[Int]].imap(i => Tagged(s"int:$i"))(v => v.x.stripPrefix("int:").toInt)
    val v2 = summon[Cacheable[String]].imap(s => Tagged(s"str:$s"))(v => v.x.stripPrefix("str:"))
    val codec = summon[Cacheable[Tagged]].withFallback(v2).withFallback(v1)

    // current JSON payload wins
    assertEquals(codec.read(codec.write(Tagged("a"))), Tagged("a"))
    // "7" is not valid JSON for Tagged; v2 is tried before v1 and accepts it
    assertEquals(codec.read("7"), Tagged("str:7"))
  }


  test("unionMostSpecific round-trips members and falls back for unmatched throwables") {
    given Cacheable[Throwable] =
      Cacheable.unionMostSpecific[(PaymentDeclined, ProviderUnavailable)](
        fallback = Cacheable.forThrowable.genericStringMessageSerializer
      )

    val declined = PaymentDeclined("insufficient funds")
    assertEquals(roundTrip(declined), declined)
    assertEquals(roundTrip(ProviderUnavailable("down")), ProviderUnavailable("down"))

    val unmatched: Throwable = new IllegalStateException("nope")
    val decoded = summon[Cacheable[Throwable]].read(summon[Cacheable[Throwable]].write(unmatched))
    decoded match
      case _: StepFailed => // expected: fallback stores portable diagnostics
      case other => fail(s"expected StepFailed from fallback, got $other")
    assert(clue(decoded.getMessage).contains("IllegalStateException"))
    assert(clue(decoded.getMessage).contains("nope"))
  }

  test("unionMostSpecific selects the most specific matching member") {
    given broad: Cacheable[RuntimeException] =
      Cacheable.forThrowable.javaSerializable.imap(t => t.asInstanceOf[RuntimeException])(r => r)
    given Cacheable[Throwable] =
      Cacheable.unionMostSpecific[(PaymentDeclined, RuntimeException)](
        fallback = Cacheable.forThrowable.genericStringMessageSerializer
      )
    val c = summon[Cacheable[Throwable]]

    val declined = PaymentDeclined("insufficient funds")
    // both members match; the more specific member (PaymentDeclined) must win
    assertEquals(c.read(c.write(declined)), declined)
    // a plain RuntimeException matches only the broad member
    val plain = new RuntimeException("plain")
    assertEquals(c.read(c.write(plain)).getMessage, "plain")
    // ... an unknown specific member goes through the fallback
    assert(clue(c.read(c.write(ProviderUnavailable("down"))).getMessage).contains("down"))
  }

  test("unionMostSpecific rejects overlapping incomparable members") {
    trait OverlappingA extends Exception
    trait OverlappingB extends Exception
    given Cacheable[OverlappingA] =
      Cacheable.forThrowable.javaSerializable.imap(t => t.asInstanceOf[OverlappingA])(r => r)
    given Cacheable[OverlappingB] =
      Cacheable.forThrowable.javaSerializable.imap(t => t.asInstanceOf[OverlappingB])(r => r)

    intercept[IllegalArgumentException] {
      Cacheable.unionMostSpecific[(OverlappingA, OverlappingB)](
        fallback = Cacheable.forThrowable.genericStringMessageSerializer
      )
    }
  }

  test("forThrowable.genericStringMessageSerializer replays StepFailed with original diagnostics") {
    val c = Cacheable.forThrowable.genericStringMessageSerializer
    assertEquals(c.stableSerializedTypeId, "throwable-generic")
    val decoded = c.read(c.write(new IllegalArgumentException("boom")))
    decoded match
      case _: StepFailed => // expected
      case other => fail(s"expected StepFailed, got $other")
    assert(clue(decoded.getMessage).contains("IllegalArgumentException"))
    assert(clue(decoded.getMessage).contains("boom"))
    // nested causes are preserved as portable diagnostics
    val withCause = new RuntimeException("outer", new IllegalArgumentException("inner"))
    val decodedWithCause = c.read(c.write(withCause))
    assert(clue(decodedWithCause.getMessage).contains("outer"))
    assert(clue(decodedWithCause.getCause).getMessage.contains("inner"))
  }

  test("forThrowable.javaSerializable round-trips concrete exception classes") {
    val c = Cacheable.forThrowable.javaSerializable
    assertEquals(c.stableSerializedTypeId, "throwable-java")
    val e = PaymentDeclined("insufficient funds")
    assertEquals(c.read(c.write(e)), e)
  }
