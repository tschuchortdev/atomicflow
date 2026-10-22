package test

import atomicflow.Fingerprintable
import munit.FunSuite
import atomicflow.Fingerprintable.given
import atomicflow.Fingerprintable.*
import atomicflow.impl.Sha256Fingerprinter

class FingerprintableSuite extends FunSuite {
  test("fingerprint strings") {
    assertEquals(Sha256Fingerprinter.fingerprint("hello world").toString, "uU0nuZNNPgilLlLX2n2r-sSE7-N6U4DukIj3rOLvzek")
    assertEquals(Sha256Fingerprinter.fingerprint("hello world2").toString, "-TwgswFx0Q53PcKi2O1ZUkslut3zgbg_zE7ED1C-2zM")
    assertEquals(Sha256Fingerprinter.fingerprint("hello world").toString, "uU0nuZNNPgilLlLX2n2r-sSE7-N6U4DukIj3rOLvzek")
  }

  test("fingerprint tuple") {
    val test = ("a", 1)
    assertEquals(Sha256Fingerprinter.fingerprint(test).toString, "szFiS_M_N_16kRhNTZ7Be5hiqHCCfZFLihkLu-PNxjs")
  }

  case class TestProduct(a: String, b: Int)derives Fingerprintable

  test("fingerprint object") {
    val test = TestProduct("a", 1)
    assertEquals(Sha256Fingerprinter.fingerprint(test).toString, "zwrq5fiagysUoK3lKEWtjtcqZ3KyI8o5h8QfgC-AuLA")
  }

  test("fingerprint normalizes equal numeric values across representations") {
    assertEquals(Sha256Fingerprinter.fingerprint(1), Sha256Fingerprinter.fingerprint(1L))
    assertEquals(
      Sha256Fingerprinter.fingerprint(2.5: Double),
      Sha256Fingerprinter.fingerprint(BigDecimal("2.500"))
    )
    assertEquals(
      Sha256Fingerprinter.fingerprint(1.5f),
      Sha256Fingerprinter.fingerprint(1.5: Double)
    )
  }

  test("fingerprint distinguishes values that differ") {
    assertNotEquals(Sha256Fingerprinter.fingerprint("a"), Sha256Fingerprinter.fingerprint("b"))
    assertNotEquals(Sha256Fingerprinter.fingerprint(Seq(1, 2)), Sha256Fingerprinter.fingerprint(Seq(2, 1)))
  }

  test("fingerprint sequences preserve order and element boundaries") {
    assertEquals(
      Sha256Fingerprinter.fingerprint(List("a", "b")),
      Sha256Fingerprinter.fingerprint(Seq("a", "b"))
    )
    assertNotEquals(
      Sha256Fingerprinter.fingerprint(Seq("a", "b")),
      Sha256Fingerprinter.fingerprint(Seq("ab"))
    )
    assertEquals(
      Sha256Fingerprinter.fingerprint(Seq.empty[Int]),
      Sha256Fingerprinter.fingerprint(List.empty[Int])
    )
  }

  test("fingerprint maps ignore insertion order but not values") {
    assertEquals(
      Sha256Fingerprinter.fingerprint(Map("a" -> 1, "b" -> 2)),
      Sha256Fingerprinter.fingerprint(Map("b" -> 2, "a" -> 1))
    )
    assertNotEquals(
      Sha256Fingerprinter.fingerprint(Map("a" -> 1)),
      Sha256Fingerprinter.fingerprint(Map("a" -> 2))
    )
  }

  test("fingerprint byte arrays and IArrays agree") {
    assertEquals(
      Sha256Fingerprinter.fingerprint(Array[Byte](1, 2, 3)),
      Sha256Fingerprinter.fingerprint(IArray[Byte](1, 2, 3))
    )
    assertNotEquals(
      Sha256Fingerprinter.fingerprint(Array[Byte](1, 2, 3)),
      Sha256Fingerprinter.fingerprint(Array[Byte](3, 2, 1))
    )
  }

  test("Fingerprint.fromString round-trips its toString") {
    val fingerprint = Sha256Fingerprinter.fingerprint("hello world")
    assertEquals(Fingerprint.fromString(fingerprint.toString), fingerprint)
  }

  test("contramap fingerprints the mapped value") {
    val byLength = Fingerprintable.contramap(Fingerprintable[Int])((s: String) => s.length)
    assertEquals(byLength.fingerprint("abc", Sha256Fingerprinter), Sha256Fingerprinter.fingerprint(3))
  }
}
