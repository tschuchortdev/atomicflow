package test

import atomicflow.Fingerprintable
import munit.FunSuite
import atomicflow.Fingerprintable.given
import atomicflow.Fingerprintable.*
import atomicflow.impl.Sha256Fingerprinter

class FingerprintableSuite extends FunSuite {
  test("fingerprint strings") {
    assertEquals(Sha256Fingerprinter.fingerprint("hello world").toString, "uU0nuZNNPgilLlLX2n2r+sSE7+N6U4DukIj3rOLvzek=")
    assertEquals(Sha256Fingerprinter.fingerprint("hello world2").toString, "+TwgswFx0Q53PcKi2O1ZUkslut3zgbg/zE7ED1C+2zM=")
    assertEquals(Sha256Fingerprinter.fingerprint("hello world").toString, "uU0nuZNNPgilLlLX2n2r+sSE7+N6U4DukIj3rOLvzek=")
  }

  test("fingerprint tuple") {
    val test = ("a", 1)
    assertEquals(Sha256Fingerprinter.fingerprint(test).toString, "szFiS/M/N/16kRhNTZ7Be5hiqHCCfZFLihkLu+PNxjs=")
  }

  case class TestProduct(a: String, b: Int)derives Fingerprintable

  test("fingerprint object") {
    val test = TestProduct("a", 1)
    assertEquals(Sha256Fingerprinter.fingerprint(test).toString, "zwrq5fiagysUoK3lKEWtjtcqZ3KyI8o5h8QfgC+AuLA=")
  }
}
