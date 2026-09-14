package test

import atomicflow.*
import atomicflow.given
import atomicflow.impl.Sha256Fingerprinter
import munit.FunSuite

class StepInputSuite extends FunSuite:

  test("a string-value pair converts to a StepInput with matching fingerprint") {
    val input: StepInput[String] = "name" -> "value"
    assertEquals(input.name, "name")
    assertEquals(input.value, "value")
    assertEquals(input.fingerprint(Sha256Fingerprinter), Sha256Fingerprinter.fingerprint("value"))
  }

  test("fingerprints are stable for equal values and differ for unequal values") {
    val a: StepInput[String] = "k" -> "same"
    val b: StepInput[String] = "k" -> "same"
    val c: StepInput[String] = "k" -> "different"
    assertEquals(a.fingerprint(Sha256Fingerprinter), b.fingerprint(Sha256Fingerprinter))
    assertNotEquals(a.fingerprint(Sha256Fingerprinter), c.fingerprint(Sha256Fingerprinter))
  }

  test("heterogeneous inputs fingerprint without a Fingerprintable[Any]") {
    val intInput: StepInput[Int] = "n" -> 7
    val seqInput: StepInput[Seq[String]] = "xs" -> Seq("a", "b")
    assertNotEquals(intInput.fingerprint(Sha256Fingerprinter), seqInput.fingerprint(Sha256Fingerprinter))
  }
