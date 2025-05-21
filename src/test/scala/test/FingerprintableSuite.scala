package test

import atomicflow.Fingerprintable
import munit.FunSuite
import atomicflow.Fingerprintable.given
import atomicflow.Fingerprintable.*
import atomicflow.impl.Sha256Fingerprinter

class FingerprintableSuite extends FunSuite {
  test("fingerprint strings") {
    assertEquals(Sha256Fingerprinter.fingerprint("hello world").value, "b94d27b9934d3e08a52e52d7da7dabfac484efe37a5380ee9088f7ace2efcde9")
    assertEquals(Sha256Fingerprinter.fingerprint("hello world2").value, "f93c20b30171d10e773dc2a2d8ed59524b25baddf381b83fcc4ec40f50bedb33")
    assertEquals(Sha256Fingerprinter.fingerprint("hello world").value, "b94d27b9934d3e08a52e52d7da7dabfac484efe37a5380ee9088f7ace2efcde9")
  }

  test("fingerprint tuple") {
    val test = ("a", 1)
    assertEquals(Sha256Fingerprinter.fingerprint(test).value, "7bc60e164a6ed5e8ffdc6a1900f2908f84cf26ec2f08dd7bf5b6d42c53eb5066")
  }

  case class TestProduct(a: String, b: Int)derives Fingerprintable

  test("fingerprint object") {
    val test = TestProduct("a", 1)
    assertEquals(Sha256Fingerprinter.fingerprint(test).value, "fa2c0d44565b018468a85276d5197d3a5dab8490cf983c96de80b106549d1253")
  }
}
