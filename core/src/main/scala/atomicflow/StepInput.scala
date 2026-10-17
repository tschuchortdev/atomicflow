package atomicflow

import atomicflow.Fingerprintable.{Fingerprint, Fingerprinter}

/** A named step input and the [[Fingerprintable]] instance used to hash it.
  *
  * Each input retains its own type, so a sequence can contain values of
  * different types without requiring `Fingerprintable[Any]`.
  *
  * Write `"key" -> value` to use the implicit conversion, or construct a
  * `StepInput` directly.
  */
case class StepInput[A: Fingerprintable](name: String, value: A) {
  def fingerprint(fingerprinter: Fingerprinter): Fingerprint = fingerprinter.fingerprint(value)
}

given [A: Fingerprintable] => Conversion[(String, A), StepInput[A]] = { (name: String, value: A) =>
  StepInput(name, value)
}
