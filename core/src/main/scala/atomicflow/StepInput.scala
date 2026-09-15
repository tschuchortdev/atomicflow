package atomicflow

import atomicflow.Fingerprintable.{Fingerprint, Fingerprinter}

/** A single, named cache key of a workflow step.
 *
  * The sole purpose of this type is to **reify the [[Fingerprintable]] evidence**
  * of the value. Keeping the concrete `A` (rather than a widened `Any`) lets the
  * runtime fingerprint heterogeneous inputs without requiring a
  * `Fingerprintable[Any]`; this is why cache keys are `Seq[StepInput[?]]` and not
  * `Map[String, Any]`.
  *
  * Callers never construct a `StepInput` directly. Instead, they write
  * `"key" -> value` and rely on the implicit conversion.
  */
case class StepInput[A: Fingerprintable](name: String, value: A) {
  def fingerprint(fingerprinter: Fingerprinter): Fingerprint = fingerprinter.fingerprint(value)
}

given [A: Fingerprintable] => Conversion[(String, A), StepInput[A]] = { (name: String, value: A) =>
  StepInput(name, value)
}
