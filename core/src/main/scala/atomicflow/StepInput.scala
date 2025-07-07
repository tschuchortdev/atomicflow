package atomicflow

import atomicflow.Fingerprintable.{Fingerprint, Fingerprinter}

case class StepInput[A: Fingerprintable](name: String, value: A) {
  def fingerprint(fingerprinter: Fingerprinter): Fingerprint = fingerprinter.fingerprint(value)
}

given [A: Fingerprintable] => Conversion[(String, A), StepInput[A]] = { (name: String, value: A) =>
  StepInput(name, value)
}
