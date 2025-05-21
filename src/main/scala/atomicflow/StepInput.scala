package atomicflow

import atomicflow.Fingerprintable.Fingerprinter

case class StepInput[A: Fingerprintable](name: String, value: A) {
  def fingerprint(fingerprinter: Fingerprinter): fingerprinter.Fingerprint = fingerprinter.fingerprint(value)
}

given [A: Fingerprintable] => Conversion[(String, A), StepInput[A]] = { (name: String, value: A) =>
  StepInput(name, value)
}
