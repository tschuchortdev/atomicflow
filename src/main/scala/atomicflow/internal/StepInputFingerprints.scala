package atomicflow.internal

import atomicflow.Fingerprintable.Fingerprint

case class StepInputFingerprints(fingerprints: Map[String, Fingerprint.Aux[String]])
