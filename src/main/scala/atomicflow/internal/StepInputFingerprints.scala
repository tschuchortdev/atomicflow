package atomicflow.internal

import atomicflow.Fingerprintable.Fingerprint
import io.circe.Codec
import cats.syntax.all.*

case class StepInputFingerprints(fingerprints: Map[String, Fingerprint])

object StepInputFingerprints {
  given Codec[StepInputFingerprints] =
    Codec.implied[Map[String, Fingerprint]].imap(StepInputFingerprints(_))(_.fingerprints)
}
