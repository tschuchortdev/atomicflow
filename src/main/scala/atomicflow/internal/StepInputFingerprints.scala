package atomicflow.internal

import atomicflow.Fingerprintable.Fingerprint
import cats.syntax.all.*
import io.circe.Codec

case class StepInputFingerprints(fingerprints: Map[String, Fingerprint])

object StepInputFingerprints {
  given Codec[StepInputFingerprints] =
    Codec.implied[Map[String, Fingerprint]].imap(StepInputFingerprints(_))(_.fingerprints)
}
