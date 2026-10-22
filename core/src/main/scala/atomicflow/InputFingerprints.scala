package atomicflow

/** The encoded fingerprints of a step's named inputs, as stored in
  * `workflow_steps.input_fingerprints`. Opaque: only the step-input encoder and
  * comparator interpret it. */
opaque type InputFingerprints = String

object InputFingerprints:
  def of(raw: String): InputFingerprints = raw
  extension (value: InputFingerprints)
    def raw: String = value
