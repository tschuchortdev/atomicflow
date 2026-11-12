package atomicflow

/** A value encoded by a [[Cacheable]] codec. Opaque on purpose: only the codec
  * that produced it may interpret it. */
opaque type SerializedValue = String

object SerializedValue:
  /** Wraps a raw encoded string at a codec boundary. */
  def of(raw: String): SerializedValue = raw
  extension (value: SerializedValue)
    /** The raw encoded string, for handing back to a codec. */
    def raw: String = value
