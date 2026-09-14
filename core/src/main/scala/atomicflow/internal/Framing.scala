package atomicflow.internal

/** Self-delimiting string frames used by composite [[atomicflow.Cacheable]] codecs
  * to embed a member codec's id and payload without assuming any inner format.
  *
  * A frame is `<length>:<content>` where `length` is the number of UTF-16 chars in
  * `content`. Frames concatenate unambiguously.
  */
private[atomicflow] object Framing {

  def write(content: String): String = s"${content.length}:$content"

  /** Reads the frame starting at `offset`; returns the frame content and the offset
    * just after it.
    */
  def read(s: String, offset: Int): (String, Int) = {
    if (offset < 0 || offset >= s.length)
      throw new IllegalArgumentException(s"Framing: no frame at offset $offset in: $s")
    val colon = s.indexOf(':', offset)
    if (colon <= offset)
      throw new IllegalArgumentException(s"Framing: missing or empty length at offset $offset in: $s")
    val lengthPart = s.substring(offset, colon)
    if (!lengthPart.forall(_.isDigit))
      throw new IllegalArgumentException(s"Framing: invalid frame length '$lengthPart' in: $s")
    val length =
      try lengthPart.toInt
      catch case _: NumberFormatException => throw new IllegalArgumentException(s"Framing: frame length overflow in: $s")
    val contentStart = colon + 1
    val contentEnd = contentStart + length
    if (contentEnd > s.length)
      throw new IllegalArgumentException(s"Framing: frame content overruns the string in: $s")
    (s.substring(contentStart, contentEnd), contentEnd)
  }
}
