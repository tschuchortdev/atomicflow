package atomicflow.impl

import atomicflow.Fingerprintable
import atomicflow.Fingerprintable.{Fingerprint, Fingerprinter}

import java.security.MessageDigest
import java.util.Base64

object Sha256Fingerprinter extends Fingerprinter {
  override type Rep = Array[Byte]

  def fromRep(rep: Rep): Fingerprint =
    Fingerprint(rep.asInstanceOf[IArray[Byte]])

  private def sha256(bytes: Array[Byte]): Rep = {
    val digest = MessageDigest.getInstance("SHA-256")
    digest.update(bytes)
    digest.digest()
  }

  def stringRep(string: String): Rep =
    sha256(string.getBytes("UTF-8"))

  def bytesRep(bytes: IArray[Byte]): Rep =
    sha256(bytes.asInstanceOf[Array[Byte]])

  private def encodeRep(rep: Rep): String = Base64.getEncoder.encodeToString(rep)

  def iterableRep(list: Iterable[Rep]): Rep =
    stringRep(list.map(encodeRep).mkString("[", ",", "]"))

  def objRep(obj: Iterable[(Rep, Rep)]): Rep =
    // Concatenate key:value pairs in order, surrounded by braces
    stringRep(
      obj
        .map { (k, v) => (encodeRep(k), encodeRep(v)) }
        .toSeq
        .sortBy(_._1)
        .map { (k, v) => s"$k:$v" }
        .mkString("{", ",", "}")
    )

  def longRep(long: Long): Rep =
    stringRep(long.toString)

  def doubleRep(double: Double): Rep =
    stringRep(BigDecimal(double).bigDecimal.stripTrailingZeros.toPlainString)

  def bigDecimalRep(bigDecimal: BigDecimal): Rep =
    stringRep(bigDecimal.bigDecimal.stripTrailingZeros.toPlainString)
}