package atomicflow.impl

import atomicflow.Fingerprintable
import atomicflow.Fingerprintable.{Fingerprint, Fingerprinter}

import java.security.MessageDigest

object Sha256Fingerprinter extends Fingerprinter {
  override type Rep = String

  def fromRep(rep: String): Fingerprint = Fingerprint(rep)

  private def sha256(bytes: Array[Byte]): Rep = {
    val digest = MessageDigest.getInstance("SHA-256")
    digest.update(bytes)
    digest.digest().map("%02x".format(_)).mkString
  }

  def stringRep(string: String): Rep =
    sha256(string.getBytes("UTF-8"))

  def bytesRep(bytes: IArray[Byte]): Rep =
    sha256(bytes.asInstanceOf[Array[Byte]])

  def iterableRep(list: Iterable[Rep]): Rep =
    stringRep(list.mkString("[", ",", "]"))

  def objRep(obj: Iterable[(Rep, Rep)]): Rep =
    // Concatenate key:value pairs in order, surrounded by braces
    stringRep(obj.toSeq.sortBy(_._1).map { case (k, v) => s"$k:$v" }.mkString("{", ",", "}"))

  def longRep(long: Long): Rep =
    stringRep(long.toString)

  def doubleRep(double: Double): Rep =
    stringRep(BigDecimal(double).bigDecimal.stripTrailingZeros.toPlainString)

  def bigDecimalRep(bigDecimal: BigDecimal): Rep =
    stringRep(bigDecimal.bigDecimal.stripTrailingZeros.toPlainString)
}