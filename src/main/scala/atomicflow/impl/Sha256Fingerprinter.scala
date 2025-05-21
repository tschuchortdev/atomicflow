package atomicflow.impl

import atomicflow.Fingerprintable
import atomicflow.Fingerprintable.{Fingerprint, Fingerprinter}

import java.security.MessageDigest

object Sha256Fingerprinter extends Fingerprinter {
  override type Rep = String

  override def fromRep(rep: String): Sha256Fingerprinter.Fingerprint = Fingerprint(rep)

  private def sha256(bytes: Array[Byte]): String = {
    val digest = MessageDigest.getInstance("SHA-256")
    digest.update(bytes)
    digest.digest().map("%02x".format(_)).mkString
  }

  def stringRep(string: String): String =
    sha256(string.getBytes("UTF-8"))

  def bytesRep(bytes: IArray[Byte]): String =
    sha256(bytes.asInstanceOf[Array[Byte]])

  def iterableRep(list: Iterable[String]): String =
    stringRep(list.mkString("[", ",", "]"))

  def objRep(obj: Iterable[(String, String)]): String =
    // Concatenate key:value pairs in order, surrounded by braces
    stringRep(obj.toSeq.sortBy(_._1).map { case (k, v) => s"$k:$v" }.mkString("{", ",", "}"))

  def longRep(long: Long): String =
    stringRep(long.toString)

  def doubleRep(double: Double): String =
    stringRep(BigDecimal(double).bigDecimal.stripTrailingZeros.toPlainString)

  def bigDecimalRep(bigDecimal: BigDecimal): String =
    stringRep(bigDecimal.bigDecimal.stripTrailingZeros.toPlainString)
}