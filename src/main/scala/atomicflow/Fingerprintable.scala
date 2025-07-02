package atomicflow

import atomicflow.Fingerprintable.{Fingerprint, Fingerprinter}
import cats.Contravariant
import cats.syntax.all.*
import io.circe.Codec

import java.util
import java.util.Base64
import scala.compiletime.*
import scala.deriving.Mirror

trait Fingerprintable[-A] {
  def fingerprintRep(value: A, fp: Fingerprinter): fp.Rep

  final def fingerprint(value: A, fp: Fingerprinter): Fingerprint =
    fp.fromRep(fingerprintRep(value, fp))
}

object Fingerprintable {
  inline def apply[A](using f: Fingerprintable[A]): Fingerprintable[A] = f

  given Contravariant[Fingerprintable] = new {
    override def contramap[A, B](fa: Fingerprintable[A])(f: B => A): Fingerprintable[B] = new Fingerprintable[B] {
      override def fingerprintRep(value: B, fp: Fingerprinter): fp.Rep =
        fa.fingerprintRep(f(value), fp)
    }
  }

  case class Fingerprint(bytes: IArray[Byte]) {
    override lazy val toString: String =
      Base64.getEncoder.encodeToString(bytes.asInstanceOf[Array[Byte]])

    override def equals(obj: Any): Boolean = obj match {
      case other: Fingerprint if util.Arrays.equals(
        bytes.asInstanceOf[Array[Byte]],
        other.bytes.asInstanceOf[Array[Byte]]
      ) => true
      case _ => false
    }
  }

  object Fingerprint {
    def fromString(string: String): Fingerprint =
      Fingerprint(Base64.getDecoder.decode(string).asInstanceOf[IArray[Byte]])

    given Codec[Fingerprint] = Codec.implied[String].imap(fromString)(_.toString)
  }

  trait Fingerprinter {
    self =>

    type Rep

    final def fingerprint[A](value: A)(using fingerprintable: Fingerprintable[A]): Fingerprint =
      fingerprintable.fingerprint(value, this)

    def fromRep(rep: Rep): Fingerprint

    def stringRep(string: String): Rep

    def bytesRep(bytes: IArray[Byte]): Rep

    def iterableRep(list: Iterable[Rep]): Rep

    def objRep(obj: Iterable[(Rep, Rep)]): Rep

    def longRep(long: Long): Rep

    def doubleRep(double: Double): Rep

    def bigDecimalRep(bigDecimal: BigDecimal): Rep
  }

  /*object Fingerprinter {
    type Aux[R] = Fingerprinter {type Rep = R}
  }*/

  given Fingerprintable[Int] with
    def fingerprintRep(value: Int, fp: Fingerprinter): fp.Rep =
      fp.longRep(value.toLong)

  given Fingerprintable[Float] with
    def fingerprintRep(value: Float, fp: Fingerprinter): fp.Rep =
      fp.doubleRep(value.toDouble)

  given Fingerprintable[Long] with
    def fingerprintRep(value: Long, fp: Fingerprinter): fp.Rep =
      fp.longRep(value)

  given Fingerprintable[Double] with
    def fingerprintRep(value: Double, fp: Fingerprinter): fp.Rep =
      fp.doubleRep(value)

  given Fingerprintable[BigDecimal] with
    def fingerprintRep(value: BigDecimal, fp: Fingerprinter): fp.Rep =
      fp.bigDecimalRep(value)

  given Fingerprintable[String] with
    def fingerprintRep(value: String, fp: Fingerprinter): fp.Rep =
      fp.stringRep(value)

  given Fingerprintable[Array[Byte]] with
    def fingerprintRep(value: Array[Byte], fp: Fingerprinter): fp.Rep =
      fp.bytesRep(IArray.unsafeFromArray(value))

  given Fingerprintable[IArray[Byte]] with
    def fingerprintRep(value: IArray[Byte], fp: Fingerprinter): fp.Rep =
      fp.bytesRep(value)

  given [E: Fingerprintable] => Fingerprintable[Seq[E]] = new Fingerprintable[Seq[E]] {
    def fingerprintRep(value: Seq[E], fp: Fingerprinter): fp.Rep =
      fp.iterableRep(value.map(summon[Fingerprintable[E]].fingerprintRep(_, fp)))
  }

  given [K: Fingerprintable, V: Fingerprintable] => Fingerprintable[Map[K, V]] = new Fingerprintable[Map[K, V]] {
    def fingerprintRep(value: Map[K, V], fp: Fingerprinter): fp.Rep =
      fp.objRep(value.map { (k, v) =>
        (summon[Fingerprintable[K]].fingerprintRep(k, fp), summon[Fingerprintable[V]].fingerprintRep(v, fp))
      })
  }

  private inline def summonTupleInstances[T <: Tuple]: List[Fingerprintable[?]] = {
    inline erasedValue[T] match {
      case _: EmptyTuple => Nil
      case _: (h *: t) => summonInline[Fingerprintable[h]] :: summonTupleInstances[t]
    }
  }

  private def fingerprintableTuple[T <: Tuple](tupleInstances: List[Fingerprintable[?]]): Fingerprintable[T] =
    new Fingerprintable[T] {
      override def fingerprintRep(value: T, fp: Fingerprinter): fp.Rep = {
        val values = tupleInstances.lazyZip(value.toList).map {
          case (instance, value) =>
            instance.asInstanceOf[Fingerprintable[Any]].fingerprintRep(value, fp)
        }
        fp.iterableRep(values)
      }
    }

  inline given [T <: Tuple]: Fingerprintable[T] =
    fingerprintableTuple[T](summonTupleInstances[T])

  private def fingerprintableProduct[T](productLabels: List[String], productInstances: List[Fingerprintable[?]]): Fingerprintable[T] =
    new Fingerprintable[T] {
      def fingerprintRep(value: T, fp: Fingerprinter): fp.Rep = {
        val values = value.asInstanceOf[Product].productIterator.toList
        val fields: Seq[(fp.Rep, fp.Rep)] = productLabels.lazyZip(productInstances.lazyZip(values)).map {
          case (label, (instance, value)) =>
            val k = fp.stringRep(label)
            val v = instance.asInstanceOf[Fingerprintable[Any]].fingerprintRep(value, fp)
            k -> v
        }
        fp.objRep(fields)
      }
    }

  inline def derived[T](using m: Mirror.Of[T]): Fingerprintable[T] = {
    val elemInstances = summonTupleInstances[m.MirroredElemTypes]
    inline m match {
      case s: Mirror.SumOf[T] => error("sum types are unsupported")
      case p: Mirror.ProductOf[T] =>
        val productLabels = constValueTuple[m.MirroredElemLabels].toList.map(_.toString)
        fingerprintableProduct[T](productLabels, elemInstances)
    }
  }
}
