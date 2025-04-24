package atomicflow

import cats.Contravariant
import cats.syntax.all.*

import java.nio.charset.StandardCharsets
import java.security.MessageDigest

trait Hashable[A] {
  def hash(value: A): IArray[Byte]
}

object Hashable {
  inline def apply[A](using instance: Hashable[A]): Hashable[A] = instance

  given Contravariant[Hashable] = new Contravariant[Hashable] {
    override def contramap[A, B](fa: Hashable[A])(f: B => A): Hashable[B] = new Hashable[B] {
      override def hash(value: B): IArray[Byte] = fa.hash(f(value))
    }
  }

  given Hashable[Array[Byte]] = { bytes =>
    val messageDigest = MessageDigest.getInstance("SHA-256")
    messageDigest.update(bytes)
    IArray.unsafeFromArray(messageDigest.digest)
  }

  given Hashable[IArray[Byte]] = Hashable[Array[Byte]].contramap(_.asInstanceOf[Array[Byte]])

  given Hashable[String] = Hashable[Array[Byte]].contramap(_.getBytes(StandardCharsets.UTF_8))

  given Hashable[Int] = Hashable[String].contramap(_.toString)

  given Hashable[Long] = Hashable[String].contramap(_.toString)

  given Hashable[Float] = Hashable[String].contramap(_.toString)

  given Hashable[Double] = Hashable[String].contramap(_.toString)

  given Hashable[Byte] = Hashable[Array[Byte]].contramap(Array(_))

  given [F[E] <: Iterable[E], A: Hashable] => Hashable[F[A]] = new Hashable[F[A]] {
    override def hash(value: F[A]): IArray[Byte] =
      Hashable[IArray[Byte]].hash(IArray.concat(value.map(Hashable[A].hash).toSeq *))
  }
}
