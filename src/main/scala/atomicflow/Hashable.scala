package atomicflow

import atomicflow.Hashable.Hashed
import cats.Contravariant
import cats.syntax.all.*

import java.nio.charset.StandardCharsets
import java.security.MessageDigest

trait Hashable[A] {
  def hash(value: A): Hashed
}

object Hashable {
  inline def apply[A](using instance: Hashable[A]): Hashable[A] = instance
  
  inline def hash[A](value: A)(using hashable: Hashable[A]): Hashed = hashable.hash(value)

  case class Hashed(hash: IArray[Byte]) {
    override def equals(obj: Any): Boolean = obj match {
      case Hashed(other) if (hash eq other) || hash.sameElements(other) => true
      case _ => false
    }
  }

  given Contravariant[Hashable] = new Contravariant[Hashable] {
    override def contramap[A, B](fa: Hashable[A])(f: B => A): Hashable[B] = new Hashable[B] {
      override def hash(value: B): Hashed = fa.hash(f(value))
    }
  }

  given Hashable[Array[Byte]] = { bytes =>
    val messageDigest = MessageDigest.getInstance("SHA-256")
    messageDigest.update(bytes)
    Hashed(IArray.unsafeFromArray(messageDigest.digest))
  }

  given Hashable[IArray[Byte]] = Hashable[Array[Byte]].contramap(_.asInstanceOf[Array[Byte]])

  given Hashable[String] = Hashable[Array[Byte]].contramap(_.getBytes(StandardCharsets.UTF_8))

  given Hashable[Int] = Hashable[String].contramap(_.toString)

  given Hashable[Long] = Hashable[String].contramap(_.toString)

  given Hashable[Float] = Hashable[String].contramap(_.toString)

  given Hashable[Double] = Hashable[String].contramap(_.toString)

  given Hashable[Byte] = Hashable[Array[Byte]].contramap(Array(_))

  given [F[E] <: Iterable[E], A: Hashable] => Hashable[F[A]] = new Hashable[F[A]] {
    override def hash(value: F[A]): Hashed =
      Hashable[IArray[Byte]].hash(IArray.concat(value.map(Hashable[A].hash(_).hash).toSeq *))
  }
}
