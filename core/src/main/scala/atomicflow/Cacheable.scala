package atomicflow

import atomicflow.internal.Framing

import java.io.{ByteArrayInputStream, ByteArrayOutputStream, ObjectInputStream, ObjectOutputStream}
import java.util.Base64
import scala.compiletime.*
import scala.deriving.Mirror
import scala.reflect.ClassTag
import scala.util.Using
import upickle.default.*

/** A codec for durably storing values of type `A` as strings.
  *
  * - `stableSerializedTypeId` identifies the durable serialized representation. It
  *   must remain stable across class and package renames; changing it is a
  *   persisted-format compatibility change.
  * - Composite codecs (sequences, options, unions, ...) embed member codec ids and
  *   treat the members' payloads as opaque, self-delimiting strings.
  *
  * `Cacheable` is invariant in `A` because it both consumes and produces `A`.
  */
trait Cacheable[A] {
  def stableSerializedTypeId: String
  def write(value: A): String
  def read(serialized: String): A

  /** Adapt to another type in both directions, preserving the serialized
    * representation (and therefore the serialized type id). Use for adapting a
    * legacy format's codec to the current type.
    */
  final def imap[B](f: A => B)(g: B => A): Cacheable[B] = new Cacheable[B] {
    override def stableSerializedTypeId: String = Cacheable.this.stableSerializedTypeId
    override def write(value: B): String = Cacheable.this.write(g(value))
    override def read(serialized: String): B = f(Cacheable.this.read(serialized))
  }

  /** Always serialize with `this`; deserialize by trying `this` first, then
    * `fallback`, then the fallback's fallbacks, in declaration order. Use for
    * evolving a cached format: new data is written with the current codec, legacy
    * payloads remain readable.
    */
  final def withFallback(fallback: Cacheable[A]): Cacheable[A] = new Cacheable[A] {
    override def stableSerializedTypeId: String =
      s"withFallback(${Cacheable.this.stableSerializedTypeId},${fallback.stableSerializedTypeId})"
    override def write(value: A): String = Cacheable.this.write(value)
    override def read(serialized: String): A =
      try Cacheable.this.read(serialized)
      catch {
        case _: Throwable => fallback.read(serialized)
      }
  }
}

object Cacheable {
  def apply[A](using cacheable: Cacheable[A]): Cacheable[A] = cacheable

  /** Derivation entry point for `derives Cacheable` on case classes; delegates to
    * [[Json.derived]].
    */
  inline def derived[A](using m: Mirror.Of[A], ct: ClassTag[A]): Cacheable[A] =
    Json.derived[A](using m, ct)

  /** Hand-rolled codecs for primitives and standard containers. Members of
    * composite types are encoded as `frame(memberId) + frame(memberPayload)`.
    */
  object Simple {
    given Cacheable[Unit] = new Cacheable[Unit] {
      override def stableSerializedTypeId: String = "unit"
      override def write(value: Unit): String = ""
      override def read(serialized: String): Unit =
        if (serialized.nonEmpty) throw new IllegalArgumentException(s"unit payload must be empty: $serialized")
    }

    given Cacheable[String] = new Cacheable[String] {
      override def stableSerializedTypeId: String = "string"
      override def write(value: String): String = value
      override def read(serialized: String): String = serialized
    }

    given Cacheable[Boolean] = new Cacheable[Boolean] {
      override def stableSerializedTypeId: String = "boolean"
      override def write(value: Boolean): String = if (value) "true" else "false"
      override def read(serialized: String): Boolean = serialized match {
        case "true"  => true
        case "false" => false
        case other   => throw new IllegalArgumentException(s"not a boolean: $other")
      }
    }

    given Cacheable[Int] = new Cacheable[Int] {
      override def stableSerializedTypeId: String = "int"
      override def write(value: Int): String = value.toString
      override def read(serialized: String): Int = serialized.toInt
    }

    given Cacheable[Long] = new Cacheable[Long] {
      override def stableSerializedTypeId: String = "long"
      override def write(value: Long): String = value.toString
      override def read(serialized: String): Long = serialized.toLong
    }

    given Cacheable[Double] = new Cacheable[Double] {
      override def stableSerializedTypeId: String = "double"
      override def write(value: Double): String = value.toString
      override def read(serialized: String): Double = serialized.toDouble
    }

    given Cacheable[Float] = new Cacheable[Float] {
      override def stableSerializedTypeId: String = "float"
      override def write(value: Float): String = value.toString
      override def read(serialized: String): Float = serialized.toFloat
    }

    private def memberWrite[A](value: A)(using member: Cacheable[A]): String =
      Framing.write(member.stableSerializedTypeId) + Framing.write(member.write(value))

    private def memberRead[A](serialized: String, offset: Int)(using member: Cacheable[A]): (A, Int) = {
      val (memberId, afterId) = Framing.read(serialized, offset)
      if (memberId != member.stableSerializedTypeId)
        throw new IllegalArgumentException(s"member codec mismatch: stored=$memberId, expected=${member.stableSerializedTypeId}")
      val (payload, afterPayload) = Framing.read(serialized, afterId)
      (member.read(payload), afterPayload)
    }

    given [A: Cacheable]: Cacheable[Option[A]] = new Cacheable[Option[A]] {
      override def stableSerializedTypeId: String = "option"
      override def write(value: Option[A]): String = value match {
        case Some(a)  => Framing.write("some") + memberWrite(a)
        case None     => Framing.write("none")
      }
      override def read(serialized: String): Option[A] = {
        val (tag, afterTag) = Framing.read(serialized, 0)
        tag match {
          case "some" =>
            val (a, _) = memberRead[A](serialized, afterTag)
            Some(a)
          case "none" => None
          case other  => throw new IllegalArgumentException(s"unknown option tag: $other")
        }
      }
    }

    given [A: Cacheable]: Cacheable[Seq[A]] = new Cacheable[Seq[A]] {
      override def stableSerializedTypeId: String = "seq"
      override def write(value: Seq[A]): String = value.map(memberWrite(_)).mkString
      override def read(serialized: String): Seq[A] = {
        val builder = List.newBuilder[A]
        var offset = 0
        while (offset < serialized.length) {
          val (a, nextOffset) = memberRead[A](serialized, offset)
          builder += a
          offset = nextOffset
        }
        builder.result()
      }
    }

    given [K: Cacheable, V: Cacheable]: Cacheable[Map[K, V]] = new Cacheable[Map[K, V]] {
      override def stableSerializedTypeId: String = "map"
      override def write(value: Map[K, V]): String =
        value.map { (k, v) => memberWrite(k) + memberWrite(v) }.mkString
      override def read(serialized: String): Map[K, V] = {
        val builder = Map.newBuilder[K, V]
        var offset = 0
        while (offset < serialized.length) {
          val (k, afterKey) = memberRead[K](serialized, offset)
          val (v, afterValue) = memberRead[V](serialized, afterKey)
          builder += ((k, v))
          offset = afterValue
        }
        builder.result()
      }
    }
  }

  /** JSON codecs backed by upickle. The serialized type id is
    * `json:<runtime class name>`; keep class names stable for persisted data, or
    * define a custom `Cacheable` with a chosen stable id.
    */
  object Json {
    private def fromWriterReader[A](className: String)(using Writer[A], Reader[A]): Cacheable[A] = new Cacheable[A] {
      override def stableSerializedTypeId: String = s"json:$className"
      override def write(value: A): String = upickle.default.write(value)
      override def read(serialized: String): A = upickle.default.read[A](serialized)
    }

    inline def derived[A](using m: Mirror.Of[A], ct: ClassTag[A]): Cacheable[A] = {
      given Reader[A] = Reader.derived[A](using m)
      given Writer[A] = Writer.derived[A](using m)
      fromWriterReader[A](ct.runtimeClass.getName)
    }

    given [A: {Writer, Reader, ClassTag}]: Cacheable[A] = new Cacheable[A] {
      override def stableSerializedTypeId: String = s"json:${summon[ClassTag[A]].runtimeClass.getName}"
      override def write(value: A): String = upickle.default.write(value)
      override def read(serialized: String): A = upickle.default.read[A](serialized)
    }
  }

  /** Application-global `Cacheable[Throwable]` choices (see `steps.md`). One of
    * these (or a custom codec) must be selected explicitly; neither is an
    * automatic given.
    */
  object forThrowable {
    /** Stores portable diagnostics (class name, message, cause chain) and replays a
      * [[StepFailed]]. Recommended default for new workflows.
      */
    def genericStringMessageSerializer: Cacheable[Throwable] = new Cacheable[Throwable] {
      override def stableSerializedTypeId: String = "throwable-generic"

      override def write(value: Throwable): String =
        upickle.default.write(toJson(value))

      override def read(serialized: String): Throwable =
        fromJson(ujson.read(serialized))

      private def toJson(t: Throwable): ujson.Value = ujson.Obj(
        "c" -> t.getClass.getName,
        "m" -> (if (t.getMessage == null) ujson.Null else ujson.Str(t.getMessage)),
        "cause" -> (if (t.getCause == null || t.getCause.eq(t)) ujson.Null else toJson(t.getCause))
      )

      private def fromJson(v: ujson.Value): StepFailed = {
        val className = v("c").str
        val message = v("m") match {
          case ujson.Null => null
          case m          => m.str
        }
        val cause = v("cause") match {
          case ujson.Null => null
          case c          => fromJson(c)
        }
        new StepFailed(if (message == null) className else s"$className: $message", cause)
      }
    }

    /** Uses Java serialization; preserves concrete exception classes, but persisted
      * data is coupled to those classes and their serializable object graphs.
      */
    def javaSerializable: Cacheable[Throwable] = new Cacheable[Throwable] {
      override def stableSerializedTypeId: String = "throwable-java"

      override def write(value: Throwable): String = {
        val bytes = new ByteArrayOutputStream()
        Using(new ObjectOutputStream(bytes))(_.writeObject(value)).get
        Base64.getEncoder.encodeToString(bytes.toByteArray)
      }

      override def read(serialized: String): Throwable = {
        val bytes = Base64.getDecoder.decode(serialized)
        Using
          .resource(new ObjectInputStream(new ByteArrayInputStream(bytes)))(_.readObject())
          .asInstanceOf[Throwable]
      }
    }
  }

  /** A `Cacheable[Throwable]` that selects the unique most-specific member codec
    * for the concrete exception, falling back to `fallback` when no member
    * matches. On read, the persisted member id selects the member directly.
    *
    * Every tuple member must be a subtype of `Throwable` and have `Cacheable` and
    * `ClassTag` evidence. Members whose runtime classes are incomparable and
    * where at least one is an interface (e.g. overlapping exception traits) are
    * rejected at construction, because no unique most-specific member could be
    * selected for a value matching both.
    */
  inline def unionMostSpecific[Types <: Tuple](fallback: Cacheable[Throwable]): Cacheable[Throwable] =
    buildUnion(
      summonAll[Tuple.Map[Types, ClassTag]].toList.asInstanceOf[List[ClassTag[?]]].map(_.runtimeClass),
      summonAll[Tuple.Map[Types, Cacheable]].toList.asInstanceOf[List[Cacheable[?]]],
      fallback
    )

  private def buildUnion(
      classes: List[Class[?]],
      codecs: List[Cacheable[?]],
      fallback: Cacheable[Throwable]
  ): Cacheable[Throwable] = new UnionMostSpecificCodec(classes, codecs, fallback)

  private final class UnionMostSpecificCodec(
      classes: List[Class[?]],
      codecs: List[Cacheable[?]],
      fallback: Cacheable[Throwable]
  ) extends Cacheable[Throwable] {

    locally {
      classes.zipWithIndex.foreach { (ci, i) =>
        classes.zipWithIndex.foreach { (cj, j) =>
          if (i < j && !ci.isAssignableFrom(cj) && !cj.isAssignableFrom(ci) && (ci.isInterface || cj.isInterface))
            throw new IllegalArgumentException(
              s"unionMostSpecific members are incomparable and could both match a value: ${ci.getName}, ${cj.getName}. " +
                "Declare an explicit Cacheable[Throwable] instead."
            )
        }
      }
    }

    override def stableSerializedTypeId: String =
      s"unionMostSpecific(${classes.map(_.getName).mkString(",")};${fallback.stableSerializedTypeId})"

    override def write(value: Throwable): String = {
      val matching = classes.indices.filter(classes(_).isInstance(value))
      if (matching.isEmpty)
        Framing.write(fallback.stableSerializedTypeId) + Framing.write(fallback.write(value))
      else {
        val mostSpecific = matching.find { i => matching.forall(k => classes(k).isAssignableFrom(classes(i))) }.get
        writeWith(codecs(mostSpecific), value)
      }
    }

    override def read(serialized: String): Throwable = {
      val (memberId, afterId) = Framing.read(serialized, 0)
      val (payload, _) = Framing.read(serialized, afterId)
      codecs
        .find(_.stableSerializedTypeId == memberId) match {
        case Some(codec) => readWith(codec, payload)
        case None =>
          if (memberId == fallback.stableSerializedTypeId) fallback.read(payload)
          else throw new IllegalArgumentException(s"unionMostSpecific: unknown member codec id: $memberId")
      }
    }

    /** Members are subtypes of `Throwable`, but `Cacheable` is invariant, so the
      * selected member codec is bridged untyped; the class test above guarantees
      * the value's runtime type matches the codec.
      */
    private def writeWith(codec: Cacheable[?], value: Any): String =
      Framing.write(codec.stableSerializedTypeId) +
        Framing.write(codec.asInstanceOf[Cacheable[Any]].write(value))

    private def readWith(codec: Cacheable[?], payload: String): Throwable =
      codec.asInstanceOf[Cacheable[Any]].read(payload).asInstanceOf[Throwable]
  }
}
