import Step.{StepId, StepIdempotencyId}
import Workflow.WorkflowCtx
import cats.Contravariant
import cats.syntax.all.*
import io.github.iltotore.iron.*
import io.github.iltotore.iron.constraint.string.*

import java.nio.charset.StandardCharsets
import java.security.MessageDigest
import scala.concurrent.duration.*

object Step {
  def apply[Out](
                  id: StepId | String :| ValidUUID,
                  version: Long,
                  name: String | Unit = (),
                  description: String | Unit = ()
                )(
                  body: StepCtx[Out] ?=> Out
                )(using workflowCtx: WorkflowCtx[?, ?]): Out = {
    val stepMeta = new StepMeta(
      id = id match {
        case stepId: StepId => stepId
        case id: (String :| ValidUUID) => StepId(id)
      },
      version = version,
      name = name match {
        case () => None
        case string: String => Some(string)
      },
      description = description match {
        case () => None
        case string: String => Some(string)
      },
      workflow = workflowCtx.workflow
    )

    val stepWorkflowCtx = workflowCtx

    given ctx: StepCtx[Out] = new StepCtx[Out] {
      override def meta: StepMeta = stepMeta

      override def workflowCtx: WorkflowCtx[?, ?] = stepWorkflowCtx
    }

    try {
      body
    } catch {
      case r: StepBreak[Out] if ctx.eq(r.ctx) =>
        r.value
    }
  }

  case class StepMeta(
                       id: StepId,
                       version: Long,
                       name: Option[String],
                       description: Option[String],
                       workflow: Workflow[?, ?]
                     )

  inline def meta(using ctx: StepCtx[?]): StepMeta = ctx.meta

  // TODO: throw IllegalStateException if there is no IdempotencyId
  //def idempotencyId(using ctx: StepCtx[?]): Option[StepIdempotencyId] = ctx.idempotencyId

  def compensate(body: => Unit)(using ctx: StepCtx[?]): Unit = {
    // TODO: register compensate action on ctx
    throw new UnsupportedOperationException("step compensation actions are not supported yet")
  }

  def cached[Out](stepInputs: StepInput[?]*)(using ctx: StepCtx[Out]): Unit = {
    val idempotencyId = ctx.workflowCtx.stepIdempotencyStore.getOrCreateStepIdempotencyId(
      meta.id,
      stepInputs
    )

    ctx.workflowCtx.stepCache.get(idempotencyId, stepInputs) match {
      case Some(value) =>
        throw new StepBreak[Out](ctx, value)

      case None =>
        // TODO: register action on ctx to save step result to cache
        ()
    }
  }

  def onlyOnce[Out](stepInputs: StepInput[?]*)(using ctx: StepCtx[Out]): Unit = {
    val idempotencyId = ctx.workflowCtx.stepIdempotencyStore.getOrCreateStepIdempotencyId(
      meta.id,
      Seq.empty
    )

    // throws ConflictException if stepInputs don't match
    ctx.workflowCtx.stepCache.get(idempotencyId, stepInputs) match {
      case Some(value) =>
        throw new StepBreak[Out](ctx, value)

      case None =>
        // TODO: register action on ctx to save step result to cache
        ()
    }
  }

  case class StepId(id: String :| ValidUUID)

  trait StepCtx[Out] {
    def meta: StepMeta

    def workflowCtx: WorkflowCtx[?, ?]
  }

  case class StepIdempotencyId(id: String :| ValidUUID)

  final class StepBreak[Out](val ctx: StepCtx[Out], val value: Out)
    extends RuntimeException(
      /*message*/ null, /*cause*/ null, /*enableSuppression=*/ false, /*writableStackTrace*/ false)
}

trait Hashable[A] {
  def hash(value: A): IArray[Byte]
}

object Hashable {
  def apply[A](using instance: Hashable[A]): Hashable[A] = instance

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
}

case class StepInput[A: Hashable](name: String, value: A) {
  lazy val hash: IArray[Byte] = summon[Hashable[A]].hash(value)
}

given [A: Hashable] => Conversion[(String, A), StepInput[A]] = { (name: String, value: A) =>
  StepInput(name, value)
}

trait WorkflowStepCache {
  //@throws[IllegalStateException] TODO: throw conflict exception
  def get[Out](
                stepIdempotencyId: StepIdempotencyId,
                stepInputs: Seq[StepInput[?]]
              ): Option[Out]

  def put[Out](
                stepIdempotencyId: StepIdempotencyId,
                stepInputs: Seq[StepInput[?]],
                value: Out,
                ttl: Option[FiniteDuration]
              ): Unit
}

trait WorkflowStepIdempotencyStore {
  def getOrCreateStepIdempotencyId(
                                    stepId: StepId,
                                    stepInputs: Seq[StepInput[?]]
                                  ): StepIdempotencyId

  def overrideStepIdempotencyId(
                                 stepId: StepId,
                                 stepIdempotencyId: StepIdempotencyId
                               ): Unit
}
