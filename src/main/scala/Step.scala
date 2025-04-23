import Step.{StepId, StepIdempotencyId}
import Workflow.{WorkflowCtx, WorkflowId, WorkflowInstanceId}
import cats.Contravariant
import cats.syntax.all.*
import io.github.iltotore.iron.*
import io.github.iltotore.iron.constraint.string.*

import java.nio.charset.StandardCharsets
import java.security.MessageDigest
import java.util.concurrent.atomic.AtomicReference
import scala.annotation.implicitNotFound
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
    val stepMeta = StepMeta(
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

      private val completeAtomic: AtomicReference[Out => Unit] = AtomicReference[Out => Unit](_ => ())

      override def onComplete(f: Out => Unit): Unit =
        completeAtomic.updateAndGet(prev => out => {
          prev(out)
          f(out)
        })

      override def complete(out: Out): Unit =
        completeAtomic.get()(out)
    }

    val result = try {
      body
    } catch {
      case r: StepBreak[Out] if ctx.eq(r.ctx) =>
        r.value
    }

    ctx.complete(result)

    result
  }

  case class StepMeta(
                       id: StepId,
                       version: Long,
                       name: Option[String],
                       description: Option[String],
                       workflow: Workflow[?, ?]
                     )

  case class StepId(id: String :| ValidUUID)

  @implicitNotFound("Cannot be used outside a Step definition: `Step(...) {  }`")
  trait StepCtx[Out] {
    def meta: StepMeta

    def workflowCtx: WorkflowCtx[?, ?]

    def onComplete(f: Out => Unit): Unit

    private[Step] def complete(out: Out): Unit
  }

  case class StepIdempotencyId(id: String :| ValidUUID)

  final class StepBreak[Out](val ctx: StepCtx[Out], val value: Out)
    extends RuntimeException(
      /*message*/ null, /*cause*/ null, /*enableSuppression=*/ false, /*writableStackTrace*/ false)

  inline def meta(using ctx: StepCtx[?]): StepMeta = ctx.meta

  // TODO: throw IllegalStateException if there is no IdempotencyId
  //def idempotencyId(using ctx: StepCtx[?]): Option[StepIdempotencyId] = ctx.idempotencyId

  def compensate(body: => Unit)(using ctx: StepCtx[?]): Unit = {
    // TODO: register compensate action on ctx
    throw new UnsupportedOperationException("step compensation actions are not supported yet")
  }

  def cache[Out](stepInputs: StepInput[?]*)(using ctx: StepCtx[Out]): Unit = {
    val idempotencyId = ctx.workflowCtx.stepIdempotencyStore.getOrCreateStepIdempotencyId(
      meta.id,
      Some(meta.version),
      stepInputs
    )

    ctx.workflowCtx.stepCache.get(idempotencyId, meta.version, stepInputs) match {
      case Some(value) =>
        throw new StepBreak[Out](ctx, value)

      case None =>
        ctx.onComplete { out =>
          ctx.workflowCtx.stepCache.put(idempotencyId, meta.version, stepInputs, out, ttl = None)
        }
    }
  }

  def onlyOnce[Out](stepInputs: StepInput[?]*)(using ctx: StepCtx[Out]): Unit = {
    val idempotencyId = ctx.workflowCtx.stepIdempotencyStore.getOrCreateStepIdempotencyId(
      meta.id,
      None,
      Seq.empty
    )

    // throws ConflictException if stepInputs don't match
    ctx.workflowCtx.stepCache.get(idempotencyId, meta.version, stepInputs) match {
      case Some(value) =>
        throw new StepBreak[Out](ctx, value)

      case None =>
        ctx.onComplete { out =>
          ctx.workflowCtx.stepCache.put(idempotencyId, meta.version, stepInputs, out, ttl = None)
        }
    }
  }
}

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

case class StepInput[A: Hashable](name: String, value: A) {
  lazy val hash: IArray[Byte] = summon[Hashable[A]].hash(value)
}

given [A: Hashable] => Conversion[(String, A), StepInput[A]] = { (name: String, value: A) =>
  StepInput(name, value)
}

sealed trait WorkflowStepCache {
  //@throws[IllegalStateException] TODO: throw conflict exception
  def get[Out](
                stepIdempotencyId: StepIdempotencyId,
                stepVersion: Long,
                stepInputs: Seq[StepInput[?]]
              ): Option[Out]

  def put[Out](
                stepIdempotencyId: StepIdempotencyId,
                stepVersion: Long,
                stepInputs: Seq[StepInput[?]],
                value: Out,
                ttl: Option[FiniteDuration]
              ): Unit
}

extension (stepCache: StepCache) {
  def withWorkflowInstance(
                            workflowId: WorkflowId,
                            workflowInstanceId: WorkflowInstanceId
                          ): WorkflowStepCache = new WorkflowStepCache {
    override def get[Out](
                           stepIdempotencyId: StepIdempotencyId,
                           stepVersion: Long,
                           stepInputs: Seq[StepInput[?]]
                         ): Option[Out] =
      stepCache.get[Out](
        workflowId = workflowId,
        workflowInstanceId = workflowInstanceId,
        stepIdempotencyId = stepIdempotencyId,
        stepVersion = stepVersion,
        stepInputs = stepInputs
      )

    override def put[Out](
                           stepIdempotencyId: StepIdempotencyId,
                           stepVersion: Long,
                           stepInputs: Seq[StepInput[?]],
                           value: Out,
                           ttl: Option[FiniteDuration]
                         ): Unit =
      stepCache.put[Out](
        workflowId = workflowId,
        workflowInstanceId = workflowInstanceId,
        stepIdempotencyId = stepIdempotencyId,
        stepVersion = stepVersion,
        stepInputs = stepInputs,
        value = value,
        ttl = ttl.getOrElse(???) // TODO workflow.defaultTtl
      )
  }
}

sealed trait WorkflowStepIdempotencyStore {
  def getOrCreateStepIdempotencyId(
                                    stepId: StepId,
                                    stepVersion: Option[Long],
                                    stepInputs: Seq[StepInput[?]]
                                  ): StepIdempotencyId

  def overrideStepIdempotencyId(
                                 stepId: StepId,
                                 stepIdempotencyId: StepIdempotencyId
                               ): Unit
}

extension (stepIdempotencyStore: StepIdempotencyStore) {
  def withWorkflowInstance(
                            workflowId: WorkflowId,
                            libraryVersion: Long,
                            workflowInstanceId: WorkflowInstanceId
                          ): WorkflowStepIdempotencyStore = new WorkflowStepIdempotencyStore {
    override def getOrCreateStepIdempotencyId(
                                               stepId: StepId,
                                               stepVersion: Option[Long],
                                               stepInputs: Seq[StepInput[?]]
                                             ): StepIdempotencyId =
      stepIdempotencyStore.getOrCreateStepIdempotencyId(
        workflowId = workflowId,
        libraryVersion = libraryVersion,
        stepId = stepId,
        stepVersion = stepVersion,
        workflowInstanceId = workflowInstanceId,
        stepInputs = stepInputs
      )

    override def overrideStepIdempotencyId(
                                            stepId: StepId,
                                            stepIdempotencyId: StepIdempotencyId
                                          ): Unit =
      stepIdempotencyStore.overrideStepIdempotencyId(
        workflowId = workflowId,
        libraryVersion = libraryVersion,
        stepId = stepId,
        workflowInstanceId = workflowInstanceId,
        stepIdempotencyId = stepIdempotencyId
      )
  }
}
