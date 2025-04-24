package atomicflow

import cats.syntax.all.*
import io.github.iltotore.iron.*
import io.github.iltotore.iron.constraint.string.*

import java.util.Base64
import java.util.concurrent.atomic.AtomicReference

object Step {
  def apply[Out](
                  id: StepId | String :| ValidUUID,
                  version: Long,
                  name: String | Unit = (),
                  description: String | Unit = ()
                )(
                  body: StepContext[Out] ?=> Out
                )(using workflowCtx: WorkflowContext[?, ?]): Out = {
    val stepMeta = StepMeta(
      id = id match {
        case stepId: StepId => stepId
        case id: (String :| ValidUUID) @unchecked => StepId(id)
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

    given ctx: StepContext[Out] = new StepContext[Out] {
      override def meta: StepMeta = stepMeta

      override def workflowCtx: WorkflowContext[?, ?] = stepWorkflowCtx

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
      case r: StepBreak[Out] @unchecked if ctx.eq(r.ctx) =>
        r.value
    }

    ctx.complete(result)

    result
  }

  final class StepBreak[Out](val ctx: StepContext[Out], val value: Out)
    extends RuntimeException(/*message*/ null, /*cause*/ null, /*enableSuppression=*/ false, /*writableStackTrace*/ false)

  inline def meta(using ctx: StepContext[?]): StepMeta = ctx.meta

  // TODO: throw IllegalStateException if there is no IdempotencyId
  //def idempotencyId(using ctx: StepCtx[?]): Option[StepIdempotencyId] = ctx.idempotencyId

  def compensate(body: => Unit)(using ctx: StepContext[?]): Unit = {
    // TODO: register compensate action on ctx
    throw new UnsupportedOperationException("step compensation actions are not supported yet")
  }

  def cache[Out](stepInputs: StepInput[?]*)(using ctx: StepContext[Out]): Unit = {
    val idempotencyId = ctx.workflowCtx.stepIdempotencyStore.getOrCreateStepIdempotencyId(
      meta.id,
      Some(meta.version),
      stepInputs
    )

    ctx.workflowCtx.stepCache.get[Out](idempotencyId, meta.version, stepInputs) match {
      case Some(value: Out) =>
        throw new StepBreak[Out](ctx, value)

      case None =>
        ctx.onComplete { out =>
          ctx.workflowCtx.stepCache.put[Out](idempotencyId, meta.version, stepInputs, out, ttl = None)
        }
    }
  }

  def onlyOnce[Out](stepInputs: StepInput[?]*)(using ctx: StepContext[Out]): Unit = {
    val idempotencyId = ctx.workflowCtx.stepIdempotencyStore.getOrCreateStepIdempotencyId(
      meta.id,
      None,
      Seq.empty
    )

    // throws ConflictException if stepInputs don't match
    ctx.workflowCtx.stepCache.get[Out](idempotencyId, meta.version, stepInputs) match {
      case Some(value) =>
        throw new StepBreak[Out](ctx, value)

      case None =>
        ctx.onComplete { out =>
          ctx.workflowCtx.stepCache.put[Out](idempotencyId, meta.version, stepInputs, out, ttl = None)
        }
    }
  }
}

case class StepMeta(
                     id: StepId,
                     version: Long,
                     name: Option[String],
                     description: Option[String],
                     workflow: Workflow[?, ?]
                   )

case class StepInput[A: Hashable](name: String, value: A) {
  lazy val hash: HashedStepInput = HashedStepInput(name, Base64.getEncoder.encodeToString(summon[Hashable[A]].hash(value).asInstanceOf[Array[Byte]]))
}

case class HashedStepInput(name: String, hash: String)

given [A: Hashable] => Conversion[(String, A), StepInput[A]] = { (name: String, value: A) =>
  StepInput(name, value)
}
