package atomicflow

import atomicflow.Hashable.Hashed
import cats.syntax.all.*
import io.github.iltotore.iron.*
import io.github.iltotore.iron.constraint.string.*

import java.util.Base64
import java.util.concurrent.atomic.AtomicReference

object Step {
  def apply[Out](using workflowCtx: WorkflowContext[?, ?])
                (
                  id: StepId | String :| ValidUUID,
                  version: Long,
                  name: String | Unit = (),
                  description: String | Unit = ()
                )(
                  body: StepContext[Out] ?=> Out
                ): Out = {
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
      workflowMeta = workflowCtx.meta,
      workflowInstanceId = workflowCtx.instanceId
    )

    val stepWorkflowCtx = workflowCtx

    val completeAtomic: AtomicReference[Out => Unit] = AtomicReference[Out => Unit](_ => ())

    given stepCtx: StepContext[Out] = new StepContext[Out] {
      override def meta: StepMeta = stepMeta

      override def workflowCtx: WorkflowContext[?, ?] = stepWorkflowCtx

      override lazy val idempotencyStore: StepIdempotencyStore = stepWorkflowCtx.getStepIdempotencyStore

      override def cache(using Cacheable[Out]): StepCache[Out] = stepWorkflowCtx.getStepCache

      override def onComplete(f: Out => Unit): Unit =
        completeAtomic.updateAndGet(prev => out => {
          prev(out)
          f(out)
        })

      override def onCompensate(f: => Unit): Unit =
        throw new UnsupportedOperationException("step compensation actions are not supported yet")
    }

    val result = try {
      body
    } catch {
      case brk: StepBreak[Out] @unchecked if stepCtx.eq(brk.ctx) =>
        brk.value
    }

    completeAtomic.get()(result)

    result
  }

  final class StepBreak[Out](val value: Out)(using val ctx: StepContext[Out])
    extends RuntimeException(/*message*/ null, /*cause*/ null, /*enableSuppression=*/ false, /*writableStackTrace*/ false)

  inline def meta(using ctx: StepContext[?]): StepMeta = ctx.meta

  inline def compensate(using ctx: StepContext[?])(f: => Unit): Unit = ctx.onCompensate(f)

  def cache[Out: Cacheable](using ctx: StepContext[Out])(stepInputs: StepInput[?]*): Unit = {
    val hashedStepInputs = HashedStepInputs.hash(stepInputs)

    val idempotencyId = ctx.idempotencyStore.acquireStepIdempotencyId(hashedStepInputs)

    ctx.cache.get(idempotencyId, hashedStepInputs) match {
      case Some(value) =>
        throw new StepBreak(value)

      case None =>
        ctx.onComplete { out =>
          ctx.cache.put(idempotencyId, hashedStepInputs, out, ttl = None /* TODO */)
        }
    }
  }

  @throws[StepInputConflictException]
  def onlyOnce[Out: Cacheable](using ctx: StepContext[Out])(stepInputs: StepInput[?]*): Unit = {
    val hashedStepInputs = HashedStepInputs.hash(stepInputs)

    val idempotencyId = ctx.idempotencyStore.acquireOnlyOnceStepIdempotencyId()

    ctx.cache.get(idempotencyId, hashedStepInputs) match {
      case Some(value) =>
        throw new StepBreak(value)

      case None =>
        ctx.onComplete { out =>
          ctx.cache.put(idempotencyId, hashedStepInputs, out, ttl = None /* TODO */)
        }
    }
  }
}

case class StepInput[A: Hashable](name: String, value: A) {
  lazy val hash: Hashed = Hashable.hash(value)
}

case class HashedStepInputs(hashedStepInputs: Map[String, Hashed])

object HashedStepInputs {
  def hash(stepInputs: Seq[StepInput[?]]): HashedStepInputs =
    HashedStepInputs(stepInputs.map(e => e.name -> e.hash).toMap)
}

given [A: Hashable] => Conversion[(String, A), StepInput[A]] = { (name: String, value: A) =>
  StepInput(name, value)
}
