package atomicflow

import atomicflow.Fingerprintable.{Fingerprint, Fingerprinter}
import atomicflow.internal.{StepCache, StepIdempotencyStore, StepInputFingerprints}
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
      workflowMeta = workflowCtx.meta,
      workflowInstanceId = workflowCtx.instanceId
    )

    val stepWorkflowCtx = workflowCtx

    val completeAtomic: AtomicReference[Out => Unit] = AtomicReference[Out => Unit](_ => ())

    given stepCtx: StepContext[Out] = new StepContext[Out] {
      override def meta: StepMeta = stepMeta

      override def workflowCtx: WorkflowContext[?, ?] = stepWorkflowCtx

      override def fingerprint(inputs: Seq[StepInput[?]]): StepInputFingerprints = {
        val fingerprinter = stepWorkflowCtx.getFingerprinter
        StepInputFingerprints(inputs.map { input =>
          input.name -> input.fingerprint(fingerprinter)
        }.toMap)
      }

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

  inline def compensate(f: => Unit)(using ctx: StepContext[?]): Unit = ctx.onCompensate(f)

  def cache[Out: Cacheable](stepInputs: StepInput[?]*)(using ctx: StepContext[Out]): Unit = {
    val inputFingerprints = ctx.fingerprint(stepInputs)

    val idempotencyId = ctx.idempotencyStore.acquireStepIdempotencyId(inputFingerprints)

    ctx.cache.get(idempotencyId, inputFingerprints) match {
      case Some(value) =>
        throw new StepBreak(value)

      case None =>
        ctx.onComplete { out =>
          ctx.cache.put(idempotencyId, inputFingerprints, out, ttl = None /* TODO */)
        }
    }
  }

  @throws[StepInputConflictException]
  def onlyOnce[Out: Cacheable](stepInputs: StepInput[?]*)(using ctx: StepContext[Out]): Unit = {
    val inputFingerprints = ctx.fingerprint(stepInputs)

    val idempotencyId = ctx.idempotencyStore.acquireOnlyOnceStepIdempotencyId()

    ctx.cache.get(idempotencyId, inputFingerprints) match {
      case Some(value) =>
        throw new StepBreak(value)

      case None =>
        ctx.onComplete { out =>
          ctx.cache.put(idempotencyId, inputFingerprints, out, ttl = None /* TODO */)
        }
    }
  }
}

