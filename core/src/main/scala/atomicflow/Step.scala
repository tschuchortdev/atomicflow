package atomicflow

import atomicflow.internal.{StepCache, StepIdempotencyStore, StepInputFingerprints}

import java.util.concurrent.atomic.AtomicReference
import scala.concurrent.duration.FiniteDuration
import scala.util.control.ControlThrowable

object Step {
  def apply[Out](
                  id: StepId,
                  version: Long,
                  name: String | Unit = (),
                  description: String | Unit = ()
                )(
                  body: StepContext[Out] ?=> Out
                )(using workflowCtx: WorkflowContext): Out = {
    val stepMeta = StepMeta(
      stepId = id,
      stepVersion = version,
      stepName = name match {
        case () => None
        case string: String => Some(string)
      },
      stepDescription = description match {
        case () => None
        case string: String => Some(string)
      },
      workflowMeta = workflowCtx.workflowInstanceMeta.workflowMeta,
      workflowInstanceId = workflowCtx.workflowInstanceMeta.workflowInstanceKey
    )

    val stepWorkflowCtx = workflowCtx

    val completeAtomic: AtomicReference[Out => Unit] = AtomicReference[Out => Unit](_ => ())

    given stepCtx: StepContext[Out] = new StepContext[Out] {
      override def meta: StepMeta = stepMeta

      override def workflowCtx: WorkflowContext = stepWorkflowCtx

      override def fingerprint(inputs: Seq[StepInput[?]]): StepInputFingerprints = {
        val fingerprinter = stepWorkflowCtx.workflowRuntime.getFingerprinter
        StepInputFingerprints(inputs.map { input =>
          input.name -> input.fingerprint(fingerprinter)
        }.toMap)
      }

      override lazy val idempotencyStore: StepIdempotencyStore = stepWorkflowCtx.workflowRuntime.getStepIdempotencyStore

      override def cache(using Cacheable[Out]): StepCache[Out] = stepWorkflowCtx.workflowRuntime.getStepCache

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

  /** In case a cached value is found, the Step execution is aborted with this exception. 
   * It extends [[ControlThrowable]] to make it less likely that users will catch this exception and break the library. */
  private final class StepBreak[Out](val value: Out)(using val ctx: StepContext[Out]) extends ControlThrowable

  inline def meta(using ctx: StepContext[?]): StepMeta = ctx.meta

  inline def compensate(f: => Unit)(using ctx: StepContext[?]): Unit = ctx.onCompensate(f)

  def cacheFor[Out](ttl: FiniteDuration)(stepInputs: StepInput[?]*)(using ctx: StepContext[Out])(using Cacheable[Out]): Unit = {
    val inputFingerprints = ctx.fingerprint(stepInputs)

    val idempotencyId = ctx.idempotencyStore.acquireStepIdempotencyId(inputFingerprints)

    ctx.cache.get(idempotencyId, inputFingerprints) match {
      case Some(value) =>
        throw new StepBreak(value)

      case None =>
        ctx.onComplete { out =>
          ctx.cache.put(idempotencyId, inputFingerprints, out, ttl)
        }
    }
  }

  def cache[Out](stepInputs: StepInput[?]*)(using ctx: StepContext[Out])(using Cacheable[Out]): Unit =
    cacheFor[Out](ctx.workflowCtx.defaultCacheTtl)(stepInputs *)

  @throws[StepInputConflictException]
  def onlyOnceFor[Out](ttl: FiniteDuration)(stepInputs: StepInput[?]*)(using ctx: StepContext[Out])(using Cacheable[Out]): Unit = {
    val inputFingerprints = ctx.fingerprint(stepInputs)

    val idempotencyId = ctx.idempotencyStore.acquireOnlyOnceStepIdempotencyId()

    ctx.cache.get(idempotencyId, inputFingerprints) match {
      case Some(value) =>
        throw new StepBreak(value)

      case None =>
        ctx.onComplete { out =>
          ctx.cache.put(idempotencyId, inputFingerprints, out, ttl)
        }
    }
  }

  @throws[StepInputConflictException]
  def onlyOnce[Out](stepInputs: StepInput[?]*)(using ctx: StepContext[Out])(using Cacheable[Out]): Unit =
    onlyOnceFor[Out](ctx.workflowCtx.defaultCacheTtl)(stepInputs *)
}
