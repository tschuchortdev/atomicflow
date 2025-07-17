package atomicflow.impl.memory

import atomicflow.*
import atomicflow.Fingerprintable.Fingerprinter
import atomicflow.impl.Sha256Fingerprinter
import atomicflow.internal.{StepCache, StepIdempotencyStore, StepInputFingerprints, SignalStore}

import java.util.concurrent.atomic.{AtomicBoolean, AtomicReference}
import scala.concurrent.duration.FiniteDuration

class InMemoryWorkflowRuntime extends WorkflowRuntime with WorkflowRuntime.GenerateIds {
  trait WorkflowIdempotencyStore {
    sealed trait IdempotencyIdKey

    case class StepIdempotencyIdKey(stepId: StepId, stepVersion: Long, inputs: StepInputFingerprints) extends IdempotencyIdKey

    case class OnceStepIdempotencyIdKey(stepId: StepId) extends IdempotencyIdKey

    val idempotencyIds: AtomicReference[Map[IdempotencyIdKey, StepIdempotencyId]] = new AtomicReference(Map.empty)

    def getIdempotencyStore(
                             stepIdempotencyIdOverrides: Map[StepId, StepIdempotencyId]
                           )(
                             using stepCtx: StepContext[?]
                           ): StepIdempotencyStore = new StepIdempotencyStore {
      override def acquireStepIdempotencyId(inputFingerprints: StepInputFingerprints): StepIdempotencyId = {
        val key = StepIdempotencyIdKey(stepCtx.meta.id, stepCtx.meta.version, inputFingerprints)
        idempotencyIds.updateAndGet(ids => ids.get(key) match {
          case Some(_) => ids
          case None =>
            val id = generateStepIdempotencyId
            ids + (key -> id)
        })(key)
      }

      override def acquireOnlyOnceStepIdempotencyId(): StepIdempotencyId = {
        val key = OnceStepIdempotencyIdKey(stepCtx.meta.id)
        stepIdempotencyIdOverrides.get(stepCtx.meta.id) match {
          case Some(idempotencyId) =>
            idempotencyIds.updateAndGet(_ + (key -> idempotencyId))
            idempotencyId
          case None =>
            idempotencyIds.updateAndGet(ids => ids.get(key) match {
              case Some(_) => ids
              case None =>
                val id = generateStepIdempotencyId
                ids + (key -> id)
            })(key)
        }
      }
    }
  }

  trait WorkflowStepCache {
    val stepCache: AtomicReference[Map[StepIdempotencyId, (Long, StepInputFingerprints, Any)]] = new AtomicReference(Map.empty)

    def getStepCache[StepOut](using ctx: StepContext[StepOut]): StepCache[StepOut] = new StepCache[StepOut] {
      override def get(
                        stepIdempotencyId: StepIdempotencyId,
                        inputFingerprints: StepInputFingerprints
                      ): Option[StepOut] = {
        val stepVersion = ctx.meta.version
        stepCache.get().get(stepIdempotencyId).map {
          case (`stepVersion`, `inputFingerprints`, out: StepOut @unchecked) => out
          case _ => throw new StepInputConflictException()
        }
      }


      override def put(
                        stepIdempotencyId: StepIdempotencyId,
                        inputFingerprints: StepInputFingerprints,
                        value: StepOut,
                        ttl: Option[FiniteDuration]
                      ): Unit = {
        // TODO: check for already existing stepIdempotencyId should not be necessary since workflow instance is locked?
        stepCache.updateAndGet(cache => cache + (stepIdempotencyId -> (ctx.meta.version, inputFingerprints, value)))
      }
    }
  }

  private case class WorkflowState[In, Out](
                                             locked: AtomicBoolean,
                                             in: In,
                                             workflowInstance: WorkflowInstanceBuilder[In, Out],
                                             stepCache: WorkflowStepCache,
                                             stepIdempotencyStore: WorkflowIdempotencyStore
                                           )

  private val workflowInstances: AtomicReference[Map[WorkflowInstanceId, WorkflowState[?, ?]]] = new AtomicReference(Map.empty)

  override def createWorkflowInstance[WorkflowIn, WorkflowOut](
                                                                workflowInstance: WorkflowInstanceBuilder[WorkflowIn, WorkflowOut],
                                                                in: WorkflowIn
                                                              )(
                                                                using Cacheable[WorkflowIn]
                                                              ): Unit = {
    given SimpleWorkflowContext {
      override def meta: WorkflowMeta = workflowInstance.workflow.meta

      override def instanceId: WorkflowInstanceId = workflowInstance.instanceId
    }

    workflowInstances.updateAndGet { instances =>
      instances.get(workflowInstance.instanceId) match {
        case Some(state) =>
          if (state.in != in) {
            throw new WorkflowInputConflictException()
          } else {
            instances
          }
        case None =>
          instances + (workflowInstance.instanceId -> WorkflowState(
            locked = new AtomicBoolean(false),
            in = in,
            workflowInstance = workflowInstance,
            stepCache = new WorkflowStepCache {},
            stepIdempotencyStore = new WorkflowIdempotencyStore {}
          ))
      }
    }
  }

  override def runWorkflowInstance[In, Out](
                                             workflowInstance: WorkflowInstanceBuilder[In, Out],
                                             in: In
                                           )(
                                             using Cacheable[In]
                                           ): Out = {
    createWorkflowInstance(workflowInstance, in)
    recoverWorkflowInstance(workflowInstance)
  }

  override def recoverWorkflowInstance[In, Out](
                                                 workflowInstance: WorkflowInstanceBuilder[In, Out]
                                               )(
                                                 using Cacheable[In]
                                               ): Out = {
    given SimpleWorkflowContext {
      override def meta: WorkflowMeta = workflowInstance.workflow.meta

      override def instanceId: WorkflowInstanceId = workflowInstance.instanceId
    }

    workflowInstances.get().get(workflowInstance.instanceId) match {
      case Some(state: WorkflowState[In, Out] @unchecked) =>
        if (state.locked.getAndSet(true)) {
          // was locked before
          throw new WorkflowLockedException()
        } else {
          // was not locked before
          try {
            val ctx = new WorkflowContext[In, Out] {
              override def meta: WorkflowMeta = workflowInstance.workflow.meta

              override def instanceId: WorkflowInstanceId = workflowInstance.instanceId

              override protected[atomicflow] def getFingerprinter: Fingerprinter = Sha256Fingerprinter

              override protected[atomicflow] def getStepIdempotencyStore(using StepContext[?]): StepIdempotencyStore =
                state.stepIdempotencyStore.getIdempotencyStore(workflowInstance.stepIdempotencyIdOverrides)

              override protected[atomicflow] def getStepCache[StepOut: Cacheable](using StepContext[StepOut]): StepCache[StepOut] =
                state.stepCache.getStepCache[StepOut]

              override protected[atomicflow] def getSignalStore: SignalStore =
                signalStore
            }
            state.workflowInstance.workflow.body(ctx, state.in)
          } finally {
            state.locked.set(false)
          }
        }

      case _ =>
        throw new WorkflowNotFoundException()
    }
  }

  private val signalStore: SignalStore = new SignalStore {
    val signalValues: AtomicReference[Map[(WorkflowId, WorkflowInstanceId, SignalId), ?]] = new AtomicReference(Map.empty)

    override def getSignalValue[A](signal: Signal[A])(using ctx: SimpleWorkflowContext): Option[A] = {
      val key = (ctx.meta.id, ctx.instanceId, signal.meta.id)
      signalValues.get().get(key).asInstanceOf[Option[A]]
    }

    override def setSignalValue[A](signal: Signal[A], value: A)(using ctx: SimpleWorkflowContext): Unit = {
      val key = (ctx.meta.id, ctx.instanceId, signal.meta.id)

      if (!workflowInstances.get().contains(ctx.instanceId))
        throw new WorkflowNotFoundException()

      signalValues.updateAndGet { map =>
        if (map.get(key).exists(_ != value))
          throw new SignalConflictException(signal)

        map + (key -> value)
      }
    }
  }

  override def setSignal[A](
                             signal: Signal[A],
                             value: A
                           )(using SimpleWorkflowContext): Unit =
    signalStore.setSignalValue(signal, value)
}
