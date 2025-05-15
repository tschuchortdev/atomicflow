package atomicflow.impl.memory

import atomicflow.*
import atomicflow.Hashable.Hashed
import io.github.iltotore.iron.*

import java.util.UUID
import java.util.concurrent.atomic.{AtomicBoolean, AtomicReference}
import scala.concurrent.duration.FiniteDuration

class InMemoryWorkflowRuntime extends WorkflowRuntime {
  trait WorkflowIdempotencyStore {
    sealed trait IdempotencyIdKey

    case class StepIdempotencyIdKey(stepId: StepId, stepVersion: Long, inputs: HashedStepInputs) extends IdempotencyIdKey

    case class OnceStepIdempotencyIdKey(stepId: StepId) extends IdempotencyIdKey

    val idempotencyIds: AtomicReference[Map[IdempotencyIdKey, StepIdempotencyId]] = new AtomicReference(Map.empty)

    def getIdempotencyStore(using stepCtx: StepContext[?]): StepIdempotencyStore = new StepIdempotencyStore {
      override def acquireStepIdempotencyId(hashedStepInputs: HashedStepInputs): StepIdempotencyId = {
        val key = StepIdempotencyIdKey(stepCtx.meta.id, stepCtx.meta.version, hashedStepInputs)
        idempotencyIds.updateAndGet(ids => ids.get(key) match {
          case Some(_) => ids
          case None =>
            val id = generateStepIdempotencyId
            ids + (key -> id)
        })(key)
      }

      override def acquireOnlyOnceStepIdempotencyId(): StepIdempotencyId = {
        val key = OnceStepIdempotencyIdKey(stepCtx.meta.id)
        idempotencyIds.updateAndGet(ids => ids.get(key) match {
          case Some(_) => ids
          case None =>
            val id = generateStepIdempotencyId
            ids + (key -> id)
        })(key)
      }

      override def overrideOnlyOnceStepIdempotencyId(stepIdempotencyId: StepIdempotencyId): Unit = {
        val key = OnceStepIdempotencyIdKey(stepCtx.meta.id)
        idempotencyIds.updateAndGet(ids => ids + (key -> stepIdempotencyId))
      }
    }
  }

  trait WorkflowStepCache {
    val stepCache: AtomicReference[Map[StepIdempotencyId, (Long, HashedStepInputs, Any)]] = new AtomicReference(Map.empty)

    def getStepCache[StepOut](using ctx: StepContext[StepOut]): StepCache[StepOut] = new StepCache[StepOut] {
      override def get(
                        stepIdempotencyId: StepIdempotencyId,
                        hashedStepInputs: HashedStepInputs
                      ): Option[StepOut] = {
        val stepVersion = ctx.meta.version
        stepCache.get().get(stepIdempotencyId).map {
          case (`stepVersion`, `hashedStepInputs`, out: StepOut) => out
          case _ => throw new StepInputConflictException()
        }
      }


      override def put(
                        stepIdempotencyId: StepIdempotencyId,
                        hashedStepInputs: HashedStepInputs,
                        value: StepOut,
                        ttl: Option[FiniteDuration]
                      ): Unit = {
        // TODO: check for already existing stepIdempotencyId should not be necessary since workflow instance is locked?
        stepCache.updateAndGet(cache => cache + (stepIdempotencyId -> (ctx.meta.version, hashedStepInputs, value)))
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

  override def generateWorkflowInstanceId: WorkflowInstanceId = WorkflowInstanceId(UUID.randomUUID().toString.refineUnsafe)

  override def generateStepIdempotencyId: StepIdempotencyId = StepIdempotencyId(UUID.randomUUID().toString.refineUnsafe)

  override def createWorkflowInstance[WorkflowIn, WorkflowOut](workflowInstance: WorkflowInstanceBuilder[WorkflowIn, WorkflowOut], in: WorkflowIn): Unit = {
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

  override def runWorkflowInstance[In, Out](workflowInstance: WorkflowInstanceBuilder[In, Out], in: In): Out = {
    createWorkflowInstance(workflowInstance, in)
    recoverWorkflowInstance(workflowInstance)
  }

  override def recoverWorkflowInstance[In, Out](workflowInstance: WorkflowInstanceBuilder[In, Out]): Out = {
    given SimpleWorkflowContext {
      override def meta: WorkflowMeta = workflowInstance.workflow.meta

      override def instanceId: WorkflowInstanceId = workflowInstance.instanceId
    }

    workflowInstances.get().get(workflowInstance.instanceId) match {
      case Some(state: WorkflowState[In, Out]) =>
        if (state.locked.getAndSet(true)) {
          // was locked before
          throw new WorkflowLockedException()
        } else {
          // was not locked before
          try {
            val ctx = new WorkflowContext[In, Out] {
              override def meta: WorkflowMeta = workflowInstance.workflow.meta

              override def instanceId: WorkflowInstanceId = workflowInstance.instanceId

              override protected[atomicflow] def getStepIdempotencyStore(using StepContext[?]): StepIdempotencyStore =
                state.stepIdempotencyStore.getIdempotencyStore

              override protected[atomicflow] def getStepCache[StepOut: Cacheable](using StepContext[StepOut]): StepCache[StepOut] =
                state.stepCache.getStepCache[StepOut]
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
}
