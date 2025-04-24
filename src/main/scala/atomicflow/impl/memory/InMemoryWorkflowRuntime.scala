package atomicflow.impl.memory

import atomicflow.*
import io.github.iltotore.iron.*

import java.util.UUID
import java.util.concurrent.atomic.{AtomicBoolean, AtomicReference}
import scala.concurrent.duration.FiniteDuration

class InMemoryWorkflowRuntime extends WorkflowRuntime {
  private case class WorkflowState[In, Out](
                                             locked: AtomicBoolean,
                                             in: In,
                                             workflowInstance: WorkflowInstanceBuilder[In, Out],
                                             stepCache: StepCache.WithWorkflow,
                                             stepIdempotencyStore: StepIdempotencyStore.WithWorkflow
                                           )

  private val workflowInstances: AtomicReference[Map[WorkflowInstanceId, WorkflowState[?, ?]]] = new AtomicReference(Map.empty)

  override def generateWorkflowInstanceId: WorkflowInstanceId = WorkflowInstanceId(UUID.randomUUID().toString.refineUnsafe)

  override def generateStepIdempotencyId: StepIdempotencyId = StepIdempotencyId(UUID.randomUUID().toString.refineUnsafe)

  override def createWorkflowInstance[In, Out](workflowInstance: WorkflowInstanceBuilder[In, Out], in: In): Unit = {
    workflowInstances.updateAndGet { instances =>
      instances.get(workflowInstance.instanceId) match {
        case Some(state) =>
          if (state.in != in) {
            throw new RuntimeException("CONFLICT")
          } else {
            instances
          }
        case None =>
          instances + (workflowInstance.instanceId -> WorkflowState(
            locked = new AtomicBoolean(false),
            in = in,
            workflowInstance = workflowInstance,
            stepCache = new StepCache.WithWorkflow {
              val stepCache: AtomicReference[Map[StepIdempotencyId, (Long, Seq[HashedStepInput], Any)]] = new AtomicReference(Map.empty)

              override def get[Out](stepIdempotencyId: StepIdempotencyId, stepVersion: Long, stepInputs: Seq[StepInput[?]]): Option[Out] = {
                lazy val stepInputHashes = stepInputs.map(_.hash)
                stepCache.get().get(stepIdempotencyId).map {
                  case (`stepVersion`, `stepInputHashes`, out: Out) =>
                    out
                  case _ => throw new RuntimeException("CONFLICT")
                }
              }

              override def put[Out](stepIdempotencyId: StepIdempotencyId, stepVersion: Long, stepInputs: Seq[StepInput[?]], value: Out, ttl: Option[FiniteDuration]): Unit = {
                lazy val stepInputHashes = stepInputs.map(_.hash)
                // TODO: check for already existing stepIdempotencyId should not be necessary since workflow instance is locked?
                stepCache.updateAndGet(cache => cache + (stepIdempotencyId -> (stepVersion, stepInputHashes, value)))
              }
            },
            stepIdempotencyStore = new StepIdempotencyStore.WithWorkflow {
              // TODO can IArray be compared?
              val idempotencyIds: AtomicReference[Map[(StepId, Option[Long], Seq[HashedStepInput]), StepIdempotencyId]] = new AtomicReference(Map.empty)

              override def getOrCreateStepIdempotencyId(stepId: StepId, stepVersion: Option[Long], stepInputs: Seq[StepInput[?]]): StepIdempotencyId = {
                val key = (stepId, stepVersion, stepInputs.map(_.hash))
                idempotencyIds.updateAndGet(ids => ids.get(key) match {
                  case Some(_) => ids
                  case None =>
                    val id = generateStepIdempotencyId
                    ids + (key -> id)
                })(key)
              }


              override def overrideStepIdempotencyId(stepId: StepId, stepIdempotencyId: StepIdempotencyId): Unit = {
                idempotencyIds.updateAndGet(ids => ids + ((stepId, None, Seq.empty) -> stepIdempotencyId))
              }
            }
          ))
      }
    }
  }

  override def runWorkflowInstance[In, Out](workflowInstance: WorkflowInstanceBuilder[In, Out], in: In): Out = {
    createWorkflowInstance(workflowInstance, in)
    recoverWorkflowInstance(workflowInstance)
  }

  override def recoverWorkflowInstance[In, Out](workflowInstance: WorkflowInstanceBuilder[In, Out]): Out = {
    workflowInstances.get().get(workflowInstance.instanceId) match {
      case Some(state: WorkflowState[In, Out]) =>
        if (state.locked.getAndSet(true)) {
          // was locked before
          throw new RuntimeException("LOCKED")
        } else {
          // was locked before
          try {
            val ctx = new WorkflowContext[In, Out] {
              override def workflow: Workflow[In, Out] = workflowInstance.workflow

              override def instanceId: WorkflowInstanceId = workflowInstance.instanceId

              override def stepCache: StepCache.WithWorkflow = state.stepCache

              override def stepIdempotencyStore: StepIdempotencyStore.WithWorkflow = state.stepIdempotencyStore
            }
            state.workflowInstance.workflow.body(ctx, state.in)
          } finally {
            state.locked.set(false)
          }
        }

      case _ =>
        throw new RuntimeException("Workflow not found!")
    }
  }
}
