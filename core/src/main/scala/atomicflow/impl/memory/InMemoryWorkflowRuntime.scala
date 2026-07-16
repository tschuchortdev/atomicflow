package atomicflow.impl.memory

import atomicflow.*
import atomicflow.Fingerprintable.Fingerprinter
import atomicflow.WorkflowRuntime.{StoppedWorkflow, WorkflowStoppedToAwaitManyConditions, WorkflowStoppedToAwaitSignal, WorkflowStoppedToAwaitTimer, WorkflowStoppedToAwaitWorkflow, WorkflowStoppedToWait}
import atomicflow.impl.Sha256Fingerprinter
import atomicflow.internal.{SignalStore, StepCache, StepIdempotencyStore, StepInputFingerprints}
import ox.discard
import WorkflowContext.given_WorkflowInstanceMeta

import java.time.Instant
import java.util.concurrent.atomic.{AtomicBoolean, AtomicReference}
import scala.collection.{immutable, mutable}
import scala.concurrent.duration.{Duration, FiniteDuration}
import scala.util.Try
import scala.util.chaining.*

class InMemoryWorkflowRuntime extends WorkflowRuntime with WorkflowRuntime.DefaultGenerateIdsMixin {
  private class WorkflowIdempotencyStore {
    private sealed trait IdempotencyIdKey

    private case class StepIdempotencyIdKey(stepId: StepId, stepVersion: Long, inputs: StepInputFingerprints) extends IdempotencyIdKey

    private case class OnceStepIdempotencyIdKey(stepId: StepId) extends IdempotencyIdKey

    private val idempotencyIds: AtomicReference[Map[IdempotencyIdKey, StepIdempotencyId]] = new AtomicReference(Map.empty)

    def getIdempotencyStore(
                             stepIdempotencyIdOverrides: Map[StepId, StepIdempotencyId]
                           )(
                             using stepCtx: StepContext[?]
                           ): StepIdempotencyStore = new StepIdempotencyStore {
      override def acquireStepIdempotencyId(inputFingerprints: StepInputFingerprints): StepIdempotencyId = {
        val key = StepIdempotencyIdKey(stepCtx.meta.stepId, stepCtx.meta.stepVersion, inputFingerprints)
        idempotencyIds.updateAndGet(ids => ids.get(key) match {
          case Some(_) => ids
          case None =>
            val id = generateStepIdempotencyId
            ids + (key -> id)
        })(key)
      }

      override def acquireOnlyOnceStepIdempotencyId(): StepIdempotencyId = {
        val key = OnceStepIdempotencyIdKey(stepCtx.meta.stepId)
        stepIdempotencyIdOverrides.get(stepCtx.meta.stepId) match {
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

  private class WorkflowStepCache {
    private val stepCache: AtomicReference[Map[StepIdempotencyId, (Long, StepInputFingerprints, Any, Option[Instant])]] = new AtomicReference(Map.empty)

    def getStepCache[StepOut](using ctx: StepContext[StepOut]): StepCache[StepOut] = new StepCache[StepOut] {
      override def get(
                        stepIdempotencyId: StepIdempotencyId,
                        inputFingerprints: StepInputFingerprints
                      ): Option[StepOut] = {
        val stepVersion = ctx.meta.stepVersion
        stepCache.get().get(stepIdempotencyId).flatMap {
          case (`stepVersion`, `inputFingerprints`, out: StepOut @unchecked, expiry) =>
            expiry match {
              case Some(expiryTime) if Instant.now().isAfter(expiryTime) => None
              case _ => Some(out)
            }
          case _ => throw new StepInputConflictException()
        }
      }

      override def put(
                        stepIdempotencyId: StepIdempotencyId,
                        inputFingerprints: StepInputFingerprints,
                        value: StepOut,
                        ttl: Option[FiniteDuration]
                      ): Unit = {
        val expiry = ttl.map(t => Instant.now().plusMillis(t.toMillis))
        stepCache.updateAndGet(cache => cache + (stepIdempotencyId -> (ctx.meta.stepVersion, inputFingerprints, value, expiry)))
      }
    }
  }

  private case class WorkflowState[In, Out](
                                             locked: AtomicBoolean,
                                             in: In,
                                             workflow: Workflow[In, Out],
                                             instanceKey: WorkflowInstanceKey,
                                             stepCache: WorkflowStepCache,
                                             stepIdempotencyStore: WorkflowIdempotencyStore,
                                             result: Option[Out] = None
                                           )

  // TODO: Map key should be workflowId+instanceKey
  private val workflowInstances: AtomicReference[Map[WorkflowInstanceKey, WorkflowState[?, ?]]] = new AtomicReference(Map.empty)

  override def createWorkflowInstance[In, Out](workflow: Workflow[In, Out],
                                                instanceKey: WorkflowInstanceKey,
                                                in: In)(using Cacheable[In]): Unit = {

    given WorkflowInstanceMeta = WorkflowInstanceMeta(instanceKey, workflow.meta)

    workflowInstances.updateAndGet { instances =>
      instances.get(instanceKey) match {
        case Some(state) =>
          if (state.in != in) {
            throw new WorkflowInputConflictException()
          } else {
            instances
          }
        case None =>
          instances + (instanceKey -> WorkflowState(
            locked = new AtomicBoolean(false),
            in = in,
            workflow = workflow,
            instanceKey = instanceKey,
            stepCache = new WorkflowStepCache(),
            stepIdempotencyStore = new WorkflowIdempotencyStore(),
            result = None
          ))
      }
    }
  }

  override def createWorkflowInstanceDiscardExisting[In, Out](workflow: Workflow[In, Out], instanceId: WorkflowInstanceKey, in: In)(using Cacheable[In]): Boolean = {
    given WorkflowInstanceMeta = WorkflowInstanceMeta(instanceId, workflow.meta)

    var hadExisting = false

    workflowInstances.updateAndGet { instances =>
      hadExisting = instances.contains(instanceId)

      instances + (instanceId -> WorkflowState(
        locked = new AtomicBoolean(false),
        in = in,
        workflow = workflow,
        instanceKey = instanceId,
        stepCache = new WorkflowStepCache(),
        stepIdempotencyStore = new WorkflowIdempotencyStore(),
        result = None
      ))
    }

    hadExisting
  }


  override def createAndRunWorkflowInstance[In, Out](
                                             workflow: Workflow[In, Out],
                                             instanceKey: WorkflowInstanceKey,
                                             in: In
                                           )(
                                             using Cacheable[In], WorkflowRunSettings
                                           ): Either[StoppedWorkflow[Out], Out] = {
    createWorkflowInstance(workflow, instanceKey, in)
    runWorkflowInstance(workflow, instanceKey)
  }

  private type WorkflowCallback[Out] = Try[Either[StoppedWorkflow[Out], Out]] => Unit
  private val workflowCallbacks = new collection.concurrent.TrieMap[WorkflowInstanceKey, immutable.HashSet[WorkflowCallback[Any]]]()
  // TODO: Is reference equality comparison of lambdas ok? I think so.

  override def runWorkflowInstance[In, Out](
                                                 workflow: Workflow[In, Out],
                                                 instanceKey: WorkflowInstanceKey,
                                               )(
                                                 using Cacheable[In], WorkflowRunSettings
                                               ): Either[StoppedWorkflow[Out], Out] = {
    
    given WorkflowInstanceMeta = WorkflowInstanceMeta(instanceKey, workflow.meta)

    workflowInstances.get().get(instanceKey) match {
      case Some(state: WorkflowState[In, Out] @unchecked) =>
        if (state.locked.getAndSet(true)) {
          // was locked before
          throw new WorkflowLockedException()
        } else {
          // was not locked before
          try {
            val ctx = WorkflowContext(
              given_WorkflowInstanceMeta,
              this,
              summon[WorkflowRunSettings].defaultCacheTtl,
              summon[WorkflowRunSettings].stepIdempotencyIdOverrides
            )

            try {
              val result = state.workflow.body(ctx, state.in)
              workflowInstances.updateAndGet { instances =>
                instances.get(instanceKey) match {
                  case Some(currentState: WorkflowState[In, Out] @unchecked) =>
                    instances + (instanceKey -> currentState.copy(result = Some(result)))
                  case _ => instances
                }
              }
              Right(result)
            }
            catch { case stopped: WorkflowStoppedToWait =>
              stopped match {
                case WorkflowStoppedToAwaitTimer(expectedRestartTime) =>
                  scheduleWakeupOnTimer(workflow.meta.workflowId, instanceKey, expectedRestartTime)
                case WorkflowStoppedToAwaitSignal(signal) =>
                  scheduleWakeupOnSignal(workflow.meta.workflowId, instanceKey, signal)
                case WorkflowStoppedToAwaitWorkflow(awaitedWorkflowId, awaitedInstanceKey) =>
                  scheduleWakeupOnWorkflowCompletion(workflow.meta.workflowId, instanceKey, awaitedWorkflowId, awaitedInstanceKey)
                case WorkflowStoppedToAwaitManyConditions(stops) =>
                  stops.foreach {
                    case WorkflowStoppedToAwaitTimer(expectedRestartTime) =>
                      scheduleWakeupOnTimer(workflow.meta.workflowId, instanceKey, expectedRestartTime)
                    case WorkflowStoppedToAwaitSignal(signal) =>
                      scheduleWakeupOnSignal(workflow.meta.workflowId, instanceKey, signal)
                    case WorkflowStoppedToAwaitWorkflow(awaitedWorkflowId, awaitedInstanceKey) =>
                      scheduleWakeupOnWorkflowCompletion(workflow.meta.workflowId, instanceKey, awaitedWorkflowId, awaitedInstanceKey)
                    case _: WorkflowStoppedToAwaitManyConditions => ()
                  }
              }

              Left(new StoppedWorkflow[Out](
                workflowId = workflow.meta.workflowId,
                workflowInstanceKey = instanceKey
              ) {

                override def addContinueListener(onWorkflowContinued: Try[Either[StoppedWorkflow[Out], Out]] => Unit) = {
                  workflowCallbacks.updateWith(instanceKey) { valueMaybe =>
                    Some(valueMaybe.getOrElse(immutable.HashSet.empty)
                      .incl(onWorkflowContinued.asInstanceOf))
                  }

                  new StoppedWorkflow.ListenerHandle {
                    override def remove(): Unit = workflowCallbacks.updateWith(instanceKey) { valueMaybe =>
                      val updated = valueMaybe.getOrElse(throw AssertionError(
                          s"Expected at least one callback to be defined for workflow instance ${instanceKey}"
                        ))
                        .excl(onWorkflowContinued.asInstanceOf)
                      if updated.nonEmpty then Some(updated) else None
                    }.discard
                  }
                }
              })
            }
          } finally {
            state.locked.set(false)
          }
        }

      case _ =>
        throw new WorkflowNotFoundException(given_WorkflowInstanceMeta)
    }
  }

  private val dummyCacheable: Cacheable[Any] = new Cacheable[Any] {
    override def serialize(value: Any): IArray[Byte] = IArray.empty
    override def deserialize(bytes: IArray[Byte]): Any = ()
  }

  private def tryRunWorkflowInstance(workflowId: WorkflowId, instanceKey: WorkflowInstanceKey): Unit = {
    workflowInstances.get().get(instanceKey) match {
      case Some(state) if state.workflow.meta.workflowId == workflowId =>
        if (state.result.isEmpty && !state.locked.get()) {
          Thread.startVirtualThread(() => {
            Try {
              given WorkflowRunSettings = WorkflowRunSettings()
              val workflow = state.workflow.asInstanceOf[Workflow[Any, Any]]
              runWorkflowInstance(workflow, instanceKey)(using dummyCacheable, summon[WorkflowRunSettings])
            }.discard
          })
        }
      case _ => ()
    }
  }

  private val signalStore: SignalStore = new SignalStore {
    val signalValues: AtomicReference[Map[(WorkflowId, WorkflowInstanceKey, SignalId), ?]] = new AtomicReference(Map.empty)

    override def getSignalValue[A](signal: Signal[A])(using ctx: WorkflowInstanceMeta): Option[A] = {
      val key = (ctx.workflowId, ctx.workflowInstanceKey, signal.meta.id)
      signalValues.get().get(key).asInstanceOf[Option[A]]
    }

    override def setSignalValue[A](signal: Signal[A], value: A, ttl: FiniteDuration)(using ctx: WorkflowInstanceMeta): Unit = {
      val key = (ctx.workflowId, ctx.workflowInstanceKey, signal.meta.id)

      if (!workflowInstances.get().contains(ctx.workflowInstanceKey))
        throw new WorkflowNotFoundException(ctx)

      signalValues.updateAndGet { map =>
        if (map.get(key).exists(_ != value))
          throw new SignalConflictException(signal)

        map + (key -> value)
      }
    }
  }

  override def setSignal[A](
                             signal: Signal[A],
                             value: A,
                             ttl: FiniteDuration
                           )(using WorkflowInstanceMeta): Unit =
    signalStore.setSignalValue(signal, value, ttl)


  override def getWorkflowInstancesByPrefix(workflowId: WorkflowId, keyPrefix: WorkflowInstanceKey): Vector[WorkflowInstanceKey] = {
    workflowInstances.get()
      .collect { case (key, state: WorkflowState[?, ?])
            if key.startsWith(keyPrefix) && state.workflow.meta.workflowId == workflowId =>

          key
      }
      .toVector
  }

  override def getUnfinishedWorkflowInstances(workflowId: WorkflowId, includeWaiting: Boolean, limit: Int): Vector[WorkflowInstanceKey] = {
    workflowInstances.get()
      .view
      .collect { case (key, state: WorkflowState[?, ?])
        if state.workflow.meta.workflowId == workflowId && state.result.isEmpty =>

        key
      }
      .pipe { it =>
        if (limit > 0) it.take(limit)
        else it
      }
      .toVector
  }

  override def deleteWorkflowInstancesByPrefix(workflowId: WorkflowId, instanceKeyPrefix: WorkflowInstanceKey): Long = {
    var deletedCount = 0L

    workflowInstances.updateAndGet { instances =>
      val (toDelete, toKeep) = instances.partition { case (key, state) =>
        key.startsWith(instanceKeyPrefix) && state.workflow.meta.workflowId == workflowId
      }

      deletedCount = toDelete.size.toLong
      toKeep
    }

    deletedCount
  }

  override def isWorkflowInstanceCompleted(workflowId: WorkflowId, workflowInstanceKey: WorkflowInstanceKey): Boolean = {
    workflowInstances.get().get(workflowInstanceKey) match {
      case Some(state) if state.workflow.meta.workflowId == workflowId =>
        state.result.isDefined
      case Some(_) =>
        throw new WorkflowNotFoundException(workflowId, Some(workflowInstanceKey))
      case None =>
        throw new WorkflowNotFoundException(workflowId, Some(workflowInstanceKey))
    }
  }

  override def isWorkflowInstanceCreated(workflowId: WorkflowId, workflowInstanceKey: WorkflowInstanceKey): Boolean =
    workflowInstances.get().get(workflowInstanceKey).exists(_.workflow.meta.workflowId == workflowId)

  override def getWorkflowResult[Out](workflow: Workflow[?, Out], workflowInstanceKey: WorkflowInstanceKey): Option[Out] = {
    workflowInstances.get().get(workflowInstanceKey) match {
      case Some(state: WorkflowState[?, Out] @unchecked) if state.workflow.meta.workflowId == workflow.meta.workflowId =>
        state.result
      case Some(_) =>
        throw new WorkflowNotFoundException(workflow.meta.workflowId, Some(workflowInstanceKey))
      case None =>
        throw new WorkflowNotFoundException(workflow.meta.workflowId, Some(workflowInstanceKey))
    }
  }

  override def scheduleWakeupOnTimer(workflowId: WorkflowId, workflowInstanceKey: WorkflowInstanceKey, wakeupTime: Instant): Unit = {
    Thread.startVirtualThread(() => {
      val delay = java.time.Duration.between(Instant.now(), wakeupTime).toMillis.max(0)
      if (delay > 0) Thread.sleep(delay)
      tryRunWorkflowInstance(workflowId, workflowInstanceKey)
    })
  }

  override def scheduleWakeupOnSignal(workflowId: WorkflowId, workflowInstanceKey: WorkflowInstanceKey, signal: Signal[?]): Unit = {
    Thread.startVirtualThread(() => {
      var done = false
      while (!done) {
        if (isWorkflowInstanceCompleted(workflowId, workflowInstanceKey)) {
          done = true
        } else {
          workflowInstances.get().get(workflowInstanceKey) match {
            case Some(state) if state.workflow.meta.workflowId == workflowId =>
              given WorkflowInstanceMeta = WorkflowInstanceMeta(workflowInstanceKey, state.workflow.meta)
              if (signalStore.getSignalValue(signal).isDefined) {
                tryRunWorkflowInstance(workflowId, workflowInstanceKey)
                done = true
              } else {
                Thread.sleep(1000)
              }
            case _ =>
              done = true
          }
        }
      }
    })
  }

  override def scheduleWakeupOnWorkflowCompletion(workflowId: WorkflowId, workflowInstanceKey: WorkflowInstanceKey, awaitedWorkflowId: WorkflowId, awaitedWorkflowInstanceKey: WorkflowInstanceKey): Unit = {
    Thread.startVirtualThread(() => {
      var done = false
      while (!done) {
        if (isWorkflowInstanceCompleted(workflowId, workflowInstanceKey)) {
          done = true
        } else {
          val completed = Try(isWorkflowInstanceCompleted(awaitedWorkflowId, awaitedWorkflowInstanceKey)).toOption.getOrElse(false)
          if (completed) {
            tryRunWorkflowInstance(workflowId, workflowInstanceKey)
            done = true
          } else {
            Thread.sleep(1000)
          }
        }
      }
    })
  }

  override def getFingerprinter: Fingerprinter = Sha256Fingerprinter

  override def getStepIdempotencyStore(using stepCtx: StepContext[?]): StepIdempotencyStore = {
    val instanceKey = stepCtx.workflowCtx.workflowInstanceMeta.workflowInstanceKey
    workflowInstances.get().get(instanceKey) match {
      case Some(state) =>
        state.stepIdempotencyStore.getIdempotencyStore(stepCtx.workflowCtx.stepIdempotencyIdOverrides)(using stepCtx)
      case None =>
        throw new WorkflowNotFoundException(stepCtx.meta)
    }
  }

  override def getStepCache[StepOut: Cacheable](using stepCtx: StepContext[StepOut]): StepCache[StepOut] = {
    val instanceKey = stepCtx.workflowCtx.workflowInstanceMeta.workflowInstanceKey
    workflowInstances.get().get(instanceKey) match {
      case Some(state) =>
        state.stepCache.getStepCache[StepOut](using stepCtx)
      case None =>
        throw new WorkflowNotFoundException(stepCtx.meta)
    }
  }

  override def getSignalStore: SignalStore = signalStore
}
object InMemoryWorkflowRuntime {

}
