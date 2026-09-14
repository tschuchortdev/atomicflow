package atomicflow

import atomicflow.impl.Sha256Fingerprinter
import atomicflow.internal.AwaitSignalCandidate

import java.nio.charset.StandardCharsets
import scala.concurrent.duration.*

/** The public step API: durable, replayed operations with per-key cache-drift
  * policies (see `spec/steps.md`).
  */
object Step {

  private enum Guarantee { case AtLeastOnce, AtMostOnce }

  /** An at-least-once step: the body executes, its result is persisted after
    * completion, and on replay an unresolved `Started` record causes
    * re-execution (crash-safe for idempotent effects).
    *
    * `ensureUnchanged` values must not change between runs (`StepInputConflictException`
    * blocks the workflow if one does); `invalidateOn` values, when they change,
    * discard the previous result and re-execute; `invalidateAfter` sets a TTL
    * over the whole step.
    *
    * Values cross the serialization boundary before being returned or thrown
    * (commit-before-observation), so the first run's value is a decoded copy.
    *
    * @param key
    *   the step's stable identity within the workflow
    * @param version
    *   positive at-least-once version; bumping it creates new retryable work and
    *   stops reusing the previous version's cached result
    * @param ensureUnchanged
    *   named inputs that must be invariant between runs
    * @param invalidateOn
    *   named inputs that invalidate the cached result when they change
    * @param invalidateAfter
    *   TTL after which the cached result expires; `Duration.Inf` disables it
    */
  def atLeastOnce[A: Cacheable](
      key: String,
      version: Long = 1L,
      ensureUnchanged: Seq[StepInput[?]] = Seq.empty,
      invalidateOn: Seq[StepInput[?]] = Seq.empty,
      invalidateAfter: Duration = Duration.Inf
  )(body: => A)(using ctx: WorkflowContext, throwableCodec: Cacheable[Throwable]): A =
    runStep(Guarantee.AtLeastOnce, key, version, "AtLeastOnce", ensureUnchanged, invalidateOn, invalidateAfter)(body).get

  /** An at-most-once step: the body executes, its result is persisted after
    * completion, and on replay an unresolved `Started` record yields `None`
    * without re-executing (a lost effect is assumed over a double execution).
    *
    * `ensureUnchanged`, `invalidateOn`, and `invalidateAfter` behave exactly as
    * in [[atLeastOnce]]. There is deliberately no `version` parameter: a new
    * version could execute after the old operation possibly produced its
    * effect, so a genuinely new operation uses a new step id instead.
    *
    * @param key
    *   the step's stable identity within the workflow
    * @param ensureUnchanged
    *   named inputs that must be invariant between runs
    * @param invalidateOn
    *   named inputs that invalidate the cached result when they change
    * @param invalidateAfter
    *   TTL after which the cached result expires; `Duration.Inf` disables it
    */
  def atMostOnce[A: Cacheable](
      key: String,
      ensureUnchanged: Seq[StepInput[?]] = Seq.empty,
      invalidateOn: Seq[StepInput[?]] = Seq.empty,
      invalidateAfter: Duration = Duration.Inf
  )(body: => A)(using ctx: WorkflowContext, throwableCodec: Cacheable[Throwable]): Option[A] =
    runStep(Guarantee.AtMostOnce, key, 0L, "AtMostOnce", ensureUnchanged, invalidateOn, invalidateAfter)(body)

  /** The shared machinery for both execution guarantees. Both persist a
    * `Started` record before the body executes and replace it with a durable
    * outcome; the guarantees differ only in how an unresolved `Started` record
    * is replayed (at-least-once re-executes, at-most-once returns `None`).
    */
  private def runStep[A: Cacheable](
      guarantee: Guarantee,
      key: String,
      stepVersion: Long,
      stepKind: String,
      ensureUnchanged: Seq[StepInput[?]],
      invalidateOn: Seq[StepInput[?]],
      invalidateAfter: Duration
  )(body: => A)(using ctx: WorkflowContext, throwableCodec: Cacheable[Throwable]): Option[A] = {
    val execution = ctx.execution
    val stepId = StepId(key, execution.currentScope)
    val valueCodec = summon[Cacheable[A]]
    val fingerprints = encodeFingerprints(ensureUnchanged ++ invalidateOn)
    val now = execution.now
    val expiresAt = invalidateAfter match {
      case d: FiniteDuration => Some(now.plus(java.time.Duration.ofNanos(d.toNanos)))
      case _                 => None
    }

    def decodeFailure(payload: String): Throwable =
      try throwableCodec.read(payload)
      catch {
        case _: Throwable => throw new StepSerializationFailed(s"Step '$key' failure could not be decoded")
      }

    def execute(): A = {
      execution.renewLease()
      execution.writeStepStarted(stepId, stepVersion, stepKind, fingerprints)
      try {
        val value = body
        val serialized =
          try valueCodec.write(value)
          catch { case _: Throwable => throw new StepSerializationFailed(s"Step '$key' result could not be encoded") }
        val decoded =
          try valueCodec.read(serialized)
          catch { case _: Throwable => throw new StepSerializationFailed(s"Step '$key' result could not be decoded") }
        execution.writeStepSucceeded(stepId, stepVersion, stepKind, fingerprints, serialized, expiresAt)
        decoded
      } catch {
        case t if isNonCacheable(t) => throw t
        case t =>
          val encoded =
            try Right(throwableCodec.write(t))
            catch {
              case _: Throwable =>
                Left(
                  new StepSerializationFailed(
                    s"Step '$key' failure could not be encoded with the configured throwable codec: ${t.getClass.getName}: ${t.getMessage}"
                  )
                )
            }
          val failure: Throwable = encoded match {
            case Right(serialized) =>
              val decoded =
                try throwableCodec.read(serialized)
                catch { case _: Throwable => new StepSerializationFailed(s"Step '$key' failure could not be decoded") }
              execution.writeStepFailed(stepId, stepVersion, stepKind, fingerprints, serialized, expiresAt)
              decoded
            case Left(ssf) =>
              val persisted =
                try throwableCodec.write(ssf)
                catch { case _: Throwable => throw t }
              execution.writeStepFailed(stepId, stepVersion, stepKind, fingerprints, persisted, expiresAt)
              ssf
          }
          throw failure
      }
    }

    val existing = execution.lookupStep(stepId, stepVersion).filterNot(_.expiresAt.exists(!_.isAfter(now)))

    existing match {
      case None =>
        Some(execute())

      case Some(row) =>
        val stored = parseFingerprints(row.inputFingerprints)
        for (input <- ensureUnchanged) {
          if (stored.get(input.name) != Some(fingerprintOf(input)))
            throw new StepInputConflictException(
              s"Step '$key' input '${input.name}' changed between runs and is pinned with ensureUnchanged; refusing to re-execute"
            )
        }
        val shouldReexecute = invalidateOn.exists(input => stored.get(input.name) != Some(fingerprintOf(input)))

        if (shouldReexecute) {
          execution.deleteStep(stepId, stepVersion)
          Some(execute())
        } else {
          row.stateKind match {
            case "succeeded" =>
              try Some(valueCodec.read(row.statePayload))
              catch {
                case _: Throwable => throw new StepSerializationFailed(s"Step '$key' result could not be decoded")
              }
            case "failed" => throw decodeFailure(row.statePayload)
            case _ =>
              guarantee match {
                case Guarantee.AtLeastOnce => Some(execute())
                case Guarantee.AtMostOnce  => None
              }
          }
        }
    }
  }

  /** Read the durable execution state of a step without executing it (no lease or
    * fence is involved). Reports the stored row even if it has expired.
    *
    * @param key
    *   the step's stable identity within the workflow
    * @param stepVersion
    *   the exact step version to inspect (`0` is the internal version for
    *   unversioned constructs)
    */
  def getExecutionState[A: Cacheable](key: String, stepVersion: Long = 0)(using
      ctx: WorkflowContext,
      throwableCodec: Cacheable[Throwable]
  ): StepExecutionState[A] = {
    val stepId = StepId(key, ctx.execution.currentScope)
    val valueCodec = summon[Cacheable[A]]
    ctx.runtime.readStep(ctx.instanceId, stepId, stepVersion) match {
      case None => StepExecutionState.NeverStarted
      case Some(row) =>
        row.stateKind match {
          case "succeeded" =>
            try StepExecutionState.Completed(valueCodec.read(row.statePayload))
            catch {
              case _: Throwable => throw new StepSerializationFailed(s"Step '$key' result could not be decoded")
            }
          case "failed" =>
            val failure =
              try throwableCodec.read(row.statePayload)
              catch {
                case _: Throwable => throw new StepSerializationFailed(s"Step '$key' failure could not be decoded")
              }
            StepExecutionState.Failed(failure)
          case _ => StepExecutionState.Started
        }
    }
  }

  /** Await a runtime-computed condition: an event on a [[Signal]], a timer, or a
    * workflow completion. The await behaves like a step — its resolved result is
    * persisted in `workflow_steps` and replayed from cache, so a re-run never
    * re-suspends on an already-resolved await.
    *
    * If the await cannot be satisfied from durable facts it durably suspends the
    * workflow by throwing [[WorkflowSuspendedException]] (an internal
    * control-flow exception the runtime catches at the boundary). User code must
    * ignore or rethrow it.
    *
    * WARNING: an await with a filter may skip past unprocessed events. When it
    * returns a matching event, all earlier rejected events of the same exact key
    * become permanently unavailable to later awaits of that key in this
    * workflow (the shared exact-key cursor advances past them).
    *
    * Drift policies apply exactly as for ordinary steps: `ensureUnchanged`
    * values must be invariant between runs, `invalidateOn` changes discard the
    * cached result and re-evaluate from scratch, and `invalidateAfter` sets a
    * TTL over the cached result.
    *
    * @param stepKey
    *   the await's stable identity within the workflow
    * @param awaitable
    *   the condition to await
    * @param invalidateOn
    *   named inputs that invalidate the cached await result when they change
    * @param ensureUnchanged
    *   named inputs that must be invariant between runs
    * @param invalidateAfter
    *   TTL after which the cached await result expires; `Duration.Inf` disables it
    */
  def await[A: Cacheable](
      stepKey: String,
      awaitable: Awaitable[A],
      invalidateOn: Seq[StepInput[?]] = Seq.empty,
      ensureUnchanged: Seq[StepInput[?]] = Seq.empty,
      invalidateAfter: Duration = Duration.Inf
  )(using ctx: WorkflowContext, throwableCodec: Cacheable[Throwable]): A =
    awaitable match {
      case Awaitable.SignalEvent(signal, filter, lookBack) =>
        awaitSignal(stepKey, signal, filter, lookBack, invalidateOn, ensureUnchanged, invalidateAfter)
      case Awaitable.Timer(deadline) =>
        awaitTimer(stepKey, deadline, invalidateOn, ensureUnchanged, invalidateAfter)
      case Awaitable.WorkflowCompletion(_) =>
        throw new UnsupportedOperationException("awaiting a workflow completion is not yet supported")
      case Awaitable.Mapped(_, _) =>
        throw new UnsupportedOperationException("awaiting a mapped awaitable is not yet supported")
    }

  /** Read the currently visible events of `s` (those after the instance's shared
    * exact-key cursor) without advancing the cursor. The returned values are the
    * decoded signal payloads.
    */
  def peekSignal[A](s: Signal[A])(using ctx: WorkflowContext): Seq[A] =
    ctx.execution.readAwaitSignalCandidates(s.key).map { c =>
      try s.cacheable.read(c.payload)
      catch {
        case _: Throwable => throw new StepSerializationFailed(s"Signal '${s.key}' payload could not be decoded")
      }
    }

  /** The runtime-computed step machinery for a [[Awaitable.SignalEvent]].
    */
  private def awaitSignal[A: Cacheable](
      stepKey: String,
      signal: Signal[A],
      filter: A => Boolean,
      lookBack: Duration,
      invalidateOn: Seq[StepInput[?]],
      ensureUnchanged: Seq[StepInput[?]],
      invalidateAfter: Duration
  )(using ctx: WorkflowContext): A = {
    val execution = ctx.execution
    val stepId = StepId(stepKey, execution.currentScope)
    val valueCodec = summon[Cacheable[A]]
    val fingerprints = encodeFingerprints(ensureUnchanged ++ invalidateOn)
    val now = execution.now
    val expiresAt = invalidateAfter match {
      case d: FiniteDuration => Some(now.plus(java.time.Duration.ofNanos(d.toNanos)))
      case _                 => None
    }

    def decode(payload: String): A =
      try valueCodec.read(payload)
      catch {
        case _: Throwable => throw new StepSerializationFailed(s"Await '$stepKey' result could not be decoded")
      }

    def accept(c: AwaitSignalCandidate): Boolean = {
      val withinLookBack = lookBack match {
        case d: FiniteDuration =>
          !c.createdAt.isBefore(now.minus(java.time.Duration.ofNanos(d.toNanos)))
        case _ => true
      }
      withinLookBack && filter(decode(c.payload))
    }

    def serializedOf(c: AwaitSignalCandidate): (Long, String) = (c.sequenceId, valueCodec.write(decode(c.payload)))

    def evaluate(): A = {
      val candidates = execution.readAwaitSignalCandidates(signal.key)
      candidates.find(accept) match {
        case Some(winning) =>
          val serialized = valueCodec.write(decode(winning.payload))
          execution.resolveAwaitSignal(
            stepId, 0L, "Await", fingerprints, signal.key, winning.sequenceId, serialized, expiresAt
          )
          decode(serialized)
        case None =>
          execution.suspendAwaitSignal(stepId, 0L, "Await", fingerprints, signal.key, expiresAt) { recheck =>
            recheck.find(accept).map(serializedOf)
          } match {
            case Some(serialized) => decode(serialized)
            case None             => throw new WorkflowSuspendedException
          }
      }
    }

    val existing = execution.lookupStep(stepId, 0L).filterNot(_.expiresAt.exists(!_.isAfter(now)))

    existing match {
      case None => evaluate()
      case Some(row) =>
        val stored = parseFingerprints(row.inputFingerprints)
        for (input <- ensureUnchanged) {
          if (stored.get(input.name) != Some(fingerprintOf(input)))
            throw new StepInputConflictException(
              s"Await '$stepKey' input '${input.name}' changed between runs and is pinned with ensureUnchanged; refusing to re-evaluate"
            )
        }
        val shouldReevaluate = invalidateOn.exists(input => stored.get(input.name) != Some(fingerprintOf(input)))
        if (shouldReevaluate) {
          execution.deleteStep(stepId, 0L)
          evaluate()
        } else {
          row.stateKind match {
            case "succeeded" => decode(row.statePayload)
            case "failed" =>
              throw new StepSerializationFailed(s"Await '$stepKey' stored a failure without a failed await")
            case _ => evaluate()
          }
        }
    }
  }

  /** The runtime-computed step machinery for a [[Awaitable.Timer]].
    */
  private def awaitTimer(
      stepKey: String,
      deadline: java.time.Instant,
      invalidateOn: Seq[StepInput[?]],
      ensureUnchanged: Seq[StepInput[?]],
      invalidateAfter: Duration
  )(using ctx: WorkflowContext): Unit = {
    val execution = ctx.execution
    val stepId = StepId(stepKey, execution.currentScope)
    val fingerprints = encodeFingerprints(ensureUnchanged ++ invalidateOn)
    val now = execution.now
    val expiresAt = invalidateAfter match {
      case d: FiniteDuration => Some(now.plus(java.time.Duration.ofNanos(d.toNanos)))
      case _                 => None
    }

    def evaluate(): Unit = {
      execution.fireDueTimers(stepId, 0L)
      if (execution.readAwaitTimerCandidates(stepId, 0L).nonEmpty) {
        execution.resolveAwaitTimer(stepId, 0L, "Await", fingerprints, "", expiresAt)
      } else {
        execution.suspendAwaitTimer(stepId, 0L, "Await", fingerprints, deadline, expiresAt)
        throw new WorkflowSuspendedException
      }
    }

    val existing = execution.lookupStep(stepId, 0L).filterNot(_.expiresAt.exists(!_.isAfter(now)))

    existing match {
      case None => evaluate()
      case Some(row) =>
        val stored = parseFingerprints(row.inputFingerprints)
        for (input <- ensureUnchanged) {
          if (stored.get(input.name) != Some(fingerprintOf(input)))
            throw new StepInputConflictException(
              s"Await '$stepKey' input '${input.name}' changed between runs and is pinned with ensureUnchanged; refusing to re-evaluate"
            )
        }
        val shouldReevaluate = invalidateOn.exists(input => stored.get(input.name) != Some(fingerprintOf(input)))
        if (shouldReevaluate) {
          execution.deleteStep(stepId, 0L)
          execution.deleteTimerSubscriptions(stepId, 0L)
          evaluate()
        } else {
          row.stateKind match {
            case "succeeded" => ()
            case "failed" =>
              throw new StepSerializationFailed(s"Await '$stepKey' stored a failure without a failed await")
            case _ => evaluate()
          }
        }
    }
  }

  /** Throwables that abort the run without durable step state: fatal JVM errors,
    * the library's control-flow exceptions (suspension, continue-as-new), lease
    * loss, and cancellation delivered at a checkpoint.
    */
  private def isNonCacheable(t: Throwable): Boolean = t match {
    case _: VirtualMachineError | _: ThreadDeath | _: LinkageError => true
    case _: WorkflowControlException                                => true
    case _: LeaseLostException                                      => true
    case _: WorkflowCancelledException                              => true
    case _                                                          => false
  }

  /** Deterministic, order-insensitive encoding of the named inputs. Each line is
    * `<hex(name)>|<base64fingerprint>`; lines are sorted by name and joined with
    * `\n`. Empty when there are no named inputs (the instance id is then the sole
    * cache key).
    */
  private def encodeFingerprints(inputs: Seq[StepInput[?]]): String =
    inputs
      .map(input => hex(input.name.getBytes(StandardCharsets.UTF_8)) + "|" + fingerprintOf(input))
      .sortBy(identity)
      .mkString("\n")

  private def parseFingerprints(encoded: String): Map[String, String] =
    if (encoded.isEmpty) Map.empty
    else
      encoded.split("\n").iterator.map { line =>
        val sep = line.indexOf('|')
        val name = new String(hexDecode(line.substring(0, sep)), StandardCharsets.UTF_8)
        name -> line.substring(sep + 1)
      }.toMap

  private def fingerprintOf(input: StepInput[?]): String =
    input.fingerprint(Sha256Fingerprinter).toString

  private def hex(bytes: Array[Byte]): String = bytes.map(b => f"${b & 0xff}%02x").mkString

  private def hexDecode(hex: String): Array[Byte] =
    hex.grouped(2).map(h => Integer.parseInt(h, 16).toByte).toArray
}
