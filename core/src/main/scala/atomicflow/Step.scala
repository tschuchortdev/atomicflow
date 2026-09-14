package atomicflow

import atomicflow.Cacheable.Simple.given
import atomicflow.impl.Sha256Fingerprinter
import atomicflow.internal.{
  AwaitRaceCandidate,
  AwaitRaceCompletionLeaf,
  AwaitRaceDecision,
  AwaitRaceLeaf,
  AwaitRaceSignalLeaf,
  AwaitRaceTimerLeaf,
  AwaitSignalCandidate
}

import java.nio.charset.StandardCharsets
import scala.concurrent.duration.*

/** The public step API: durable, replayed operations with per-key cache-drift
  * policies (see `spec/steps.md`).
  */
object Step {

  private enum Guarantee { case AtLeastOnce, AtMostOnce }

  /** A built-in retry policy for `Step.atLeastOnce` (see `spec/steps.md`,
    * "Built-in retries"). A policy decides, from the exact failure thrown by the
    * step body and the retry bookkeeping accumulated so far, whether to retry
    * and after what delay.
    *
    * `RetryPolicy` is orthogonal to the at-least-once/at-most-once guarantee:
    * the guarantee governs crash recovery (whether an unresolved `Started`
    * record re-executes), while a retry policy governs what happens when the
    * body *completes* with an exception.
    *
    * Delays at or below the runtime's durable-retry threshold are applied as
    * inline `Thread.sleep`s inside the run; delays above it become durable
    * suspensions (an ordinary timer subscription) so the workflow survives a
    * crash while waiting.
    */
  trait RetryPolicy {

    /** Decide the next retry delay, or `None` to stop retrying and persist the
      * failure.
      *
      * @param failure
      *   the exact exception thrown by the step body (never serialized/cached
      *   while a retry is pending)
      * @param failedAttempts
      *   how many complete body attempts have failed before this one
      * @param cumulativeDelay
      *   the total delay already scheduled across prior retries
      * @param lastDelay
      *   the delay used for the most recent retry, or `None` on the first
      *   failure
      */
    def nextDelay(
        failure: Throwable,
        failedAttempts: Int,
        cumulativeDelay: FiniteDuration,
        lastDelay: Option[FiniteDuration]
    ): Option[FiniteDuration]
  }

  object RetryPolicy {

    /** The default: never retry. A body failure is persisted immediately. */
    val never: RetryPolicy = new RetryPolicy {
      override def nextDelay(
          failure: Throwable,
          failedAttempts: Int,
          cumulativeDelay: FiniteDuration,
          lastDelay: Option[FiniteDuration]
      ): Option[FiniteDuration] = None
    }

    /** Retry a retriable failure up to `maxRetries` times, always after the
      * same `delay`.
      */
    def fixedDelay(
        maxRetries: Long,
        delay: FiniteDuration,
        isRetriable: Throwable => Boolean = (t: Throwable) => true
    ): RetryPolicy = new RetryPolicy {
      override def nextDelay(
          failure: Throwable,
          failedAttempts: Int,
          cumulativeDelay: FiniteDuration,
          lastDelay: Option[FiniteDuration]
      ): Option[FiniteDuration] =
        if (failedAttempts < maxRetries && isRetriable(failure)) Some(delay) else None
    }

    /** Exponential backoff, bounded by `maxRetries` and/or `maxCumulativeDelay`
      * (whichever is hit first; `Long.MaxValue` and `Duration.Inf` disable the
      * respective bound). The delay for the `failedAttempts`-th retry is
      * `initialDelay * multiplier^failedAttempts`.
      *
      * This is exposed as one method whose named-parameter forms match both
      * spec variants: `exponentialBackoff(initialDelay, maxCumulativeDelay, ...)`
      * and `exponentialBackoff(maxRetries, initialDelay, ...)`. (Scala forbids
      * two overloaded methods both carrying default arguments.)
      */
    def exponentialBackoff(
        initialDelay: FiniteDuration,
        maxRetries: Long = Long.MaxValue,
        maxCumulativeDelay: Duration = Duration.Inf,
        multiplier: Float = 2,
        isRetriable: Throwable => Boolean = (t: Throwable) => true
    ): RetryPolicy = new RetryPolicy {
      override def nextDelay(
          failure: Throwable,
          failedAttempts: Int,
          cumulativeDelay: FiniteDuration,
          lastDelay: Option[FiniteDuration]
      ): Option[FiniteDuration] =
        if (!isRetriable(failure) || failedAttempts >= maxRetries) None
        else {
          val delay = backoffDelay(initialDelay, multiplier, failedAttempts)
          val exceedsCumulative = maxCumulativeDelay match {
            case d: FiniteDuration => cumulativeDelay + delay > d
            case _                 => false
          }
          if (exceedsCumulative) None else Some(delay)
        }
    }

    /** `initialDelay * multiplier^failedAttempts`, floored at zero. */
    private def backoffDelay(initialDelay: FiniteDuration, multiplier: Float, failedAttempts: Int): FiniteDuration = {
      val nanos = initialDelay.toNanos * math.pow(multiplier.toDouble, failedAttempts.toDouble)
      FiniteDuration(nanos.toLong.max(0L), NANOSECONDS)
    }
  }

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
    * @param retry
    *   a [[Step.RetryPolicy]] deciding whether (and after what delay) a thrown
    *   body failure is retried; default `Step.RetryPolicy.never`
    */
  def atLeastOnce[A: Cacheable](
      key: String,
      version: Long = 1L,
      ensureUnchanged: Seq[StepInput[?]] = Seq.empty,
      invalidateOn: Seq[StepInput[?]] = Seq.empty,
      invalidateAfter: Duration = Duration.Inf,
      retry: Step.RetryPolicy = Step.RetryPolicy.never
  )(body: => A)(using ctx: WorkflowContext, throwableCodec: Cacheable[Throwable]): A =
    runStep(Guarantee.AtLeastOnce, key, version, "AtLeastOnce", ensureUnchanged, invalidateOn, invalidateAfter, retry)(body).get

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
    runStep(Guarantee.AtMostOnce, key, 0L, "AtMostOnce", ensureUnchanged, invalidateOn, invalidateAfter, Step.RetryPolicy.never)(body)

  /** The shared machinery for both execution guarantees. Both persist a
    * `Started` record before the body executes and replace it with a durable
    * outcome; the guarantees differ only in how an unresolved `Started` record
    * is replayed (at-least-once re-executes, at-most-once returns `None`).
    *
    * An at-least-once step with a non-`never` `retry` policy retries a thrown
    * body failure: a delay at or below the runtime's durable-retry threshold
    * sleeps inline inside the run; a delay above it durably suspends the step
    * (a `started` row carrying retry bookkeeping plus an ordinary timer
    * subscription) and resumes after the deadline.
    */
  private def runStep[A: Cacheable](
      guarantee: Guarantee,
      key: String,
      stepVersion: Long,
      stepKind: String,
      ensureUnchanged: Seq[StepInput[?]],
      invalidateOn: Seq[StepInput[?]],
      invalidateAfter: Duration,
      retry: Step.RetryPolicy
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
    val threshold = execution.durableRetryThreshold
    val useRetry = guarantee == Guarantee.AtLeastOnce && (retry ne Step.RetryPolicy.never)

    final case class RetryBookkeeping(
        failedAttempts: Int,
        cumulativeDelay: FiniteDuration,
        lastDelay: Option[FiniteDuration]
    )

    def encodeRetry(b: RetryBookkeeping): String =
      s"retry:${b.failedAttempts}:${b.cumulativeDelay.toNanos}:${b.lastDelay.fold(-1L)(_.toNanos)}"

    def decodeRetry(payload: String): RetryBookkeeping = {
      val parts = payload.split(":")
      val failed = parts(1).toInt
      val cumulative = FiniteDuration(parts(2).toLong, NANOSECONDS)
      val last =
        if (parts(3).toLong == -1L) None
        else Some(FiniteDuration(parts(3).toLong, NANOSECONDS))
      RetryBookkeeping(failed, cumulative, last)
    }

    def decodeFailure(payload: String): Throwable =
      try throwableCodec.read(payload)
      catch {
        case _: Throwable => throw new StepSerializationFailed(s"Step '$key' failure could not be decoded")
      }

    def encodeAndPersistFailure(t: Throwable, persistFailed: String => Unit): Throwable = {
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
      encoded match {
        case Right(serialized) =>
          val decoded =
            try throwableCodec.read(serialized)
            catch { case _: Throwable => new StepSerializationFailed(s"Step '$key' failure could not be decoded") }
          persistFailed(serialized)
          decoded
        case Left(ssf) =>
          val persisted =
            try throwableCodec.write(ssf)
            catch { case _: Throwable => throw t }
          persistFailed(persisted)
          ssf
      }
    }

    /** The non-retry path (a `never` policy or at-most-once): persist the
      * `Started` row, run the body once, and persist its outcome.
      */
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
          throw encodeAndPersistFailure(
            t,
            s => execution.writeStepFailed(stepId, stepVersion, stepKind, fingerprints, s, expiresAt)
          )
      }
    }

    /** One retry-aware body attempt. On success the `succeeded` row is persisted
      * (retiring any retry subscription); on a terminal failure the `failed` row
      * is persisted; on a retryable failure the policy decides between an inline
      * sleep-and-retry and a durable suspension. The body is re-evaluated (the
      * by-name `body`) per attempt.
      */
    def attempt(b: RetryBookkeeping): A = {
      execution.renewLease()
      try {
        val value = body
        val serialized =
          try valueCodec.write(value)
          catch { case _: Throwable => throw new StepSerializationFailed(s"Step '$key' result could not be encoded") }
        val decoded =
          try valueCodec.read(serialized)
          catch { case _: Throwable => throw new StepSerializationFailed(s"Step '$key' result could not be decoded") }
        execution.resolveStepRetry(stepId, stepVersion, stepKind, "succeeded", fingerprints, serialized, expiresAt)
        decoded
      } catch {
        case t if isNonCacheable(t) => throw t
        case t =>
          retry.nextDelay(t, b.failedAttempts, b.cumulativeDelay, b.lastDelay) match {
            case None =>
              throw encodeAndPersistFailure(
                t,
                s => execution.resolveStepRetry(stepId, stepVersion, stepKind, "failed", fingerprints, s, expiresAt)
              )
            case Some(delay) =>
              val nb = RetryBookkeeping(b.failedAttempts + 1, b.cumulativeDelay + delay, Some(delay))
              if (delay <= threshold) {
                Thread.sleep(delay.toMillis)
                attempt(nb)
              } else {
                val deadline = execution.now.plus(java.time.Duration.ofNanos(delay.toNanos))
                execution.suspendStepRetry(
                  stepId, stepVersion, stepKind, fingerprints, encodeRetry(nb), deadline, expiresAt
                )
                throw new WorkflowSuspendedException
              }
          }
      }
    }

    /** A fresh retry-aware execution: persist the `Started` row (bookkeeping
      * starts empty) then run the first attempt.
      */
    def executeWithRetry(): A = {
      execution.renewLease()
      execution.writeStepStarted(stepId, stepVersion, stepKind, fingerprints)
      attempt(RetryBookkeeping(0, 0.seconds, None))
    }

    val rawExisting = execution.lookupStep(stepId, stepVersion)
    val expired = rawExisting.exists(_.expiresAt.exists(!_.isAfter(now)))
    if (expired) execution.deleteStepRetry(stepId, stepVersion)
    val existing = rawExisting.filterNot(_.expiresAt.exists(!_.isAfter(now)))

    existing match {
      case None =>
        if (useRetry) Some(executeWithRetry()) else Some(execute())

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
          execution.deleteStepRetry(stepId, stepVersion)
          if (useRetry) Some(executeWithRetry()) else Some(execute())
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
                case Guarantee.AtLeastOnce =>
                  if (useRetry && row.statePayload.startsWith("retry:")) {
                    execution.fireDueStepRetries(stepId, stepVersion)
                    if (execution.readStepRetryCandidates(stepId, stepVersion).nonEmpty)
                      Some(attempt(decodeRetry(row.statePayload)))
                    else
                      throw new WorkflowSuspendedException
                  } else {
                    Some(executeWithRetry())
                  }
                case Guarantee.AtMostOnce => None
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
      case wc @ Awaitable.WorkflowCompletion(_) =>
        awaitRace0(stepKey, Seq(wc), invalidateOn, ensureUnchanged, invalidateAfter)
      case Awaitable.Mapped(_, _) =>
        throw new UnsupportedOperationException("awaiting a mapped awaitable is not yet supported")
    }

  /** Race several awaitables and resolve with the earliest satisfying durable
    * event, compared by global `sequenceId` so signals, timers, and workflow
    * completions compete fairly regardless of kind or workflow tree.
    *
    * Creates one subscription per awaitable leaf in the corresponding table.
    * Each signal leaf retains the shared exact-key cursor for its own key; only
    * the winning signal key's cursor advances. A satisfied race persists a
    * `succeeded` step row (result = the winning leaf's value) and retires every
    * leaf's subscription atomically. If no candidate is satisfiable the workflow
    * durably suspends with every leaf's subscription registered and no cursor
    * movement.
    *
    * Due timer leaves of the race are materialized at evaluation in deadline
    * order, so the earliest due timer wins deterministically among timers.
    *
    * Drift policies apply exactly as for [[await]]: `ensureUnchanged` values must
    * be invariant between runs, and `invalidateOn` changes discard the cached
    * result and re-evaluate from scratch (re-registering subscriptions).
    *
    * All awaitables must already be mapped onto the common result type `A` (see
    * [[Awaitable.map]]), whose `Cacheable` is required at this call site.
    *
    * @param stepKey
    *   the race's stable identity within the workflow
    * @param invalidateOn
    *   named inputs that invalidate the cached race result when they change
    * @param ensureUnchanged
    *   named inputs that must be invariant between runs
    * @param awaits
    *   the awaitables to race, all of result type `A`
    */
  def awaitRace[A: Cacheable](
      stepKey: String,
      invalidateOn: Seq[StepInput[?]] = Seq.empty,
      ensureUnchanged: Seq[StepInput[?]] = Seq.empty
  )(awaits: Awaitable[A]*)(using ctx: WorkflowContext, throwableCodec: Cacheable[Throwable]): A =
    awaitRace0(stepKey, awaits.toVector, invalidateOn, ensureUnchanged, Duration.Inf)

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
        execution.resolveAwaitTimer(stepId, 0L, "Await", fingerprints, summon[Cacheable[Unit]].write(()), expiresAt)
      } else {
        execution.suspendAwaitTimer(stepId, 0L, "Await", fingerprints, deadline, expiresAt)
        throw new WorkflowSuspendedException
      }
    }

    def retireAndSuspend(): Nothing = {
      execution.invalidateTimer(stepId, 0L, deadline)
      throw new WorkflowSuspendedException
    }

    val rawExisting = execution.lookupStep(stepId, 0L)
    val existing = rawExisting.filterNot(_.expiresAt.exists(!_.isAfter(now)))

    existing match {
      case None =>
        if (rawExisting.exists(_.expiresAt.exists(!_.isAfter(now)))) retireAndSuspend()
        else evaluate()
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
          retireAndSuspend()
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

  /** The shared drift-aware machinery for `awaitRace` and for a plain completion
    * await (routed here as a single completion leaf). Mirrors `awaitSignal`: an
    * existing non-expired `succeeded` row replays; `ensureUnchanged` conflicts
    * throw; an `invalidateOn` change deletes the row and re-evaluates from
    * scratch; an expired row re-evaluates.
    */
  private def awaitRace0[A: Cacheable](
      stepKey: String,
      awaits: Seq[Awaitable[A]],
      invalidateOn: Seq[StepInput[?]],
      ensureUnchanged: Seq[StepInput[?]],
      invalidateAfter: Duration
  )(using ctx: WorkflowContext, throwableCodec: Cacheable[Throwable]): A = {
    require(awaits.nonEmpty, s"awaitRace('$stepKey') requires at least one awaitable")
    val execution = ctx.execution
    val stepId = StepId(stepKey, execution.currentScope)
    val valueCodec = summon[Cacheable[A]]
    val fingerprints = encodeFingerprints(ensureUnchanged ++ invalidateOn)
    val now = execution.now
    val expiresAt = invalidateAfter match {
      case d: FiniteDuration => Some(now.plus(java.time.Duration.ofNanos(d.toNanos)))
      case _                 => None
    }

    val leaves: Vector[(AwaitRaceLeaf, RaceLeafPicker[A])] =
      awaits.zipWithIndex.map { case (a, idx) => buildRaceLeaf(a, idx, valueCodec, now) }.toVector

    def decode(payload: String): A =
      try valueCodec.read(payload)
      catch {
        case _: Throwable => throw new StepSerializationFailed(s"Await '$stepKey' result could not be decoded")
      }

    def evaluate(): A = {
      execution.fireDueTimerLeaves(stepId, 0L)
      val decided: Vector[AwaitRaceCandidate] => Option[AwaitRaceDecision] =
        pickRaceWinner(leaves)
      execution.evaluateAwaitRace(stepId, 0L, "AwaitRace", fingerprints, leaves.map(_._1), expiresAt)(decided) match {
        case Some(payload) => decode(payload)
        case None          => throw new WorkflowSuspendedException
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

  /** Flattens the `Mapped` wrappers of an awaitable down to its raw base
    * (`SignalEvent`, `Timer`, or `WorkflowCompletion`) plus the composition of
    * its `map` transforms, which maps the raw value onto the awaited type `A`.
    */
  private def flattenAwaitable[A](a: Awaitable[A]): (Awaitable[?], Any => A) = a match {
    case Awaitable.Mapped(u, f) =>
      val (base, g) = flattenAwaitable(u)
      (base, g.andThen(f))
    case base => (base, (x: Any) => x.asInstanceOf[A])
  }

  /** Selects the earliest satisfying candidate across every leaf by global
    * `sequenceId`. Each leaf's picker independently filters and serializes its
    * own candidates; the minimum sequence id wins, and only a signal leaf reports
    * the cursor key to advance.
    */
  private def pickRaceWinner[A](
      pickers: Vector[(AwaitRaceLeaf, RaceLeafPicker[A])]
  ): Vector[AwaitRaceCandidate] => Option[AwaitRaceDecision] = { candidates =>
    val picks = pickers.flatMap { case (_, picker) => picker.pick(candidates) }
    if (picks.isEmpty) None
    else {
      val (seq, payload, key) = picks.minBy(_._1)
      Some(AwaitRaceDecision(seq, payload, key))
    }
  }

  /** A single race leaf's candidate logic: how to accept and serialize the raw
    * candidates it is handed.
    */
  private trait RaceLeafPicker[A] {
    def pick(candidates: Vector[AwaitRaceCandidate]): Option[(Long, String, Option[SignalKey])]
  }

  private def buildRaceLeaf[A](
      a: Awaitable[A],
      leafIdx: Int,
      valueCodec: Cacheable[A],
      now: java.time.Instant
  ): (AwaitRaceLeaf, RaceLeafPicker[A]) = {
    val (base, toA) = flattenAwaitable(a)
    base match {
      case Awaitable.SignalEvent(signal, filter, lookBack) =>
        val leaf = AwaitRaceSignalLeaf(leafIdx, signal.key)
        val signalCodec = signal.cacheable
        val picker = new RaceLeafPicker[A] {
          override def pick(candidates: Vector[AwaitRaceCandidate]): Option[(Long, String, Option[SignalKey])] = {
            val matching = candidates.filter(_.leafIdx == leafIdx).find { c =>
              val withinLookBack = lookBack match {
                case d: FiniteDuration =>
                  !c.createdAt.isBefore(now.minus(java.time.Duration.ofNanos(d.toNanos)))
                case _ => true
              }
              withinLookBack && filter(signalCodec.read(c.payload))
            }
            matching.map { c =>
              val serialized =
                try valueCodec.write(toA(signalCodec.read(c.payload)))
                catch {
                  case _: Throwable =>
                    throw new StepSerializationFailed(s"Race leaf '$leafIdx' result could not be encoded")
                }
              (c.sequenceId, serialized, Some(signal.key))
            }
          }
        }
        (leaf, picker)
      case Awaitable.Timer(deadline) =>
        val leaf = AwaitRaceTimerLeaf(leafIdx, deadline)
        val picker = new RaceLeafPicker[A] {
          override def pick(candidates: Vector[AwaitRaceCandidate]): Option[(Long, String, Option[SignalKey])] =
            candidates.find(_.leafIdx == leafIdx).map { c =>
              val serialized =
                try valueCodec.write(toA(()))
                catch {
                  case _: Throwable =>
                    throw new StepSerializationFailed(s"Race leaf '$leafIdx' result could not be encoded")
                }
              (c.sequenceId, serialized, None)
            }
        }
        (leaf, picker)
      case wc @ Awaitable.WorkflowCompletion(_) =>
        val leaf = AwaitRaceCompletionLeaf(
          leafIdx,
          wc.workflowInstanceId.workflowId,
          wc.workflowInstanceId.workflowInstanceKey,
          wc.workflowInstanceId.scope
        )
        val picker = new RaceLeafPicker[A] {
          override def pick(candidates: Vector[AwaitRaceCandidate]): Option[(Long, String, Option[SignalKey])] =
            candidates.find(_.leafIdx == leafIdx).map { c =>
              val decoded =
                try wc.completionCacheable.read(c.payload)
                catch {
                  case _: Throwable =>
                    throw new StepSerializationFailed(s"Race leaf '$leafIdx' completion could not be decoded")
                }
              val serialized =
                try valueCodec.write(toA(decoded))
                catch {
                  case _: Throwable =>
                    throw new StepSerializationFailed(s"Race leaf '$leafIdx' result could not be encoded")
                }
              (c.sequenceId, serialized, None)
            }
        }
        (leaf, picker)
      case other =>
        throw new UnsupportedOperationException(s"Unsupported awaitable in a race: $other")
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
