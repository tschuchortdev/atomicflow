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
  AwaitSignalCandidate,
  AwaitUpdateDecision,
  ScopePath,
  UpdateCandidate
}

import java.nio.charset.StandardCharsets
import scala.annotation.experimental
import scala.annotation.targetName
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

    private val neverPolicy: RetryPolicy = new RetryPolicy {
      override def nextDelay(
          failure: Throwable,
          failedAttempts: Int,
          cumulativeDelay: FiniteDuration,
          lastDelay: Option[FiniteDuration]
      ): Option[FiniteDuration] = None
    }

    /** The default: never retry. A body failure is persisted immediately. */
    def never: RetryPolicy = neverPolicy

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
      execution.checkCancellation()
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

    /** One retry-aware body attempt sequence. On success the `succeeded` row is
      * persisted (retiring any retry subscription); on a terminal failure the
      * `failed` row is persisted; on a retryable failure the policy decides
      * between an inline sleep-and-retry and a durable suspension. The body is
      * re-evaluated (the by-name `body`) per attempt; inline retries loop rather
      * than recurse so an unbounded policy with tiny delays cannot grow the
      * stack.
      */
    def attempt(initialB: RetryBookkeeping): A = {
      var b = initialB
      var result: Option[A] = None
      while (result.isEmpty) {
        try {
          execution.renewLease()
          execution.checkCancellation()
          val value = body
          val serialized =
            try valueCodec.write(value)
            catch { case _: Throwable => throw new StepSerializationFailed(s"Step '$key' result could not be encoded") }
          val decoded =
            try valueCodec.read(serialized)
            catch { case _: Throwable => throw new StepSerializationFailed(s"Step '$key' result could not be decoded") }
          execution.resolveStepRetry(stepId, stepVersion, stepKind, "succeeded", fingerprints, serialized, expiresAt)
          result = Some(decoded)
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
                  java.util.concurrent.TimeUnit.NANOSECONDS.sleep(delay.toNanos)
                  b = nb
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
      result.get
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

  /** Await a synchronous, direct-addressed [[Update]] and answer it with a
    * response.
    *
    * Behaves like a step (the result is cached and replayed), but unlike a
    * `Signal` the sender is blocked waiting for a response. When an unhandled
    * update record for `u.key` exists (the OLDEST one), `respond(input)` is
    * called with the update's decoded input; it returns `(response, output)`.
    * The `response` is durably written to the update's record (so the blocked
    * sender observes it as `UpdateSendResult.Success`) and `output` is what this
    * await returns to the workflow body. When no unhandled record exists, the
    * workflow durably suspends (a subscription is registered so a later
    * `sendUpdate` can wake it).
    *
    * Updates are addressed directly to this instance and are never inherited:
    * only records addressed to this instance are candidates, so a child never
    * sees an update sent to its parent.
    *
    * @param stepKey
    *   the await's stable identity within the workflow
    * @param u
    *   the update to await
    * @param respond
    *   computes the synchronous `response` (written to the record, read by the
    *   sender) and the `output` (returned to the body)
    */
  def awaitUpdate[I, R, O: Cacheable](stepKey: String, u: Update[I, R])(respond: I => (R, O))(using
      ctx: WorkflowContext,
      throwableCodec: Cacheable[Throwable]
  ): O =
    awaitUpdate0(stepKey, u, respond)

  /** Runs several branches concurrently (via Ox `par`) and returns the result of
    * the first branch to complete normally, without suspending. If every branch
    * suspends, one combined [[WorkflowSuspendedException]] carrying each
    * branch's suspension in `causes` is thrown, exactly like `Workflow.parallel`.
    * If at least one branch completes, the first completed branch wins: its
    * result is returned and the other branches' suspensions are DISCARDED (their
    * pending subscriptions are cleaned up best-effort). A non-suspension failure
    * in any branch fails the construct (propagated after the branches settle,
    * parallel-failure semantics) and records no winner.
    *
    * Unlike `Workflow.parallel`, this construct is EDGE-triggered, so it persists
    * its own step row (`FirstToRunWithoutSuspension`) recording the winning
    * branch's index and result. On replay the first-ever-completed branch's
    * result is returned even if later reruns would unblock a different branch
    * first: once a winner is recorded it is never recomputed, and no branch runs
    * again.
    *
    * WARNING — dangerous pitfalls: racing arbitrary code against an await depends
    * on WHEN the code is run, and code that runs as a branch may be re-executed
    * on replay. A `Thread.sleep` or other blocking call that completes inline
    * will beat an await that merely suspends: the await throws
    * [[WorkflowSuspendedException]] and is not unblocked until the whole workflow
    * re-runs, while the blocking call finishes immediately on this run.
    *
    * Even when every branch suspends, the construct only behaves as expected when
    * the workflow is re-run for each incoming event individually. If the workflow
    * is re-run for multiple events at once (for example because of a long queue
     * in the job runner), several branches may become unblocked in the same run
     * and the code cannot tell which event came first: the winner is whichever the
     * implementation observes first, NOT a spec-guaranteed order (though the
     * recorded winner is durable first-wins). When several branches complete in
     * the same run, the lowest-index completed branch is the deterministic
     * tie-break winner.
    *
    * Each branch runs in its own branch-scoped identity, so two branches awaiting
    * the same key register distinct subscriptions and a losing branch's cleanup
    * cannot delete the winner's (or another branch's) rows.
    *
    * Drift policies apply exactly as for ordinary steps: `ensureUnchanged` values
    * must be invariant between runs, and `invalidateOn` changes discard the
    * cached winner and re-run from scratch.
    *
    * @param stepId
    *   the construct's stable identity within the workflow
    * @param invalidateOn
    *   named inputs that invalidate the cached result when they change
    * @param ensureUnchanged
    *   named inputs that must be invariant between runs
    * @param branches
    *   the branches to race; the first to complete normally wins
    */
  @experimental
  def firstToRunWithoutSuspension[R: Cacheable](
      stepId: String,
      invalidateOn: Seq[StepInput[?]],
      ensureUnchanged: Seq[StepInput[?]]
  )(branches: Seq[() => R])(using ctx: WorkflowContext, throwableCodec: Cacheable[Throwable]): R =
    firstToRun0(stepId, invalidateOn, ensureUnchanged, branches.toVector)

  /** Vararg form of [[firstToRunWithoutSuspension]]. */
  @targetName("firstToRunWithoutSuspensionVararg")
  @experimental
  def firstToRunWithoutSuspension[R: Cacheable](
      stepId: String,
      invalidateOn: Seq[StepInput[?]] = Seq.empty,
      ensureUnchanged: Seq[StepInput[?]] = Seq.empty
  )(branches: (() => R)*)(using ctx: WorkflowContext, throwableCodec: Cacheable[Throwable]): R =
    firstToRun0(stepId, invalidateOn, ensureUnchanged, branches.toVector)

  private def firstToRun0[R: Cacheable](
      stepId: String,
      invalidateOn: Seq[StepInput[?]],
      ensureUnchanged: Seq[StepInput[?]],
      branches: Vector[() => R]
  )(using ctx: WorkflowContext, throwableCodec: Cacheable[Throwable]): R = {
    require(branches.nonEmpty, s"firstToRunWithoutSuspension('$stepId') requires at least one branch")
    val execution = ctx.execution
    val stepIdv = StepId(stepId, execution.currentScope)
    val valueCodec = summon[Cacheable[R]]
    val fingerprints = encodeFingerprints(ensureUnchanged ++ invalidateOn)
    val now = execution.now

    def decodeWinner(payload: String): R = {
      val nl = payload.indexOf('\n')
      if (nl < 0) throw new StepSerializationFailed(s"Step '$stepId' stored a malformed first-to-run result")
      val encoded = payload.substring(nl + 1)
      try valueCodec.read(encoded)
      catch {
        case _: Throwable => throw new StepSerializationFailed(s"Step '$stepId' result could not be decoded")
      }
    }

    def branchSegment(i: Int): String =
      ScopePath.escapeScopeSegment(stepId) + "/" + ScopePath.escapeScopeSegment("branch" + i)

    def branchPath(base: String, i: Int): String = {
      val seg = branchSegment(i)
      if (base.isEmpty) seg else base + "/" + seg
    }

    def evaluate(): R = {
      execution.checkCancellation()
      val baseScope = execution.currentScope
      val snapshot = execution.snapshotBranchContext()
      val outcomes: Seq[Either[WorkflowSuspendedException, R]] =
        ox.par(branches.indices.map { i =>
          () =>
            val pristine = execution.snapshotBranchContext()
            execution.restoreBranchContext(snapshot)
            try Right {
              execution.pushScope(branchSegment(i))
              try branches(i)()
              finally execution.popScope()
            }
            catch { case e: WorkflowSuspendedException => Left(e) }
            finally execution.restoreBranchContext(pristine)
        })
      val suspensions = outcomes.collect { case Left(s) => s }
      if (suspensions.size == outcomes.size) {
        throw new WorkflowSuspendedException(suspensions)
      } else {
        val completed = outcomes.zipWithIndex.collect { case (Right(r), i) => (i, r) }
        val (winnerIdx, winnerResult) = completed.head
        val serialized =
          try valueCodec.write(winnerResult)
          catch {
            case _: Throwable => throw new StepSerializationFailed(s"Step '$stepId' result could not be encoded")
          }
        val decoded =
          try valueCodec.read(serialized)
          catch {
            case _: Throwable => throw new StepSerializationFailed(s"Step '$stepId' result could not be decoded")
          }
        val loserPathsToClean = outcomes.zipWithIndex.collect {
          case (Left(_), i) if i != winnerIdx => branchPath(baseScope, i)
        }
        execution.resolveFirstToRun(
          stepIdv, 0L, "FirstToRunWithoutSuspension", fingerprints, loserPathsToClean, s"$winnerIdx\n$serialized", None
        )
        decoded
      }
    }

    val rawExisting = execution.lookupStep(stepIdv, 0L)
    val existing = rawExisting.filterNot(_.expiresAt.exists(!_.isAfter(now)))

    existing match {
      case None => evaluate()
      case Some(row) =>
        val stored = parseFingerprints(row.inputFingerprints)
        for (input <- ensureUnchanged) {
          if (stored.get(input.name) != Some(fingerprintOf(input)))
            throw new StepInputConflictException(
              s"Step '$stepId' input '${input.name}' changed between runs and is pinned with ensureUnchanged; refusing to re-execute"
            )
        }
        val shouldReevaluate = invalidateOn.exists(input => stored.get(input.name) != Some(fingerprintOf(input)))
        if (shouldReevaluate) {
          execution.deleteStep(stepIdv, 0L)
          evaluate()
        } else {
          row.stateKind match {
            case "succeeded" => decodeWinner(row.statePayload)
            case _           => evaluate()
          }
        }
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
      execution.checkCancellation()
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

  /** The runtime-computed step machinery for an [[Update]] await.
    */
  private def awaitUpdate0[I, R, O: Cacheable](
      stepKey: String,
      u: Update[I, R],
      respond: I => (R, O)
  )(using ctx: WorkflowContext): O = {
    val execution = ctx.execution
    val stepId = StepId(stepKey, execution.currentScope)
    val outCodec = summon[Cacheable[O]]
    val fingerprints = encodeFingerprints(Seq.empty)
    val now = execution.now
    val expiresAt = None

    def decodeInput(encoded: String): I =
      try u.inputCacheable.read(encoded)
      catch {
        case _: Throwable => throw new StepSerializationFailed(s"Update '${u.key}' input could not be decoded")
      }

    def decodeOutput(encoded: String): O =
      try outCodec.read(encoded)
      catch {
        case _: Throwable => throw new StepSerializationFailed(s"AwaitUpdate '$stepKey' output could not be decoded")
      }

    def respondTo(c: UpdateCandidate): AwaitUpdateDecision = {
      val input = decodeInput(c.encodedInput)
      val (response, output) = respond(input)
      val encodedResponse =
        try u.responseCacheable.write(response)
        catch {
          case _: Throwable => throw new StepSerializationFailed(s"Update '${u.key}' response could not be encoded")
        }
      val encodedOutput =
        try outCodec.write(output)
        catch {
          case _: Throwable => throw new StepSerializationFailed(s"AwaitUpdate '$stepKey' output could not be encoded")
        }
      AwaitUpdateDecision(c, encodedResponse, encodedOutput)
    }

    def evaluate(): O = {
      execution.checkCancellation()
      val candidates = execution.readAwaitUpdateCandidates(u.key)
      candidates.headOption match {
        case Some(winning) =>
          val decision = respondTo(winning)
          val won = execution.resolveAwaitUpdate(
            stepId, 0L, "AwaitUpdate", fingerprints, u.key, winning, decision.encodedResponse,
            decision.encodedOutput, expiresAt
          )
          if (won) decodeOutput(decision.encodedOutput)
          else evaluate()
        case None =>
          execution.suspendAwaitUpdate(stepId, 0L, "AwaitUpdate", fingerprints, u.key, expiresAt) { recheck =>
            recheck.headOption.map(respondTo)
          } match {
            case Some(decision) => decodeOutput(decision.encodedOutput)
            case None           => throw new WorkflowSuspendedException
          }
      }
    }

    val existing = execution.lookupStep(stepId, 0L).filterNot(_.expiresAt.exists(!_.isAfter(now)))

    existing match {
      case None => evaluate()
      case Some(row) =>
        row.stateKind match {
          case "succeeded" => decodeOutput(row.statePayload)
          case "failed" =>
            throw new StepSerializationFailed(s"AwaitUpdate '$stepKey' stored a failure without a failed await")
          case _ => evaluate()
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
      execution.checkCancellation()
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
      execution.checkCancellation()
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
