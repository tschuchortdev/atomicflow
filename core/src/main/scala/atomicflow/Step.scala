package atomicflow

import atomicflow.Cacheable.Simple.given
import atomicflow.impl.Sha256Fingerprinter

import java.nio.charset.StandardCharsets
import scala.annotation.experimental
import scala.annotation.targetName
import scala.concurrent.duration.*

/** Durable operations with stored results and named-input cache policies.
  */
object Step {

  private enum ExecutionGuarantee { case AtLeastOnce, AtMostOnce }

  /** A retry policy for `Step.atLeastOnce`. A policy decides, from the failure thrown by the
    * step body and the retry bookkeeping accumulated so far, whether to retry
    * and after what delay.
    *
    * Retry policy and crash recovery are separate:
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
    runStep(ExecutionGuarantee.AtLeastOnce, key, version, ensureUnchanged, invalidateOn, invalidateAfter, retry)(body).get

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
    runStep(ExecutionGuarantee.AtMostOnce, key, 0L, ensureUnchanged, invalidateOn, invalidateAfter, Step.RetryPolicy.never)(body)

  /** The cancellation checkpoint for new work (a Step body about to execute, or
    * an await about to be evaluated): unless the `Workflow.uncancellable` depth
    * of the call site's context is non-zero, re-reads the durable
    * `cancel_requested_at` flag via the runtime and throws
    * [[WorkflowCancelledException]] when set. Cached replays never call it, so
    * they never deliver.
    */
  private def throwIfCancelled(using ctx: WorkflowContext): Unit =
    if (ctx.uncancellableDepth == 0 && ctx.runtime.isCancellationRequested(ctx.instanceId))
      throw WorkflowCancelledException()

  /** The shared machinery for both execution guarantees. Both persist a
    * `Started` record before the body executes and replace it with a durable
    * outcome; the guarantees differ only in how an unresolved `Started` record
    * is replayed (at-least-once re-executes, at-most-once returns `None`).
    *
    * A thrown body failure is always handed to `retry`; at-most-once passes
    * [[Step.RetryPolicy.never]] (as does the at-least-once default), so both
    * guarantees share a single execution path. A retry delay at or below the
    * runtime's durable-retry threshold sleeps inline inside the run; a delay
    * above it durably suspends the step (a `started` row carrying retry
    * bookkeeping plus an ordinary timer subscription) and resumes after the
    * deadline.
    */
  private def runStep[A: Cacheable as valueCodec](
      guarantee: ExecutionGuarantee,
      stepKey: String,
      stepVersion: Long,
      ensureUnchanged: Seq[StepInput[?]],
      invalidateOn: Seq[StepInput[?]],
      invalidateAfter: Duration,
      retry: Step.RetryPolicy
  )(body: => A)(using ctx: WorkflowContext, throwableCodec: Cacheable[Throwable]): Option[A] = {
    val rt: ctx.runtime.type = ctx.runtime
    val run = ctx.currentExecution
    val stepId = StepId(stepKey, ctx.currentScope)
    val stepKind = guarantee.toString
    val fingerprints = encodeFingerprints(ensureUnchanged ++ invalidateOn)
    val now = rt.clock.instant()
    val expiresAt = expiryOf(invalidateAfter, now)

    /** Encodes a body failure, returning the payload to persist together with the
      * throwable to rethrow. When the configured codec cannot round-trip the
      * original failure, the persisted payload describes a
      * [[StepSerializationFailed]] instead, and the original failure is rethrown
      * only if even that cannot be encoded (in which case nothing is persisted).
      */
    def serializeFailure(t: Throwable): (String, Throwable) = {
      val encoded =
        try Right(throwableCodec.write(t))
        catch {
          case _: Throwable =>
            Left(
              new StepSerializationFailed(
                s"Step '$stepKey' failure could not be encoded with the configured throwable codec: ${t.getClass.getName}: ${t.getMessage}"
              )
            )
        }
      encoded match {
        case Right(serialized) =>
          val decoded =
            try throwableCodec.read(serialized)
            catch { case _: Throwable => new StepSerializationFailed(s"Step '$stepKey' failure could not be decoded") }
          (serialized, decoded)
        case Left(ssf) =>
          val persisted =
            try throwableCodec.write(ssf)
            catch { case _: Throwable => throw t }
          (persisted, ssf)
      }
    }

    /** Runs the body until the retry policy stops retrying. `resumed` carries the
      * bookkeeping of a durable retry being resumed, or `None` for a fresh
      * execution (which writes the `Started` row first). This is the step's
      * single cancellation checkpoint, taken once before any body execution or
      * `Started` write.
      *
      * On success the `succeeded` row is persisted; on a terminal failure the
      * `failed` row is persisted; otherwise the policy decides between an inline
      * sleep-and-retry and a durable suspension. The body is re-evaluated (the
      * by-name `body`) per attempt; inline retries loop rather than recurse so an
      * unbounded policy with tiny delays cannot grow the stack.
      */
    def runBody(resumed: Option[RetryState]): A = {
      throwIfCancelled
      var bookkeeping = resumed.getOrElse {
        rt.writeStepState(
          run,
          stepId,
          stepVersion,
          stepKind,
          StoredStep.State.Started,
          fingerprints,
          "",
          None
        )
        RetryState.initial
      }
      while (true) {
        try {
          val (serialized, decoded) = encodeAndDecode(valueCodec, stepKey)(body)
          rt.writeStepState(
            run, stepId, stepVersion, stepKind, StoredStep.State.Succeeded, fingerprints, serialized, expiresAt
          )
          return decoded
        } catch {
          case t if isNonCacheable(t) => throw t
          case t =>
            retry.nextDelay(
              t,
              bookkeeping.failedAttempts,
              bookkeeping.cumulativeDelay,
              bookkeeping.lastDelay
            ) match {
              case None =>
                val (payload, toThrow) = serializeFailure(t)
                rt.writeStepState(
                  run, stepId, stepVersion, stepKind, StoredStep.State.Failed, fingerprints, payload, expiresAt
                )
                throw toThrow
              case Some(delay) =>
                bookkeeping = bookkeeping.after(delay)
                if (delay <= rt.durableRetryThreshold)
                  java.util.concurrent.TimeUnit.NANOSECONDS.sleep(delay.toNanos)
                else {
                  val deadline = rt.clock.instant().plus(java.time.Duration.ofNanos(delay.toNanos))
                  rt.suspendStepRetry(
                    run,
                    stepId,
                    stepVersion,
                    stepKind,
                    fingerprints,
                    RetryState.encode(bookkeeping),
                    deadline,
                    expiresAt
                  )
                  throw new WorkflowSuspendedException
                }
            }
        }
      }
      throw new IllegalStateException("unreachable: the retry loop returns or throws")
    }

    val rawExisting = rt.lookupStep(run, stepId, stepVersion)
    val replayable =
      rawExisting.filterNot(row =>
        isExpired(row, now) || haveInputsChanged(stepKey, row, ensureUnchanged, invalidateOn)
      )
    if (rawExisting.isDefined && replayable.isEmpty) rt.deleteStepOrAwait(run, stepId, stepVersion)

    replayable match {
      case None => Some(runBody(None))

      case Some(row) =>
        row.state match {
          case StoredStep.State.Succeeded =>
            Some(decodeStored(valueCodec, stepKey)(row.statePayload))
          case StoredStep.State.Failed =>
            throw decodeStoredFailure(throwableCodec, stepKey)(row.statePayload)
          case StoredStep.State.Started if guarantee == ExecutionGuarantee.AtMostOnce =>
            None
          case StoredStep.State.Started if row.statePayload.startsWith(RetryState.payloadPrefix) =>
            if (!rt.fireStepRetryIfDue(run, stepId, stepVersion)) throw new WorkflowSuspendedException
            Some(runBody(Some(RetryState.decode(row.statePayload))))
          case StoredStep.State.Started =>
            Some(runBody(None))
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
    val stepId = StepId(key, ctx.currentScope)
    val valueCodec = summon[Cacheable[A]]
    ctx.runtime.readStep(ctx.instanceId, stepId, stepVersion) match {
      case None => StepExecutionState.NeverStarted
      case Some(row) =>
        row.state match {
          case StoredStep.State.Succeeded =>
            StepExecutionState.Completed(decodeStored(valueCodec, key)(row.statePayload))
          case StoredStep.State.Failed =>
            StepExecutionState.Failed(decodeStoredFailure(throwableCodec, key)(row.statePayload))
          case StoredStep.State.Started => StepExecutionState.Started
        }
    }
  }

  /** Await a runtime-computed condition: an event on a [[Signal]], a timer, or a
    * workflow completion. The await behaves like a step — its resolved result is
    * persisted in `workflow_steps` and replayed from cache, so a re-run never
    * re-suspends on an already-resolved await.
    *
    * If no stored event satisfies the wait, it suspends the
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
      case Awaitable.Mapped(_, _) =>
        throw new UnsupportedOperationException("awaiting a mapped awaitable is not yet supported")
      case _ =>
        evaluateAwaitRace(stepKey, Seq(("", awaitable)), invalidateOn, ensureUnchanged, invalidateAfter)
    }

  /** Race several awaitables and resolve with the earliest satisfying durable
    * event, compared by global `sequenceId` so signals, timers, and workflow
    * completions compete fairly regardless of kind or workflow tree.
    *
    * Each branch is a `key -> awaitable` pair: the key is the branch's
    * subscriber key, a stable identity that names the awaitable within this race
    * independently of its position. Keys must be non-empty, unique within the
    * race, and must not start with `"__"` (reserved for engine-internal
    * subscribers). Reordering the branches never changes their subscription
    * identity.
    *
    * Creates one subscription per branch in the corresponding table. Each signal
    * branch retains the shared exact-key cursor for its own key; only the winning
    * signal key's cursor advances. A satisfied race persists a `succeeded` step
    * row (result = the winning branch's value) and retires every branch's
    * subscription atomically. If no candidate matches, the workflow suspends
    * with all subscriptions registered and signal cursors advanced past rejected events.
    *
    * Due timer branches of the race are fired at evaluation in deadline
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
    * @param branches
    *   the `key -> awaitable` branches to race, all of result type `A`
    */
  def awaitRace[A: Cacheable](
      stepKey: String,
      invalidateOn: Seq[StepInput[?]] = Seq.empty,
      ensureUnchanged: Seq[StepInput[?]] = Seq.empty
  )(branches: (String, Awaitable[A])*)(using ctx: WorkflowContext, throwableCodec: Cacheable[Throwable]): A = {
    validateBranchKeys(stepKey, branches.map(_._1))
    evaluateAwaitRace(stepKey, branches.toVector, invalidateOn, ensureUnchanged, Duration.Inf)
  }

  /** Validates user-supplied race branch keys (an [[awaitRace]] member key or a
    * [[firstToRunWithoutSuspension]] branch key). The `""` key is reserved for
    * single-subscriber sites (plain awaits), and the `"__"` prefix for
    * engine-internal subscribers (such as step retries).
    */
  private def validateBranchKeys(stepKey: String, branchKeys: Seq[String]): Unit = {
    branchKeys.foreach { branchKey =>
      require(branchKey.nonEmpty, s"'$stepKey' branch keys must be non-empty")
      require(
        !branchKey.startsWith("__"),
        s"'$stepKey' branch key '$branchKey' uses the reserved '__' prefix"
      )
    }
    val duplicateKeys = branchKeys.groupBy(identity).collect { case (k, ms) if ms.size > 1 => k }.toVector.sorted
    require(
      duplicateKeys.isEmpty,
      s"'$stepKey' branch keys must be unique; duplicated: ${duplicateKeys.mkString(", ")}"
    )
  }

  /** Read the currently visible events of `signal` (those after the instance's shared
    * exact-key cursor) without advancing the cursor. The returned values are the
    * decoded signal payloads.
    */
  def peekSignal[A](signal: Signal[A])(using ctx: WorkflowContext): Seq[A] =
    ctx.runtime.readAwaitSignalCandidates(ctx.currentExecution, signal.key).map { candidate =>
      try signal.cacheable.read(candidate.payload)
      catch {
        case _: Throwable => throw new StepSerializationFailed(s"Signal '${signal.key}' payload could not be decoded")
      }
    }

  /** Runs branches concurrently and waits for them to finish or suspend.
    * Returns and stores the result of the first declared branch that completed
    * normally. If all branches suspend, throws their combined suspension. A
    * non-suspension failure propagates without storing a winner.
    *
    * Replay returns the stored result without running branches again, unless
    * `invalidateOn` inputs change. `ensureUnchanged` inputs must remain equal.
    * Each branch has its own step scope. All branch subscriptions, including
    * nested scopes, are deleted before the winning result is stored. A crash
    * before that write can rerun branches and select a different result.
    *
    * This operation does not select by event time. A blocking branch can finish
    * while another branch suspends on a wait. When several branches complete
    * during one run, declaration order selects the result. Use [[awaitRace]]
    * when the earliest stored event must win.
    *
    * @param stepId
    *   the construct's stable identity within the workflow
    * @param invalidateOn
    *   named inputs that invalidate the cached result when they change
    * @param ensureUnchanged
    *   named inputs that must be invariant between runs
    * @param branches
    *   the `key -> branch` pairs to race; keys are non-empty, unique within the
    *   race, and must not start with `"__"` (reserved for engine-internal
    *   subscribers); the first branch to complete normally wins. Each branch
    *   body is wrapped in a `() => WorkflowContext ?=> R` lambda
    *   (`"key" -> { () => ... }`) so it stays lazy: constructing the lambda has
    *   no side effects and the body runs only when the construct applies it with
    *   the branch's own context. A bare `("key", { ... })` context function
    *   would be typed eagerly and evaluated at the call site.
    */
  @experimental
  def firstToRunWithoutSuspension[R: Cacheable](
      stepId: String,
      invalidateOn: Seq[StepInput[?]] = Seq.empty,
      ensureUnchanged: Seq[StepInput[?]] = Seq.empty
  )(branches: (String, () => WorkflowContext ?=> R)*)(using ctx: WorkflowContext, throwableCodec: Cacheable[Throwable]): R =
    evaluateFirstCompletedBranch(stepId, invalidateOn, ensureUnchanged, branches.toVector)

  private def evaluateFirstCompletedBranch[R: Cacheable](
      stepId: String,
      invalidateOn: Seq[StepInput[?]],
      ensureUnchanged: Seq[StepInput[?]],
      branches: Vector[(String, () => WorkflowContext ?=> R)]
  )(using ctx: WorkflowContext, throwableCodec: Cacheable[Throwable]): R = {
    require(branches.nonEmpty, s"firstToRunWithoutSuspension('$stepId') requires at least one branch")
    validateBranchKeys(stepId, branches.map(_._1))
    val rt: ctx.runtime.type = ctx.runtime
    val run = ctx.currentExecution
    val scopedStepId = StepId(stepId, ctx.currentScope)
    val valueCodec = summon[Cacheable[R]]
    val fingerprints = encodeFingerprints(ensureUnchanged ++ invalidateOn)
    val now = rt.clock.instant()

    def branchSegment(key: String): String =
      s"$stepId/$key"

    def branchPath(base: Vector[String], key: String): Vector[String] =
      base :+ branchSegment(key)

    def evaluate(): R = {
      throwIfCancelled
      val baseScope = ctx.currentScope
      val baseScopePath = ctx.scopePath
      val branchPaths = branches.map((key, _) => branchPath(baseScope, key))
      val branchContexts = branches.map((key, _) => ctx.copy(scopePath = baseScopePath :+ branchSegment(key)))
      val outcomes: Seq[Either[WorkflowSuspendedException, R]] =
        ox.par(branches.indices.map { i =>
          () =>
            try Right(branches(i)._2()(using branchContexts(i)))
            catch { case e: WorkflowSuspendedException => Left(e) }
        })
      val suspensions = outcomes.collect { case Left(s) => s }
      if (suspensions.size == outcomes.size) {
        throw new WorkflowSuspendedException(suspensions)
      } else {
        val winnerResult = outcomes.collect { case Right(r) => r }.head
        val (serialized, decoded) = encodeAndDecode(valueCodec, stepId)(winnerResult)
        // Delete-first: retire every branch's subscriptions (including nested
        // subscopes) before persisting the winner. The winner row is the durable
        // first-wins marker, so a crash in between leaves no marker and the next
        // run re-evaluates from scratch and re-deletes.
        rt.deleteSubscriptionsUnderScopePaths(run, branchPaths)
        rt.writeStepState(
          run, scopedStepId, 0L, "FirstToRunWithoutSuspension", StoredStep.State.Succeeded, fingerprints, serialized, None
        )
        decoded
      }
    }

    val rawExisting = rt.lookupStep(run, scopedStepId, 0L)
    val replayable =
      rawExisting.filterNot(row =>
        isExpired(row, now) || haveInputsChanged(stepId, row, ensureUnchanged, invalidateOn)
      )
    if (rawExisting.isDefined && replayable.isEmpty) rt.deleteStepOrAwait(run, scopedStepId, 0L)

    replayable match {
      case Some(row) if row.state == StoredStep.State.Succeeded => decodeStored(valueCodec, stepId)(row.statePayload)
      case _                                                    => evaluate()
    }
  }

  /** The shared cached-await replay for every await kind: a stored
    * non-expired `succeeded` row replays via `decode`, an `ensureUnchanged`
    * fingerprint conflict throws, and an existing row that has drifted
    * (`invalidateOn` fingerprint change) or expired (its TTL elapsed) triggers
    * the site's single `onDiscard` policy. `onDiscard` discards the site
    * (`deleteStepOrAwait`) and then evaluates it afresh; an unresolved
    * (`started`) site also just evaluates. The stored-failure
    * `StepSerializationFailed` message uses the `"Await"` prefix.
    */
  private def replayCachedAwait[A](
      stepKey: String,
      ensureUnchanged: Seq[StepInput[?]],
      invalidateOn: Seq[StepInput[?]]
  )(using ctx: WorkflowContext)(
      decode: String => A,
      onDiscard: () => A,
      evaluate: () => A
  ): A = {
    val rt: ctx.runtime.type = ctx.runtime
    val run = ctx.currentExecution
    val stepId = StepId(stepKey, ctx.currentScope)
    val now = rt.clock.instant()
    val rawExisting = rt.lookupStep(run, stepId, 0L)
    val existing = rawExisting.filterNot(isExpired(_, now))

    existing match {
      case None =>
        if (rawExisting.exists(isExpired(_, now))) onDiscard()
        else evaluate()
      case Some(row) =>
        if (haveInputsChanged(stepKey, row, ensureUnchanged, invalidateOn)) onDiscard()
        else
          row.state match {
            case StoredStep.State.Succeeded => decode(row.statePayload)
            case StoredStep.State.Failed =>
              throw new StepSerializationFailed(s"Await '$stepKey' stored a failure without a failed await")
            case StoredStep.State.Started => evaluate()
          }
    }
  }

  /** The one evaluation path for every runtime-computed await: a plain signal,
    * timer, or workflow-completion await, and a multi-member `awaitRace`. A
    * plain await is a single-member race carrying the reserved `""` subscriber
    * key.
    *
    * An existing non-expired `succeeded` row replays; `ensureUnchanged`
    * conflicts throw; an `invalidateOn` change or an expired row discards the
    * site (`deleteStepOrAwait`: its step row plus every subscription) and
    * re-evaluates from scratch, so no stale registration can satisfy the new
    * registration.
    */
  private def evaluateAwaitRace[A: Cacheable](
      stepKey: String,
      branches: Seq[(String, Awaitable[A])],
      invalidateOn: Seq[StepInput[?]],
      ensureUnchanged: Seq[StepInput[?]],
      invalidateAfter: Duration
  )(using ctx: WorkflowContext, throwableCodec: Cacheable[Throwable]): A = {
    require(branches.nonEmpty, s"awaitRace('$stepKey') requires at least one branch")
    val rt: ctx.runtime.type = ctx.runtime
    val run = ctx.currentExecution
    val stepId = StepId(stepKey, ctx.currentScope)
    val valueCodec = summon[Cacheable[A]]
    val fingerprints = encodeFingerprints(ensureUnchanged ++ invalidateOn)
    val now = rt.clock.instant()
    val expiresAt = expiryOf(invalidateAfter, now)

    val subscribers: Vector[(Subscriber, SubscriberMatcher[A])] =
      branches.map { case (branchKey, a) => buildSubscriber(a, branchKey, valueCodec, now) }.toVector

    def decode(payload: String): A =
      try valueCodec.read(payload)
      catch {
        case _: Throwable => throw new StepSerializationFailed(s"Await '$stepKey' result could not be decoded")
      }

    def evaluate(): A = {
      throwIfCancelled
      val site = WaitSite(stepId, 0L, "Await", fingerprints, expiresAt)
      rt.evaluateWait(run, site, subscribers.map(_._1))(pickRaceWinner(subscribers)) match {
        case Some(payload) => decode(payload)
        case None          => throw new WorkflowSuspendedException
      }
    }

    def discardAndEvaluate(): A = {
      rt.deleteStepOrAwait(run, stepId, 0L)
      evaluate()
    }

    replayCachedAwait[A](stepKey, ensureUnchanged, invalidateOn)(
      decode,
      discardAndEvaluate,
      evaluate
    )
  }

  /** Flattens the `Mapped` wrappers of an awaitable down to its raw base
    * (`SignalEvent`, `Timer`, or `WorkflowCompletion`) plus the composition of
    * its `map` transforms, which maps the raw value onto the awaited type `A`.
    */
  private def flattenAwaitable[A](awaitable: Awaitable[A]): (Awaitable[?], Any => A) = awaitable match {
    case Awaitable.Mapped(underlying, transform) =>
      val (base, mapUnderlying) = flattenAwaitable(underlying)
      (base, mapUnderlying.andThen(transform))
    case base => (base, (value: Any) => value.asInstanceOf[A])
  }

  /** Selects the earliest satisfying candidate across every subscriber by global
    * `sequenceId`. Each subscriber's matcher independently filters and serializes
    * its own candidates; the minimum sequence id wins, and only a signal
    * subscriber reports the cursor key to advance.
    */
  private def pickRaceWinner[A](
      matchers: Vector[(Subscriber, SubscriberMatcher[A])]
  ): Vector[SubscriberMatch] => Option[WaitResolution] = { candidates =>
    val picks = matchers.flatMap { case (_, matcher) => matcher.pick(candidates) }
    if (picks.isEmpty) None
    else {
      val (seq, payload, key) = picks.minBy(_._1)
      Some(WaitResolution(payload, key.map(k => (k, seq))))
    }
  }

  /** A single race subscriber's candidate logic: how to accept and serialize the
    * raw candidates it is handed.
    */
  private trait SubscriberMatcher[A] {
    def pick(candidates: Vector[SubscriberMatch]): Option[(Long, String, Option[SignalKey])]
  }

  private def buildSubscriber[A](
      awaitable: Awaitable[A],
      branchKey: String,
      valueCodec: Cacheable[A],
      now: java.time.Instant
  ): (Subscriber, SubscriberMatcher[A]) = {
    val (base, mapResult) = flattenAwaitable(awaitable)
    base match {
      case Awaitable.SignalEvent(signal, filter, lookBack) =>
        val subscriber = Subscriber.Signal(branchKey, signal.key)
        val signalCodec = signal.cacheable
        val matcher = new SubscriberMatcher[A] {
          override def pick(candidates: Vector[SubscriberMatch]): Option[(Long, String, Option[SignalKey])] = {
            val matching = candidates.filter(_.subscriberKey == branchKey).find { c =>
              val withinLookBack = lookBack match {
                case d: FiniteDuration =>
                  !c.createdAt.isBefore(now.minus(java.time.Duration.ofNanos(d.toNanos)))
                case _ => true
              }
              withinLookBack && {
                val decoded =
                  try signalCodec.read(c.payload)
                  catch {
                    case _: Throwable =>
                      throw new StepSerializationFailed(s"Signal '${signal.key}' payload could not be decoded")
                  }
                filter(decoded)
              }
            }
            matching.map { c =>
              val serialized =
                try valueCodec.write(mapResult(signalCodec.read(c.payload)))
                catch {
                  case _: Throwable =>
                    throw new StepSerializationFailed(s"Subscriber '$branchKey' result could not be encoded")
                }
              (c.sequenceId, serialized, Some(signal.key))
            }
          }
        }
        (subscriber, matcher)
      case Awaitable.Timer(deadline) =>
        val subscriber = Subscriber.Timer(branchKey, deadline)
        val matcher = new SubscriberMatcher[A] {
          override def pick(candidates: Vector[SubscriberMatch]): Option[(Long, String, Option[SignalKey])] =
            candidates.find(_.subscriberKey == branchKey).map { c =>
              val serialized =
                try valueCodec.write(mapResult(()))
                catch {
                  case _: Throwable =>
                    throw new StepSerializationFailed(s"Subscriber '$branchKey' result could not be encoded")
                }
              (c.sequenceId, serialized, None)
            }
        }
        (subscriber, matcher)
      case wc @ Awaitable.WorkflowCompletion(_) =>
        val subscriber = Subscriber.Completion(
          branchKey,
          wc.workflowInstanceId.workflowId,
          wc.workflowInstanceId.workflowInstanceKey,
          wc.workflowInstanceId.scope
        )
        val matcher = new SubscriberMatcher[A] {
          override def pick(candidates: Vector[SubscriberMatch]): Option[(Long, String, Option[SignalKey])] =
            candidates.find(_.subscriberKey == branchKey).map { c =>
              val decoded =
                try wc.completionCacheable.read(c.payload)
                catch {
                  case _: Throwable =>
                    throw new StepSerializationFailed(s"Subscriber '$branchKey' completion could not be decoded")
                }
              val serialized =
                try valueCodec.write(mapResult(decoded))
                catch {
                  case _: Throwable =>
                    throw new StepSerializationFailed(s"Subscriber '$branchKey' result could not be encoded")
                }
              (c.sequenceId, serialized, None)
            }
        }
        (subscriber, matcher)
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
    case _: WorkflowControlFlowException                                => true
    case _: LeaseLostException                                      => true
    case _: WorkflowCancelledException                              => true
    case _                                                          => false
  }

  /** Deterministic, order-insensitive encoding of the named inputs. Each line is
    * `<hex(name)>|<base64fingerprint>`; lines are sorted by name and joined with
    * `\n`. Empty when there are no named inputs; step identity and version still
    * select the stored record.
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

  /** Serializes `value` and then decodes that encoding back, returning both the
    * payload to persist and a freshly decoded copy. Values cross the
    * serialization boundary before being observed (commit-before-observation), so
    * a body's result is always a decoded copy of what was persisted.
    */
  private def encodeAndDecode[A](codec: Cacheable[A], key: String)(value: A): (String, A) = {
    val serialized = encodeStored(codec, key)(value)
    (serialized, decodeStored(codec, key)(serialized))
  }

  private def encodeStored[A](codec: Cacheable[A], key: String)(value: A): String =
    try codec.write(value)
    catch { case _: Throwable => throw new StepSerializationFailed(s"Step '$key' result could not be encoded") }

  private def decodeStored[A](codec: Cacheable[A], key: String)(payload: String): A =
    try codec.read(payload)
    catch { case _: Throwable => throw new StepSerializationFailed(s"Step '$key' result could not be decoded") }

  private def decodeStoredFailure(codec: Cacheable[Throwable], key: String)(payload: String): Throwable =
    try codec.read(payload)
    catch { case _: Throwable => throw new StepSerializationFailed(s"Step '$key' failure could not be decoded") }

  /** Enforces the `ensureUnchanged` pin (throwing [[StepInputConflictException]]
    * when a pinned input changed) and reports whether any `invalidateOn` input
    * differs from what the stored row recorded. Shared by steps, first-to-run
    * constructs, and cached awaits.
    */
  private def haveInputsChanged(
      key: String,
      row: StoredStep,
      ensureUnchanged: Seq[StepInput[?]],
      invalidateOn: Seq[StepInput[?]]
  ): Boolean = {
    val stored = parseFingerprints(row.inputFingerprints)
    for (input <- ensureUnchanged) {
      if (stored.get(input.name) != Some(fingerprintOf(input)))
        throw new StepInputConflictException(
          s"'$key': input '${input.name}' changed between runs but is pinned with ensureUnchanged"
        )
    }
    invalidateOn.exists(input => stored.get(input.name) != Some(fingerprintOf(input)))
  }

  private def isExpired(row: StoredStep, now: java.time.Instant): Boolean =
    row.expiresAt.exists(!_.isAfter(now))

  private def expiryOf(invalidateAfter: Duration, now: java.time.Instant): Option[java.time.Instant] =
    invalidateAfter match {
      case d: FiniteDuration => Some(now.plus(java.time.Duration.ofNanos(d.toNanos)))
      case _                 => None
    }

  /** Retry bookkeeping persisted in a `started` row's payload while an
    * at-least-once step waits for a durable retry deadline. Encoded as
    * `retry:<failedAttempts>:<cumulativeNanos>:<lastNanos>` (`-1` for no last
    * delay).
    */
  private final case class RetryState(
      failedAttempts: Int,
      cumulativeDelay: FiniteDuration,
      lastDelay: Option[FiniteDuration]
  ) {
    def after(delay: FiniteDuration): RetryState =
      RetryState(failedAttempts + 1, cumulativeDelay + delay, Some(delay))
  }

  private object RetryState {
    val payloadPrefix = "retry:"
    val initial: RetryState = RetryState(0, 0.seconds, None)

    def encode(state: RetryState): String =
      s"$payloadPrefix${state.failedAttempts}:${state.cumulativeDelay.toNanos}:${state.lastDelay.fold(-1L)(_.toNanos)}"

    def decode(payload: String): RetryState = {
      val parts = payload.split(":")
      val lastDelay =
        if (parts(3).toLong == -1L) None
        else Some(FiniteDuration(parts(3).toLong, NANOSECONDS))
      RetryState(parts(1).toInt, FiniteDuration(parts(2).toLong, NANOSECONDS), lastDelay)
    }
  }

  private def hex(bytes: Array[Byte]): String = bytes.map(b => f"${b & 0xff}%02x").mkString

  private def hexDecode(hex: String): Array[Byte] =
    hex.grouped(2).map(h => Integer.parseInt(h, 16).toByte).toArray
}
