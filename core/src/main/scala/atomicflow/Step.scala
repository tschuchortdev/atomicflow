package atomicflow

import atomicflow.Cacheable.Simple.given
import atomicflow.impl.Sha256Fingerprinter

import java.nio.charset.StandardCharsets
import java.time.Instant
import scala.annotation.experimental
import scala.annotation.targetName
import scala.concurrent.duration.*
import scala.reflect.TypeTest

/** Durable operations with stored results and named-input cache policies.
  */
object Step {

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
    * suspensions, persisted as a `RetryPending` step row whose deadline governs
    * when the body may run again (a timer subscription is kept only to wake the
    * instance at that deadline) so the workflow survives a crash while waiting.
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

  /** An at-least-once step: A step that was started but did not persist a result is re-executed.
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
    runSimpleStep(atMostOnce = false, key, version, ensureUnchanged, invalidateOn, invalidateAfter, retry)(body).get

  /** An at-most-once step: A step that was started but did not persist a result is not re-executed and
   * instead returns [[None]].
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
    runSimpleStep(atMostOnce = true, key, stepVersion = 0L, ensureUnchanged, invalidateOn, invalidateAfter, Step.RetryPolicy.never)(body)

  /** The shared machinery for both execution guarantees. Both persist a
    * `Started` record before the body executes and replace it with a durable
    * outcome; the guarantees differ only in how an unresolved `Started` record
    * is replayed (at-least-once re-executes, at-most-once returns `None`).
    *
    * A thrown body failure is always handed to `retry`; at-most-once passes
    * [[Step.RetryPolicy.never]] (as does the at-least-once default), so both
    * guarantees share a single execution path. A retry delay at or below the
    * runtime's durable-retry threshold sleeps inline inside the run; a delay
    * above it durably suspends the step as a `RetryPending` row carrying the
    * retry bookkeeping and the deadline at which the body may run again and
    * resumes once that deadline is due.
    */
  private def runSimpleStep[A: Cacheable as valueCodec](
      atMostOnce: Boolean,
      stepKey: String,
      stepVersion: Long,
      ensureUnchanged: Seq[StepInput[?]],
      invalidateOn: Seq[StepInput[?]],
      invalidateAfter: Duration,
      retry: Step.RetryPolicy
  )(body: => A)(using ctx: WorkflowContext, throwableCodec: Cacheable[Throwable]): Option[A] = {
    ScopePath.requireValidKey("step key", stepKey)
    val internals: ctx.internals.type = ctx.internals
    given ctx.internals.Lease = ctx.currentExecution
    val stepId = StepId(stepKey, ctx.currentScope)
    val fingerprints = encodeFingerprints(ensureUnchanged ++ invalidateOn)
    val now = internals.clock.instant()
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
      ctx.throwIfCancelled()
      var bookkeeping = resumed.getOrElse {
        internals.writeStep(
          stepId,
          stepVersion,
          StoredStep(
            StepState.SimpleStep.Started,
            InputFingerprints.of(fingerprints),
            None
          )
        )
        RetryState.initial
      }
      while (true) {
        try {
          val (serialized, decoded) = encodeAndDecode(valueCodec, stepKey)(body)
          internals.writeStep(
            stepId, stepVersion,
            StoredStep(
              StepState.SimpleStep.Succeeded(SerializedValue.of(serialized)),
              InputFingerprints.of(fingerprints),
              expiresAt
            )
          )
          return decoded
        } catch {
          case CacheableStepException(t) =>
            retry.nextDelay(
              t,
              bookkeeping.failedAttempts,
              bookkeeping.cumulativeDelay,
              bookkeeping.lastDelay
            ) match {
              case None =>
                val (payload, toThrow) = serializeFailure(t)
                internals.writeStep(
                  stepId, stepVersion,
                  StoredStep(
                    StepState.SimpleStep.Failed(SerializedValue.of(payload)),
                    InputFingerprints.of(fingerprints),
                    expiresAt
                  )
                )
                throw presentStepFailure(stepId, toThrow)
              case Some(delay) =>
                bookkeeping = bookkeeping.after(delay)
                if (delay <= internals.durableRetryThreshold)
                  java.util.concurrent.TimeUnit.NANOSECONDS.sleep(delay.toNanos)
                else {
                  val deadline = internals.clock.instant().plus(java.time.Duration.ofNanos(delay.toNanos))
                  internals.writeStep(
                    stepId,
                    stepVersion,
                    StoredStep(
                      StepState.SimpleStep.RetryPending(SerializedValue.of(RetryState.encode(bookkeeping)), deadline),
                      InputFingerprints.of(fingerprints),
                      expiresAt
                    )
                  )
                  throw new WorkflowSuspendedException
                }
            }
        }
      }
      throw new IllegalStateException("unreachable: the retry loop returns or throws")
    }

    loadStepStateOrDeleteIfInvalid[StepState.SimpleStep](stepKey, stepId, stepVersion, now, ensureUnchanged, invalidateOn) match {
      case None => Some(runBody(None))

      case Some(row) =>
        row match {
          case StepState.SimpleStep.Succeeded(payload) =>
            Some(decodeStored(valueCodec, stepKey)(payload.raw))

          case StepState.SimpleStep.Failed(payload) =>
            throw presentStepFailure(stepId, decodeStoredFailure(throwableCodec, stepKey)(payload.raw))

          case StepState.SimpleStep.Started if atMostOnce =>
            None

          case StepState.SimpleStep.Started =>
            Some(runBody(None))

          case StepState.SimpleStep.RetryPending(bookkeeping, deadline) =>
            // A fresh instant, not the `now` read at the top of runSimpleStep: the
            // wakeup that resumed this run is produced against the sweeper's
            // clock, which can be ahead of this JVM's, and a stale comparison
            // would re-suspend an already-due retry that will never be woken again.
            if (!deadline.isAfter(internals.clock.instant()))
              Some(runBody(Some(RetryState.decode(bookkeeping.raw))))
            else
              throw new WorkflowSuspendedException
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
    *   unversioned step kinds)
    */
  def getExecutionState[A](key: String, stepVersion: Long = 0)(using
      ctx: WorkflowContext,
      valueCodec: Cacheable[A],
      throwableCodec: Cacheable[Throwable]
  ): StepExecutionState[A] = {
    val stepId = StepId(key, ctx.currentScope)
    ctx.internals.readStep(ctx.instanceId, stepId, stepVersion) match {
      case None => StepExecutionState.NeverStarted
      case Some(row) =>
        expectStepState[StepState.SimpleStep](key, row) match {
          case StepState.SimpleStep.Succeeded(payload) =>
            StepExecutionState.Completed(decodeStored(valueCodec, key)(payload.raw))
          case StepState.SimpleStep.Failed(payload) =>
            StepExecutionState.Failed(decodeStoredFailure(throwableCodec, key)(payload.raw))
          case StepState.SimpleStep.Started | StepState.SimpleStep.RetryPending(_, _) =>
            StepExecutionState.Started
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
    awaitRaceImpl(stepKey, Seq(("", awaitable)), invalidateOn, ensureUnchanged, invalidateAfter)

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
    require(branches.nonEmpty, s"awaitRace('$stepKey') requires at least one branch")
    validateBranchKeys(stepKey, branches.map(_._1))
    awaitRaceImpl(stepKey, branches.toVector, invalidateOn, ensureUnchanged, Duration.Inf)
  }

  /** Validates user-supplied race branch keys (an [[awaitRace]] member key or a
    * [[firstToRunWithoutSuspension]] branch key). The `""` key is reserved for
    * single-subscriber sites (plain awaits), and the `"__"` prefix for
    * engine-internal subscribers (such as step retries).
    *
    * A `firstToRun` branch key is also a scope-path segment (each branch runs
    * one scope level below the race's key), so it must satisfy the key rules of
    * [[ScopePath.requireValidKey]].
    */
  private def validateBranchKeys(stepKey: String, branchKeys: Seq[String], keysArePathSegments: Boolean = false): Unit = {
    branchKeys.foreach { branchKey =>
      require(branchKey.nonEmpty, s"'$stepKey' branch keys must be non-empty")
      require(
        !branchKey.startsWith("__"),
        s"'$stepKey' branch key '$branchKey' uses the reserved '__' prefix"
      )
      if keysArePathSegments then ScopePath.requireValidKey(s"'$stepKey' branch key", branchKey)
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
    ctx.internals.readAwaitSignalCandidates(signal.key)(using ctx.currentExecution).map { candidate =>
      tryOrThrowStepSerializationFailed(s"Signal '${signal.key}' payload could not be decoded")(signal.cacheable.read(candidate.payload.raw))
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
    *   the step's stable identity within the workflow
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
    *   no side effects and the body runs only when the race applies it with
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

  private def evaluateFirstCompletedBranch[R](
      stepKey: String,
      invalidateOn: Seq[StepInput[?]],
      ensureUnchanged: Seq[StepInput[?]],
      branches: Vector[(String, () => WorkflowContext ?=> R)]
  )(using ctx: WorkflowContext, valueCodec: Cacheable[R], throwableCodec: Cacheable[Throwable]): R = {
    require(branches.nonEmpty, s"firstToRunWithoutSuspension('$stepKey') requires at least one branch")
    validateBranchKeys(stepKey, branches.map(_._1), keysArePathSegments = true)
    ScopePath.requireValidKey("step key", stepKey)
    val internals: ctx.internals.type = ctx.internals
    given ctx.internals.Lease = ctx.currentExecution
    val scopedStepId = StepId(stepKey, ctx.currentScope)
    val fingerprints = encodeFingerprints(ensureUnchanged ++ invalidateOn)
    val now = internals.clock.instant()

    // Each race branch runs one scope level below the race's key, so sibling
    // branches of the same race cannot collide.
    def branchPath(base: ScopePath, key: String): ScopePath =
      base.appended(scopedStepId.key).appended(key)

    def evaluate(): R = {
      ctx.throwIfCancelled()
      val branchPaths = branches.map((key, _) => branchPath(ctx.currentScope, key))
      val branchContexts = branches.map((key, _) =>
        ctx.copy(scopePath = ctx.currentScope.appended(scopedStepId.key).appended(key)))
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
        val (serialized, decoded) = encodeAndDecode(valueCodec, stepKey)(winnerResult)
        // Delete-first: retire every branch's subscriptions (including nested
        // subscopes) before persisting the winner. The winner row is the durable
        // first-wins marker, so a crash in between leaves no marker and the next
        // run re-evaluates from scratch and re-deletes.
        internals.deleteSubscriptionsUnderScopePaths(branchPaths)
        internals.writeStep(
          scopedStepId, 0L,
          StoredStep(
            StepState.FirstToRun(SerializedValue.of(serialized)),
            InputFingerprints.of(fingerprints),
            None
          )
        )
        decoded
      }
    }

    loadStepStateOrDeleteIfInvalid[StepState.FirstToRun](stepKey, scopedStepId, stepVersion = 0L, now, ensureUnchanged, invalidateOn) match {
      case Some(StepState.FirstToRun(result)) => decodeStored(valueCodec, stepKey)(result.raw)
      case None                               => evaluate()
    }
  }

  /** Reads the step row stored at `stepId` and returns it only when it is still
    * replayable: not expired, with no changed `invalidateOn` input, and with the
    * `ensureUnchanged` pin satisfied. A row of a different step kind fails the
    * run with [[StepKeyConflictException]] before any invalidation logic could
    * delete it. A present but non-replayable row is deleted (together with the
    * site's subscriptions) so the site re-evaluates from scratch.
    */
  private def loadStepStateOrDeleteIfInvalid[S <: StepState](
      stepKey: String,
      stepId: StepId,
      stepVersion: Long,
      now: java.time.Instant,
      ensureUnchanged: Seq[StepInput[?]],
      invalidateOn: Seq[StepInput[?]]
  )(using ctx: WorkflowContext, tt: TypeTest[StepState, S])(using ctx.internals.Lease): Option[S] = {
    val internals: ctx.internals.type = ctx.internals
    val rawExisting = internals.readStep(ctx.instanceId, stepId, stepVersion)
    // Reject a row of a different step kind before invalidation logic could delete it.
    rawExisting.foreach(expectStepState[S](stepKey, _))
    val replayable =
      rawExisting.filterNot(row => isExpired(row, now) || haveInputsChanged(stepKey, row, ensureUnchanged, invalidateOn))

    if (rawExisting.isDefined && replayable.isEmpty)
      internals.deleteSteps(Seq((stepId = stepId, version = stepVersion)))

    replayable.map(expectStepState[S](stepKey, _))
  }

  /** The one evaluation path for every runtime-computed await: a plain signal,
    * timer, or workflow-completion await, and a multi-member `awaitRace`. A
    * plain await is a single-member race carrying the reserved `""` subscriber
    * key.
    *
    * An existing non-expired row replays; `ensureUnchanged` conflicts throw; an
    * `invalidateOn` change or an expired row discards the site (`deleteStep`:
    * its step row plus every subscription) and re-evaluates from scratch, so no
    * stale registration can satisfy the new registration.
    */
  private def awaitRaceImpl[A](
      stepKey: String,
      branches: Seq[(String, Awaitable[A])],
      invalidateOn: Seq[StepInput[?]],
      ensureUnchanged: Seq[StepInput[?]],
      invalidateAfter: Duration
  )(using ctx: WorkflowContext, valueCodec: Cacheable[A], throwableCodec: Cacheable[Throwable]): A = {
    ScopePath.requireValidKey("step key", stepKey)
    val internals: ctx.internals.type = ctx.internals
    given ctx.internals.Lease = ctx.currentExecution
    val stepId = StepId(stepKey, ctx.currentScope)
    val fingerprints = encodeFingerprints(ensureUnchanged ++ invalidateOn)
    val now = internals.clock.instant()
    val expiresAt = expiryOf(invalidateAfter, now)

    val raceBranches = branches.map((branchKey, a) => branchOf(branchKey, a, now)).toVector

    loadStepStateOrDeleteIfInvalid[StepState.Await](stepKey, stepId, 0L, now, ensureUnchanged, invalidateOn) match {
      case Some(StepState.Await(payload)) =>
        decodeStored(valueCodec, stepKey)(payload.raw)

      case None =>
        ctx.throwIfCancelled()
        val waitSite = WaitSite(stepId, 0L, InputFingerprints.of(fingerprints), expiresAt)
        internals.evaluateWait(waitSite, raceBranches.map(_.subscriber))(pickRaceWinner(valueCodec, raceBranches)) match {
          case Some(payload) => decodeStored(valueCodec, stepKey)(payload.raw)
          case None          => throw new WorkflowSuspendedException
        }
    }
  }

  /** One race branch: the subscriber registered for it plus a pure function
    * deciding, for each raw candidate handed to the branch, the branch's value
    * (`None` when the candidate is rejected — outside the look-back window or
    * failing the signal filter).
    */
  private final case class Branch[A](subscriber: Subscriber, accept: SubscriberMatch => Option[A]) {
    def map[B](f: A => B): Branch[B] = Branch(subscriber, accept(_).map(f))
  }

  /** Builds the branch for one `key -> awaitable` member of a race (a plain
    * await is a single-member race). `Mapped` awaitables flatten recursively
    * onto their base (`SignalEvent`, `Timer`, or `WorkflowCompletion`),
    * composing the transforms.
    */
  private def branchOf[A](branchKey: String, awaitable: Awaitable[A], now: Instant): Branch[A] =
    awaitable match {
      case Awaitable.SignalEvent(signal, filter, lookBack) =>
        val cutoff = lookBack match {
          case d: FiniteDuration => Some(now.minus(java.time.Duration.ofNanos(d.toNanos)))
          case _                 => None
        }
        Branch(
          Subscriber.Signal(branchKey, signal.key),
          c =>
            if (cutoff.exists(c.createdAt.isBefore)) None
            else
              Some(tryOrThrowStepSerializationFailed(s"Signal '${signal.key}' payload could not be decoded")(
                signal.cacheable.read(c.payload.raw)
              )).filter(filter)
        )

      case Awaitable.Timer(deadline) =>
        Branch(Subscriber.Timer(branchKey, deadline), _ => Some(()))

      case wc @ Awaitable.WorkflowCompletion(instanceId) =>
        Branch(
          Subscriber.Completion(
            branchKey,
            instanceId.workflowId,
            instanceId.workflowInstanceKey,
            instanceId.scope
          ),
          c =>
            Some(tryOrThrowStepSerializationFailed(s"Subscriber '$branchKey' completion could not be decoded")(
              wc.completionCacheable.read(c.payload.raw)
            ))
        )

      case Awaitable.Mapped(underlying, f) =>
        branchOf(branchKey, underlying, now).map(f)
    }

  /** Selects the earliest satisfying candidate across every branch by global
    * `sequenceId`: candidates arrive sorted by `sequenceId`, so the first
    * candidate accepted by its branch wins, and only the winner's value is
    * serialized. Only a signal branch reports the cursor key to advance.
    */
  private def pickRaceWinner[A](
      valueCodec: Cacheable[A],
      branches: Vector[Branch[A]]
  ): Vector[SubscriberMatch] => Option[WaitResolution] = {
    val branchesByKey = branches.map(b => b.subscriber.key -> b).toMap
    candidates =>
      candidates.iterator
        .flatMap(c => branchesByKey.get(c.subscriberKey).flatMap(_.accept(c)).map(c -> _))
        .nextOption()
        .map { case (candidate, value) =>
          WaitResolution(
            SerializedValue.of(tryOrThrowStepSerializationFailed(s"Subscriber '${candidate.subscriberKey}' result could not be encoded")(
              valueCodec.write(value)
            )),
            branchesByKey(candidate.subscriberKey).subscriber match {
              case Subscriber.Signal(_, key) => Some((key, candidate.sequenceId))
              case _                         => None
            }
          )
        }
  }

  /** The exceptions that may become a Step's durable outcome: ordinary failures,
    * including a user-thrown `ControlThrowable`, which is not library control
    * flow ([[WorkflowControlFlowException]] is sealed) and is cached like any
    * other application failure. Workflow and Step functions callable inside a
    * Step body (`Step.await`, `Workflow.continueAsNew`, `Workflow.heartbeat`,
    * restartable-region control) throw the runtime's own exceptions instead, and
    * those are excluded here: fatal JVM errors, the sealed control-flow
    * exceptions, lease loss, and cancellation abort the run without durable step
    * state rather than being persisted and retried as a Step outcome.
    *
    * The cacheable set is `WorkflowNonFatal`'s matches plus user-thrown
    * `ControlThrowable`; the runtime's own are its complement plus lease loss
    * and cancellation, which `WorkflowNonFatal` deliberately matches so user
    * cleanup can run.
    */
  private object CacheableStepException {
    def unapply(t: Throwable): Option[Throwable] = t match {
      case _: VirtualMachineError | _: LinkageError | _: WorkflowControlFlowException
           | _: LeaseLostException | _: WorkflowCancelledException => None
      case _ => Some(t)
    }
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

  /** Evaluates `value`, converting any failure into a [[StepSerializationFailed]]
    * carrying `message`.
    */
  private def tryOrThrowStepSerializationFailed[A](message: => String)(value: => A): A =
    try value
    catch { case _: Throwable => throw new StepSerializationFailed(message) }

  private def encodeStored[A](codec: Cacheable[A], key: String)(value: A): String =
    tryOrThrowStepSerializationFailed(s"Step '$key' result could not be encoded")(codec.write(value))

  private def decodeStored[A](codec: Cacheable[A], key: String)(payload: String): A =
    tryOrThrowStepSerializationFailed(s"Step '$key' result could not be decoded")(codec.read(payload))

  private def decodeStoredFailure(codec: Cacheable[Throwable], key: String)(payload: String): Throwable =
    tryOrThrowStepSerializationFailed(s"Step '$key' failure could not be decoded")(codec.read(payload))

  /** How a decoded step failure is rethrown: codecs with the
    * [[Cacheable.StepFailurePresenter]] capability wrap it into a
    * [[StepFailed]] naming the failing step; other codecs rethrow it unchanged,
    * preserving its original class.
    */
  private def presentStepFailure(stepId: StepId, decoded: Throwable)(using codec: Cacheable[Throwable]): Throwable =
    codec match {
      case presenter: Cacheable.StepFailurePresenter => presenter.presentStepFailure(stepId, decoded)
      case _                                         => decoded
    }

  /** Enforces the `ensureUnchanged` pin (throwing [[StepInputConflictException]]
    * when a pinned input changed) and reports whether any `invalidateOn` input
    * differs from what the stored row recorded. Shared by steps, first-to-run
    * races, and cached awaits.
    */
  private def haveInputsChanged(
      key: String,
      row: StoredStep,
      ensureUnchanged: Seq[StepInput[?]],
      invalidateOn: Seq[StepInput[?]]
  ): Boolean = {
    val stored = parseFingerprints(row.inputFingerprints.raw)
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

  /** A step key belongs to exactly one step kind: the row stored at a
    * step's key must have been written by the step kind that reads it. Anything
    * else means the key was reused for a different step kind (a definition
    * change across workflow versions), which fails the run instead of reading,
    * deleting, or overwriting the foreign row.
    */
  private def expectStepState[S <: StepState](stepKey: String, row: StoredStep)(using TypeTest[StepState, S]): S = {
    row.state match
      case s: S => s

      case _ =>
        throw new StepKeyConflictException(
          s"Step key '$stepKey' is already stored as a '${row.state.rowKindText}' row and cannot be reused for a different step kind; a step key belongs to exactly one step kind"
        )
  }

  private def expiryOf(invalidateAfter: Duration, now: java.time.Instant): Option[java.time.Instant] =
    invalidateAfter match {
      case d: FiniteDuration => Some(now.plus(java.time.Duration.ofNanos(d.toNanos)))
      case _                 => None
    }

  /** Retry bookkeeping carried in a `RetryPending` step row while an
    * at-least-once step waits for a durable retry deadline. Encoded as
    * `failedAttempts:cumulativeNanos:lastNanos` (`-1` for no last delay).
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
    val initial: RetryState = RetryState(0, 0.seconds, None)

    def encode(state: RetryState): String =
      s"${state.failedAttempts}:${state.cumulativeDelay.toNanos}:${state.lastDelay.fold(-1L)(_.toNanos)}"

    def decode(payload: String): RetryState = {
      val parts = payload.split(":")
      if (parts.length != 3)
        throw new StepSerializationFailed(s"Stored retry bookkeeping could not be decoded: '$payload'")
      val lastDelay =
        if (parts(2).toLong == -1L) None
        else Some(FiniteDuration(parts(2).toLong, NANOSECONDS))
      RetryState(parts(0).toInt, FiniteDuration(parts(1).toLong, NANOSECONDS), lastDelay)
    }
  }

  private def hex(bytes: Array[Byte]): String = bytes.map(b => f"${b & 0xff}%02x").mkString

  private def hexDecode(hex: String): Array[Byte] =
    hex.grouped(2).map(h => Integer.parseInt(h, 16).toByte).toArray
}
