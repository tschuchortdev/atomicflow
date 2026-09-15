# Atomicflow Postgres Runtime — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Implement the spec-conformant atomicflow runtime on PostgreSQL (per `spec/`), replacing the deleted prototype.

**Architecture:** Public API + run engine seam in `core`, Postgres implementation (doobie + Flyway) in `db`. Tests run against a real Postgres via testcontainers. All events share one global log; execution uses leases with fencing tokens; wakeups are the single scheduling queue.

**Tech Stack:** Scala 3.7.3, sbt, doobie, Flyway, testcontainers-scala (java), munit, upickle, ox, slf4j/logback.

**Spec:** `spec/*.md` (all sections). The spec is the binding authority; `spec/design.md` is a wishlist — ignore points not in the other files.

## Global Constraints

- **Build environment (JDK 21 required; default `java` on PATH is 11!):**
  ```
  export JAVA_HOME=/Users/thilo/Library/Java/JavaVirtualMachines/corretto-21.0.3/Contents/Home
  export PATH="$JAVA_HOME/bin:$PATH"
  ```
- Test command: `sbt -batch test` (or scoped: `sbt -batch "db/test"`). Docker must be running (testcontainers).
- **TDD is mandatory**: write failing test first, watch it fail, implement minimal, watch it pass.
- Metrics are out of scope. Logging uses slf4j (`slf4j-api`) in library code; logback-classic is the test/backend dependency.
- Public API is what user workflows need; everything else is `private[atomicflow]`.
- Keep tests small; more tests for essential/difficult behavior, fewer for nice-to-haves.
- Every deviation from `spec/` gets recorded in `DEVIATIONS.md` (repo root) with a reason.
- Do not modify branches `example`, `master`, `main`, `before_impl`. Work happens on `impl`.
- Pinned public API (Phase 1, already implemented in `core/src/main/scala/atomicflow/`): `Cacheable` (String-based, `stableSerializedTypeId`/`write`/`read`, `imap`, `withFallback`, `unionMostSpecific`, `forThrowable`, `Simple`/`Json` givens), `Fingerprintable`, `StepInput` + `Conversion[(String, A), StepInput[A]]`, identity types in `Types.scala` (`WorkflowId`/`WorkflowInstanceKey`/`SignalKey` aliases, `WorkflowInstanceId(workflowId, key, scope="")`, `StepId(key, scope="")`, `WorkflowTerminalState`, `WorkflowRunResult`), `WorkflowCompletionResult` (+ composite Cacheable given), `Exceptions.scala` (`StepFailed`, `WorkflowCancelledException`), `ControlFlow.scala` (`WorkflowControlException`, `WorkflowSuspendedException`, `ContinueAsNewException`, `WorkflowNonFatal`), `internal/Framing`, `internal/ScopePath` (escape/join/shorten, `MaxEncodedLength=512`).

---

## Phase 2 — Postgres run engine (no awaits yet)

Phase 2 implements workflow instances, the execution lease with fencing, the step engine
(atLeastOnce/atMostOnce with drift policies), terminal outcomes, and instance queries.
Signals/timers/awaits/wakeups-consumers come in Phase 3. Retries (`RetryPolicy`) are deferred
to Phase 3 (they require timer subscriptions); the `retry` parameter is added then.

### Task 2.1: Schema, runtime factory, Workflow definition, instance creation

**Files:**
- Create: `db/src/main/resources/db/migration/V001__init.sql`
- Create: `db/src/main/scala/atomicflow/impl/db/PostgresWorkflowRuntime.scala`
- Create: `core/src/main/scala/atomicflow/Workflow.scala`
- Create: `core/src/main/scala/atomicflow/WorkflowRuntime.scala`
- Create: `db/src/test/scala/test/PostgresWorkflowRuntimeSuite.scala` (shared harness)
- Test: `db/src/test/scala/test/CreationSuite.scala`

**Interfaces:**
- Produces: `object PostgresWorkflowRuntime { def apply(ds: DataSource)(using ExecutionContext): PostgresWorkflowRuntime }` — runs Flyway migrations, then constructs. Also `def apply(ds: DataSource, clock: Clock)(using ExecutionContext)` (spec: runtime takes a `clock` parameter, default `Clock.systemUTC()`).
- Produces (core): `final class Workflow[In, Out]` with `id: WorkflowId`, `version: Long`, `name: String`, `description: Option[String]`, body, and BOTH codecs (`Cacheable[In]`, `Cacheable[Out]`) captured from its construction context bounds (spec core-types: "body, codecs"; re-run determinism decodes with "the definition's Cacheable[Out]").
  ```scala
  object Workflow:
    def apply[In: Cacheable, Out: Cacheable](
        id: WorkflowId,
        version: Long = 1L,
        name: String = "",
        description: Option[String] = None
    )(
        body: In => WorkflowContext ?=> Out,
        onUnconsumedSignals: Map[SignalKey, Seq[Any]] => Unit = _ => ()
    ): Workflow[In, Out]
  ```
- Produces (core): `trait WorkflowRuntime` with Phase-2 abstract primitives and final convenience methods:
  ```scala
  @implicitNotFound("No WorkflowRuntime available. Add a using clause (using WorkflowRuntime).")
  trait WorkflowRuntime:
    // abstract primitives (implemented per backend)
    @throws[WorkflowInputConflictException]
    def createWorkflowInstance[In, Out](workflow: Workflow[In, Out], instanceKey: WorkflowInstanceKey, in: In)(using Cacheable[In]): WorkflowInstance[In, Out]
    def createWorkflowInstanceDiscardExisting[In, Out](workflow: Workflow[In, Out], instanceKey: WorkflowInstanceKey, in: In)(using Cacheable[In]): Boolean
    @throws[WorkflowNotFoundException]
    def runWorkflowInstance[In, Out](workflow: Workflow[In, Out], instanceId: WorkflowInstanceId)(using Cacheable[Throwable]): WorkflowRunResult[Out]
    private[atomicflow] def upsertWakeup(instanceId: WorkflowInstanceId, delay: FiniteDuration): Unit  // backend derives scheduled_at = clock.now + delay (single time source)
    // final convenience (written once, built from primitives)
    final def createAndSchedule[In, Out](workflow: Workflow[In, Out], instanceKey: WorkflowInstanceKey, in: In)(using Cacheable[In]): WorkflowInstance[In, Out]  // create + upsertWakeup(Duration.ZERO)
    final def createAndRun[In, Out](workflow: Workflow[In, Out], instanceKey: WorkflowInstanceKey, in: In)(using Cacheable[Throwable]): WorkflowRunResult[Out]
    def getWorkflowInstance[In, Out](workflow: Workflow[In, Out], instanceId: WorkflowInstanceId): WorkflowInstance[In, Out]  // stub returning handle; Task 2.6 completes validation
    // Task 2.6 adds queries + awaitResult
  ```
  `object WorkflowRuntime { def apply(using runtime: WorkflowRuntime): WorkflowRuntime = runtime }`.
- Produces (core): `Workflow.create(instanceKey, in)(using WorkflowRuntime): WorkflowInstance[In, Out]`, `createAndSchedule(instanceKey, in)(using WorkflowRuntime)`, `run(instanceKey)(using WorkflowRuntime, Cacheable[Throwable]): WorkflowRunResult[Out]`, `createAndRun` — one-line forwarders per spec "Core operations". All codecs except `Cacheable[Throwable]` come from the definition (no using bounds for them). `WorkflowInstance` handle may be minimal in this task (`id` + the workflow); its runtime methods arrive in later tasks.

**Schema (V001__init.sql) — implements `spec/signals-timers.md` "Persistence model" completely:**
- `workflow_instances`: PK `(workflow_id, key, scope)`; columns: `input TEXT NOT NULL`, `workflow_version_at_creation BIGINT NOT NULL`, `generation BIGINT NOT NULL DEFAULT 0`, `created_at/updated_at TIMESTAMPTZ NOT NULL DEFAULT now()`, `last_run_at TIMESTAMPTZ`, `times_executed INT NOT NULL DEFAULT 0`, `terminal_state TEXT` (`completed|failed|cancelled|terminated`, NULL before), `terminal_outcome TEXT`, `cancel_requested_at TIMESTAMPTZ`, `lease_owner TEXT`, `fencing_token BIGINT NOT NULL DEFAULT 0`, `lease_expires_at TIMESTAMPTZ`, `is_accepting_signals BOOLEAN NOT NULL DEFAULT true`, parent/inheritance columns for Phase 6 (`parent_workflow_id TEXT`, `parent_instance_key TEXT`, `parent_scope TEXT` — all NULL for top-level; `inherit_signals TEXT`, `inherit_past_events BOOLEAN NOT NULL DEFAULT false`, `inherited_events_start_sequence_id BIGINT`).
- `workflow_events`: `sequence_id BIGINT PRIMARY KEY`, `event_kind TEXT NOT NULL`, `workflow_id/key/scope TEXT NOT NULL`, `event_key TEXT NOT NULL` (empty string for WorkflowCompleted), `payload TEXT NOT NULL`, `created_at TIMESTAMPTZ NOT NULL DEFAULT now()`. Sequence `workflow_event_sequence AS BIGINT CACHE 1`. Partial unique index on `(workflow_id, key, scope, event_key) WHERE event_kind = 'TimerFired'` (event kind VALUES are CamelCase per the spec's envelope table: `Signal`, `TimerFired`, `WorkflowCompleted`). Index `(event_kind, event_key, workflow_id, key, scope, sequence_id)`.
- `workflow_steps`: natural key `(workflow_id, key, scope, step_id, step_version)`; `step_kind TEXT NOT NULL`, `state_kind TEXT NOT NULL` (`started|succeeded|failed`), `state_payload TEXT NOT NULL`, `input_fingerprints TEXT NOT NULL`, `expires_at TIMESTAMPTZ`, `created_at/updated_at TIMESTAMPTZ NOT NULL DEFAULT now()`. FK to instances `ON DELETE CASCADE`.
- `signal_cursor` `(workflow_id, key, scope, signal_key)` PK + `sequence_id BIGINT NOT NULL`, FK cascade.
- `workflow_signal_subscriptions` `(workflow_id, key, scope, step_id, step_version, leaf_idx, signal_key)` PK, FK cascade.
- `workflow_timer_subscriptions` `(subscription_id UUID PRIMARY KEY, workflow_id, key, scope, step_id, step_version, leaf_idx, deadline TIMESTAMPTZ)`, FK cascade, index on `deadline`.
- `workflow_completion_subscriptions` `(workflow_id, key, scope, step_id, step_version, leaf_idx, completed_workflow_id, completed_key, completed_scope)` PK, FK cascade.
- `workflow_wakeups` `(workflow_id, key, scope)` PK, `created_at TIMESTAMPTZ NOT NULL`, `scheduled_at TIMESTAMPTZ NOT NULL`, `attempts INT NOT NULL DEFAULT 0`, FK cascade.

**Behavior (tests):**
- `create` inserts the instance row (idempotent for equal input — compare serialized input; `WorkflowInputConflictException` on different input), top-level scope `""`.
- `createWorkflowInstanceDiscardExisting` deletes and re-creates; returns whether an existing instance was discarded.
- `createAndSchedule` = create + wakeup upsert (`ON CONFLICT DO NOTHING`, never resets existing timestamps).
- Instance row stores `workflow_version_at_creation`; duplicate `workflowId` registration is impossible (runtime has no registry — validation happens later at runner start, Phase 5).

### Task 2.2: Run engine — lease, body execution, terminal outcome

**Files:**
- Modify: `db/src/main/scala/atomicflow/impl/db/PostgresWorkflowRuntime.scala`
- Create: `core/src/main/scala/atomicflow/WorkflowContext.scala` (trait: `instanceId`, `versionAtCreation`, `runtime`, `private[atomicflow] execution`)
- Create: `core/src/main/scala/atomicflow/internal/WorkflowExecution.scala` (private[atomicflow] engine seam — design freely, must serve Step/await needs)
- Create: `core/src/main/scala/atomicflow/WorkflowInstance.scala` (handle + `Info`)
- Test: `db/src/test/scala/test/RunEngineSuite.scala`

**Behavior (tests, per spec `running-workflows.md`):**
- `run` on unknown instance → `WorkflowNotFoundException`.
- Workflow without steps: `createAndRun` → `WorkflowRunResult.Result(out)`; instance terminal `completed`; `terminal_outcome` decodes to `Completed(out)`.
- Body throwing → exception propagates from run; instance terminal `failed`; re-run throws the decoded failure (re-run determinism); `run` on terminal instance never executes the body (verify via side-effect counter).
- Failing createAndRun: input already persisted (run after failed... — input conflict checked at create).
- Lease: while held (unreleased), second `run` waits up to `leaseAcquireTimeout` then throws `LeaseUnavailableException` (name pinned; "runtime-owned lease-unavailable exception"). Success path releases the lease (a second run starts immediately).
- Exactly one terminal transition: `run` twice concurrently? — simplified single-threaded test: terminal state written once (guarded update `WHERE terminal_state IS NULL`); the `WorkflowCompleted` event is appended in the same transaction.
- `last_run_at`/`times_executed` maintained.

### Task 2.3: Step engine — atLeastOnce

**Files:**
- Create: `core/src/main/scala/atomicflow/Step.scala`
- Create: `core/src/main/scala/atomicflow/StepExecutionState.scala`
- Modify: engine seam as needed
- Test: `db/src/test/scala/test/StepAtLeastOnceSuite.scala`

**Interfaces:**
```scala
object Step:
  def atLeastOnce[A: Cacheable](
      key: String,
      version: Long = 1L,
      ensureUnchanged: Seq[StepInput[?]] = Seq.empty,
      invalidateOn: Seq[StepInput[?]] = Seq.empty,
      invalidateAfter: Duration = Duration.Inf
  )(body: => A)(using WorkflowContext, Cacheable[Throwable]): A

  def getExecutionState[A: Cacheable](key: String, stepVersion: Long = 0)(using WorkflowContext, Cacheable[Throwable]): StepExecutionState[A]

enum StepExecutionState[+A]:
  case NeverStarted
  case Started
  case Failed(failure: Throwable)
  case Completed(value: A)
```
(`atMostOnce` and `retry` come in Task 2.4 / Phase 3. Named parameters are mandatory — no positional drift-policy args. Step scope comes from the context (Phase 6 adds `Workflow.scoped`).)

**Behavior (tests, per `spec/steps.md`):**
- Step body executes once; replay (second run of a suspended-free workflow via re-`run`... use a workflow that completes, then re-run: body NOT re-executed; result from cache; side-effect counter stays 1).
- `Started` row persisted before body executes (visible mid-body via SQL, or via getExecutionState after simulating crash).
- Success: serialize→deserialize→persist `Succeeded`→return decoded value (commit-before-observation: identity not preserved — assert decoded value equality only).
- Failure: `Failed` row persisted; decoded failure thrown on first run too (e.g. `StepFailed` from generic throwable codec); re-run replays the same failure without executing the body.
- `getExecutionState`: NeverStarted (no row), Started (unresolved), Failed, Completed (decoded with `Cacheable[A]`).
- `version` bump creates new work (new natural key; body re-executes).
- Fenced: a stale step write (token changed) affects 0 rows → `LeaseLostException`... (test via SQL token bump mid-run? — via a body that bumps `fencing_token` through SQL then continues; step persist must then fail with LeaseLost). Design the test with the suite's SQL helper.

### Task 2.4: atMostOnce + drift policies

**Files:**
- Modify: `core/src/main/scala/atomicflow/Step.scala`
- Test: `db/src/test/scala/test/StepGuaranteesSuite.scala`

**Interfaces:**
```scala
def atMostOnce[A: Cacheable](
    key: String,
    ensureUnchanged: Seq[StepInput[?]] = Seq.empty,
    invalidateOn: Seq[StepInput[?]] = Seq.empty,
    invalidateAfter: Duration = Duration.Inf
)(body: => A)(using WorkflowContext, Cacheable[Throwable]): Option[A]
```

**Behavior (tests):**
- atMostOnce happy path returns `Some(value)`, replay returns `Some(cached)`.
- Unresolved `Started` on replay → `None` without re-executing (simulate crash: delete the Succeeded row via SQL, leaving Started).
- `ensureUnchanged` value differs on re-run → `StepInputConflictException` (workflow blocked).
- `invalidateOn` value differs → cached result discarded, body re-executes.
- `invalidateAfter` TTL: cached result expired (use runtime `clock` — construct runtime with a mutable test clock, advance it, re-run → body re-executes).
- No explicit keys → instance id is the sole cache key (same instance re-runs reuse cache; a different instance re-executes).
- atMostOnce has no version parameter (compile-level: API shape).

### Task 2.5: Lease renewal, heartbeat, fenced writes

**Files:**
- Create: `core/src/main/scala/atomicflow/WorkflowCompanionOps.scala`? No — extend `object Workflow` in `Workflow.scala` with `heartbeat`.
- Modify: runtime/engine.
- Test: `db/src/test/scala/test/LeaseSuite.scala`

**Interfaces:**
```scala
object Workflow:
  /** Renews the execution lease of the instance executing on the current thread.
    * @throws LeaseLostException when the lease was taken over or the instance is terminal */
  def heartbeat()(using WorkflowContext): Unit
  def versionAtCreation(using WorkflowContext): Long
```

**Behavior (tests, per "The execution lease"):**
- `Workflow.heartbeat` inside a step body extends `lease_expires_at` (observe via SQL before/after).
- Heartbeat after token bump (SQL-simulated takeover) → `LeaseLostException`.
- Any fenced write after takeover → `LeaseLostException` (e.g. bump token mid-body, then step persistence fails).
- Expired lease is acquirable by an external run (SQL: set `lease_expires_at` in the past with an owner; then `run` acquires, bumps `fencing_token`, executes).
- `LeaseLostException` and `LeaseUnavailableException` are public RuntimeExceptions; `WorkflowNonFatal` matches both.

### Task 2.6: Handles, Info, queries, awaitResult

**Files:**
- Modify/complete: `WorkflowInstance.scala`, `WorkflowRuntime.scala`, `PostgresWorkflowRuntime.scala`
- Test: `db/src/test/scala/test/QueriesSuite.scala`

**Interfaces:**
```scala
trait WorkflowRuntime:
  def getWorkflowInstance[In, Out](workflow: Workflow[In, Out], instanceId: WorkflowInstanceId): WorkflowInstance[In, Out]  // validates workflowId, throws on mismatch
  def getWorkflowInstancesByPrefix(workflowId: WorkflowId, keyPrefix: WorkflowInstanceKey, scope: String = ""): Vector[WorkflowInstance.Info]
  def getUnfinishedWorkflowInstances(workflowId: WorkflowId, includeWaiting: Boolean = false, limit: Int = -1): Vector[WorkflowInstance.Info]
  def deleteWorkflowInstancesByPrefix(workflowId: WorkflowId, keyPrefix: WorkflowInstanceKey, scope: String = ""): Long
  def awaitResult[Out](instance: WorkflowInstance[?, Out], timeout: FiniteDuration)(using Cacheable[Throwable]): WorkflowRunResult[Out]  // passive poller

final class WorkflowInstance[In, Out](workflow, instanceId):
  def id: WorkflowInstanceId
  def run()(using WorkflowRuntime, Cacheable[Throwable]): WorkflowRunResult[Out]
  def awaitResult(timeout: FiniteDuration)(using WorkflowRuntime, Cacheable[Throwable]): WorkflowRunResult[Out]
  def getInfo()(using WorkflowRuntime): WorkflowInstance.Info

object WorkflowInstance:
  final case class Info(
    id: WorkflowInstanceId, parentId: Option[WorkflowInstanceId], generation: Long,
    terminalState: Option[WorkflowTerminalState], workflowVersionAtCreation: Long,
    createdAt: Instant, lastRunAt: Option[Instant], timesExecuted: Int
  )
```

**Behavior (tests):**
- `getWorkflowInstance` validates workflowId (mismatch → exception; type-safe handle returned).
- Prefix queries are scoped (`scope=""` → top-level only), unfinished includes CREATED instances (from `create` without run), `limit` respected.
- `deleteWorkflowInstancesByPrefix` removes instance + cascade (steps, events).
- `awaitResult` returns immediately for terminal instances; blocks until timeout for suspended/non-terminal (then throws a timeout exception — pinned: `java.util.concurrent.TimeoutException`); passive: never executes the workflow.
- `Info` fields match row state.

## Phase 3 — Signals, timers, awaits, event log, wakeups

Implements `spec/signals-timers.md` end-to-end for the Postgres runtime (manual test mode:
caller threads drive everything). All event appends use the global protocol
(`pg_advisory_xact_lock(EventAppendLockKey)` + `nextval('workflow_event_sequence')`).

### Task 3.1: Event append primitive, Signal type, send, TestClock

**Files:**
- Modify: `core/.../Types.scala` (SignalSendResult), `core/.../WorkflowRuntime.scala`, `db/.../PostgresWorkflowRuntime.scala`
- Create: `core/src/main/scala/atomicflow/Signal.scala`, `core/src/main/scala/atomicflow/TestClock.scala`
- Test: `db/src/test/scala/test/SignalSuite.scala`

**Interfaces:**
```scala
final class Signal[A] private (val key: SignalKey)(using val cacheable: Cacheable[A])
object Signal { def apply[A: Cacheable](key: SignalKey): Signal[A] }
// on Signal, WorkflowInstanceId, WorkflowInstance (forwarders) and trait primitive:
@throws[WorkflowNotFoundException]
def sendSignal[A: Cacheable](workflowInstanceId: WorkflowInstanceId, key: SignalKey, value: A): SignalSendResult
trait WorkflowRuntime { def clock: Clock }  // exposed so apps/tests share one time source
final class TestClock(start: Instant) extends Clock { def advanceBy(d: FiniteDuration): Unit; ... }
```

**Behavior (tests, per `signals-timers.md` "Basic Signal API" + "Event storage"):**
- send appends one `Signal` event (row lock on the instance + `is_accepting_signals` check → `InstanceAlreadyCompleted` for terminal instances; `WorkflowNotFoundException` for missing instances) and upserts a wakeup for the instance when a matching pending subscription exists (subscription check returns empty in this task — exercised by 3.2).
- Sequence IDs strictly increase; concurrent-ish appends (sequential here) commit-ordered.
- Events are immutable; payload serialized with the signal's `Cacheable[A]`.

### Task 3.2: `Awaitable` + `Step.await` for signals (cursors, filters, lookBack, subscriptions)

**Files:**
- Create: `core/src/main/scala/atomicflow/Awaitable.scala`, extend `core/.../Step.scala` (`await`, `peekSignal`), engine seam + runtime
- Test: `db/src/test/scala/test/AwaitSignalSuite.scala`

**Interfaces:**
```scala
enum Awaitable[R](using val resultCacheable: Cacheable[R]):
  case SignalEvent(s: Signal[?], filter: Any => Boolean = _ => true, lookBack: Duration = Duration.Inf) extends Awaitable[Any]  // refined below
  case Timer(deadline: Instant) extends Awaitable[Unit]
  case WorkflowCompletion(workflowInstanceId: WorkflowInstanceId) extends Awaitable[WorkflowCompletionResult[?]]  // refined in 3.4
  def map[B](f: R => B): Awaitable[B]

object Step:
  def await[A](stepKey: String, awaitable: Awaitable[A], invalidateOn: Seq[StepInput[?]] = Seq.empty,
      ensureUnchanged: Seq[StepInput[?]] = Seq.empty, invalidateAfter: Duration = Duration.Inf)
    (using WorkflowContext, Cacheable[Throwable]): A
  def peekSignal[A](s: Signal[A])(using WorkflowContext): Seq[A]
```
(Exact variance/type refinement is pinned in the task brief; invariant in R, `map` needs no `Cacheable[B]`.)

**Behavior (tests, per "Core model: await is a runtime-computed step" + "Basic Signal API"):**
- Await-satisfied event found → atomic resolve: persist await Step row (`stepKind='Await'`, version 0) + advance cursor + delete subscriptions in one transaction; returns decoded value.
- No satisfying event → suspend: register/refresh subscriptions, throw `WorkflowSuspendedException`; run returns `WorkflowSuspended`; subscription rows exist; wakeup row upserted.
- After the event is sent, a re-`run` resolves the await; body resumes from cached steps (no re-execution of earlier steps).
- Filter: matching event wins; earlier rejected events are skipped permanently (cursor jumps past them) — test cursor position via SQL.
- No match (all rejected) → cursor unchanged, suspension.
- lookBack: events older than `now - lookBack` (event's original acceptance time, runtime clock) are ignored.
- peekSignal: visible events after cursor without advancing.
- Drift policies (`invalidateOn`/`ensureUnchanged`/`invalidateAfter`) apply to await rows like step rows.
- Same key awaited twice sequentially: second await consumes the NEXT event.
- Events sent while instance is running (before suspension commit): the recheck-before-commit rule — suspension transaction re-reads events; no lost wakeup. (Test: send during a run via a step body, then the await at the frontier resolves without a second run.)

### Task 3.3: Timer awaits (subscriptions, inline firing, exactly-once)

**Files:**
- Extend: `Awaitable.scala` (Timer with `apply(delay)(using Clock)`), `Step.scala`, engine seam + runtime (timer-firing primitive)
- Test: `db/src/test/scala/test/AwaitTimerSuite.scala`

**Behavior (tests, per "Why timer awaits need a subscription" + "Timer firing: two paths, one primitive"):**
- `Awaitable.Timer(5.seconds)(using clock)` computes `now + delay` from the contextual clock; the subscription stores the ABSOLUTE deadline; replays never recompute (deadline fixed at first registration) — test with TestClock: advance past deadline, run resolves without pushing the deadline forward.
- Evaluation fires the site's own due timers inline (appends `TimerFired` keyed by subscription ID) then resolves — manual mode works with no sweep.
- Timer not yet due → suspension; due → resolves even if the sweep never ran.
- `TimerFired` appended exactly once per subscription (partial unique index + row-lock re-check; test repeated runs).
- Subscription row SURVIVES firing; deleted only when the await resolves (or terminal cleanup — Phase 4/6).
- Timer invalidated by `invalidateOn` change → re-registers fresh subscription (new subscription ID, recomputed deadline); the old incarnation's `TimerFired` event is inert (cannot satisfy the new evaluation).

### Task 3.4: awaitRace + completion awaits

**Files:**
- Extend: `Step.scala` (`awaitRace`), `Awaitable.scala` (`WorkflowCompletion` refined), engine + runtime (completion subscriptions, terminal-transition wakeup for subscribers)
- Test: `db/src/test/scala/test/AwaitRaceSuite.scala`

**Interfaces:**
```scala
def awaitRace[A](stepKey: String, invalidateOn: Seq[StepInput[?]] = Seq.empty, ensureUnchanged: Seq[StepInput[?]] = Seq.empty)
    (awaits: Awaitable[A]*)(using WorkflowContext, Cacheable[Throwable]): A
// WorkflowInstance.completion: Awaitable.WorkflowCompletion[Out] (on the handle)
```

**Behavior (tests):**
- Race of signal vs timer vs completion: earliest `sequenceId` wins; only the winning signal key's cursor advances; losers' subscriptions are cleaned.
- Due timers of the race are materialized at evaluation in deadline order (earliest due wins among timers) — deterministic under TestClock.
- All-candidates-unsatisfiable → suspension with subscriptions for every leaf; NO cursor advances.
- Completion await: `WorkflowCompletion` yields `WorkflowCompletionResult[Out]`; child instance completing (top-level second workflow in tests) triggers the awaiting instance's wakeup; await resolves on next run; failed completion decodes `Failed(throwable)`.
- The terminal transition (already implemented in 2.2) upserts wakeups for completion subscribers — verify.

### Task 3.5: Step retries (RetryPolicy, durable suspensions, inline sleeps)

**Files:**
- Create: `core/src/main/scala/atomicflow/RetryPolicy.scala`; extend `Step.atLeastOnce` (`retry` param) + engine + runtime
- Test: `db/src/test/scala/test/RetrySuite.scala`

**Behavior (tests, per `steps.md` "Built-in retries" + timer mechanism):**
- `RetryPolicy.never` (default): unchanged behavior.
- `fixedDelay(maxRetries, delay, isRetriable)`: body throws retriable → retry; non-retriable → fail immediately.
- `exponentialBackoff` (both variants per spec signature).
- Delay below the durable-suspension threshold (runtime setting, default 30s, configurable on `PostgresWorkflowRuntime`) → inline `Thread.sleep` retry within the run.
- Delay above the threshold → durable suspension via an internal timer subscription keyed to the retry bookkeeping; the run returns `WorkflowSuspended`; re-run after the deadline (TestClock advance) re-executes the step body.
- `attempts`/`cumulativeDelay`/`lastDelay` inputs to `nextDelay`; retry state is Step-row bookkeeping (`stateKind='started'`, retry payload), not user-visible.
- Crash during a retry = retry never begun (scheduled retry untouched until body returned/threw).
- `invalidateAfter` expires ongoing retries ("as if the step never executed").
- Failure after retries exhausted → normal Failed persistence.

### Task 3.6: onUnconsumedSignals + accepting-signals boundary

**Files:**
- Modify: `Workflow.scala` (constructor param exists), engine + runtime (completion path)
- Test: `db/src/test/scala/test/UnconsumedSignalsSuite.scala`

**Behavior (tests):**
- Workflow completion sets `is_accepting_signals := false` BEFORE running the handler; handler receives `Map[SignalKey, Seq[Any]]` of visible unconsumed events after cursors (decoded with each event's recorded codec — events store `Cacheable` ids per the composition scheme; decode with the runtime's available codecs or keep values opaque: pinned in brief).
- Handler is at-least-once (may run on replay before terminal commit).
- After completion, `send` returns `InstanceAlreadyCompleted`.
- Default handler: ignore.

## Phase 4 — Cancellation and termination

Implements `spec/running-workflows.md` "Cancellation and termination" (COMPLETE section is
NORMATIVE). The escalation sweep lands in Phase 5; this phase implements `cancel`, `terminate`,
checkpoint delivery, sticky redelivery, `Workflow.uncancellable`, and the boundary behavior.

### Task 4.1: `cancel` — cooperative stop with checkpoint delivery

**Files:**
- Modify: `core/.../WorkflowRuntime.scala` (trait ops), `core/.../Exceptions.scala` (WorkflowCancelledException exists), `core/.../Workflow.scala` (uncancellable), engine seam + runtime
- Test: `db/src/test/scala/test/CancelSuite.scala`

**Interfaces:**
```scala
trait WorkflowRuntime:
  def cancel(instanceId: WorkflowInstanceId): Unit  // idempotent; sets cancel_requested_at
```

**Behavior (tests):**
- cancel before first start (CREATED, no execution state): finalizes CANCELLED immediately — body never runs (side-effect counter), terminal transition + WorkflowCompleted(Cancelled) event.
- cancel a terminal instance: no-op.
- cancel while suspended (no live lease): sets `cancel_requested_at`, upserts wakeup (schedule resume). On the next run, the frontier checkpoint (pending await or next not-yet-run step) throws `WorkflowCancelledException`; if user code doesn't catch, run boundary returns `WorkflowRunResult.WorkflowCancelled` + terminal CANCELLED transition; earlier cached steps are NOT re-executed (delivery at the frontier).
- cancel while running: flag set; the live owner re-reads around Step/Await boundaries; next new-work checkpoint throws.
- Sticky redelivery: user catches WorkflowCancelledException and continues → next new-work checkpoint throws again; only reaching a terminal state escapes; completing anyway → COMPLETED (first-terminal-event rule).
- Replay of cached steps never delivers cancellation (frontier-only).
- `cancel_requested_at` is set once and never reset (SQL assert).
- Delivery points: before a step body executes (not cached) and at await evaluation; NOT between cached replays.

### Task 4.2: `Workflow.uncancellable` + `terminate`

**Files:**
- Modify: `core/.../Workflow.scala`, `WorkflowRuntime.scala`, engine + runtime
- Test: `db/src/test/scala/test/TerminateSuite.scala`

**Interfaces:**
```scala
object Workflow:
  def uncancellable[R](f: WorkflowContext ?=> R): R  // lexical, re-entrant region
trait WorkflowRuntime:
  def terminate(instanceId: WorkflowInstanceId): Unit  // force stop
```

**Behavior (tests):**
- uncancellable: checkpoints inside the region do not deliver cancellation; steps execute; pending awaits still resolve; after exit, next checkpoint throws again; region does not clear the flag; replay-deterministic (compensation steps cached/replayed).
- terminate: atomically TERMINATED via guarded transition (+ WorkflowCompleted(Terminated) event, lease revoked via fencing-token bump, wakeups deleted, subscriptions deleted — terminal cleanup); never runs user code; running orphan's next fenced write fails (LeaseLost) and its next checkpoint re-check discards execution; re-run returns WorkflowTerminated (terminal decode).
- terminal cleanup at every terminal transition: delete this instance's subscriptions (signal/timer/completion) and wakeup row (extend the 2.2/3.4/3.6 transition transaction).
- WorkflowCancelledException is a plain public RuntimeException; `scala.util.control.NonFatal` catches it (unit assert); the run boundary converts an escaping one to WorkflowCancelled.
- The run boundary on LeaseLost during cancellation delivery: propagates LeaseLost (not WorkflowCancelled) — the new owner delivers.

## Phase 5 — Job runner

Implements `spec/running-workflows.md` "Job runner and scheduling" (COMPLETE section is
NORMATIVE: the driver loop pseudocode, claim query with fairness + registry filter +
`FOR UPDATE SKIP LOCKED`, outcome classification by durable state, sweeps, requeue, caps,
lifecycle rules, `JobRunnerSettings` incl. `forTests`).

### Task 5.1: JobRunnerSettings, startJobRunner, claim + dispatch loop

**Files:**
- Create: `core/src/main/scala/atomicflow/JobRunnerSettings.scala`, `core/src/main/scala/atomicflow/JobRunner.scala` (trait + lifecycle), `db/src/main/scala/atomicflow/impl/db/PostgresJobRunner.scala`
- Modify: `core/.../WorkflowRuntime.scala` (`startJobRunner`), runtime internals
- Test: `db/src/test/scala/test/JobRunnerSuite.scala`

**Interfaces (defaults pinned):**
```scala
case class JobRunnerSettings(
  workerThreads: Int = 8,                       // runtime-owned daemon pool; ignored when executor is set
  executor: Option[Executor] = None,
  maxConcurrentInstances: WorkflowId => Int = _ => Int.MaxValue,
  capacityRetryDelay: FiniteDuration = 1.second,
  perWorkflowBatchShare: Int = 8,
  pollInterval: FiniteDuration = 250.millis,
  wakeupBatchSize: Int = 32,
  timerSweepInterval: FiniteDuration = 250.millis,
  timerBatchSize: Int = 128,
  sweepInterval: FiniteDuration = 1.second,
  leaseDuration: FiniteDuration = 5.minutes,
  leaseAcquireTimeout: FiniteDuration = 30.seconds,
  cancelTimeout: FiniteDuration = 5.minutes,
)
object JobRunnerSettings { def forTests: JobRunnerSettings /* tiny intervals, 1 worker, short lease/cancel timeouts */ }

trait JobRunner { def stop(gracePeriod: FiniteDuration): Unit }
trait WorkflowRuntime { def startJobRunner(definitions: Seq[Workflow[?, ?]], settings: JobRunnerSettings = JobRunnerSettings.default): JobRunner }
```
Package-private test hook: `JobRunner.runDriverCycle(): Unit` (one driver-loop iteration: sweeps + claim + dispatch — for stepping deterministically).

**Behavior (tests):**
- `startJobRunner` constructs AND starts; duplicate start while active throws; after `stop` (idempotent), start works again.
- Registry validated once at start (duplicate workflowId → error); immutable.
- Claim query: `workflow_id IN (registry)` filter (unknown workflow's wakeup invisible), per-workflow `perWorkflowBatchShare` fairness (ROW_NUMBER partition), `scheduled_at <= now`, `FOR UPDATE OF wakeup SKIP LOCKED`, ordered by scheduledAt, LIMIT batch.
- Claim is atomic with lease acquisition: wakeup deleted only when the lease was obtained (delete matches seen scheduled_at); lease conflict → row stays.
- Executor resolves the definition from the registry by workflowId, decodes input, runs via the SAME code path as the public run API (re-use the engine; pass the runner's leaseDuration/leaseAcquireTimeout).
- Suspension drains: on suspension, if a wakeup row exists again → loop again immediately.
- Terminal outcome: done; FAILED outcomes logged, never rescheduled.
- Runner mode end-to-end: `createAndSchedule` + started runner → instance completes autonomously (awaitResult); signal sent while runner runs → resumes and completes.
- `runDriverCycle` package-private hook works for deterministic stepping.
- stop(gracePeriod): stops claiming, waits for in-flight runs.

### Task 5.2: Background sweeps (timer firing, cancellation escalation, lease recovery)

**Files:**
- Modify: `db/.../PostgresJobRunner.scala` + runtime internals
- Test: `db/src/test/scala/test/SweepsSuite.scala`

**Behavior (tests, per "Background sweeps" table):**
- Timer sweep: due, not-yet-fired subscriptions (batched, deadline order) → fire (the two-paths-one-primitive operation) + upsert owner wakeups; idempotent across passes; unattended instances progress with no runner-side run (manual run also works — Phase 3 already covered inline firing).
- Cancellation escalation sweep: `cancel_requested_at <= now - cancelTimeout` AND no terminal → guarded TERMINATED transition (same as terminate's); bounded batch.
- Lease recovery sweep: `lease_owner IS NOT NULL AND lease_expires_at <= now AND terminal_state IS NULL` → clear lease_owner + upsert wakeup.
- All sweeps leaderless/idempotent (run twice → same state), definition-agnostic (service other applications' workflows in shared tables), bounded batches.

### Task 5.3: Transient-failure requeue, caps, lifecycle polish

**Files:**
- Modify: `db/.../PostgresJobRunner.scala`
- Test: extend `db/src/test/scala/test/JobRunnerSuite.scala`

**Behavior (tests):**
- Requeue: claimed run aborts with a transient failure (simulate via package-private fault injection or by directly exercising the requeue op) → `created_at` unchanged, `scheduled_at = GREATEST(existing, now + backoff(attempts))`, `attempts + 1`; capped exponential backoff, unbounded retries.
- Outcome classification by durable state, not exception type (terminal_state set → done incl. FAILED; lease lost → new owner responsible; only otherwise transient).
- Caps: per-workflow in-process permit count; at capacity → in-place defer (`scheduled_at = now + capacityRetryDelay`, no lease taken) — a hot workflow neither starves the batch nor pins a worker.
- Fairness observable: a burst from one workflow cannot monopolize the batch (two workflows, N wakeups each, share honored).
- `capacityRetryDelay` from settings; caller-thread run bypasses caps.
- Executor override honored (user-supplied `Executor` used instead of the daemon pool).

## Phase 6 — Children, inheritance, parallelism

Implements `spec/sub-workflows-iteration.md` (complete), `spec/child-signal-inheritance.md`
(complete), and the parallelism primitives. Scope derivation uses `ScopePath` (Phase 1).

### Task 6.1: `Workflow.scoped`, `runToSuspension`, `Workflow.parallel`

**Files:**
- Modify: `core/.../Workflow.scala`, engine seam (scope tracking)
- Test: `db/src/test/scala/test/ScopedParallelSuite.scala`

**Interfaces:**
```scala
object Workflow:
  def scoped[R](scopeKey: String)(body: WorkflowContext ?=> R): WorkflowContext ?=> R
  def scoped[A: Fingerprintable, R](elem: A)(body: WorkflowContext ?=> R): WorkflowContext ?=> R  // key derived from fingerprint
  def runToSuspension[R](body: WorkflowContext ?=> R): Either[WorkflowSuspendedException, R]
  def parallel[R](branches: Seq[() => R]): Seq[R]
  def parallel[R](branches: (() => R)*): Seq[R]
```

**Behavior (tests, per sub-workflows-iteration.md "Primitives"):**
- scoped prefixes Step/Await IDs (scope path joined by `/`); same step definitions execute independently per element; nests (path = enclosing + key).
- runToSuspension: catches suspension, returns Left; used by tests/parallel.
- parallel: all branches run concurrently (ox `par`/`mapPar`); waits for ALL; if some complete and others suspend, collects all results first, then throws one combined `WorkflowSuspendedException` carrying each branch's suspension (causes); returns Seq[R] when all complete; suspension propagates to the boundary; combined exception is still excluded by WorkflowNonFatal; `restart`/`break`/control-flow crossing a parallel boundary is Phase 7's concern.
- Sequential iteration: plain loops with scoped — foreach/map/fold patterns work (no library helpers).

### Task 6.2: `startAsChild` + scope derivation + `ParentClosePolicy` + `getChildWorkflowInstances`

**Files:**
- Modify: `core/.../Workflow.scala` (`startAsChild`), `core/.../WorkflowRuntime.scala`, `core/.../Types.scala` (SignalInheritance), `core/.../WorkflowInstance.scala` (Info parentId), runtime + migration columns (parent/inheritance already in V001)
- Test: `db/src/test/scala/test/ChildrenSuite.scala`

**Interfaces:**
```scala
enum SignalInheritance:
  case None_, All, Some_(prefixes: Seq[SignalKey])  // naming: spec says `SignalInheritance.some("a/", ...)` / `.all`; default none
final class Workflow[In, Out]:
  def startAsChild(childKey: WorkflowInstanceKey, input: In, parentClosePolicy: ParentClosePolicy = ParentClosePolicy.Cancel,
      inheritSignals: SignalInheritance = SignalInheritance.none, inheritPastEvents: Boolean = false)(using WorkflowContext): WorkflowInstance[In, Out]
enum ParentClosePolicy: case Cancel, Abandon
trait WorkflowRuntime:
  def getChildWorkflowInstances(parentId: WorkflowInstanceId): Vector[WorkflowInstance.Info]
```
(Exact object/case naming for SignalInheritance is pinned in the brief.)

**Behavior (tests):**
- startAsChild: create-if-absent (idempotent on parent replay); derived scope from parent identity + generation + enclosing scope path (escaped, `@generation` markers); child's first wakeup upserted; children NEVER executed inline on the parent's thread (parent run does not run child bodies); parentId in Info.
- Scope derivation per spec examples: parent (orders, order-42, "") → child (worker, worker-1, "orders/order-42@3/..."); collision-escaping verified via ScopePath (unit, Phase 1); key uniqueness (workflowId, key, scope).
- Parent terminal → ParentClosePolicy applied in the same transaction: Cancel → cooperative cancel semantics per child state (CREATED → immediate CANCELLED; SUSPENDED → flag + wakeup; RUNNING → flag, next checkpoint); Abandon → active-parent pointer cleared only.
- Idempotent application (re-triggering the policy is a no-op).
- Child completing does not cancel parent; parent awaiting child completion works (3.4's completion awaitables).
- getChildWorkflowInstances queries the ACTIVE parent relationship.

### Task 6.3: Signal inheritance (ancestor queries, inheritPastEvents, policy updates)

**Files:**
- Modify: await-evaluation candidate gathering (runtime), `startAsChild` (policy fields on instance row)
- Test: `db/src/test/scala/test/InheritanceSuite.scala`

**Behavior (tests, per child-signal-inheritance.md):**
- Inheritance defaults to none; `some(prefixes)` permits matching key prefixes; `all` permits every key.
- An event addressed to an ancestor is visible only when EVERY parent-child edge on the path currently permits its key (recursive ancestor query; transitive).
- `inheritPastEvents = false`: visibility starts after the child relationship's `inheritedEventsStartSequenceId` (recorded at child creation under the event append mutex — events after the commit have greater sequenceIds); `true`: retained older events visible if ahead of the child's cursor.
- One committed send is atomically readable by all currently eligible descendants.
- Policy broadening (replaying startAsChild with wider prefixes) durably schedules wakeups for the affected descendant subtree; narrowing hides unresolved events; cached results unaffected.
- Detachment (Abandon / parent terminal): inherited visibility removed for unresolved awaits; cached results replayable.
- Synchronous Updates never inherited (Phase 8 note — nothing to test yet).

### Task 6.4: `Step.firstToRunWithoutSuspension`

**Files:**
- Modify: `core/.../Step.scala`, engine
- Test: `db/src/test/scala/test/FirstToRunSuite.scala`

**Interfaces:**
```scala
object Step:
  @experimental def firstToRunWithoutSuspension[R](stepId: String, invalidateOn: Seq[StepInput[?]] = Seq.empty,
      ensureUnchanged: Seq[StepInput[?]] = Seq.empty)(branches: Seq[() => R])(using WorkflowContext, Cacheable[Throwable]): R
  @experimental def firstToRunWithoutSuspension[R](stepId: String, ...)(branches: (() => R)*)(using WorkflowContext, Cacheable[Throwable]): R
```

**Behavior (tests):**
- Edge-triggered: all branches suspend → combined suspension; at least one completes → first result returned, other suspensions DISCARDED.
- State saved in its own step row (stepKind `FirstToRunWithoutSuspension`) — on replay, the first-ever-completed branch's result is returned even if later reruns would unblock others in a different order.
- Dangerous-pitfall Scaladoc: racing arbitrary code vs an await depends on when the code is run; and the batch-unblock caveat (spec text is NORMATIVE for the doc).
- Branch cleanup TODOs from the spec (child cleanup, race-await cleanup when one branch completes) — implement best-effort: subscriptions of losing branches are cleaned when a winner completes.

## Phase 7 — continueAsNew, restartable regions/loops, fork/reset

Implements `spec/continue-as-new-fork-reset.md` (complete) and `spec/restartable-regions-loops.md` (complete). Reuses Phase 6's parent-close machinery and scope derivation.

### Task 7.1: `Workflow.continueAsNew`

**Files:**
- Modify: `core/.../Workflow.scala` (companion forwarder), `core/.../ControlFlow.scala` (new exception + WorkflowNonFatal), `core/.../WorkflowRuntime.scala` (internal primitive), engine seam, `db/.../PostgresWorkflowRuntime.scala` (in-place transition)
- Test: `db/src/test/scala/test/ContinueAsNewSuite.scala`

**Interfaces:**
```scala
object Workflow:
  def continueAsNew[A: Cacheable](nextInput: A)(using WorkflowContext): Nothing
// internal (private[atomicflow]) runtime primitive + ContinueAsNewException carrying the ENCODED nextInput
// WorkflowRunResult gains a ContinueAsNew outcome (or equivalent — check existing outcome set)
```

**Behavior (tests, per continue-as-new-fork-reset.md):**
- Control-flow exception: returns Nothing, body does not continue; excluded by WorkflowNonFatal; committed atomically: old-gen Step rows/subscriptions/wakeups erased (cursors kept), children closed per ParentClosePolicy (Cancel requests, Abandon detaches; continuation doesn't wait), generation incremented + new input installed, directly-addressed Signal events deleted — one in-place transition; `onUnconsumedSignals` runs BEFORE the transaction while signal acceptance is closed.
- Key unique/unchanged; no separate old-gen instances; generation exposed only via Info.
- Global event sequence not reset; new sequenceIds stay greater than all previously allocated (cursors may point into gaps).
- Child that continues-as-new stays attached to ITS parent with unchanged inheritance config; ancestor events untouched.
- New input flows: next run decodes nextInput as the workflow's input.
- Next execution scheduled (wakeup upserted); manual `runWorkflowInstance` also works.

### Task 7.2: `Workflow.restartable` / `Workflow.loop`

**Files:**
- Modify: `core/.../Workflow.scala` (both views + `RestartableScope`/`LoopScope`), `core/.../ControlFlow.scala` (restart/break exceptions + WorkflowNonFatal), engine seam (region scope entries carry restartCount), `db/.../PostgresWorkflowRuntime.scala` (region transitions)
- Test: `db/src/test/scala/test/RestartableSuite.scala`

**Interfaces (spec's conceptual shapes are binding):**
```scala
object Workflow:
  def restartable[S: Cacheable, R](id: String, initialState: => S)(body: (S, RestartableScope[S]) => R)(using WorkflowContext): R
  def loop[S: Cacheable, R](id: String, initialState: => S)(body: (S, LoopScope[R]) => S)(using WorkflowContext): R
trait RestartableScope[S]: def restartCount: Long; def restart(nextState: S): Nothing
trait LoopScope[R]: def restartCount: Long; def break(result: R): Nothing
```

**Behavior (tests, per restartable-regions-loops.md):**
- Equivalent dual views (document in Scaladoc); internally convert one into the other.
- Region = subscope via the scoped mechanism (region ID mandatory, stable); region row stores encoded current state + restartCount; `initialState` by-name, evaluated+persisted only on first creation, ignored on replay.
- Restart: atomically replaces the region row's state and discards nested Step rows + subscriptions owned by the previous looping's scope subtree (region-scoped prefix delete — construct-isolated like 6.4); re-enters locally (outer workflow not replayed to begin the next looping); restartCount counts committed restarts only (not crash replays).
- Normal return: restartable completes the region with R / loop starts next generation; `break(result)` completes with R; neither caches the whole region value; on outer replay the body re-runs reusing cached records.
- Reusing Step/sleep IDs in the successor looping creates fresh work (polling intervals repeat).
- Signal cursors survive across loopings (discarded await results don't make events re-consumable); global event sequence untouched.
- Children created in a discarded looping: closed per ParentClosePolicy; successor loopings use distinct child identities — enclosing regions' key + restartCount contribute to child scope derivation (extend the seam's scope entries; see sub-workflows-iteration.md derivation rules).
- restart/break crossing a `Workflow.parallel` boundary: parallel cleans up other branches and propagates (like other control-flow exceptions).
- Thousands of iterations do not create unbounded state (SQL row-count assertion over a many-iteration loop).

### Task 7.3: `forkWorkflow` + `resetWorkflow`

**Files:**
- Modify: `core/.../WorkflowRuntime.scala` (public ops), `db/.../PostgresWorkflowRuntime.scala` (copy/erase), V001 (ensure a last-updated timestamp on `workflow_steps` — add if missing)
- Test: `db/src/test/scala/test/ForkResetSuite.scala`

**Interfaces:**
```scala
trait WorkflowRuntime:
  def forkWorkflow[In, Out](sourceInstanceId: WorkflowInstanceId, newInstanceKey: WorkflowInstanceKey,
      restartFromStep: StepId)(using workflow: Workflow[In, Out]): WorkflowInstance[In, Out]  // shape per existing create-op conventions
  def resetWorkflow[In, Out](sourceInstanceId: WorkflowInstanceId, restartFromStep: StepId)(using workflow: Workflow[In, Out]): Unit
```
(Exact parameter/generic shape pinned in the brief from existing runtime-op conventions.)

**Behavior (tests, per continue-as-new-fork-reset.md "Forking"/"Resetting"):**
- Fork: brand-new instance id/key, generation 0, same input as source; independent top-level (no parent, no inherited signals, no inheritance columns); copies cached history STRICTLY BEFORE `restartFromStep` (boundary exclusive — selected step + subsequent history omitted so it re-executes); `restartFromStep` must identify an already-executed step (error otherwise); the workflow function still executes from its beginning (replays until the first uncopied operation; no jump); copied rows carry the new instance identity.
- Reset: same instance identity, generation incremented in place, no old generation retained; history strictly before the boundary stays cached, selected + subsequent erased; replays from the top; boundary ordering by the step rows' last-updated timestamp (the spec's rule).
- Both: after the operation the instance is runnable (wakeup scheduled); runs and completes correctly from the replayed/preserved state.
- Parallel-branch boundary caveat: single step ID only (spec TODO) — document the limitation in Scaladoc.

## Phase 8 — Updates

Implements `spec/signals-timers.md` "Updates: signals that return a value" (the spec marks the concept as not fully thought out — the listed API shapes and semantics are binding; open choices get pinned and recorded in DEVIATIONS.md).

### Task 8.1: `Update`, `sendUpdate`, `Step.awaitUpdate`

**Files:**
- Modify: `core/.../Signal.scala` or new `core/.../Update.scala`, `core/.../Step.scala` (awaitUpdate), `core/.../WorkflowRuntime.scala` (sendUpdate op), `core/.../WorkflowInstanceId.scala`/`WorkflowInstance.scala` (sendUpdate forwarders), V001 (update records table)
- Test: `db/src/test/scala/test/UpdateSuite.scala`

**Interfaces (spec shapes are binding):**
```scala
class Update[I, R](val key: String)(using val inputCacheable: Cacheable[I], responseCacheable: Cacheable[R])
enum UpdateSendResult[+R]: case Success(value: R); case Unhandled; case InstanceAlreadyCompleted
// Update.send / WorkflowInstanceId.sendUpdate / WorkflowInstance.sendUpdate forwarders (match Signal.send's runtime-threading pattern)
trait WorkflowRuntime:
  def sendUpdate[I, R](instanceId: WorkflowInstanceId, updateKey: String, input: I, idempotencyKey: String = "",
      persistUnhandledUpdates: Boolean = false)(using u: Update[I, R]): UpdateSendResult[R]  // shape adapted to codebase conventions
object Step:
  def awaitUpdate[I, R, O](stepKey: String, u: Update[I, R])(respond: I => (R, O))(using WorkflowContext, Cacheable[Throwable]): O
```

**Behavior (tests, per spec):**
- Updates addressed directly to one instance; never inherited (Scaladoc note — no inheritance machinery).
- Send flow: workflow completed → `InstanceAlreadyCompleted`; subscribed/awaited + instance locked → wait for the lease to expire, retry from beginning; subscribed + free → run the workflow ON THE SENDER'S THREAD until finished; result set on the update's DB record → `Success(result)`; run finished without handling → `Unhandled`.
- `awaitUpdate` awaits the update like a signal await (subscription, recheck-before-commit), then `respond` computes `(response, output)` — response is durably written to the update record (visible to the waiting sender), output returned to the workflow.
- idempotencyKey: runtime deduplicates — same key twice → second send is idempotent, sender gets the same result.
- persistUnhandledUpdates=false: unhandled update record deleted after the run; =true: kept (later `awaitUpdate` can consume a persisted-but-unhandled update; deleted after handling).
- A new update record table (V001): instance, key, encoded input, idempotency key, result (nullable), timestamps.
- Update-visible-to-await concurrency: sender blocks until the run completes (bounded by lease waits).

## Phase 9 (expanded at phase boundary)

- **Phase 3:** signals, timers, awaits, event log append protocol (advisory lock), cursors, subscriptions, wakeups, `Awaitable`, `Step.await`/`awaitRace`/`peekSignal`, durable step retries (`RetryPolicy`), `onUnconsumedSignals`, `Signal.send`, `TestClock` (public utility — deviation: shipped in `core`, not the in-memory backend).
- **Phase 4:** cancellation & termination (`cancel`, checkpoint delivery, sticky redelivery, `Workflow.uncancellable`, `terminate`, `WorkflowCancelledException` flow into `WorkflowRunResult.WorkflowCancelled`).
- **Phase 5:** job runner (`startJobRunner`, driver loop, `FOR UPDATE SKIP LOCKED` claiming with fairness + registry filter, sweeps: timer firing / escalation / lease recovery, requeue with backoff, `JobRunnerSettings` incl. `forTests`, caps).
- **Phase 6:** children (`startAsChild`, scope derivation with escaping + generation markers, `ParentClosePolicy`, completion awaits via `WorkflowCompletionResult`, signal inheritance with recursive ancestor queries, `Workflow.scoped`, `runToSuspension`, `Workflow.parallel`, `Step.firstToRunWithoutSuspension`, `getChildWorkflowInstances`).
- **Phase 7:** `Workflow.continueAsNew`, restartable regions (`Workflow.restartable`/`loop`), `forkWorkflow`, `resetWorkflow`.
- **Phase 8:** Updates (`Update`, `sendUpdate`, `awaitUpdate`, idempotency keys, persistUnhandledUpdates).
- **Phase 9:** workflow-evolution polish (`versionAtCreation` branches verified), examples update (un-comment `DocumentProcessingAtomicflow`), final spec-conformance review, DEVIATIONS.md final pass.
