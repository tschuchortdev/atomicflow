# Signals & Timers

Guideline for waiting on external events and timers.

## Core model: await is a runtime-computed step

A step's result is computed by *user code* and cached. An **await's** result is computed by the *runtime* (from persisted events) and cached in the same `workflow_steps` table. When replay reaches an await-site, evaluation proceeds in this exact order:

1. a completed Step row already exists → return its recorded result. This prevents an await from re-suspending on every re-run.
2. no completed row → **fire existing due timers**: for each stored timer subscription with `deadline <= now` and no firing event, lock the subscription and append `TimerFired`. Commit timer firing before the evaluation transaction.
3. **register subscriptions and read candidates**: visible signal events after the shared exact-key cursor, firing events for the current timer ids, and completion events for the awaited instances. Filters and mappings run inside this transaction. See "Await-signal searches" for cursor behavior.
4. if any candidate satisfies the condition → **resolve atomically**: persist the result, advance signal cursors, delete the site's subscriptions; otherwise **suspend**: advance the shared exact-key cursor past every examined-and-rejected event, register/refresh pending subscriptions and throw `WorkflowSuspendedException` — caught at the outer workflow boundary, which returns `WorkflowSuspended` from `run` (see `running-workflows.md`).

**Manual evaluation does not require a scheduler pass.** A run reads stored events, cached results, and the wait's timer subscriptions. It fires any existing due timers before evaluating candidates. Signal and completion subscriptions tell the runner which instances may have work; timer subscriptions also retain the deadline and timer id required to resolve the wait. On first evaluation the runtime registers subscriptions before reading their candidates. A newly registered timer is fired on a subsequent evaluation or timer sweep, including when its deadline is already due.

Awaits thus reuse the entire step machinery (explicit ids, persisted results, manual intervention via reset) instead of inventing a parallel mechanism.

### Why timer awaits need a subscription — and why the scheduler must not delete it

Every pending timer await owns one row in `workflow_timer_subscriptions` (site, timer id, absolute deadline). It is not a wakeup hint; it matters in three ways:

1. **It is the durable deadline record.** The deadline is computed once (`now + delay`, runtime clock) and stored absolutely; every replay of the await-site reads it from the row. Recomputing it from code would push timeouts forward — with a test clock advanced between runs, the recomputed deadline would land in the future again and the await would suspend forever instead of resolving.
2. **It is the scheduling source.** The timer sweep's only input is this row's stored deadline; without the row, due timers of unattended instances would never produce a wakeup and no runner would ever come.
3. **It identifies the firing event.** Each registration gets a timer id reused on replay. Invalidation creates a new subscription and timer id, so an old `TimerFired` event cannot satisfy the replacement wait. The partial unique index on `(workflowInstanceId, eventKey) WHERE eventKind = 'TimerFired'` prevents concurrent firing from storing duplicate events.

**The scheduler must not delete the subscription when it fires.** Firing appends the `TimerFired` event; deleting the row would (a) make the existing event unmatchable, because evaluation correlates events to the site via the row's timer id, and (b) destroy the stored deadline, forcing the replay recomputation of point 1. Both break the resume that the fired event was supposed to enable. The row survives firing and is retired **only when its await resolves** — atomically with the step row and cursor moves — or by terminal cleanup. Until then, the "no existing event" re-check under the subscription row lock keeps repeated sweeps idempotent no-ops. Deletion is therefore exclusively the await's retirement decision, never the scheduler's.

### Timer firing: two paths, one primitive

A due timer becomes a durable fact — a `TimerFired` event keyed by its timer id — through two paths that share one primitive: **lock the subscription row, re-check that no event exists yet, append `TimerFired` (global append mutex), commit.** The subscription row survives firing; it is retired only when its await resolves.

**Path 1 — the scheduler (sweep).** Runs as a driver-loop step of the JobRunner (see `running-workflows.md`, "Job runner and scheduling") every `timerSweepInterval`:

```text
every timerSweepInterval:
  rows = SELECT timerId, workflowInstanceId FROM workflow_timer_subscriptions s
         WHERE s.deadline <= now()                          -- runtime clock
           AND NOT EXISTS (matching TimerFired event)       -- idempotent across passes
           AND instance non-terminal
         ORDER BY s.deadline LIMIT :timerBatch
         FOR UPDATE OF s SKIP LOCKED                        -- the claim (row lock, no delete)
  for each row, in its own transaction:
    BEGIN
      re-check "no event exists" under the lock
      pg_advisory_xact_lock(:eventSequenceLock)             -- normal event-append protocol
      append TimerFired (nextval + INSERT); upsert wakeup of the owning instance
    COMMIT
```

**Path 2 — await evaluation.** Step 2 of the core model: a run reaching a wait fires its existing due timer subscriptions under the instance's lease before reading candidates.

- **Why the sweep pre-fires: race predictability.** Eager firing puts the timer's occurrence at ≈ deadline + sweep latency, so a signal arriving after the deadline (but before the next sweep pass) still *loses* the race. Evaluation-only firing would stretch the occurrence window to "whenever the instance next runs", making race outcomes depend on runner timing.
- **Why evaluation also fires: self-sufficiency.** Manual mode has no sweep; the run materializes its own due timers, so `advanceBy` + `run` decides race outcomes deterministically (see `running-workflows.md`, "Testing"). A deadline that passed while the runner was down is likewise unstuck by the next `run`.
- **Exactly once.** The subscription row lock plus the re-check arbitrates concurrent firing by sweep and evaluation; the partial unique index on `(workflowInstanceId, eventKey) WHERE eventKind = 'TimerFired'` is the safety net.
- Firing is **late, never early** — a timer means "wait at least until its deadline". Firing lag (`now - deadline` at append time) is a monitored metric.
- Bounded, deadline-ordered batches keep timer catch-up (after downtime or mass registration) from starving signal and completion events.
- Step retries with delays above the durable-suspension threshold are ordinary timer subscriptions and ride this mechanism; below the threshold they are inline sleeps inside the run (see `steps.md`).

Several different things can be awaited:
```scala
enum Awaitable[R : Cacheable as resultCacheable] {
  case SignalEvent(
      s: Signal[R], 
      filter: R => Boolean = { _ => true },
      lookBack: Duration = Duration.Inf // Only consider events that arrived within `now - lookBack`
  ) extends Awaitable[R]
  
  case Timer(deadline: Instant) extends Awaitable[Unit]
  object Timer {
    def apply(delay: FiniteDuration)(using clk: Clock): Awaitable[Unit] = 
      Timer(Instant.now(clk).plus(delay))
  }
  
  /** Yields the child's terminal outcome as a `WorkflowCompletionResult[R]`
    * (see `running-workflows.md`). The child's output type comes from the
    * typed handle that constructs the awaitable
    * (`WorkflowInstance[In, Out].completion`); the codecs needed to persist
    * and decode the outcome are required at the `Step.await` call site, like
    * for mapped results. */
  case WorkflowCompletion[R](workflowInstanceId: WorkflowInstanceId)
      extends Awaitable[WorkflowCompletionResult[R]]

  /** Transforms the awaited result. Mapping does not require a `Cacheable[B]` for
    * the target type `B`: a `Cacheable` is only needed at the `Step.await` call
    * site, where the mapped result is persisted. */
  def map[B](f: R => B): Awaitable[B]
}
```

- `Awaitable` is **invariant** in `R` (like `Cacheable` and `Signal`), so its
  cases may safely embed `Signal[R]`.
- `map` exists so that `Step.await` can race or combine awaitables whose raw
  results (`Unit` timers, `Out` completion results) differ in type, mapping them
  onto one common result type. It does not require a `Cacheable` for the mapped
  type; the `Cacheable` needed to persist the awaited result is required at the
  `Step.await` call site.
- A `WorkflowCompletion` yields the child's terminal outcome as a
  `WorkflowCompletionResult[R]`: `Completed(result)` on success, `Failed(decoded
  failure)` when the child failed — the case carries the decoded `Throwable`,
  ready to rethrow for direct-style handling — plus `Cancelled` and
  `Terminated`. Awaiting never throws the child's failure implicitly; the
  outcome crosses the serialization boundary before it is observed, exactly
  like a Step result. `WorkflowInstance.completion` returns one directly; see
  `core-types.md`. A bare `WorkflowInstanceId` carries no output type — upgrade
  it to a typed handle from the workflow definition to await a completion.
- `Timer.apply(delay)` resolves its clock through the `WorkflowContext` of the
  calling workflow body (`ctx.runtime.clock`), so workflow code computes
  deadlines from the executing runtime's single time source — an ambient
  `given Clock` is neither needed nor possible — and tests substitute a virtual
  clock on the runtime, advancing it between runs. Building a timer outside a
  workflow body uses the absolute `Timer(deadline: Instant)` case directly.

Convenience accessors:
```scala
// on WorkflowInstance[In, Out]
def completion: Awaitable.WorkflowCompletion[Out] = Awaitable.WorkflowCompletion(id)
```

## Event storage: globally ordered log + signal cursor per (workflow-instance, signal-key)

- `sendSignal` is an **append** of an immutable `Signal` event and never
  overwrites an event.
- Signals, fired timers, and workflow completions share one global
  `workflow_events` log. Every event has a unique, increasing
  `sequenceId`, so `Step.awaitRace` can compare signals, timers, and completion
  events even when they belong to unrelated workflow trees.
- `sequenceId` defines the durable event order. If one append commits before
  another begins, the first has the lower ID; concurrent appends are ordered by
  the global event allocator. A timer means "wait at least until its deadline":
  a delayed scheduler appends its `TimerFired` event when it runs, and races use
  that event's `sequenceId`, not the deadline timestamp.
- Lifecycle cleanup may delete event rows. Deletion never updates an event to
  record consumption and never reuses its `sequenceId`.
- The total event order is not a mandatory consumption order. An await for one
  exact key may consume a later event while an earlier event of another key
  remains unconsumed.
- Signal events have no global or per-recipient processed flag. Independent
  consumption is tracked by one cursor per `(workflowInstanceId, signalKey)`.
- Await-sites in the same workflow instance that use the same exact signal key
  share its cursor. Cursors are not shared between keys or workflow instances.
- A successful await stores only its public returned value in `workflow_steps`.
  Replay returns that value without consulting `workflow_events` or advancing a
  cursor again.
- Sending appends only one event. Child signal inheritance is read-time
  visibility over `workflow_events`, not event copying; see
  `child-signal-inheritance.md`.

### Why the event log is global

A parent-child-tree log would make direct and inherited signals within that
tree easy to order and would permit independent trees to append concurrently.
It cannot, however, implement the contract of `Step.awaitRace` when a race
includes a workflow completion from another tree: sequence IDs from separate
logs are not comparable, and copying completion events to every possible
waiter's tree would be both expensive and incomplete for late subscriptions.

One global log gives every persisted event the same comparable `sequenceId`.
`awaitRace` can therefore choose the earliest durable signal, timer, or
completion event regardless of which workflow produced it. The tradeoff is a
single short append critical section. PostgreSQL queues appenders on the global
advisory mutex rather than allowing conflicting allocations to fail and retry;
the implementation must keep that section small and benchmark the resulting
throughput. Parent-child trees remain necessary for signal inheritance and
lifecycle, but no longer define an ordering boundary.


## Basic Signal API

Creating signals:
```scala
class Signal[A](val key: String)(using val cacheable: Cacheable[A])

val interruptSignal = Signal[Unit]("interrupt")
```
while it is possible to send events to signals by untyped key, creating a `Signal[A]` has several advantages:
- Type `A` is associated with the key, so the compiler can check that the sender and receiver agree on the type.
- `Cacheable[A]` is saved inside the `Signal` class, so the `Cacheable` does not have to be imported at the call-site and is controlled completely by the definition-site.
- Easier code evolution: Key is independent of variable name

Writing signals:
```scala
class Signal[A](...) {
  @throws[WorkflowNotFoundException]
  def send(workflowInstanceId: WorkflowInstanceId, value: A): SignalSendResult = ???
}

// A terminal instance rejects the signal with InstanceAlreadyCompleted.
enum SignalSendResult {
  case Success, InstanceAlreadyCompleted
}

class WorkflowInstanceId(...) {
  @throws[WorkflowNotFoundException]
  def sendSignal[A](s: Signal[A], value: A): SignalSendResult = ???
}

class WorkflowInstance[In, Out](...) {
  @throws[WorkflowNotFoundException]
  def sendSignal[A](s: Signal[A], value: A): SignalSendResult = ???
}

trait WorkflowRuntime {
  @throws[WorkflowNotFoundException]
  def sendSignal[A: Cacheable](workflowInstanceId: WorkflowInstanceId, key: String, value: A): SignalSendResult
}
```

- `sendSignal` does not acquire the workflow execution lease, so a
  busy or long-running workflow never blocks senders. They only append signal
  data; they do not modify Step results or other
  execution records owned by the running workflow, and they schedule no work
  themselves.
- Sending a Signal event briefly acquires a row lock on the addressed workflow
  instance and checks that it has not reached a terminal state (`terminal_state
  IS NULL`) to prevent appending an event when that instance is completed or
  completing. It then acquires the global event
  allocator mutex (pg_advisory_lock in postgres), obtains the next `sequenceId` (`SEQUENCE CACHE 1` in postgres), appends the one immutable event, and commits.
  The sender creates no wakeups: signal and completion wakeups are produced
  asynchronously by the job runner's watermark dispatch and full subscription
  scan (see `running-workflows.md`, "Sources of wakeups").
- One committed append is immediately readable by the addressed workflow and
  every child currently allowed to inherit it. This is atomic for all children
  because the immutable fact is stored once; there is no per-child delivery or
  copy transaction.
- `SignalSendResult.Success` means this append committed and the fact is
  available under the inheritance rules. It does not mean that any workflow has
  run or consumed the event.
- `lookBack` uses the event's original acceptance time. Child visibility does
  not create a new delivery timestamp because no copy is made.

Reading Signals:
```scala
object Step {
  def peekSignal[A](s: Signal[A]): Seq[A] = ???
  
  def await[A](
    stepKey: String, 
    awaitable: Awaitable[A],
    invalidateOn: Seq[StepInput[?]] = Seq.empty,
    ensureUnchanged: Seq[StepInput[?]] = Seq.empty,
    invalidateAfter: Duration = Duration.Inf,
  ): A = ???
}
```

- `Step.await` throws a `WorkflowSuspendedException` control-flow exception when the
  await cannot be satisfied immediately. The workflow runtime catches this
  exception and suspends the workflow. User code must always ignore or rethrow
  this exception.
- Peek reads currently visible events after the shared exact-key cursor without
  advancing it.
- Await-signal searches visible events for the exact signal key after the
  workflow instance's shared cursor and suspends if none satisfies the filter.
  - Child workflows also search events addressed to allowed ancestors using the
    current inheritance configuration; see `child-signal-inheritance.md`.
   - When suspending, it registers subscriptions for the currently visible
     sources; wakeups are produced asynchronously by the job runner (see
     `running-workflows.md`, "Sources of wakeups"). The filter is evaluated in
     workflow code, so any new event for the key wakes the workflow for
     reevaluation.
   - If an event matches, advancing the shared cursor and persisting the
     successful await Step row happen atomically before its value is returned.
  - If a matching event is found, moving the cursor to that event also skips
    every earlier event of the key that the filter rejected. **The Javadoc must
    clearly state that an await with a filter may skip past unprocessed events:
    when it returns a matching event, all earlier rejected events become
    permanently unavailable to later awaits of the same key in this workflow.**
  - If no event satisfies the filter, the suspending await advances the shared
    exact-key cursor to the maximum `sequenceId` it examined, so every event it
    examined and rejected is treated as consumed for scheduling: those events
    are not re-examined on reruns and do not repeatedly cause wakeups. Because
    awaits on the same instance and key share the cursor, a rejection discards
    those events for all of them; a later change to the filter will not
    reconsider them (see "Signal invalidation").

## Racing signals/timers

`Step.awaitRace` allows racing multiple signals and timers. This is less flexible than racing arbitrary code but much safer: `Step.awaitRace` guarantees that whatever event/timer occured first will definitely win the race, whereas the result of racing arbitrary code with `Step.firstToRunWithoutSuspension` literally depends on when the code is run. 

```scala
object Step {
    def awaitRace[A](
        stepKey: String,
        invalidateOn: Seq[StepInput[?]] = Seq.empty,
        ensureUnchanged: Seq[StepInput[?]] = Seq.empty
    )(members: (String, Awaitable[A])*): A = ???
}
```

`awaitRace` creates one subscription for each member in the database. Each member
is a `key -> awaitable` pair; the key is that member's **subscriber key**, a
stable identity within the race that is independent of the member's position.
Keys must be non-empty, unique within the race, and must not start with `"__"`
(reserved for engine-internal subscribers). The empty key `""` is reserved for
single-subscriber sites: plain `Step.await`.

- Each signal member retains the cursor for its own exact key; `awaitRace` does
  not introduce a cursor shared between keys.
- All candidates are compared by global `sequenceId`, so the earliest
  satisfying signal, timer, or completion event wins even when the events
  belong to unrelated workflow trees.
- Due timer members of the race are materialized at evaluation time (step 3 of
  the core model) before comparison; appending them in deadline order makes the
  earliest due timer win deterministically among timers. In runner mode the
  sweep has usually already fired them (≈ deadline + sweep latency); in manual
  mode evaluation fires them at `run` time — the test controls the order.
- When a race succeeds, only the winning signal key's cursor advances. The
  successful Step row and that cursor movement are persisted atomically. If no
  candidate is satisfiable, none of the signal cursors advance.

## Updates (unimplemented)

Updates were designed and prototyped but are deliberately **not implemented**
for now. The feature was dropped without replacement; the two candidate designs
below are kept for future reference only, and **neither is implemented by the
runtime**.

The motivation was to give workflows some synchronous interactivity (for
example, updating a user's name, which requires timely validation and a
response). Both designs are always addressed directly to one workflow instance;
they are never inherited by children, because a synchronous request cannot have
one unambiguous response if several descendants handle it.

### Design A — request/response records (the previously implemented design)

An `Update[I, R]` is a `Signal` with a **synchronous response channel**. The
sender writes an update record; if the instance is free, the sender runs the
workflow on **its own thread** so the run can handle the record, and then reads
the response back off the record. Awaiting sites use `Step.awaitUpdate` to
handle the OLDEST unhandled record for the key: `respond(input)` returns
`(response, output)`, the `response` is durably persisted onto the record (which
the blocked sender reads as `UpdateSendResult.Success`), and `output` is
returned to the workflow body. Optional `idempotencyKey` deduplication reuses an
existing record for a repeated send, and `persistUnhandledUpdates` controls
whether a record the run did not handle survives for a later await.

Known weaknesses:

- The sender-thread run needs the **workflow definition** at the send site (the
  runtime has no registry), so `send` carries a leading `Workflow` argument.
- A race with a concurrently scheduled runner run can **consume the update on
  the runner's thread**, leaving the sender reading a stale `Unhandled` even
  though the update was handled durably.
- The sender can enter **lease-wait loops**, polling for a busy instance's lease
  to expire before retrying.
- The concept carried a lot of machinery: **two extra tables**
  (`workflow_updates` and `workflow_update_subscriptions`) and **four engine
  operations** (read candidates, resolve, suspend, plus the send path).

### Design B — signal-based updates

A second design builds entirely on **ordinary signals**, requiring no new engine
machinery:

- The caller sends a `Signal` carrying a correlation id and the request payload.
- The workflow awaits that signal in an ordinary `Step.await`, computes the
  response, and persists it where the caller can read it — for example an
  at-most-once step keyed by the correlation id, or a response signal addressed
  back to the caller.
- The caller polls for, or awaits, the response.

There is no sender-thread execution and no extra engine operations. The
weakness is caller-side **latency and polling**.

**Both designs are unimplemented alternatives.** The API shapes below are
illustrative of Design A and are not part of the current library:

```scala
class Update[I, R](val key: String)(using val inputCacheable: Cacheable[I], responseCacheable: Cacheable[R])

val nameUpdate = Update[String, ValidationResult]("name-update")

enum UpdateSendResult[+R] {
  case Success(value: R)
  case Unhandled
  case InstanceAlreadyCompleted
}

class Update[I, R](...) {
  @throws[WorkflowNotFoundException]
  def send(workflowInstanceId: WorkflowInstanceId, input: I): UpdateSendResult[R] = ???
}

object Step {
  def awaitUpdate[I, R, O](stepKey: String, u: Update[I, R])(respond: I => (R, O)): O = ???
}
```

## Causal boundaries between signals

An API for checkpoints, sharing a cursor between different signal keys, or
manually moving a signal cursor is deferred. The current API can use `lookBack`
when old events should be ignored, but this does not express a precise causal
boundary such as "only accept approval events after the request step." Such a
boundary can be added later without changing the per-exact-key cursor model.

## Queries

Queries are functions that can synchronously ask about the current workflow state (ideally without executing the workflow). They exist in Temporal but are deliberately out of scope in atomicflow for now, since all our state is only readable from local variables in the workflow body.

## Awaiting workflow completion

The library provides a function to await completion of other workflows.

## Unconsumed signals at completion

Every workflow can have an `onUnconsumedSignals` handler, mainly for logging.

A signal arriving after execution passed its await-site is not lost: the
immutable fact remains available until workflow completion, continue-as-new, or
another retention boundary removes it.

- **Default: ignore.** Unless a handler is declared, unconsumed signals are
  ignored. The gathering and deletion of directly addressed `Signal` events, the
  closing of child relationships, and the terminal transition commit together;
  children retain cached Step results for
  inherited signals they already consumed, so replay does not require the
  source rows.
- Optional `onUnconsumedSignals` handler can be passed to the `Workflow` constructor in the same parameter group as the body. It runs after the instance has been marked complete (or has continued as new) and receives a map of all unconsumed signals:
  ```scala
  val wf = Workflow("order")(
    (order: Order) => {
      val orderEvents = Step.await("read-orders", Awaitable.SignalEvent(orders))
      ...
    },
    onUnconsumedSignals = { unconsumed =>
      unconsumed.foreach { case (signalId, events) =>
        ...
      }
    }
  )
  ```
  When only the body is needed, the handler defaults to ignoring unconsumed signals:
  ```scala
  val wf = Workflow("order") { (order: Order) => 
    ...
  }
  ```
  The handler receives `Map[SignalKey, Seq[String]]` where the values are
  currently visible, raw serialized event payloads after this workflow
  instance's cursor for each exact signal key (see DEVIATIONS entry 19 for why
  the payloads are not decoded).
- The `onUnconsumedSignals` handler is a regular function (may not contain `Step`s). It is invoked **after** the terminal (or continue-as-new) transaction commits, so it is **at-most-once**: a crash between the commit and the invocation loses the delivery, which is acceptable for a logging/debug aid. A handler that throws cannot mask the workflow outcome.
- Directly addressed `Signal` events are gathered (per key, after cursors) and
  deleted inside the same terminal/continue-as-new transaction that sets the
  instance terminal, under that transaction's instance row lock. A signal
  therefore either commits before the transition — and is included in the
  handler's map — or arrives after and is rejected as
  `InstanceAlreadyCompleted` because the instance is already terminal
  (`terminal_state IS NOT NULL`). This resolves the "completion delivery
  boundary" question below: a signal can never be reported as delivered and
  then omitted.
- On paths with no workflow definition available — force-stop/termination,
  cancellation escalation, and cancellation of a never-started instance
  (including a never-started child closed by its parent) — the gathered map
  cannot be handed to a listener; the runtime logs it at WARN level instead of
  silently dropping it.

## Invalidation and await stability

When an upstream step is invalidated (TTL expired, `invalidateOn` dependency changed), downstream constructs are invalidated **only if they explicitly list the changed value in their `invalidateOn` cache-key**. No automatic cascade.

**Awaits inherit this rule:** an await can declare `invalidateOn` dependencies just as steps do:

```scala
Step.awaitRace(
  "approval-race",
  invalidateOn = Seq("context" -> someContextValue)
)("approval" -> Awaitable.SignalEvent(approval), "deadline" -> Awaitable.Timer(deadline))
```

If `someContextValue` changes, the await's cached Step result is discarded; on the next replay it re-evaluates from scratch.

### Timer invalidation

A timer whose expression (duration/deadline) is keyed on an invalidated upstream value recomputes from scratch:
- If a timer already fired and then its deadline recomputes to a *later* moment, the timer is treated as if it had **never executed** — the workflow suspends again waiting for the new deadline.
- Re-evaluation registers a new timer id. Firing events for the previous subscription cannot satisfy the replacement wait (see "Why timer awaits need a subscription").

### Signal invalidation

- Events consumed by an await, earlier events skipped when its filter found that
  result, and events examined-and-rejected by a suspension all remain behind the
  workflow instance's shared cursor for that exact signal key.
- Discarding an await's cached Step result does not rewind that cursor. The invalidated
  await resumes after it and may consume an event that arrived before the
  invalidation but has not yet been examined.
- If invalidation changes the filter, skipped events are not reconsidered.
  Explicit cursor-reset semantics would be required to rescan them and are not
  currently provided.

## Persistence model

The PostgreSQL runtime uses these internal tables:

```
workflow_events (
  sequenceId, eventKind, workflowInstanceId, eventKey, payload, createdAt
)
workflow_steps (
  workflowInstanceId, stepId, stepVersion, stepKind, stateKind, statePayload,
  inputFingerprints, expiresAt, createdAt, updatedAt
)
signal_cursor (
  workflowInstanceId, signalKey, sequenceId
)
workflow_signal_subscriptions (
  workflowInstanceId, stepId, subscriberKey, signalKey
)
workflow_timer_subscriptions (
  workflowInstanceId, stepId, subscriberKey, timerId, deadline
)
workflow_completion_subscriptions (
  workflowInstanceId, stepId, subscriberKey, completedWorkflowInstanceId
)
workflow_wakeups (workflowInstanceId PRIMARY KEY, createdAt, scheduledAt, attempts)
workflow_event_dispatch (id, dispatched_through)
```

`workflow_events` has one non-null envelope shape for every event kind:

| Event kind | `workflowInstanceId` | `eventKey` | `payload` |
|---|---|---|---|
| `Signal` | The addressed instance | Exact signal key | Serialized signal value |
| `TimerFired` | The instance owning the timer | Timer id | Encoded `Unit` |
| `WorkflowCompleted` | The completed instance | Empty string | Serialized `WorkflowCompletionResult` — one event per terminal transition; see `running-workflows.md` |

- `workflow_events.sequenceId` is the global durable event order. It is backed
  by a PostgreSQL `SEQUENCE ... CACHE 1`, not an application counter table.
  Rollbacks and lifecycle deletion may leave gaps; IDs are never reused or
  reset.
- Every event appender obtains the same blocking transaction-scoped advisory
  lock with `pg_advisory_xact_lock` before calling `nextval` and keeps it until
  commit. The lock makes sequence allocation and visibility commit-ordered.
  Without it, one transaction could allocate a lower ID, stall, and commit
  after an event with a higher ID.
- The mutex is a deliberate global append bottleneck required by the strict
  cross-workflow `awaitRace` guarantee. It queues writers instead of causing
  unique-key collisions and retry storms. Keep its critical section to the
  sequence allocation, event insertion, any wakeup writes the appender performs
  (timer firing), and commit; never serialize payloads, run user code, or make
  network calls while holding it. Monitor lock wait time, append latency, and connection-pool pressure, and
  benchmark expected event throughput.
- PostgreSQL uses a dedicated sequence and a well-known advisory-lock key:
  ```sql
  CREATE SEQUENCE workflow_event_sequence AS BIGINT CACHE 1;

  SELECT pg_advisory_xact_lock(:eventSequenceLock);
  SELECT nextval('workflow_event_sequence');
  INSERT INTO workflow_events (...);
  ```
  `pg_advisory_xact_lock` blocks and queues competing appenders until commit or
  rollback. It avoids the unique-key failures and immediate-retry storms that
  concurrent `MAX(sequenceId) + 1` allocation would cause.
- The same mutex protects child creation while it records
  `inheritedEventsStartSequenceId`: events appended after the relationship
  commits have a greater `sequenceId`.
- `workflow_steps` is the shared durable record for user Steps, awaits,
  `firstToRunWithoutSuspension`, and restartable regions. `stepKind` and
  `stateKind` select the runtime interpretation of the always-present
  `statePayload`; see `steps.md` and `restartable-regions-loops.md`.
- Awaits and `firstToRunWithoutSuspension` persist only their public returned
  value. Await event selection and the winning branch are transaction-local:
  they are used to advance signal cursors and clean up subscriptions, then are
  not retained.
- The three subscription tables contain only pending awaits and are never event
  rows. A timer subscription holds its absolute deadline and its subscription
  ID (assigned at registration, reused on replay); firing — by the scheduler or
  by await evaluation — appends a `TimerFired` event keyed by that ID, guarded
  by the partial unique index on `(workflowInstanceId, eventKey) WHERE
  eventKind = 'TimerFired'`. The subscription row survives firing and is
  deleted only when its await resolves. A first-to-run construct additionally
  retires every branch's subscriptions — including nested subscopes, via
  `deleteSubscriptionsUnderScopePaths` — BEFORE the winner's step row is
  written, so a committed winner row never coexists with branch subscriptions.
  Workflow completion appends one
  `WorkflowCompleted` event in the same transaction as the guarded terminal
  instance transition, whether or not a subscriber already exists.
- A child await uses a recursive ancestor query over `workflow_instances` to
  find the sources allowed by every inheritance edge. An index over
  `(eventKind, eventKey, workflowInstanceId, sequenceId)` supports exact signal
  lookup.
- Continue-as-new deletes directly addressed `Signal` events and discarded Step
  and subscription records, but preserves exact-key signal cursors. Its
  successor remains attached to its own parent and retains the original
  inheritance relationship and `inheritedEventsStartSequenceId`.
- Cursor advancement and successful `workflow_steps` persistence happen in one
  atomic transaction. Signal events themselves have no acknowledged state.
- Wakeups are produced asynchronously, not by writers. Signal sends and terminal
  transitions only append events (short transactions, no deadlock against
  subscribers); two definition-agnostic job-runner passes materialize the
  coalesced `workflow_wakeups` rows. The best-effort watermark dispatcher
  (`JobRunnerSettings.eventDispatchBatchSize`, default 1000; every driver cycle)
  locks the single-row `workflow_event_dispatch` table with `FOR UPDATE SKIP
  LOCKED`, reads events with `sequence_id > dispatched_through` in order, upserts
  the wakeups each implies (`Signal` → direct subscriber + eligible descendants;
  `WorkflowCompleted` → completion subscribers; `TimerFired` skipped), and
  advances the watermark to the last processed event in the same transaction. At
  most one dispatcher works at a time; concurrent callers return immediately. The
  full subscription scan (`JobRunnerSettings.fullSubscriptionScanInterval`,
  default 1 minute) wakes every non-terminal instance holding a pending signal or
  completion subscription with a visible, unconsumed candidate. No payload filter
  is evaluated by the sender, and it does not alter cached Step results.
- No lost wakeups: the periodic full subscription scan is the liveness guarantee
  — it wakes every non-terminal instance holding a pending signal or completion
  subscription whose candidate is visible and unconsumed (a direct signal event
  after the shared cursor; an inherited event from an eligible ancestor after the
  cursor and, unless `inheritPastEvents`, after
  `inherited_events_start_sequence_id`; or the awaited instance's
  `WorkflowCompleted` event). The watermark dispatcher is only latency: it can
  miss a subscription that commits concurrently with an event and then advance
  its watermark past that event, so correctness cannot rest on the writer; only
  the independent, periodic re-scan of subscriptions guarantees no lost wakeup.
  This is why writers only append: the dispatcher gives low-latency wakeups when
  the subscription already committed before the event, and the scan covers the
  concurrent case.
- The wakeup row is removed only when a worker has obtained the workflow lease
  and accepted responsibility for that wakeup. A delivery that races with that
  removal upserts a replacement row, so an event cannot be missed while an
  instance is running.
- Wakeup claiming, the execution lease, timer firing, and the background
  sweeps are specified in running-workflows.md ("Job runner and scheduling").
- Timer deadlines are stored absolute when the subscription is registered, so
  replays never push timeouts forward. Timer subscriptions live from suspension
  until their await resolves: firing (by the sweep or by await evaluation)
  appends the `TimerFired` event under the subscription row lock and leaves the
  row in place — it is deleted only by the await's resolution or terminal
  cleanup (see "Why timer awaits need a subscription"). The sweep processes due
  timers in deadline order and uses bounded batches so timer catch-up cannot
  starve signals or workflow completions.

## Rationale summary

- Immutable global event sequence + one cursor per `(workflowInstanceId, signalKey)` solve
  no-loss, filtering, accumulation, and single-consumption semantics without
  per-event acknowledgements or child copies.
- Await = runtime-computed Step: reuses Step machinery (ids, records, manual intervention) instead of inventing parallel mechanisms.
- `race`/`all` only: covers all concrete use cases in the draft; future algebra can add derived combinators compatibly.
- Single-instance addressing keeps `send` simple. Child signal inheritance is a
  read-time projection declared on `startAsChild`; broadcast-by-instance-prefix
  remains a separate derived runtime operation.
- Explicit await ids for stability under evolution and invalidation-driven rerouting.
- Invalidation by explicit dependency only; no cascade. Means the user controls what changes trigger downstream re-work.
- **Manual runs resolve waits from stored state.** Evaluation fires its existing due timers and reads matching events. The event dispatcher and full subscription scan schedule unattended instances; the full scan also covers events missed during concurrent subscription registration.
- **Timer subscriptions retain the deadline and timer id.** The scheduler appends the firing event without deleting the subscription. Resolution, invalidation, or instance cleanup removes the subscription.
- **Why no Temporal-style always-on handlers**: Without mutable fields or a live object in scope, signal handlers can only affect execution via the value returned from an await. Since workflows re-run after every suspension, checking signals at the workflow start (or other strategic points) is often sufficient for patterns like cancellation — users decide where to check, cooperatively, rather than relying on handlers living ambient in the background. This trades Temporal-like ambient ergonomics for simpler, explicit semantics.

## Open questions and TODOs

1. **Unsatisfied await performance** — **TODO**: A suspending await now advances
   its shared exact-key cursor past every event it examined and rejected, so it no
   longer repeats the same lookup of rejected events on every rerun. What remains
   uncertain is the cost of the periodic full subscription scan (it re-examines
   every pending signal/completion subscription each
   `fullSubscriptionScanInterval`) and the inherited-lookup cost of the
   ancestor-source query for child awaits. Inspect the PostgreSQL query plans and
   benchmark whether the exact-key, ancestor-source, relationship-boundary, and
   cursor indexes make these negligible or whether an additional non-semantic scan
   hint is needed.

2. **Signal checkpoints and cross-key cursors** — **TODO**: Design an API for
accepting events only after a durable causal boundary. Cursors are currently
neither shared nor coordinated between different exact signal keys.

3. **Prefix awaits** — **TODO**: Awaiting currently requires an exact signal
key. Decide whether a typed prefix-group API has a concrete use case before
introducing cursor interaction between overlapping prefixes and exact keys.

4. **Completion delivery boundary** — **Resolved**: the unconsumed `Signal`
   events are gathered and deleted inside the terminal/continue-as-new
   transaction that sets the instance terminal, under that transaction's
   instance row lock, and the `onUnconsumedSignals` handler is invoked after the
   commit. A concurrent `send` first row-locks the same instance: if it commits
   before the transition it is included in the handler's batch; if it arrives
   after, the committed `terminal_state` makes `send` reject it as
   `InstanceAlreadyCompleted`. It is therefore never reported as delivered and
   then omitted from handling.

5. **Broadcast by key prefix** — **TODO**: Define snapshot and aggregate-result
semantics for the derived bulk operation described above.

6. **Queries**: For now, queries are ordinary functions that load a workflow
context and manually inspect persisted results by `StepId`. Result lookup is
untyped; callers deserialize or cast the value themselves. This is the
simplest mechanism and does not introduce a query-definition API.

7. **Temporal-style always-on handlers** — **DELIBERATELY OUT OF SCOPE**: Queries and Updates via handler methods live for the entire workflow lifetime, independent of execution state. We omit these because handler validity is positional (tied to a specific await-site) and no mutable fields exist to read/write. Structured concurrency via `Thread.interrupt` + explicit `race` is the alternative for cancellation/ambient patterns.

8. **Event retention** — **TODO**: Completion and continue-as-new delete
   directly addressed `Signal` events after child relationships are closed.
   Define any additional retention policy for events belonging to still-active
   workflow instances. `inheritPastEvents` is necessarily limited to events
   that remain retained.
