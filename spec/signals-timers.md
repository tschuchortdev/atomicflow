# Signals & Timers

Guideline for waiting on external events and timers.

## Core model: await is a runtime-computed step

A step's result is computed by *user code* and cached. An **await's** result is computed by the *runtime* (from persisted events) and cached in the same `workflow_steps` table. When replay reaches an await-site:

- a completed Step row already exists → return its recorded result. This prevents an await from re-suspending on every re-run.
- no completed row exists → evaluate the condition against the event stream; if satisfiable now, atomically persist the result and continue; otherwise persist pending subscriptions and return `WorkflowSuspended` from the outer workflow boundary.

Awaits thus reuse the entire step machinery (explicit ids, persisted results, manual intervention via reset) instead of inventing a parallel mechanism.

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

/* InstanceAlreadyCompleted is a first-class return type instead of an exception like WorkflowNotFoundException, because we want to make it clear to the user that this case has to be handled. */
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
  busy or long-running workflow never blocks senders. They append only signal
  data and schedule work; they do not modify Step results or other
  execution records owned by the running workflow.
- Sending a Signal event briefly acquires a row lock on the addressed workflow
  instance and checks `is_accepting_signals` to prevent appending an event when
  that instance is completed or completing. It then acquires the global event
  allocator mutex (pg_advisory_lock in postgres), obtains the next `sequenceId` (`SEQUENCE CACHE 1` in postgres), appends the event, schedules
  relevant wakeups, and commits.
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

- `Step.await` throws a `WorkflowSuspended` control-flow exception when the
  await cannot be satisfied immediately. The workflow runtime catches this
  exception and suspends the workflow. User code must always ignore or rethrow
  this exception.
- Peek reads currently visible events after the shared exact-key cursor without
  advancing it.
- Await-signal searches visible events for the exact signal key after the
  workflow instance's shared cursor and suspends if none satisfies the filter.
  - Child workflows also search events addressed to allowed ancestors using the
    current inheritance configuration; see `child-signal-inheritance.md`.
   - When suspending, it registers subscriptions/wakeups for the currently
     visible sources. The filter is evaluated in workflow code, so any new event
     for the key wakes the workflow for reevaluation.
   - If an event matches, advancing the shared cursor and persisting the
     successful await Step row happen atomically before its value is returned.
  - If a matching event is found, moving the cursor to that event also skips
    every earlier event of the key that the filter rejected. **The Javadoc must
    clearly state that an await with a filter may skip past unprocessed events:
    when it returns a matching event, all earlier rejected events become
    permanently unavailable to later awaits of the same key in this workflow.**
  - If no event satisfies the filter, the await leaves the cursor unchanged,
   including after examining rejected events. This allows accumulated events,
   including events made visible by a later inheritance-policy change, to
    satisfy it on a future run.

## Racing signals/timers

`Step.awaitRace` allows racing multiple signals and timers. This is less flexible than racing arbitrary code but much safer: `Step.awaitRace` guarantees that whatever event/timer occured first will definitely win the race, whereas the result of racing arbitrary code with `Step.firstToRunWithoutSuspension` literally depends on when the code is run. 

```scala
object Step {
    def awaitRace[A](
        stepKey: String,
        invalidateOn: Seq[StepInput[?]] = Seq.empty,
        ensureUnchanged: Seq[StepInput[?]] = Seq.empty
    )(awaits: Awaitable[A]*): A = ???
}
```

`awaitRace` creates one subscription for each awaitable in the database.

- Each signal leaf retains the cursor for its own exact key; `awaitRace` does
  not introduce a cursor shared between keys.
- All candidates are compared by global `sequenceId`, so the earliest
  satisfying signal, timer, or completion event wins even when the events
  belong to unrelated workflow trees.
- When a race succeeds, only the winning signal key's cursor advances. The
  successful Step row and that cursor movement are persisted atomically. If no
  candidate is satisfiable, none of the signal cursors advance.

## Updates: signals that return a value

An **Update** is a `Signal` with a **synchronous response channel**. The purpose of updates is to give some synchronous interactivity to workflows (for example: updating a user's name which requires timely validation), but the concept is not well-thought-out yet. 

Updates are always addressed directly to one workflow instance. They are not
inherited by children: a synchronous request cannot have one
unambiguous response if several descendants handle it.

When an Update is sent, the runtime checks if there is a subscription/await currently registered for that Update, runs the workflow and returns a result, if that run handled the update.

```
:beginning

if (workflow is completed) {
  return InstanceAlreadyCompleted to sender
}

if (update key is currently subscribed/awaited) {
  if (workflow is locked/already running) {
    wait for lease to run out
    goto beginning
  } else {
    run workflow on the sender's thread, until run finished
    
    if (result is set on update's DB record) {
      return Success(result) to sender
    } else {
      return Unhandled to sender
    }
  }
}
```

```scala
class Update[I, R](val key: String)(using val inputCacheable: Cacheable[I], responseCacheable: Cacheable[R])

val nameUpdate = Update[String, ValidationResult]("name-update")
```

Sending updates:
```scala
enum UpdateSendResult[+R] {
  case Success(value: R)
  case Unhandled
  case InstanceAlreadyCompleted
}

class Update[I, R](...) {
  @throws[WorkflowNotFoundException]
  def send(workflowInstanceId: WorkflowInstanceId, input: I): UpdateSendResult[R] = ???
}

class WorkflowInstanceId(...) {
  @throws[WorkflowNotFoundException]
  def sendUpdate[I, R](u: Update[I, R], input: I): UpdateSendResult[R] = ???
}

class WorkflowInstance[In, Out](...) {
  @throws[WorkflowNotFoundException]
  def sendUpdate[I, R](u: Update[I, R], input: I): UpdateSendResult[R] = ???
}

trait WorkflowRuntime {
  @throws[WorkflowNotFoundException]
  def sendUpdate[I: Cacheable, R: Cacheable](
      instanceId: WorkflowInstanceId,
      updateKey: String,
      input: I,
      idempotencyKey: String = "", // runtime deduplicates
      persistUnhandledUpdates: Boolean = false // keep unhandled persisted or drop?
  ): UpdateResult[R]
}
```
- **idempotencyKey**: runtime deduplicates by this key; same key arriving twice → second is idempotent, sender gets the same result.
- **persistUnhandledUpdates**: if `true`, an unhandled update remains persisted
  for later handling; if `false`, it is deleted after workflow completion.

Responding to updates:
```scala
object Step {
  def awaitUpdate[I, R, O](stepKey: String, u: Update[I, R])(respond: I => (R, O)): O = ???
}
```

### Causal boundaries between signals

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

A signal/update arriving after execution passed its await-site is not lost: the
immutable fact remains available until workflow completion, continue-as-new, or
another retention boundary removes it.

- **Default: ignore.** Unless a handler is declared, unconsumed signals are
  ignored. After handling, child relationships are closed and directly
  addressed `Signal` events are deleted. Children retain cached Step results for
  inherited signals they already consumed, so replay does not require the
  source rows.
- Optional `onUnconsumedSignals` handler can be passed to the `Workflow` constructor in the same parameter group as the body. It runs as a final step before the instance is marked complete and receives a map of all unconsumed signals:
  ```scala
  val wf = Workflow("order")(
    (order: Order) => {
      val orderEvents = orders.await("read-orders")
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
  The handler receives `Map[SignalKey, Seq[Any]]` where the values are currently
  visible events after this workflow instance's cursor for each exact signal
  key.
- The onUnconsumedSignals handler is a regular function (may not contain `Step`s) and executed at-least-once.
- A workflow that is completing sets `is_accepting_signals := false` before
  executing the unconsumed-signals handler. If signals are no longer accepted,
  `send` returns `InstanceAlreadyCompleted`.

## Invalidation and await stability

When an upstream step is invalidated (TTL expired, `invalidateOn` dependency changed), downstream constructs are invalidated **only if they explicitly list the changed value in their `invalidateOn` cache-key**. No automatic cascade.

**Awaits inherit this rule:** an await can declare `invalidateOn` dependencies just as steps do:

```scala
Step.awaitRace("approval-race", approval, Awaitable.Timer(deadline)),
  invalidateOn = Seq("context" -> someContextValue)
)
```

If `someContextValue` changes, the await's cached Step result is discarded; on the next replay it re-evaluates from scratch.

### Timer invalidation

A timer whose expression (duration/deadline) is keyed on an invalidated upstream value recomputes from scratch:
- If a timer already fired and then its deadline recomputes to a *later* moment, the timer is treated as if it had **never executed** — the workflow suspends again waiting for the new deadline.

### Signal invalidation

- Events consumed by an await, and earlier events skipped when its filter found
  that result, remain behind the workflow instance's shared cursor for that
  exact signal key.
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
  workflowInstanceId, stepId, leafIdx, signalKey
)
workflow_timer_subscriptions (
  workflowInstanceId, stepId, leafIdx, subscriptionId, deadline
)
workflow_completion_subscriptions (
  workflowInstanceId, stepId, leafIdx, completedWorkflowInstanceId
)
workflow_wakeups (workflowInstanceId PRIMARY KEY, scheduledAt)
```

`workflow_events` has one non-null envelope shape for every event kind:

| Event kind | `workflowInstanceId` | `eventKey` | `payload` |
|---|---|---|---|
| `Signal` | The addressed instance | Exact signal key | Serialized signal value |
| `TimerFired` | The instance owning the timer | Timer subscription ID | Encoded `Unit` |
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
  sequence allocation, event insertion, precomputed wakeup writes, and commit;
  never serialize payloads, run user code, or make network calls while holding
  it. Monitor lock wait time, append latency, and connection-pool pressure, and
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
  rows. A timer subscription holds its absolute deadline; when the scheduler
  fires it, it appends a `TimerFired` event. Workflow completion appends one
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
- Wake-on-relevant-events: an incoming event checks the appropriate pending
  subscription table and upserts a coalesced `workflow_wakeups` row. The sender
  does not evaluate payload filters or alter cached Step results.
- No lost wakeups: on suspension, register source-specific subscriptions and
  recheck `workflow_events` from the shared exact-key cursor before committing;
  on append, insert the event then check and schedule interested workflows.
- The wakeup row is removed only when a worker has obtained the workflow lease
  and accepted responsibility for that wakeup. A delivery that races with that
  removal upserts a replacement row, so an event cannot be missed while an
  instance is running.
- Timer deadlines are stored absolute when the subscription is registered, so
  replays never push timeouts forward. The scheduler appends due timers in
  deadline order and uses bounded batches so timer catch-up cannot starve
  signals or workflow completions.

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
- **Why no Temporal-style always-on handlers**: Without mutable fields or a live object in scope, signal handlers can only affect execution via the value returned from an await. Since workflows re-run after every suspension, checking signals at the workflow start (or other strategic points) is often sufficient for patterns like cancellation — users decide where to check, cooperatively, rather than relying on handlers living ambient in the background. This trades Temporal-like ambient ergonomics for simpler, explicit semantics.

## Open questions and TODOs

1. **Unsatisfied await performance** — **TODO**: An unsatisfied await does not
advance its shared exact-key cursor, so it may repeat the same empty lookup on
every workflow rerun. Inspect the PostgreSQL query plan and benchmark whether
the exact-key, ancestor-source, relationship-boundary, and cursor indexes make
this negligible or whether an additional non-semantic scan hint is needed. A
large set of same-key events rejected by a user-code filter is the worst case,
because every rerun may reload and evaluate the same payloads.

2. **Signal checkpoints and cross-key cursors** — **TODO**: Design an API for
accepting events only after a durable causal boundary. Cursors are currently
neither shared nor coordinated between different exact signal keys.

3. **Prefix awaits** — **TODO**: Awaiting currently requires an exact signal
key. Decide whether a typed prefix-group API has a concrete use case before
introducing cursor interaction between overlapping prefixes and exact keys.

4. **Completion delivery boundary** — **TODO**: Define whether a signal that
arrives concurrently with the unconsumed-events handler is accepted into that
handler's batch or rejected as `InstanceAlreadyCompleted`. It must not be
reported as delivered and then omitted from handling.

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
