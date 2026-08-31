# Signals & Timers

Guideline for waiting on external events and timers.

## Core model: await is a runtime-computed step

A step's result is computed by *user code* and cached. An **await's** result is computed by the *runtime* (from persisted events) and cached in the same way. When replay reaches an await-site:

- a **decision record** already exists → return the recorded outcome (which leaf won, what it consumed). This prevents an await from re-suspending on every re-run.
- no record → evaluate the condition against the log; if satisfiable now, atomically record the decision and continue; otherwise persist a pending subscription and return `WorkflowStopped` from the outer workflow boundary.

Awaits thus reuse the entire step machinery (explicit ids, persisted results, manual intervention via reset) instead of inventing a parallel mechanism.

Several different things can be awaited:
```scala
enum Awaitable[+R : Cacheable] {
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
  
  case WorkflowCompletion(workflowInstanceId: WorkflowInstanceId) extends Awaitable[R]
  object WorkflowCompletion {
    def apply(workflowInstance: WorkflowInstance[?, R]): Awaitable[R] = 
      WorkflowCompletion(workflowInstance.instanceId)
  }
}
```

## Signal storage: append-log + per-await cursor

`setSignal` is an **append** (INSERT with monotonic `seqNr`), never an overwrite of a slot. Consequences:

- A signal arriving at any moment — including mid-execution — is durable and can only be *not yet consumed*, never lost.
- A signal is like an inbox and can contain multiple events ordered by arrival time.
- Every event entry in the DB records whether this event has been processed by anyone. An await marks an event as processed when it wants to prevent other awaits from processing the same event.
- Every await-site, persists its own cursor. The cursor records how far that await has scanned the event log (to identify which events are new). In case of `race` and `all` the cursor(s) must record position of multiple signals. The cursor could also be the place where the await's result is persisted (i.e. the offset of the event that satisfied this await). Cursors are never shared between distinct awaits.
- **Accumulation is free**: unacknowledged events remain in the log. The runtime owns the log and cursors; the workflow keeps no custom DB state (the Kafka-feeder case: an external job appends every event and never blocks).


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
  def sendSignal[A: Cacheable](instanceId: WorkflowInstanceId, key: String, value: A): SignalSendResult
}
```

- `sendSignal`does not acquire the workflow execution lease, so a
  busy or long-running workflow never blocks senders. They append only signal
  data and schedule work; they do not modify step, await-decision, or other
  execution records owned by the running workflow.
- Sending a Signal event briefly acquires a row-lock on the workflow instance row and check a field `is_accepting_signals` to prevent senders from inserting new events when the workflow is completed/completing.

Reading Signals:
```scala
object Step {
  def peekSignal[A](s: Signal[A]): Seq[A] = ???
  
  def await[A](
    stepKey: String, 
    awaitable: Awaitable[A],
    invalidateOn: Map[String, Any] = Map.empty,
    ensureUnchanged: Map[String, Any] = Map.empty,
    invalidateAfter: Duration = Duration.Inf,
  ): A = ???
}
```

- `Step.await` throw a `WorkflowSuspended` control-flow exception when the await cannot be satisfied immediately. The workflow runtime catches this exception and suspends the workflow. User code must always ignore or rethrow this exception.
- Peek reads the list for a signal, if any, without advancing the await cursor.
- Await-signal reads the log for an event that matches the filter and suspends the workflow if none is found. 
  - When suspending, it registers a subscription/wake-up for that signal with the runtime. The filter is evaluated in workflow code. 
  - The runtime cannot know which event will satisfy the filter, so the runtime must wake the workflow when any new event arrives. 
  - If an event matches, it is atomically acknowledged and returned. The await must persist what the result of the await was for future replays.
  - The await advances its cursor whenever it is called and scans forward

## Racing signals/timers

`Step.awaitRace` allows racing multiple signals and timers. This is less flexible than racing arbitrary code but much safer: `Step.awaitRace` guarantees that whatever event/timer occured first will definitely win the race, whereas the result of racing arbitrary code with `Step.firstToRunWithoutSuspension` literally depends on when the code is run. 

```scala
object Step {
    def awaitRace[A](
        stepKey: String,
        invalidateOn: Map[String, Any] = Map.empty,
        ensureUnchanged: Map[String, Any] = Map.empty
    )(awaits: Awaitable[A]*): A = ???
}
```

`awaitRace` creates one subscription for each awaitable in the database.

## Updates: signals that return a value

An **Update** is a `Signal` with a **synchronous response channel**. The purpose of updates is to give some synchronous interactivity to workflows (for example: updating a user's name which requires timely validation), but the concept is not well-thought-out yet. 

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
      persistUnhandledUpdates: Boolean = false // keep unhandled in log or drop?
  ): UpdateResult[R]
}
```
- **idempotencyKey**: runtime deduplicates by this key; same key arriving twice → second is idempotent, sender gets the same result.
- **persistUnhandledUpdates**: if `true`, an unhandled update stays in the signal log (unconsumed); if `false`, it is deleted after workflow completion.

Responding to updates:
```scala
object Step {
  def awaitUpdate[I, R, O](stepKey: String, u: Update[I, R])(respond: I => (R, O)): O = ???
}
```

### Manual advancement of the cursor

Sometimes we want to ensure that we react only to events that arrived after some other step:

```scala 
Step("request-approval") {
  requestApproval(item)
}

Step.await(itemApprovedSignal) // should only accept approval events that happened after the approval request!
```

One option for this is to manually mark all events (matching the signal key filter) up to that point as processed:

```scala
object Step {
  def markAllSignalEventsAsProcessed(
    stepKey: String, 
    signalKeyPrefix: String,
    invalidateOn: Map[String, Any] = Map.empty,
    ensureUnchanged: Map[String, Any] = Map.empty
  ): Unit
}
```

```scala 
Step("request-approval") {
  requestApproval(item)
}

Step.markAllSignalEventsAsProcessed("mark-events-processed", signalKeyPrefix = "approval-signal")

Step.await(itemApprovedSignal)
```

The step key ensures that this is not executed again on every re-run of the workflow body. 

Another option is to use `lookBack` on the signal awaitable, which will ignore events that arrived before a certain time. Unfortunately, both solutions have potential for race conditions.

## Queries

Queries are functions that can synchronously ask about the current workflow state (ideally without executing the workflow). They exist in Temporal but are deliberately out of scope in atomicflow for now, since all our state is only readable from local variables in the workflow body.

## Awaiting workflow completion

The library provides a function to await completion of other workflows.

## Unconsumed signals at completion

Every workflow can have an onOnconsumedSignals-handler function, which is mainly to be used for logging puroses.

A signal/update arriving after execution passed its await-site is not lost — it remains in the event log.

- **Default: ignore.** Unconsumed events at completion are discarded unless a handler is declared.
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
  The handler receives `Map[SignalId, Seq[Any]]` where the values are unconsumed events for each signal.
- The onUnconsumedSignals handler is a regular function (may not contain `Step`s) and executed at-least-once.
- A workflow that is completing sets `is_accepting_signals := false` before executing the unprocessed signals handler. If `is_accepting_signals == true` then the `send` function returns `InstanceAlreadyCompleted`.

## Invalidation and await stability

When an upstream step is invalidated (TTL expired, `invalidateOn` dependency changed), downstream constructs are invalidated **only if they explicitly list the changed value in their `invalidateOn` cache-key**. No automatic cascade.

**Awaits inherit this rule:** an await can declare `invalidateOn` dependencies just as steps do:

```scala
Step.awaitRace("approval-race", approval, Timer(deadline),ne),
  invalidateOn = Seq("context" -> someContextValue)
)
```

If `someContextValue` changes, the await's decision record is discarded; on the next replay it re-evaluates from scratch.

### Timer invalidation

A timer whose expression (duration/deadline) is keyed on an invalidated upstream value recomputes from scratch:
- If a timer already fired and then its deadline recomputes to a *later* moment, the timer is treated as if it had **never executed** — the workflow suspends again waiting for the new deadline.

### Signal invalidation

- Events consumed by an await before invalidation remain acknowledged.
- The invalidated await retains its previous cursor and resumes scanning after
  that position. Because the cursor may be far in the past, it may consume an
  unacknowledged event that arrived before the invalidation.
- If invalidation changes the filter, events this await previously scanned past
  are not reconsidered. Explicit cursor movement or reset semantics would be
  required to rescan them.

## Persistence model (sketch) — **NOT A FINAL DECISION**

Four tables for one possible runtime implementation (not part of the public API). This design may change based on resolution of open questions:

```
signal_events (instanceId, signalId, seqNr, payload, arrivedAt, acknowledged) -- append-only log
await         (instanceId, awaitId, mode: Race|All, status, decidedAt)
await_leaf    (instanceId, awaitId, leafIdx, source, cursorSeqNr, satisfied, consumedSeqNr)
                 source = Signal(signalId) | Timer(deadline) | Completion(childId)
workflow_wakeup (instanceId PRIMARY KEY, scheduledAt)               -- one pending wakeup
```

- `signal_events` is append-only; `setSignal` is an INSERT.
- Each signal event stores its acknowledged state. For every await form, event
  acknowledgement, await result/decision persistence, and cursor advancement
  happen in one atomic transaction.
- `await_leaf` (with `mode=All` or `mode=Race`) indexes condition leaves per await-site.
- Wake-on-relevant-events: an incoming event checks only *pending* `await_leaf`
  rows for that source; no pending leaf → no wake. For a matching source, it
  upserts the instance's durable wakeup record. The sender does not decide
  filtered satisfaction or alter the await decision; the awakened workflow does.
- No lost wake-ups: on suspend, register subscription, re-scan the log from the await's cursor before committing; on deliver, append then check-and-schedule.
- The wakeup row is removed only when a worker has obtained the workflow lease
  and accepted responsibility for that wakeup. A delivery that races with that
  removal upserts a replacement row, so an event cannot be missed while an
  instance is running.
- Timer deadlines are stored absolute (computed once at first registration), so replays never push timeouts forward.

## Rationale summary

- Log + per-await cursor and per-event acknowledgement solve no-loss,
  filtering, accumulation, and single-consumption semantics in one primitive.
- Await = runtime-computed step: reuses step machinery (ids, records, manual intervention) instead of inventing parallel mechanisms.
- `race`/`all` only: covers all concrete use cases in the draft; future algebra can add derived combinators compatibly.
- Single-instance addressing keeps the primitive simple; broadcast-by-prefix is a derived runtime operation.
- Explicit await ids for stability under evolution and invalidation-driven rerouting.
- Invalidation by explicit dependency only; no cascade. Means the user controls what changes trigger downstream re-work.
- **Why no Temporal-style always-on handlers**: Without mutable fields or a live object in scope, signal handlers can only affect execution via the value returned from an await. Since workflows re-run after every suspension, checking signals at the workflow start (or other strategic points) is often sufficient for patterns like cancellation — users decide where to check, cooperatively, rather than relying on handlers living ambient in the background. This trades Temporal-like ambient ergonomics for simpler, explicit semantics.

## Open questions and TODOs

1. **Advance one await to now** — **TODO**: Design an API for moving an await's
cursor to the current log end when pre-existing events must be ignored.

2. **Racing code with signals** — **TODO**: Design an API for racing arbitrary code (Activities, computation) against signals using Ox or similar concurrency library. Define cancellation, crash, and replay semantics.

3. **Completion delivery boundary** — **TODO**: Define whether a signal that
arrives concurrently with the unconsumed-events handler is accepted into that
handler's batch or rejected as `InstanceAlreadyCompleted`. It must not be
reported as delivered and then omitted from handling.

4. **Broadcast by key prefix** — **TODO**: Define snapshot and aggregate-result
semantics for the derived bulk operation described above.

5. **Queries**: For now, queries are ordinary functions that load a workflow
context and manually inspect persisted results by `StepId`. Result lookup is
untyped; callers deserialize or cast the value themselves. This is the
simplest mechanism and does not introduce a query-definition API.

6. **Temporal-style always-on handlers** — **DELIBERATELY OUT OF SCOPE**: Queries and Updates via handler methods live for the entire workflow lifetime, independent of execution state. We omit these because handler validity is positional (tied to a specific await-site) and no mutable fields exist to read/write. Structured concurrency via `Thread.interrupt` + explicit `race` is the alternative for cancellation/ambient patterns.
