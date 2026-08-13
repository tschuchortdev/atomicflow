# Signals & Timers

Guideline for waiting on external events and handling concurrent interruption. Companion to `design.md`, `steps.md`, `core-types.md`, `running-workflows.md`.

## Core model: await is a runtime-computed step

A step's result is computed by *user code* and cached. An **await's** result is computed by the *runtime* (from persisted events) and cached in the same way. When replay reaches an await-site:

- a **decision record** already exists → return the recorded outcome (which leaf won, what it consumed). This prevents an await from re-suspending on every second run.
- no record → evaluate the condition against the log; if satisfiable now, atomically record the decision and continue; otherwise persist a pending subscription and **suspend** (`Left(StoppedWorkflow)`).

Awaits thus reuse the entire step machinery (explicit ids, persisted results, manual intervention via reset) instead of inventing a parallel mechanism.

## Signal storage: append-log + cursor

`setSignal` is an **append** (INSERT with monotonic `seqNr`), never an overwrite of a slot. Consequences:

- A signal arriving at any moment — including mid-execution — is durable and can only be *not yet consumed*, never lost.
- **Accumulation is free**: unconsumed events are just the gap between the log's head and the last consumed position. The runtime owns both; the workflow keeps no custom DB state (the Kafka-feeder case: an external job appends every event, never blocks, never accumulates).
- Two **views** over the one primitive:
  - `Signal.stream[T]("id")` — full log, supports `await`, `awaitBatch`, `fold`. Events are consumed by read position and not replayed.
  - `Signal.state[T]("id")` — compacted "latest value" only. Multiple independent awaits on the same state signal do not interfere. Older entries may be garbage-collected.

```scala
val approval = Signal.state[Approval]("approval")
val orders   = Signal.stream[OrderEvent]("orders", onUnconsumed = deadLetter)  // handler optional
```

## Awaits: explicit ids, race / all

Every consuming await takes a **mandatory explicit id** (like `stepId`). This keeps the decision record and consumed range stable across replays and across invalidation-driven control-flow rerouting.

Two combinators over a heterogeneous mix of `Signal | Timer | completion`:

```scala
val a: Approval = approval.await("await-approval")

val decision: Approval | TimerFired =
  Wait.race("decision", approval, Timer(3.days))   // first to fire wins

val (a, r): (Approval, Review) =
  Wait.all("both", approval, review)               // wake once, when all leaves are satisfied

val results: Seq[ItemResult] =
  Wait.all("fan-in", children.map(_.completion))   // homogeneous overload
```

- **`race` result type is determined by the first winning leaf.** Pattern matching on source identity is not directly supported (union types of signals with extractors don't compose). The typical pattern is to use direct equality checks on the outcome:
  ```scala
  val decision = Wait.race("d", approval, rejection)
  if (decision.signalId == approval.id) {
    val a: Approval = decision.asInstanceOf[Approval]
    ...
  } else {
    val r: Rejection = decision.asInstanceOf[Rejection]
    ...
  }
  ```
  Or map each leaf to a tagged type before racing:
  ```scala
  Wait.race("d", 
    approval.map(a => ApprovedResult(a)), 
    rejection.map(r => RejectedResult(r))
  ) match
    case ApprovedResult(a)  => ...
    case RejectedResult(r)  => ...
  ```
- **Winner is decided at event time, not replay time**: when a subscription flips satisfied, the runtime records which leaf won and what it consumed. Replay reads that decision, so late re-runs can never flip the winner.
- **AND is first-class**: `all` writes N await-leaf rows with `mode=All`; the await flips only when every sibling leaf is satisfied.

## Updates: signals that return a value

An **Update** is a `Signal` with a **response channel**. The sender is blocked until one of:
- an await-site on that update signal consumes it and the handler returns a value → sender receives it synchronously.
- the workflow completes without consuming it → sender receives `Left` (unhandled).

```scala
val newPriority: Priority =
  priorityUpdate.await("handle-priority-update") { priority =>
    // sender will receive this return value
    priority
  }
// execution continues with the value from the update
```

Update sending includes two parameters:

```scala
runtime.sendUpdate(
  instanceId, 
  priorityUpdate, 
  newPriority,
  idempotencyKey = "order-123-adj-attempt-1",  // runtime deduplicates
  persistUnhandledUpdates = true                // keep unhandled in log or drop?
)
```

- **idempotencyKey**: runtime deduplicates by this key; same key arriving twice → second is idempotent, sender gets the same result.
- **persistUnhandledUpdates**: if `true`, an unhandled update stays in the signal log (unconsumed); if `false`, it is deleted after workflow completion. Default depends on the signal's `atLeastOnce`/`atMostOnce` axis (shared with steps).

## Unconsumed signals & updates at completion

A signal/update arriving after execution passed its await-site is not lost — it sits unconsumed in the log. Detection is cursor-based (gap between log head and last consumed position).

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
        if (signalId == orders.id)
          Step.atLeastOnce("dead-letter", 1) { sendToDeadLetter(events.asInstanceOf[Seq[OrderEvent]]) }
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
- `setSignal` / `sendUpdate` are **lock-free** (don't acquire the execution lease), so busy or long-running workflows never block senders. They return `Delivered` or `InstanceAlreadyCompleted`, so senders learn the one fact knowable without executing workflow logic.

## Invalidation and await stability

When an upstream step is invalidated (TTL expired, `invalidateOn` dependency changed), downstream constructs are invalidated **only if they explicitly list the changed value in their `invalidateOn` cache-key**. No automatic cascade.

**Awaits inherit this rule:** an await can declare `invalidateOn` dependencies just as steps do:

```scala
Wait.all("approval-race", approval, Timer(deadline),
  invalidateOn = Seq("context" -> someContextValue)
)
```

If `someContextValue` changes, the await's decision record is discarded; on the next replay it re-evaluates from scratch.

### Timer invalidation

A timer whose expression (duration/deadline) is keyed on an invalidated upstream value recomputes from scratch:
- If a timer already fired and then its deadline recomputes to a *later* moment, the timer is treated as if it had **never executed** — the workflow suspends again waiting for the new deadline.

### Signal invalidation — **UNRESOLVED**

When a stream-signal await is invalidated, the semantics of consumed events and cursor position are **unresolved**. Options:

- **Ignore all pre-invalidation events**: cursor resets to the log head at invalidation time. Means unconsumed events before invalidation are lost.
- **Ignore only unconsumed events**: keep the previous consumed range, only discard events between the old consumed position and invalidation time that didn't match. Means some old events might be re-scanned.
- **Other**: TBD.

This is a genuine gap in the model and needs concrete use cases to decide. Deferred to next design session.

## Persistence model (sketch) — **NOT A FINAL DECISION**

Three tables for one possible runtime implementation (not part of the public API). This design may change based on resolution of open questions:

```
signal_events (instanceId, signalId, seqNr, payload, arrivedAt)      -- append-only log
await         (instanceId, awaitId, mode: Race|All, status, decidedAt)
await_leaf    (instanceId, awaitId, leafIdx, source, satisfied, consumedSeqNr)
                 source = Signal(signalId) | Timer(deadline) | Completion(childId)
```

- `signal_events` is append-only; `setSignal` is an INSERT.
- `await_leaf` (with `mode=All` or `mode=Race`) indexes condition leaves per await-site.
- Wake-on-relevant-events: an incoming event joins only against *pending* `await_leaf` rows for that source; no pending leaf → no wake. An instance wakes only when one of its currently-registered awaits actually flips.
- No lost wake-ups: on suspend, register subscription, re-scan log from cursor before committing; on deliver, append then check-and-schedule.
- Timer deadlines are stored absolute (computed once at first registration), so replays never push timeouts forward.

## Rationale summary

- Log + cursor solves no-loss, accumulation, and completion semantics in one primitive.
- Await = runtime-computed step: reuses step machinery (ids, records, manual intervention) instead of inventing parallel mechanisms.
- `race`/`all` only: covers all concrete use cases in the draft; future algebra can add derived combinators compatibly.
- Single-instance addressing keeps the primitive simple; broadcast-by-prefix is a derived runtime operation.
- Explicit await ids for stability under evolution and invalidation-driven rerouting.
- Invalidation by explicit dependency only; no cascade. Means the user controls what changes trigger downstream re-work.
- **Why no Temporal-style always-on handlers**: Without mutable fields or a live object in scope, signal handlers can only affect execution via the value returned from an await. Since workflows re-run after every suspension, checking signals at the workflow start (or other strategic points) is often sufficient for patterns like cancellation — users decide where to check, cooperatively, rather than relying on handlers living ambient in the background. This trades Temporal-like ambient ergonomics for simpler, explicit semantics.

## Open questions and TODOs

1. **Signal/stream cursor granularity** — **UNRESOLVED**: When multiple independent awaits consume from the same stream signal, do they share one cursor (first await advances it, blocking later awaits) or maintain independent read positions (Kafka consumer-group style)? The problem: an earlier await can consume events a later await needs. Deferred to next session.

2. **Signal invalidation semantics** — **UNRESOLVED**: When a stream-signal await is invalidated, what happens to consumed events and the cursor? Deferred to next session.

3. **Signal filter predicates** — **UNRESOLVED**: Do signal awaits need an optional filter lambda parameter, so the await is only satisfied when the signal value satisfies a condition? How would that interact with "winner is decided at event time"? If the predicate changes during invalidation, how is it re-evaluated? Deferred to next session.

4. **Racing code with signals** — **TODO**: Design an API for racing arbitrary code (Activities, computation) against signals using Ox or similar concurrency library. Ensure the pattern is expressible.

5. **Queries** — **OUT OF SCOPE FOR NOW**: Providing `runtime.getStepResult[T: Cacheable](instanceId, stepId): Option[T]` as a building block for a future query mechanism (separate from workflow code, matching Temporal's query semantics). Full query design deferred.

6. **Temporal-style always-on handlers** — **DELIBERATELY OUT OF SCOPE**: Queries and Updates via handler methods live for the entire workflow lifetime, independent of execution state. We omit these because handler validity is positional (tied to a specific await-site) and no mutable fields exist to read/write. Structured concurrency via `Thread.interrupt` + explicit `race` is the alternative for cancellation/ambient patterns.

