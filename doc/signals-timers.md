# Signals & Timers

Guideline for waiting on external events and handling concurrent interruption. Companion to `design.md`, `steps.md`, `core-types.md`, `running-workflows.md`.

## Core model: await is a runtime-computed step

A step's result is computed by *user code* and cached. An **await's** result is computed by the *runtime* (from persisted events) and cached in the same way. When replay reaches an await-site:

- a **decision record** already exists → return the recorded outcome (which leaf won, what it consumed). This prevents an await from re-suspending on every second run.
- no record → evaluate the condition against the log; if satisfiable now, atomically record the decision and continue; otherwise persist a pending subscription and return `WorkflowStopped` from the outer workflow boundary.

Awaits thus reuse the entire step machinery (explicit ids, persisted results, manual intervention via reset) instead of inventing a parallel mechanism.

## Signal storage: append-log + per-await cursor

`setSignal` is an **append** (INSERT with monotonic `seqNr`), never an overwrite of a slot. Consequences:

- A signal arriving at any moment — including mid-execution — is durable and can only be *not yet consumed*, never lost.
- Every stream event records whether it has been acknowledged. An acknowledged
  event cannot be consumed by another await. `Signal.state` is a compacted,
  non-consuming view, so independent state awaits do not acknowledge away its
  latest value.
- Every await-site, including `Wait.race`, persists its own cursor. The cursor
  records how far that await has scanned and identifies the event whose payload
  satisfies it. There is no shared cursor for a signal.
- **Accumulation is free**: unacknowledged events remain in the log. The runtime owns the log and cursors; the workflow keeps no custom DB state (the Kafka-feeder case: an external job appends every event and never blocks).
- Two **views** over the one primitive:
  - `Signal.stream[T]("id")` — full log, supports `await`, `awaitBatch`, `fold`. Events are consumed by read position and not replayed.
  - `Signal.state[T]("id")` — compacted "latest value" only. Multiple independent awaits on the same state signal do not interfere. Older entries may be garbage-collected.

```scala
val approval = Signal.state[Approval]("approval")
val orders   = Signal.stream[OrderEvent]("orders", onUnconsumed = deadLetter)  // handler optional
```

### Filtering

Every signal await accepts a workflow-code predicate:

```scala
val paid = payments.await("paid", filter = _.status == Paid)
val large = orders.await("large-order", filter = _.total >= 1000)
```

- The backend wakes the workflow when an event arrives; it does not evaluate
  arbitrary workflow code while the workflow is dormant.
- During the workflow run, the await scans forward from its cursor and executes
  `filter: A => Boolean`, whose default is `_ => true`.
- A matching event is atomically acknowledged and returned. A rejected event is
  not acknowledged, although this await advances its own cursor past it, so a
  different await may still consume it.

## Awaits: explicit ids, race / all

Every consuming await takes a **mandatory explicit id** (like `stepId`). This keeps the decision record and consumed range stable across replays and across invalidation-driven control-flow rerouting.

Two combinators over a heterogeneous mix of `Signal | Timer | completion`:

```scala
val a: Approval = approval.await("await-approval")

val decision: RaceWinner[Approval | TimerFired] =
  Wait.race("decision", approval, Timer(3.days))   // first to fire wins

val (a, r): (Approval, Review) =
  Wait.all("both", approval, review)               // wake once, when all leaves are satisfied

val results: Seq[ItemResult] =
  Wait.all("fan-in", children.map(_.completion))   // homogeneous overload
```

- **`race` returns both source identity and value.** `RaceWinner[A]` contains a
  `source` and `value: A`; for heterogeneous leaves, `A` is their union. The
  typical pattern is to use the source before narrowing the value:
  ```scala
  val decision = Wait.race("d", approval, rejection)
  if (decision.source == approval.id) {
    val a: Approval = decision.value.asInstanceOf[Approval]
    ...
  } else {
    val r: Rejection = decision.value.asInstanceOf[Rejection]
    ...
  }
  ```
  Or map each leaf to a tagged type before racing:
  ```scala
  Wait.race("d", 
    approval.map(a => ApprovedResult(a)), 
    rejection.map(r => RejectedResult(r))
  ).value match
    case ApprovedResult(a)  => ...
    case RejectedResult(r)  => ...
  ```
- **Winner is decided once, during the awakened workflow run**: event arrival
  wakes the workflow, then signal filters execute in workflow code. The runtime
  atomically records the eligible winning leaf and what it consumed. Replay
  reads that decision, so later runs cannot flip the winner.
- **AND is first-class**: `all` writes N await-leaf rows with `mode=All`; the await flips only when every sibling leaf is satisfied. Selecting and acknowledging all signal events for a satisfied `all` is one atomic decision, so a competing await cannot take only part of its inputs.
- `race` currently accepts durable wait sources: signals, timers, and workflow
  completions. TODO: design racing these sources against regular code or
  Activities. Regular code has different crash, cancellation, and replay
  semantics and cannot safely be treated as another persisted await leaf.

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

A signal/update arriving after execution passed its await-site is not lost — it remains unacknowledged in the log.

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

## Broadcast by key prefix

Broadcast is a derived bulk runtime operation from the original design draft:
append the same signal to every instance of one workflow whose instance key
matches a prefix.

```scala
runtime.setSignalByPrefix(workflow.id, tenantPrefix, maintenanceSignal, Enabled)
```

It is convenience over enumerating matching instances and calling `setSignal`;
it is not a different signal-delivery primitive.

- TODO: define whether matching instances are selected from one snapshot.
- TODO: define the aggregate result when some instances are already complete or
  complete concurrently, and whether instances created during the operation
  are included.

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

### Signal invalidation

- Events consumed by an await before invalidation remain acknowledged.
- The invalidated await retains its previous cursor and resumes scanning after
  that position. Because the cursor may be far in the past, it may consume an
  unacknowledged event that arrived before the invalidation.
- TODO: provide an API that advances one await's cursor to the current end of
  the log when only events arriving after a chosen point are valid. There is no
  signal-wide cursor to advance because each await has an independent cursor.
- If invalidation changes the filter, events this await previously scanned past
  are not reconsidered. Explicit cursor movement or reset semantics would be
  required to rescan them.

## Persistence model (sketch) — **NOT A FINAL DECISION**

Three tables for one possible runtime implementation (not part of the public API). This design may change based on resolution of open questions:

```
signal_events (instanceId, signalId, seqNr, payload, arrivedAt, acknowledged) -- append-only log
await         (instanceId, awaitId, mode: Race|All, status, decidedAt)
await_leaf    (instanceId, awaitId, leafIdx, source, cursorSeqNr, satisfied, consumedSeqNr)
                 source = Signal(signalId) | Timer(deadline) | Completion(childId)
```

- `signal_events` is append-only; `setSignal` is an INSERT.
- Each signal event stores its acknowledged state; acknowledging an event and
  recording the await decision happen atomically.
- `await_leaf` (with `mode=All` or `mode=Race`) indexes condition leaves per await-site.
- Wake-on-relevant-events: an incoming event joins only against *pending* `await_leaf` rows for that source; no pending leaf → no wake. An instance wakes only when one of its currently-registered awaits actually flips.
- No lost wake-ups: on suspend, register subscription, re-scan the log from the await's cursor before committing; on deliver, append then check-and-schedule.
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
