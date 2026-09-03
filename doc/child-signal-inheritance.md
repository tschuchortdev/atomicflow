# Child Signal Inheritance

Guideline for making signals sent to a workflow visible to its children while
keeping derived child identities private. Event storage and cursor semantics
are defined in `signals-timers.md`; the child API is also shown in
`sub-workflows-iteration.md`.

## API

```scala
workerWorkflow.startAsChild(
  childKey,
  input,
  inheritSignals = SignalInheritance.some("approval/", "order/cancelled"),
  inheritPastEvents = true
)

workerWorkflow.startAsChild(
  childKey,
  input,
  inheritSignals = SignalInheritance.all
)
```

- Signal inheritance defaults to none and `inheritPastEvents` defaults to
  `false`.
- `some` contains signal-key prefixes. Awaits themselves still use exact keys.
- An event addressed to an ancestor is visible only when every parent-child
  edge on the path currently permits its key.
- With `inheritPastEvents = false`, inherited visibility starts after the child
  relationship's `inheritedEventsStartSequenceId`. With `true`, retained earlier
  events may be visible if they are still ahead of the child's cursor. Events
  deleted when their owning workflow completed or continued as new cannot be
  made visible again.
- Replaying `startAsChild` replaces the current policy. Added prefixes can
  expose retained events; removed prefixes hide unresolved events. Cached Step
  results remain valid.
- Policy broadening durably schedules coalesced reruns for the affected child
  subtree. Descendant scheduling may be asynchronous.
- Synchronous `Update`s are not inherited.

## Events and ordering

- Signals are immutable `workflow_events` facts. The same global event sequence
  also stores `TimerFired` and `WorkflowCompleted` events. Events are never
  updated in place, but lifecycle cleanup may delete them.
- Each event has one global `sequenceId`, the workflow instance it belongs to,
  its non-null key, payload, and creation time. `sequenceId` is unique across
  all workflows and determines the durable order.
- All event appends have one total order. Non-overlapping appends preserve
  happens-before order; concurrent appends are ordered by the global allocator.
- The sequence is global rather than one log per parent-child tree because
  `Step.awaitRace` must compare a local signal or timer with a completion event
  from an unrelated workflow. See `signals-timers.md` for the ordering and
  throughput tradeoff.
- Lifecycle deletion leaves gaps but never resets or reuses `sequenceId`s.
- Sending atomically appends one event. That fact is immediately readable by
  the addressed workflow and all currently eligible descendants, so there is no
  partially copied child delivery.
- Atomic visibility is based on current read-time policy; it does not freeze a
  recipient list at send time.
- The global order does not force strict consumption across signal keys.
  Exact-key awaits may leave an earlier event of another key unconsumed.

## Reading and lifecycle

- An unresolved child await recursively queries parent and grandparent sources
  and selects the earliest visible event for its exact key after the child's
  `(workflowInstanceId, signalKey)` cursor.
- The active parent, inheritance selector, `inheritPastEvents`, and
  `inheritedEventsStartSequenceId` live on the child workflow-instance storage
  record. A separate relationship record is unnecessary.
- Updating inheritance fields does not acquire the child's execution lease. If
  it races with persistence of an await result, either the old or new policy may
  be observed; no visibility revision is required.
- Detachment removes inherited visibility for unresolved awaits. Cached Step
  results remain replayable, and directly addressed signals remain valid.
- When a workflow completes or continues as new, its child relationships are
  closed and directly addressed `Signal` events are deleted. Cancelled or
  abandoned children replay consumed inherited signals from their cached Step
  results; they do not require the source event to remain stored.
- A workflow that continues as new remains attached to its own parent, retains
  its inheritance configuration and exact-key cursors, and may still inherit
  retained parent events from before the transition when `inheritPastEvents` is
  enabled. Its own children are closed and its directly addressed `Signal`
  events are deleted.

## PostgreSQL notes

- A global PostgreSQL sequence with `CACHE 1` allocates `sequenceId`s. Every
  event appender takes the same blocking transaction-scoped advisory lock before
  `nextval` and keeps it through commit, so IDs are visible in append order.
- Use a recursive CTE over `workflow_instances` to obtain allowed ancestor
  sources for an await.
- Policy broadening can use a recursive CTE and the existing durable wakeup
  mechanism to schedule unfinished descendants without copying signal payloads.
- The execution lease must not be a row lock held while user code runs.

## Why this design

- External callers address a stable parent while children remain implementation
  details.
- One immutable append provides atomic child visibility without write
  amplification or a distributed inheritance transaction.
- Global `sequenceId`s give direct and inherited signals, timers, and workflow
  completions one clear order.
- Read-time policies make transitive and past-event visibility inexpensive to
  change; the tradeoff is a recursive ancestor query during unresolved awaits.
- Per-workflow, per-key cursors keep consumption local without a processed row
  for every event and recipient.
