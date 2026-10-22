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
- The inheritance policy and `inheritPastEvents` are **fixed at child
  creation**. A replay of `startAsChild` that specifies a different
  configuration is ignored — the stored configuration is kept — and logs an
  SLF4J WARN; an unchanged configuration is a silent no-op. There is no
  policy-change mechanism and no ancestry-driven subtree wakeup: a narrowed or
  broadened subtree is never woken proactively. A child whose policy differs
  from what a replay specifies can only exist via manual row edits, which the
  next run's read-path check handles under the stored policy.
- The unimplemented Update design (see `signals-timers.md`) is intentionally not
  inherited: a synchronous request cannot have one unambiguous response if
  several descendants handle it.

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
- The sender walks no descendants and creates no descendant wakeups: descendant
  wakeups are produced asynchronously by the runner's watermark dispatcher and
  full subscription scan using the same read-time policy. Waking a superset is
  therefore harmless — the run re-applies the current policy when it gathers
  candidates, so a descendant woken by an over-broad or stale policy simply finds
  nothing and re-suspends.
- Atomic visibility is based on current read-time policy; it does not freeze a
  recipient list at send time.
- The global order does not force strict consumption across signal keys.
  Exact-key awaits may leave an earlier event of another key unconsumed.

## Reading and lifecycle

- An unresolved child await recursively queries parent and grandparent sources
  and selects the earliest visible event for its exact key after the child's
  `(workflowInstanceId, signalKey)` cursor.
- The active parent, the stored inheritance mode and prefixes, the
  `inherit_past_events` flag (persisted form of the `inheritPastEvents`
  constructor parameter), and `inheritedEventsStartSequenceId` live on the
  child workflow-instance storage record. A separate relationship record is
  unnecessary.
- The inheritance fields are written once, at child creation, in the same
  transaction that records the parent relationship and captures
  `inheritedEventsStartSequenceId`. They are not updated afterwards, so they
  never race with persistence of an await result and no visibility revision is
  required.
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
- One recursive CTE over `workflow_instances` enumerates each subscription's
  eligible ancestor sources by climbing the active-parent chain. The run read
  path, the watermark dispatcher, and the full subscription scan all compose the
  same SQL builder, so eligibility cannot drift between them. The walk is
  bounded by the parent chain (no depth cap), and every subscriber's
  `inherit_past_events` flag and `inherited_events_start_sequence_id` are
  carried up the recursion unchanged.
- Inheritance configuration is stored on `workflow_instances` as one column,
  `inherited_signal_prefixes` (`VARCHAR[]`): the set of permitted prefixes.
  An empty array inherits nothing; a single empty-string element (a prefix of
  every key) inherits every key; any other array inherits a key when it starts
  with one of the listed prefixes. Alongside it: `inherit_past_events` and
  `inherited_events_start_sequence_id`.
- The execution lease must not be a row lock held while user code runs.

## Why this design

- External callers address a stable parent while children remain implementation
  details.
- One immutable append provides atomic child visibility without write
  amplification or a distributed inheritance transaction.
- Global `sequenceId`s give direct and inherited signals, timers, and workflow
  completions one clear order.
- Visibility is a read-time projection over immutable events and the stored
  configuration, so no payloads are copied and one append reaches every
  currently eligible descendant; the tradeoff is a recursive ancestor query
  during unresolved awaits. The configuration itself is fixed at child
  creation.
- Per-workflow, per-key cursors keep consumption local without a processed row
  for every event and recipient.
