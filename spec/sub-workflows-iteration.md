# Subworkflows, Child Workflows & Iteration

Guideline for inline scoping, concurrent fan-out, and independent child workflows. Companion to `running-workflows.md`, `signals-timers.md`, `child-signal-inheritance.md`, and `steps.md`.

## Core distinction

Three genuinely different concepts; the distinction answers both the O(n²) resume problem and the lifecycle question:

- **Subworkflow** = an inline code section whose Step and Await IDs are prefixed by a scope key. No independent existence; lives inside the parent's history. The parent re-runs on every event that touches any element. Replay of already-completed elements is cheap (cache lookups), but total work is O(events × elements). Correct when you need to capture parent-scope variables; use for modest n.

- **Child workflow** = a separately registered `Workflow` started from within
  another workflow via `startAsChild`. Has its own instance, key, history,
  exact-signal-key cursors, and observability. Signal events are stored once in
  global `workflow_events`, not copied into a child-local log. The parent is
  **not** re-run for an event addressed only to the child.
  Lifecycle is coupled to the parent via `ParentClosePolicy`. Use when
  per-element independence, scale, or separate versioning matters.

- **Independent workflow** = a workflow started from outside (via the runtime directly) with no parent linkage. Out of scope for this doc; see `running-workflows.md`.

**Deciding question:** "Does the loop body need to close over parent-scope variables that aren't serializable as inputs?" Yes → subworkflow. No, or n is large → child workflow.

## Primitives

Three primitives cover all subworkflow patterns. Everything else is regular Scala control flow.

### `Workflow.scoped`

Wraps a body in an ID namespace. All `Step` and Await IDs inside are prefixed with the scope key, so the same step definitions can execute independently per element.

```scala
// explicit stable key
Workflow.scoped("approval-check") { ... }

// derived from a Fingerprintable value (key derived automatically from the element)
Workflow.scoped(order) { process(order) }
```

Scopes nest arbitrarily; the full prefix is the path of enclosing scope keys joined by a separator. Each DB entry for a step or await has a nullable scope-path column. Suspension propagates outward by default, exactly like a plain Step.

### `Workflow.runToSuspension`

Runs one by-name block, catches its suspension instead of propagating it, and returns `Either[WorkflowSuspendedException, R]`. This is the opt-in primitive for local suspension handling.

```scala
val result: Either[WorkflowSuspendedException, OrderResult] =
  Workflow.runToSuspension { Workflow.scoped(order) { process(order) } }
```

Use this when you need to inspect whether a block suspended before deciding what to do next. Most code should not use this — suspensions propagate to the runtime automatically, which is the right default. `par` is built on this primitive internally.

### `Workflow.parallel`

Runs multiple branches concurrently (using Ox `par`/`mapPar` under the hood). Waits for **all** branches before re-throwing: if some complete and others suspend, `par` collects all results first, then throws one combined `WorkflowSuspendedException` carrying each branch's suspension as a cause. Returns `Seq[R]` when every branch completes.

```scala
object Workflow {
    def parallel[R](branches: Seq[() => R]): Seq[R]
    def parallel[R](branches: (() => R)*): Seq[R]
}
```

```scala
val results: Seq[R] = Workflow.parallel(
  items.map(item => () => Workflow.scoped(item) { process(item) })
)
```

### `Step.firstToRunWithoutSuspension`

Runs multiple branches concurrently. Waits either until all branches complete or suspend. If all branches suspended, it rethrows one combined `WorkflowSuspendedException` like `Workflow.parallel`. If at least one completes normally, it discards the other suspensions and returns the first result. 

TODO: how does it handle cancellation of child workflows started from a branch?

```scala
object Step {
    @experimental
    def firstToRunWithoutSuspension[R](
        stepId: String,
        invalidateOn: Seq[StepInput[?]] = Seq.empty,
        ensureUnchanged: Seq[StepInput[?]] = Seq.empty
    )(branches: (String, () => WorkflowContext ?=> R)*): R
}
```

Like `Step.awaitRace` members, branches carry an explicit key: a stable, non-empty, unique identity (the `"_"`-leading form is reserved) that names the branch's scope. The branch runs one scope level below the race's key, so its scope path is the enclosing scope plus the race key plus the branch key (`.../stepId/branchKey`). Branch keys obey the scope-key rules of `ScopePath.requireValidKey` (see `steps.md`). Reordering branches never changes their identity.

Note the `() => WorkflowContext ?=> R` branch type. The extra `() =>` layer is required: a bare `("key", { ... })` tuple whose element is a context function is typed eagerly in this Scala version, so the body would run at the call site (with the enclosing scope) instead of inside the branch. Wrapping the body in a lambda (`"key" -> { () => ... }`) makes the tuple element an ordinary function value — constructing a lambda has no side effects, and the body only runs when the race applies it with the branch's own `WorkflowContext`. The same deferral also applies to `Workflow.parallel`'s branch blocks.

Because it is edge-triggered (not level-triggered like `Workflow.parallel`), this function must save its own state in the database to avoid losing track of which branch was the first ever to complete (in future reruns, more branches may become unblocked and the completion order won't be the same). Because the function caches its own state, it is found under `Step` and not under `Workflow`.

The winner row persists only the winning branch's public returned value (the winning branch's identity is transaction-local and not retained, like an await's event selection). When the race resolves, every branch's pending subscriptions — including those of nested subscopes — are deleted BEFORE the winner row is written, so a committed winner row never coexists with branch subscriptions. The ordering is deliberate: the winner row is the durable first-wins marker, so a crash in between leaves no marker and the step's next run re-evaluates from scratch and re-deletes (the deletion runs at most twice).

This function can be used to race arbitrary sub functions, but it has a lot of dangerous pitfalls because of the way that suspensions work. Example:

```scala
val result: R = Step.firstToRunWithoutSuspension("race-branches",
    "sleep" -> { () => Thread.sleep(10.minutes) },
    "timer" -> { () => Step.await("timer", Awaitable.Timer(1.minute)) }
)
```
One would expect the timer to win, but it will not: The timer suspends immediately by throwing an exception and will not become unblocked until the entire workflow is re-executed. The `Thread.sleep` will just block the thread and complete before the other branch has a chance to re-run. This must be clearly documented!

Even if both branches are suspending, the code only works as expected when the workflow is re-run for each incoming event individually (== if no two branches become unblocked in the same run). If the workflow is re-run for multiple events at once (for example because there was a long queue in the job runner), code cannot tell which event came first. 

## Sequential iteration: plain loops

Because suspension propagates naturally, sequential iteration needs no library function:

```scala
// foreach
for item <- items do
  Workflow.scoped(item) { process(item) }

// map (results collected in order)
val results = items.map(item => Workflow.scoped(item) { compute(item) })

// fold
val total = items.foldLeft(0) { (acc, item) =>
  acc + Workflow.scoped(item) { fetchValue(item) }
}
```

No `Workflow.foreach`/`map`/`fold` helpers are provided. This is intentional: writing the loop yourself keeps `scoped` as the visible unit and avoids a combinator library that would obscure the underlying model.

**If the collection changes between runs:** previously cached step results for elements still in the collection are reused; results for elements no longer present are simply never read again (the runtime has no way to detect removal). Users must ensure that stale cached state does not cause correctness problems, or use child workflows (which have independent lifecycle and can be explicitly cancelled).

## Child workflows

### `startAsChild`

A single method on the `Workflow` class covers all use cases. No separate `create`/`run` split is needed — the parent controls timing by *where in its code* it calls `startAsChild`.

```scala
val child: WorkflowInstance[In, Out] =
  workerWf.startAsChild(
    childKey,
    input,
    parentClosePolicy = ParentClosePolicy.Cancel, // default
    inheritSignals = SignalInheritance.some("approval/", "order/cancelled"),
    inheritPastEvents = false                      // default
  )

val result = Step.await("worker-result", child.completion)
```

- **Idempotent on parent replay**: `startAsChild` is create-if-absent + run. Replaying the parent after a crash does not start a duplicate child; it returns the existing handle.
- **Deterministic identity**: A child workflow's identity (`WorkflowInstanceId`) is made up from user-supplied `WorkflowInstanceKey` and a scope. The scope is derived from the parent's identity (including parent generation) and
  enclosing workflow scopes.
- **No synchronous/blocking variant**: blocking the parent thread for a child's duration would pin the lease for the whole duration. If you want inline execution sharing the parent's scope, use `scoped` + Steps. If you want the result durably, await the child's completion signal.
- **Asynchronous start**: the child's first execution is scheduled through the normal wakeup mechanism and runs on a runner thread; it is never executed inline on the parent's thread (see `running-workflows.md`, "Job runner and scheduling").

### Inherited signals

Because child workflows are hardly addressable from the outside (their exact key is difficult to know up front), they must be able to process the parent's signals to be useful.

`inheritSignals` declares which signals addressed to the parent are visible to
the child. It defaults to none; `SignalInheritance.all` permits every key and
`SignalInheritance.some(prefixes*)` permits matching key prefixes.

- Inheritance is transitive only when `inheritSignals` on every child edge also
  permits the key.
- `inheritPastEvents = false` limits inherited visibility to events with a
  `sequenceId` greater than the child relationship's
  `inheritedEventsStartSequenceId`. With `true`, retained older events can also
  be visible if they remain ahead of the child's cursor.
- When the parent reaches a terminal state (success, failure, cancellation, or
  termination) or calls `continueAsNew`, directly addressed parent `Signal`
  events are deleted. The child becomes cancelled or abandoned per its
  `ParentClosePolicy`, and any await Step result it already made remains cached
  for replay.
- The inheritance configuration is **fixed at child creation**. Replaying
  `startAsChild` with a different configuration does not replace it: the stored
  configuration is kept and an SLF4J WARN is logged. Replaying an unchanged
  configuration is a silent no-op. Cached Step results are unaffected. A child
  whose policy differs from what a replay specifies can only exist via manual
  row edits, and the run's read path then applies the stored policy.
- The active parent, inheritance fields, and
  `inheritedEventsStartSequenceId` are persisted on the child's
  workflow-instance record.
- Signals are immutable event facts, not copied deliveries. One committed
  send is therefore atomically visible to all currently eligible descendants.
  Awaiting inherited events uses a recursive ancestor query.
- Because the configuration is fixed at creation, there is no
  inheritance-policy update and therefore no ancestry-driven subtree wakeup:
  signal and completion wakeups come only from the run's two wakeup passes,
  which apply the stored policy (see `running-workflows.md`, "Sources of
  wakeups").
- The unimplemented Update design (see `signals-timers.md`) is intentionally not
  inherited: a synchronous request cannot have one unambiguous response if
  several descendants handle it.

See `child-signal-inheritance.md` for ordering, cursor, and detachment semantics.

### Fan-out and fan-in

No bespoke combinators are provided. Fan-out is a plain `.map`; fan-in reuses `Step.await` on `child.completion` (see `signals-timers.md`):

```scala
// fan-out
val children = items.map(i => workerWf.startAsChild(key(i), i))

// wait for all to complete
val results = Workflow.parallel(children.map { child => () => Step.await("complete-" + child.id.key, child.completion) })

// first to finish wins
val result = Step.firstToRunWithoutSuspension("race-children",
  children.map { child => child.id.key -> { () => Step.await("complete-" + child.id.key, child.completion) } }
)

// partial: advance each, collect what's ready
val outcomes: Seq[Either[WorkflowSuspendedException, R]] =
  children.map(c => Workflow.runToSuspension { Step.await(c.id.key, c.completion) })
```

Child failure surfaces as the `Failed` case of the `WorkflowCompletionResult`
yielded by the `completion` await (see `running-workflows.md`); the case
carries the decoded `Throwable`, ready to rethrow for direct-style handling.

### `ParentClosePolicy`

Controls what happens to a child when the parent reaches a terminal state —
success, failure, cancellation, or termination — or calls `continueAsNew`,
restarts an enclosing region, or is reset:

| Policy | Behaviour |
|---|---|
| `Cancel` (default) | Deliver a cooperative cancellation to the child (see `running-workflows.md` for cancel semantics). |
| `Abandon` | Child detaches and continues independently. Unresolved inherited signals cease to be visible; cached Step results and directly addressed signals remain valid. |

Application rules:

- **Asynchronous delivery.** The parent's terminal transition
  (success/failure/cancellation/termination), its `continueAsNew`, its
  region-restart, and its reset never touch child rows. Each instead records a
  row in the dedicated **`child_close_requests`** table keyed by the parent's
  own identity plus a `scope_prefix` (and `created_at`); the composite primary
  key deduplicates identical targets, and the foreign key cascades when the
  parent row is deleted. The prefix is an **absolute** prefix of the targeted
  children's `workflow_instance_scope`, composed by the same function that
  derives child identities: the parent's identity plus `key@generation` for a
  terminal parent, `continueAsNew`, and reset (exactly the closing generation's
  children), and that prefix plus the discarded region interior for a region
  restart (exactly the discarded iteration). There is no empty or wildcard
  prefix. A terminal parent targets only its current generation's children;
  children of older generations remain covered by the requests their own
  `continueAsNew`/reset transitions recorded, which cannot be removed while
  such a child is still attached. No request row is written when no attached
  child matches, so the childless common case leaves no bookkeeping. Targeting
  by exact prefix is what prevents collisions between old and new children
  across generations and region iterations: a later generation's or iteration's
  child carries markers past the prefix and never matches. This is why no
  transaction ever locks another instance's rows.
- **Delivery** happens at two asynchronous points, and the active parent pointer
  on each child is cleared then: (a) the **run-start self-check** — whenever a
  child's own run starts, before its body executes, it applies its own pending
  policy; (b) the periodic **child-close sweep** — the driver loop applies every
  pending closure in bounded batches, one child per short transaction. Under
  the child's own row lock, the parent's existence decides between applying the
  recorded policy and a bare detach; both paths are idempotent, so they
  cooperate safely with each other and with recovery. (The sweep does not
  re-check the request row: a request is deleted only once no attached child
  matches it, and the locked child is still attached, so the request that
  selected the candidate cannot be gone.) The sweep deletes a
  `child_close_requests` row once no attached child matches it.
- **A gone parent detaches.** A child whose parent instance row has been
  hard-deleted is detached (pointer cleared) by the self-check or sweep; it is
  never cancelled, matching the inheritance walk, which also stops at a missing
  parent.
- Per child state, `Cancel` follows the standard per-state cancel semantics
  (see `running-workflows.md`): the child gets `cancel_requested_at` set and a
  wakeup scheduled — the sweep does so whether or not a run is in flight — and
  it delivers the cancellation itself at its next checkpoint, exactly like
  `runtime.cancel`. A `CREATED` child therefore executes its body up to its
  first checkpoint before cancelling; because a checkpoint precedes every Step
  body and await evaluation, no durable step effect runs. A `RUNNING` child
  delivers at its next checkpoint. The run-start self-check schedules no
  wakeup: it sets the flag inside the child's own live run, before any
  checkpoint, so that run is the delivery vehicle. `Abandon` clears the
  pointer only.
- **Inherited-signal visibility cuts off at the closure event, not at
  delivery.** The inheritance walk treats a parent as unusable — terminating the
  walk exactly as a detached pointer does — when it is terminal or holds a
  `child_close_requests` row whose prefix covers the child's scope. So even
  before the pointer is cleared, no transitive ancestor event can leak through a
  closed parent; a suspended child cannot consume a grandparent signal through a
  parent that has continued as new or completed. Directly addressed `Signal`
  events of the closing parent are deleted in its transition (before children
  are delivered); with the walk cutoff, this ordering inversion is unobservable
  to children.
- **A cancelling parent never waits for the children it cancels.** The parent is
  terminal as soon as its own transition commits; child cleanup is a side
  obligation. Observably, a child may run (and even complete) in the window
  between the parent's closure and its delivery, and the global `cancelTimeout`
  escalation is measured from delivery, bounding any child that never observes
  its cancellation.
- **`startChild` is fenced.** A stale run can never attach a child that escapes
  a closure record. Under the parent's own row lock, the insert is refused
  (lease lost) when the parent's generation differs from the run's, when the
  parent is terminal, or when a pending `child_close_requests` prefix already
  covers the child's derived scope (a same-generation orphan attaching into a
  discarded region interior). A legitimate new child never matches a pending
  prefix, because prefixes name only discarded interiors and old generations.

`Terminate` is intentionally absent from the policy enum. Force-stopping a child at parent close is a rare ops need, not a lifecycle need; it is reachable via `runtime.terminate` for manual/ops use only.

### Key derivation

- A child keeps the user-supplied key unchanged. Its full identity is
  `(childWorkflowId, childKey, derivedScope)`, where `derivedScope` is set only
  by `startAsChild`. Top-level creation APIs always use the empty scope.
- Conceptually, the derived scope is the parent's existing scope followed by
  the parent workflow ID, parent instance key with workflow generation, and the
  complete enclosing `Workflow.scoped` path. Restartable and loop scopes appear
  as `scopeId@restartCount`; ordinary scopes appear as their scope ID. Segments
  retain their outer-to-inner order.
- **Representation.** The scope is a structured **segment array** (a
  `TEXT[]` in Postgres, `Vector[String]` in Scala), not a joined string. The
  array positions — not any delimiter — are what separate segments, so **no
  escaping or shortening is performed anywhere**. User-supplied scope keys
  (`Workflow.scoped` keys, restartable / loop IDs, step keys, race branch keys)
  are validated by `ScopePath.requireValidKey` and never contain `/`, `*`, `@`,
  `[`, `]`, `{`, `}` or a leading `_`; workflow IDs and instance keys are stored
  verbatim. A single runtime function composes the array:
  ```scala
  def deriveChildScope(parentInstanceScope, parentWorkflowId, parentKey,
                       parentGeneration, enclosingScopePath): Vector[String] =
    parentInstanceScope ++ Vector(parentWorkflowId, s"$parentKey@$parentGeneration") ++ enclosingScopePath
  ```
  The `@count` / `@generation` marker is appended to the segment text
  (`worker-1@0`), so the numeric count is part of the segment, never a
  separate field. Injectivity is structural: a key crafted to look like a
  nested chain (say `x@0/B/y`) is a single array element `"x@0/B/y"`, which
  can never collide with the genuine chain `["x@0", "B", "y@0"]`. This is what
  makes the `(workflowId, instanceKey, scope)` uniqueness key safe even though
  scope is derived from user-supplied keys.
- **Prefix search.** Instance-key prefix operations
  (`getWorkflowInstancesByPrefix`, `deleteWorkflowInstancesByPrefix`) query the
  raw `workflowInstanceKey` column with a literal prefix (escaped only for SQL
  `LIKE`). Scope-scoped queries (e.g. all descendants of a parent) match a
  segment-array **prefix** by comparing array slices: a child row's
  `workflow_instance_scope` equals the parent's derived array on its first
  `n` positions, where `n` is the parent's array length. Because segments are
  verbatim, a prefix match on the array never over-matches a sibling or a
  segment boundary.
- Example:
  ```text
  parent:      (orders, order-42, {})
  child:       (worker, worker-1, {orders, order-42@3, items, poll@7})
  grandchild:  (audit, audit-1, {orders, order-42@3, items, poll@7, worker, worker-1@0, chunk, retry@2})
  ```
- The parent workflow generation distinguishes children started before and
  after `continueAsNew`. Every enclosing restart count distinguishes children
  started in different region iterations, including when an inner count starts
  again after an outer region restarts.
- Scope is part of immutable identity. If a child is abandoned, its active
  parent relationship is cleared but its derived scope does not change.
- The database uniqueness key is `(workflowId, instanceKey, scope)`. Queries
  accept an optional scope whose default is the empty array `{}`; omitting it
  means exactly the top-level scope, never all matching scopes.
- The scope has a maximum encoded length. When the composed array's joined
  `/`-form would exceed the limit, derivation is **rejected** with an
  `IllegalArgumentException` (nothing is truncated or hashed): the limit is
  generous enough for deep legitimate nesting, and the Postgres btree index
  entry that holds the `(workflowId, instanceKey, scope)` key would otherwise
  fail at a slightly higher length. This replaces the former silent
  prefix-preserving shortening, which existed only to keep string prefix
  matching working and is obsolete now that scopes are structured arrays.
- The readable slash-joined form (without escaping) is for debugging only.
  Callers must not construct or parse derived scopes or rely on their textual
  layout.
- The returned handle is the normal way to address a child. Users should not
  normally reconstruct the derived scope.
- The active parent relationship is stored separately as an optional
  `parentWorkflowInstanceId` on the storage record; it does not include the
  generation. It is cleared when the parent's closure is delivered to the child
  (run-start self-check or child-close sweep); it may therefore still be set for
  a short asynchronous window after the parent reaches a terminal state or
  continues as new. `WorkflowInstance.Info` exposes this as `parentId`.

## Recursion

**Structural recursion (trees, divide-and-conquer)** → spawn self as a child. Each node has its own bounded history, is independently observable, and can fail/retry without affecting siblings:

```scala
lazy val treeWf: Workflow[Node, Sum] = Workflow("tree") { (node: Node) =>
  val kids = node.children.map(c => treeWf.startAsChild(key(c), c))
  val kidSums = Workflow.parallel(kids.map(k => () => Step.await("kids-" + k.id.key, k.completion).get))
  node.value + kidSums.sum
}
```

**Tail recursion / eternal event loops** → use `continueAsNew` (see `running-workflows.md`). This atomically resets the instance's history and restarts it with new input, preventing unbounded growth:

```scala
val processorWf = Workflow("processor") { (state: State) =>
  val event = Step.await("next-event", Awaitable.SignalEvent(eventSignal))
  val newState = processEvent(state, event)
  Workflow.continueAsNew(newState)  // restarts with fresh history; never returns
}
```

**Shallow bounded recursion** → nested `scoped` is sufficient (the whole stack replays on each event; only use for small, bounded depth).

## Rationale

- Sequential helpers (`Workflow.foreach`/`map`/`fold`) are omitted: plain loops with `scoped` are no more verbose, keep the model visible, and avoid a combinatory library that would not scale to new iteration shapes.
- `Workflow.runToSuspension` is opt-in, not the default. Making every scope return `Either` would reintroduce monadic threading at every call site, defeating the purpose of direct style. `Either` is the right return type only when you actually need to handle suspension locally — which is `Workflow.parallel` and the fan-in partial-collection pattern.
- Single `startAsChild` method: parent controls timing positionally, not via a deferred-creation API. `create`/`run` separation is only needed for external callers who must register a workflow in the same transaction as other business data.
- Deriving child scope from parent identity, workflow generation, and enclosing
  scope generations avoids persisting a randomized start ID while preventing
  collisions with top-level instances and children from discarded generations.
- Parent reference without generation: there is only one active generation for
  an instance ID, and the parent pointer represents active ownership rather
  than historical provenance. Clearing it when the parent closes also matches
  the child's view that the parent has completed.
- Events can be sent to a parent or a child. Global event sequence IDs give
  signals, timers, and workflow completions one order; each workflow instance
  maintains its own exact-key signal cursors into that sequence.
- Fan-out/fan-in via `.map` + `Workflow.parallel` of `Step.await(child.completion)`: consistent with the signals/timers model; no new primitive concepts.
- `Terminate` outside `ParentClosePolicy`: force-kill at parent close is an ops concern, not a lifecycle concern. Keeping it off the policy enum reduces the decision surface for users writing workflow code.
- O(n²) subworkflow problem: subworkflows are documented as O(events × elements) by design; this is acceptable for modest n. For large n or latency-sensitive flows, child workflows solve the problem structurally.

## Open questions and TODOs

1. **Child failure and cancellation** — **Resolved**: cooperative cancellation
   semantics, delivery to suspended children, sticky redelivery, and the
   global `cancelTimeout` escalation to `TERMINATED` are specified in
   `running-workflows.md` ("Cancellation and termination"); parent-close
   application rules are specified above.

2. **Parallel branch child cleanup** — **TODO**: Define how `Workflow.parallel` and `Step.firstToRunWithoutSuspension` clean up child workflows started inside a branch when that branch exits early or is cancelled because another branch completed with an exception or library control-flow exception.

3. **Race awaits cleanup** — **Resolved**: when a branch completes and the
   race resolves, every branch's pending subscriptions — including those
   of nested subscopes — are deleted (a subtree delete over all three
   subscription tables) BEFORE the winner's step row is written, so a losing
   await can never wake the workflow later and a committed winner row never
   coexists with branch subscriptions. The runtime primitive is
   `WorkflowRuntime.deleteSubscriptionsUnderScopePaths`. Child-workflow
   lifecycle on early branch exit remains the open TODO (2) above.
