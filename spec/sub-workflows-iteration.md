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

Runs one by-name block, catches its suspension instead of propagating it, and returns `Either[SuspensionException, R]`. This is the opt-in primitive for local suspension handling.

```scala
val result: Either[SuspensionException, OrderResult] =
  Workflow.runToSuspension { Workflow.scoped(order) { process(order) } }
```

Use this when you need to inspect whether a block suspended before deciding what to do next. Most code should not use this — suspensions propagate to the runtime automatically, which is the right default. `par` is built on this primitive internally.

### `Workflow.parallel`

Runs multiple branches concurrently (using Ox `par`/`mapPar` under the hood). Waits for **all** branches before re-throwing: if some complete and others suspend, `par` collects all results first, then throws one combined suspension carrying each branch's suspension as a cause. Returns `Seq[R]` when every branch completes.

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

Runs multiple branches concurrently. Waits either until all branches complete or suspend. If all branches suspended, it rethrows one combined suspension like `Workflow.parallel`. If at least one completes normally, it discards the other suspensions and returns the first result. 

TODO: how does it handle cancellation of child workflows started from a branch?

```scala
object Step {
    @experimental
    def firstToRunWithoutSuspension[R](
        stepId: String,
        invalidateOn: Seq[StepInput[?]] = Seq.empty,
        ensureUnchanged: Seq[StepInput[?]] = Seq.empty
    )(branches: Seq[() => R]): R
    
    @experimental
    def firstToRunWithoutSuspension[R](
        stepId: String,
        invalidateOn: Seq[StepInput[?]] = Seq.empty,
        ensureUnchanged: Seq[StepInput[?]] = Seq.empty
    )(branches: (() => R)*): R
}
```

Because it is edge-triggered (not level-triggered like `Workflow.parallel`), this function must save its own state in the database to avoid losing track of which branch was the first ever to complete (in future reruns, more branches may become unblocked and the completion order won't be the same). Because the function caches its own state, it is found under `Step` and not under `Workflow`.

This function can be used to race arbitrary sub functions, but it has a lot of dangerous pitfalls because of the way that suspensions work. Example:

```scala
val result: R = Step.firstToRunWithoutSuspension("race-branches",
    { Thread.sleep(10.minutes) },
    { Step.await("timer", Awaitable.Timer(1.minute)) }
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
- When the parent completes or calls `continueAsNew`, directly addressed parent
  `Signal` events are deleted. The child becomes cancelled or abandoned, and
  any await Step result it already made remains cached for replay.
- Replaying `startAsChild` replaces the current inheritance configuration.
  Added prefixes can expose retained events; removed prefixes hide unresolved
  events. Cached Step results are unaffected. Replaying an unchanged
  configuration is a no-op and does not wake the subtree.
- Updating the inheritance configuration does not acquire the child's execution
  lease. A race with an await concurrently committing its result is deliberately left unordered; either the old or new configuration may be observed.
- The active parent, inheritance fields, and
  `inheritedEventsStartSequenceId` are persisted on the child's
  workflow-instance record.
- Signals are immutable event facts, not copied deliveries. One committed
  send is therefore atomically visible to all currently eligible descendants.
  Awaiting inherited events uses a recursive ancestor query.
- When the inheritance policy is updated, this may unblock awaits in the child. Thus, all grandchildren need to be scheduled for execution after an inheritance policy update. Descendant scheduling may be asynchronous and reuse the normal
  workflow wakeup mechanism.
- Synchronous `Update`s are never inherited because multiple descendant
  responses would be ambiguous.

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
  children.map { child => () => Step.await("complete-" + child.id.key, child.completion) }
)

// partial: advance each, collect what's ready
val outcomes: Seq[Either[SuspensionException, R]] =
  children.map(c => Workflow.runToSuspension { Step.await(c.id.key, c.completion) })
```

Child failure surfaces as a normal exception from the `completion` await.

### `ParentClosePolicy`

Controls what happens to a child when the parent completes, fails, or is cancelled:

| Policy | Behaviour |
|---|---|
| `Cancel` (default) | Deliver a cooperative cancellation to the child (see `running-workflows.md` for cancel semantics). |
| `Abandon` | Child detaches and continues independently. Unresolved inherited signals cease to be visible; cached Step results and directly addressed signals remain valid. |

`Terminate` is intentionally absent from the policy enum. Force-killing a child at parent close is a rare ops need, not a lifecycle need; it is reachable via `runtime.terminate` for manual/ops use only.

### Key derivation

- A child keeps the user-supplied key unchanged. Its full identity is
  `(childWorkflowId, childKey, derivedScope)`, where `derivedScope` is set only
  by `startAsChild`. Top-level creation APIs always use the empty scope.
- Conceptually, the derived scope is the parent's existing scope followed by
  the parent workflow ID, parent instance key with workflow generation, and the
  complete enclosing `Workflow.scoped` path. Restartable and loop scopes appear
  as `scopeId@restartCount`; ordinary scopes appear as their scope ID. Segments
  retain their outer-to-inner order.
- Example:
  ```text
  parent:      (orders, order-42, "")
  child:       (worker, worker-1, "orders/order-42@3/items/poll@7")
  grandchild:  (audit, audit-1, "orders/order-42@3/items/poll@7/worker/worker-1@0/chunk/retry@2")
  ```
- The parent workflow generation distinguishes children started before and
  after `continueAsNew`. Every enclosing restart count distinguishes children
  started in different region loopings, including when an inner count starts
  again after an outer region restarts.
- Scope is part of immutable identity. If a child is abandoned, its active
  parent relationship is cleared but its derived scope does not change.
- The database uniqueness key is `(workflowId, instanceKey, scope)`. Queries
  accept an optional scope whose default is `""`; omitting it means exactly the
  top-level scope, never all matching scopes.
- The scope has a maximum encoded length. Before appending the current local
  `Workflow.scoped`/restartable path, an overlong parent portion is replaced by
  a marked stable hash such as `shortened[sha256:...]`. Local scope segments are
  not part of that shortening calculation, ensuring that all children of one
  parent instance and generation begin with the same normalized scope prefix.
  When such a child later becomes a parent, its former local path is ancestry
  and may be included in a shortened parent portion.
- The readable slash-and-`@` form is for debugging only. Callers must not construct or
  parse derived scopes or rely on their textual layout.
- The returned handle is the normal way to address a child. Users should not
  normally reconstruct the derived scope.
- The active parent relationship is stored separately as an optional
  `parentWorkflowInstanceId` on the storage record; it does not include the
  generation and is cleared when the parent completes or continues as new.
  `WorkflowInstance.Info` exposes this as `parentId`.

## Recursion

**Structural recursion (trees, divide-and-conquer)** → spawn self as a child. Each node has its own bounded history, is independently observable, and can fail/retry without affecting siblings:

```scala
lazy val treeWf: Workflow[Node, Sum] = Workflow("tree") { (node: Node) =>
  val kids = node.children.map(c => treeWf.startAsChild(key(c), c))
  val kidResults = Workflow.parallel(kids.map(k => () => Step.await("kids-" + k.id.key, k.completion)))
  node.value + kidResults.sum
}
```

**Tail recursion / eternal event loops** → use `continueAsNew` (see `running-workflows.md`). This atomically resets the instance's history and restarts it with new input, preventing unbounded growth:

```scala
val processorWf = Workflow("processor") { (state: State) =>
  val event = eventSignal.await("next-event")
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
  scope incarnations avoids persisting a randomized start ID while preventing
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

1. **Child failure and cancellation** — **TODO**: Define the detailed cooperative cancellation behavior, including delivery to suspended children and escalation after the runtime-configured timeout.

2. **Parallel branch child cleanup** — **TODO**: Define how `Workflow.parallel` and `Step.firstToRunWithoutSuspension` clean up child workflows started inside a branch when that branch exits early or is cancelled because another branch completed with an exception or library control-flow exception.

3. **Race awaits cleanup** — **TODO**: Define how `Step.firstToRunWithoutSuspension` cleans up registered awaits from other branches when one branch has completed.
