# Subworkflows, Child Workflows & Iteration

Guideline for inline scoping, concurrent fan-out, and independent child workflows. Companion to `running-workflows.md`, `signals-timers.md`, `steps.md`.

## Core distinction

Three genuinely different concepts; the distinction answers both the O(n²) resume problem and the lifecycle question:

- **Subworkflow** = an inline code section whose Step and Await IDs are prefixed by a scope key. No independent existence; lives inside the parent's history. The parent re-runs on every event that touches any element. Replay of already-completed elements is cheap (cache lookups), but total work is O(events × elements). Correct when you need to capture parent-scope variables; use for modest n.

- **Child workflow** = a separately registered `Workflow` started from within another workflow via `startAsChild`. Has its own instance, key, history, log, cursor, and observability. The parent suspends and is **not** re-run on child events. Lifecycle is coupled to the parent via `ParentClosePolicy`. Use when per-element independence, scale, or separate versioning matters.

- **Independent workflow** = a workflow started from outside (via the runtime directly) with no parent linkage. Out of scope for this doc; see `running-workflows.md`.

**Deciding question:** "Does the loop body need to close over parent-scope variables that aren't serialisable as inputs?" Yes → subworkflow. No, or n is large → child workflow.

## Primitives

Three primitives cover all subworkflow patterns. Everything else is regular Scala control flow.

### `Workflow.scoped`

Wraps a body in an ID namespace. All `Step` and `Wait` IDs inside are prefixed with the scope key, so the same step definitions can execute independently per element.

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

### `Workflow.par`

Runs multiple branches concurrently (using Ox `par`/`mapPar` under the hood). Advances **all** branches before re-throwing: if some complete and others suspend, `par` collects all results first, then throws one combined suspension carrying each branch's suspension as a cause. Returns `Seq[R]` when every branch completes.

```scala
val results: Seq[R] = Workflow.par(
  items.map(item => () => Workflow.scoped(item) { process(item) })
)
```

`par` is the only construct that requires runtime-level support; sequential iteration is plain Scala loops with `scoped`.

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
    parentClosePolicy = ParentClosePolicy.Cancel  // default
  )

val result = child.completion.await("worker-result")
```

- **Idempotent on parent replay**: `startAsChild` is create-if-absent + run. Replaying the parent after a crash does not start a duplicate child; it returns the existing handle.
- **Deterministic identity**: the child instance key is derived from the parent,
  supplied child key, and parent generation. No separately persisted `startId`
  is needed. The returned handle contains the resolved child instance key and
  exposes a durable completion source.
- **No synchronous/blocking variant**: blocking the parent thread for a child's duration would pin the lease for the whole duration. If you want inline execution sharing the parent's scope, use `scoped` + Steps. If you want the result durably, await the child's completion signal.

### Fan-out and fan-in

No bespoke combinators are provided. Fan-out is a plain `.map`; fan-in reuses the existing `Wait` machinery from `signals-timers.md`:

```scala
// fan-out
val children = items.map(i => workerWf.startAsChild(key(i), i))

// wait for all to complete
val results = Wait.all("fan-in", children.map(_.completion))

// first to finish wins
val winner = Wait.race("first", children.map(_.completion))

// partial: advance each, collect what's ready
val outcomes: Seq[Either[SuspensionException, R]] =
  children.map(c => Workflow.runToSuspension { c.completion.await(c.id.key) })
```

Child failure surfaces as a normal exception from the `completion` await — handle with ordinary `try`/`catch`:

```scala
try
  Wait.all("fan-in", children.map(_.completion))
catch case FailedChild(id, cause) =>
  Step.atLeastOnce("compensate") { compensate(id) }
```

### `ParentClosePolicy`

Controls what happens to a child when the parent completes, fails, or is cancelled:

| Policy | Behaviour |
|---|---|
| `Cancel` (default) | Deliver a cooperative cancellation to the child (see `running-workflows.md` for cancel semantics). |
| `Abandon` | Child detaches and continues independently as if it were a top-level workflow. |

`Terminate` is intentionally absent from the policy enum. Force-killing a child at parent close is a rare ops need, not a lifecycle need; it is reachable via `runtime.terminate` for manual/ops use only.

### Key derivation

- A child instance key is derived from the parent instance key, the supplied
  child key, and the parent's generation. Conceptually its shape is
  `parentKey + childKey + generation`; the exact encoding is part of the key
  value because child workflows can be queried by key.
- The generation distinguishes children started by different generations of
  the same parent, including while old children are still cancelling or have
  been abandoned.
- The returned handle is the normal way to address a child. Users should not
  normally reconstruct the derived key.
- The active parent relationship is stored separately as an optional
  `parentInstanceId`; it does not include the generation and is cleared when
  the parent completes or continues as new.

## Recursion

**Structural recursion (trees, divide-and-conquer)** → spawn self as a child. Each node has its own bounded history, is independently observable, and can fail/retry without affecting siblings:

```scala
lazy val treeWf: Workflow[Node, Sum] = Workflow("tree") { (node: Node) =>
  val kids = node.children.map(c => treeWf.startAsChild(key(c), c))
  node.value + Wait.all("kids", kids.map(_.completion)).sum
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
- `runToSuspension` is opt-in, not the default. Making every scope return `Either` would reintroduce monadic threading at every call site, defeating the purpose of direct style. `Either` is the right return type only when you actually need to handle suspension locally — which is `par` and the fan-in partial-collection pattern.
- Single `startAsChild` method: parent controls timing positionally, not via a deferred-creation API. `create`/`run` separation is only needed for external callers who must register a workflow in the same transaction as other business data.
- Child key. Two options
  - Compute key from parent info that stays the same in every generation
  - Randomized child key that is persisted like a Step
  We chose to include generation number in the child key. That makes persisting the key unnecessary.
- Parent reference without generation: there is only one active generation for an instance key, and the parent pointer represents active ownership rather than historical provenance. Clearing it when the parent closes also matches the child's view that the parent has completed.
- Fan-out/fan-in via `.map` + `Wait.all`/`Wait.race`: consistent with the signals/timers model; no new primitive concepts.
- `Terminate` outside `ParentClosePolicy`: force-kill at parent close is an ops concern, not a lifecycle concern. Keeping it off the policy enum reduces the decision surface for users writing workflow code.
- O(n²) subworkflow problem: subworkflows are documented as O(events × elements) by design; this is acceptable for modest n. For large n or latency-sensitive flows, child workflows solve the problem structurally.

## Open questions and TODOs

1. **Child failure and cancellation** — **TODO**: Define the detailed cooperative cancellation behavior, including delivery to suspended children and escalation after the runtime-configured timeout.

2. **Parallel branch child cleanup** — **TODO**: Define how `Workflow.par` cleans up child workflows started inside a branch when that branch exits early or is cancelled because another branch completed with an exception or library control-flow exception.
