# Continue-As-New and Forking

## Continue-as-new

`continueAsNew` is a runtime operation with a forwarder in the `Workflow`
companion object.

```scala
Workflow.continueAsNew(nextInput)
```

The operation is a recursive tail transition: the current execution ends and
the same logical workflow starts again with new input and a fresh history.
It returns `Nothing` and is implemented as runtime control flow, so the
workflow body does not continue after the call.

- The runtime updates the existing instance in place.
- The generation is incremented and all Step rows, subscriptions, and other
  execution records from the old generation are erased (except for cursors).
- Older generations are not persisted separately.
- The workflow key remains unique and unchanged. There are not multiple
  historical workflow instances with the same key.
- The generation is exposed only through `WorkflowInstance.Info`. Users should
  not normally need to inspect or use it.
- The transition is committed atomically by the runtime: current execution
  records and directly addressed `Signal` events are removed, a
  `child_close_requests` row targeting exactly the closing generation's children
  is written in the dedicated `child_close_requests` table (keyed by the
  parent's identity plus the target prefix), and the incremented generation and
  new input are installed as one durable in-place state transition. Current
  execution records include old await subscriptions and wakeups; pending
  `child_close_requests` rows live in their own table and survive the
  generation erase. The unconsumed directly addressed `Signal` events are
  deleted inside this same transaction. The directly
  addressed `Signal` events are deleted before children are delivered; this
  ordering is unobservable because the inheritance walk treats a parent holding
  a matching `child_close_requests` row as detached (see
  `sub-workflows-iteration.md`, `ParentClosePolicy`).
- Children of the old generation are handled asynchronously according to their
  `ParentClosePolicy`: `Cancel` requests cancellation and `Abandon` detaches
  them, delivered at each child's next run start or by the periodic child-close
  sweep. The continuation does not wait for cooperative child cancellation, and
  the parent's transition touches no child rows. Children of the successor
  generation carry a distinct scope (`key@generation+1`) and are never matched
  by the old generation's closure target.
- Signals receive the same completion-boundary treatment as normal completion.
  The unconsumed `Signal` events are deleted inside the transition
  transaction; there is no user-facing handler for them (unconsumed signals
  are ignored). State needed by the successor must be passed in
  `nextInput`.
- Cancelled or abandoned children no longer need unprocessed
  inherited events; cached Step results contain their replayable values and do
  not depend on the event row remaining present.
- The instance keeps its exact-key signal cursors. The global event sequence is
  not reset or reused after rows are deleted, so new event `sequenceId`s remain
  greater than all previously allocated IDs. Cursors may point into gaps left by
  deletion; this is expected.
- If the continuing workflow is itself a child, it remains attached to its own
  parent with the same signal-inheritance configuration. Ancestor events cannot
  be deleted by the child transition; retained events remain visible according
  to `inheritSignals`, `inheritPastEvents`, and the preserved cursors.
- The operation is available to users only as `Workflow.continueAsNew` from
  inside an executing workflow. The runtime primitive is internal and requires
  the current workflow context.

## Generations and child identity

- The generation of a workflow is informational metadata (`WorkflowInstance.Info`), not part of its identity (`WorkflowInstanceId`), since there is only ever one active generation of a particular instance. For child workflows however, the parent's generation number must be part of their identity to prevent key collisions between children of the previous parent generation which are still running and newly started children of the successive parent generation. The complete child-scope derivation rules are specified in `sub-workflows-iteration.md`.
- The parent reference is cleared when the parent's close is delivered to the
  child — at the child's next run start or by the child-close sweep — after the
  parent completes or continues as new; it may remain set for a short
  asynchronous window, including while cancellation of the child is still in
  progress.
- Stopping of the current execution is achieved by a throwing a special exception. This exception must also be ignored by the `NonFatal`-like extractor that the library provides.

## Forking

Forking is a recovery and manual-intervention operation, distinct from
continue-as-new:

```scala
runtime.forkWorkflow(
  sourceInstanceId,
  newInstanceKey,
  invalidateIndividualSteps = Seq.empty,
  invalidateAllExecutedAfter = Seq(stepId"/orders/charge")
)
```

- A fork always receives a completely new instance ID.
- The fork has generation = 0 and starts with the same input as the source instance.
- A fork is an independent top-level workflow. It does not inherit the source
  workflow's parent relationship or inherited signals.
- The fork takes two invalidation lists, both selectors (the `stepId`
  interpolator, see `steps.md`). At least one must be non-empty; when both are,
  their effects compound (the union is omitted from the copied history).
- `invalidateIndividualSteps` omits exactly the executed steps matching any of
  the selectors; all other history is copied. It is meant to be used together
  with the `invalidateOn` parameter on Step definitions: you invalidate one or a
  few steps and the invalidation propagates according to the dependency graph
  the steps specify — downstream steps whose `invalidateOn` inputs changed
  re-execute, all others reuse their cached results.
- `invalidateAllExecutedAfter` is an exclusive boundary: cached history strictly
  before the boundary is copied, while the matched steps and subsequent history
  are omitted so that they execute again. Multiple selectors compound: the
  earliest of their boundaries applies.
- The boundary is the earliest matched row's last-updated timestamp: rows
  strictly before the boundary are copied, everything with an equal or later
  timestamp is omitted. A selector that matches several rows therefore resets
  every match.
- The workflow function still executes from its beginning. Copied history is
  replayed until execution reaches the first uncopied operation; the runtime
  does not jump directly into the workflow body.
- A selector matching no executed step is tolerated (it simply invalidates
  nothing); only when no selector matches any executed step — the fork would
  invalidate nothing at all — does it throw `NoMatchingStepException`.
  `runtime.listSteps(instanceId, selector)` lists the matched steps for
  previewing.
- TODO: define a causal boundary for parallel branches, where executed steps do
  not necessarily have one total order. A single boundary timestamp may be
  insufficient (for `invalidateAllExecutedAfter`).
- Forking does not require old generations to remain as separate workflow
  instances; it only operates on the source state that currently exists.

## Resetting

Reset keeps the same instance identity but erases selected history. It is the
in-place counterpart of a fork.

```scala
runtime.resetWorkflow(
  sourceInstanceId,
  invalidateIndividualSteps = Seq.empty,
  invalidateAllExecutedAfter = Seq(stepId"orders/charge")
)
```

- Generation is incremented in place; no old generation is retained.
- The two invalidation lists follow the same rules as forking (at least one
  non-empty, both compound, selectors matching nothing are tolerated as long as
  some selector matches).
- All history except the invalidated steps remains cached and is replayed; the
  workflow replays from the top.
- `invalidateIndividualSteps` erases exactly the matched steps and is meant to
  be used with the `invalidateOn` parameter on Step definitions: invalidate one
  or a few steps, and the invalidation propagates according to the specified
  dependency graph.
- `invalidateAllExecutedAfter` determines the boundary exactly as for forking:
  the earliest matched row's last-updated timestamp, rows with an equal or later
  timestamp are erased.
- **Reset closes all children of the old generation.** Like `continueAsNew`, the
  reset transaction records a `child_close_requests` row targeting exactly the
  pre-increment generation's children (`parent identity + key@generation`),
  delivered asynchronously per `ParentClosePolicy` (see
  `sub-workflows-iteration.md`). Children created before the reset boundary are
  closed; body replay recreates what is needed. A `startAsChild` inside a cached
  step strictly before the boundary is closed and **not** recreated (the cached
  step replays without re-executing its body), and the successor generation's
  children carry the incremented marker and are never matched by the old
  generation's closure target.

## Why this design

- Putting generation inside the parent key would make it part of the public identity and require users to
  understand and manage it.
- Generation as part of keys introduces an additional key type (like RunId in Temporal) and complicates the public API.
- A runtime primitive rather than a loop helper makes the recursive nature of
the operation explicit and avoids inventing a second iteration abstraction.
- Forking uses a new identity and copied cached steps, so it does not need to
  address or preserve an old continue-as-new generation.
- An exclusive `invalidateAllExecutedAfter` boundary reruns the selected
  operation and everything executed after it, which matches the primary
  recovery case. An inclusive boundary would preserve the operation most likely
  to require correction. `invalidateIndividualSteps` covers the finer-grained
  case of rerunning one or a few steps with `invalidateOn`-driven propagation.
