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
  records and directly addressed `Signal` events are removed, child
  relationships are closed before those events are deleted, and the incremented
  generation and new input are installed as one durable in-place state
  transition. Current execution records include old await subscriptions and
  wakeups. The unconsumed-signal handler runs before this transaction while
  signal acceptance is closed.
- Children of the old generation are handled according to their
  `ParentClosePolicy`: `Cancel` requests cancellation and `Abandon` detaches
  them. The continuation does not wait for cooperative child cancellation.
- Signals receive the same completion-boundary treatment as normal completion.
  The `onUnconsumedSignals` handler runs before the transition. Once it
  finishes, directly addressed `Signal` events are deleted. State needed by the
  successor must be passed in
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
- The parent reference is cleared when the parent completes or continues as
  new, including when cancellation of a child is still in progress.
- Stopping of the current execution is achieved by a throwing a special exception. This exception must also be ignored by the `NonFatal`-like extractor that the library provides.

## Forking

Forking is a recovery and manual-intervention operation, distinct from
continue-as-new:

```scala
runtime.forkWorkflow(sourceInstanceId, newInstanceKey, restartFromStep = stepId)
```

- A fork always receives a completely new instance ID.
- The fork has generation = 0 and starts with the same input as the source instance.
- A fork is an independent top-level workflow. It does not inherit the source
  workflow's parent relationship or inherited signals.
- `restartFromStep` is an exclusive boundary: cached history strictly before
  the selected step is copied, while the selected step and subsequent history
  are omitted so that the selected step executes again.
- The workflow function still executes from its beginning. Copied history is
  replayed until execution reaches the first uncopied operation; the runtime
  does not jump directly into the workflow body.
- `restartFromStep` must identify a step already executed by the source.
- TODO: define a causal boundary for parallel branches, where executed steps do
  not necessarily have one total order. A single step ID may be insufficient.
- Forking does not require old generations to remain as separate workflow
  instances; it only operates on the source state that currently exists.

## Resetting

Reset keeps the same instance identity but erases history from a selected step
onward. It is the in-place counterpart of a fork.

```scala
runtime.resetWorkflow(sourceInstanceId, restartFromStep = stepId)
```

- Generation is incremented in place; no old generation is retained.
- History strictly before `restartFromStep` remains cached. The selected step
  and subsequent history are erased, and the workflow replays from the top.
- Runtime determines which steps are "after" or "before" by looking at the timestamp where the step row was last updated.

## Why this design

- Putting generation inside the parent key would make it part of the public identity and require users to
  understand and manage it.
- Generation as part of keys introduces an additional key type (like RunId in Temporal) and complicates the public API.
- A runtime primitive rather than a loop helper makes the recursive nature of
the operation explicit and avoids inventing a second iteration abstraction.
- Forking uses a new identity and copied cached steps, so it does not need to
  address or preserve an old continue-as-new generation.
- An exclusive `restartFromStep` boundary reruns the selected operation, which
  matches the primary recovery case. An inclusive boundary would preserve the
  operation most likely to require correction.
