# Continue-As-New and Forking

## Continue-as-new

`continueAsNew` is a runtime operation with a forwarder in the `Workflow`
companion object. It has no `Workflow.loop` helper.

```scala
Workflow.continueAsNew(nextInput)
```

The operation is a recursive tail transition: the current execution ends and
the same logical workflow starts again with new input and a fresh history.
It returns `Nothing` and is implemented as runtime control flow, so the
workflow body does not continue after the call.

- The runtime updates the existing instance in place.
- The generation is incremented and all steps, awaits, and other execution
  records from the old generation are erased.
- Older generations are not persisted separately.
- The workflow key remains unique and unchanged. There are not multiple
  historical workflow instances with the same key.
- The generation is exposed only through `WorkflowInstance.Info`. Users should
  not normally need to inspect or use it.
- The transition is committed atomically by the runtime: current execution
  records are removed and the incremented generation and new input are
  installed as one durable in-place state transition.
- Children of the old generation are handled according to their
  `ParentClosePolicy`: `Cancel` requests cancellation and `Abandon` detaches
  them. The continuation does not wait for cooperative child cancellation.
- Signals and other pending external events receive the same completion-boundary
  treatment as normal completion. The `onUnconsumedSignals` handler runs before
  the transition; once it finishes, all events from the old generation are
  discarded. State needed by the successor must be passed in `nextInput`.
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
- TODO: define reset treatment for parallel branch history and for associated
  awaits, signals, updates, and children at the reset boundary.

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
