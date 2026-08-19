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
- The workflow key remains unique and unchanged. There are not multiple
  historical workflow instances with the same key.
- The generation is exposed only through `WorkflowInstance.Info`. Users should
  not normally need to inspect or use it.
- The transition is committed atomically by the runtime: the old generation is
  closed, its execution records are removed, and the new input/generation is
  installed as one durable state transition.
- Children of the old generation are handled according to their
  `ParentClosePolicy`: `Cancel` requests cancellation and `Abandon` detaches
  them. The continuation does not wait for cooperative child cancellation.
- Signals and other pending external events require the same completion-boundary
  treatment as a normal completion. Workflow code must establish the desired
  policy before continuing; the runtime cannot infer whether an unprocessed
  event should be carried into the new generation or discarded.

## Generations and child identity

There is only one active generation for a workflow instance key. Generation is
internal execution state, not a second public address type. A child instance
key has the conceptual shape:

```text
parent-key + child-key + parent-generation
```

The encoding remains part of the public key value because all workflow
instances can be queried by key. Child callers normally avoid depending on the
encoding and use the handle returned by `startAsChild`, which contains the
child's instance key and supports awaiting it.

- Including the generation prevents old children from colliding with children
  started by the successor generation.
- The parent reference does not include a generation. It is an active
  `parentInstanceId` relationship, and there is only one active generation.
- The parent reference is cleared when the parent completes or continues as
  new, including when cancellation of a child is still in progress.
- `WorkflowInstanceId` remains the single public identity type. No public
  `WorkflowRunId` is introduced at this stage.
- Stopping of the current execution is achieved by a throwing a special exception. This exception must also be ignored by the `NonFatal`-like extractor that the library provides.

## Forking

Forking is a recovery and manual-intervention operation, distinct from
continue-as-new:

```scala
runtime.forkWorkflow(sourceInstanceId, fromStep = stepId)
```

- A fork always receives a completely new instance ID.
- The fork has generation = 0 and starts with the same input as the source instance.
- The fork starts at the specified step ID, which must be a step that has already
  been executed in the source instance.
- Cached steps up to the selected point are copied to the new instance. (TODO: Can we assume that there is such an order between executed steps?)
- Forking does not require old generations to remain as separate workflow
  instances; it only operates on the source state that currently exists.

## Resetting

Like a combination of fork and reset: It's like continueAsNew (keeps same key) but only erases the history after a certain step. 

```scala
runtime.resetWorkflow(sourceInstanceId, fromStep = stepId)
```

- Generation is incremented
- Behaves mostly like continueAsNew

## Why this design

- Putting generation inside the parent key would make it part of the public identity and require users to
  understand and manage it.
- Generation as part of keys introduces an additional key type (like RunId in Temporal) and complicates the public API.
- Generation in child keys allows old children to finish cancellation or keep
  running after a parent transition without colliding with successor children.
- A runtime primitive rather than a loop helper makes the recursive nature of
the operation explicit and avoids inventing a second iteration abstraction.
- Forking uses a new identity and copied cached steps, so it does not need to
  address or preserve an old continue-as-new generation.
