# Restartable Regions and Loops

`Workflow.restartable` and `Workflow.loop` are primitives to implement infinite-loop-like behavior (such as polling) where it is undesirable to use regular loops with index-keyed subworkflows/steps because those would accumulate a huge history of useless step caches, timer awaits, etc.

The goal of this feature was to have a region of steps and timers that can be easily reset:
```scala 
while (true) {
  Step.atLeastOnce(...) { ... }
  Timer.await(...) { ... }
  // both Timer and Step must be reset in every iteration to make sense
}
```
Alternatively, polling could also be implemented through a Step with built-in unlimited retries:
```scala
Step.atLeastOnce("polling_step", retryPolicy = RetryPolicy.fixedDelay(1.minute)) { // built-in retry functionality for Steps in library
  val r = poll()
  if (r == NoResult)
    throw RuntimeException("poll again")
}
```
but this is not as flexible (cannot wait for signals, only timer-based retry) and somewhat of a hack.

## Equivalent APIs

`Workflow.restartable` and `Workflow.loop` are two equivalent views of the same region primitive (document this in scaladoc). Which one to use is a matter of taste:

Use `restartable` when continuing is exceptional control flow and returning
the result is the normal path:

```scala
Workflow.restartable("poll-job", initialState) { (state, scope) =>
  if !state.isFinished then scope.restart(nextState(state))
  else result(state)
}
```

Use `loop` when continuing is the normal path and breaking with the result is
exceptional control flow:

```scala
Workflow.loop("poll-job", initialState) { (state, loop) =>
  if state.isFinished then loop.break(result(state))
  else nextState(state)
}
```

The two forms have exactly the same durable transitions:

- A normal return from `restartable` completes the region; `scope.restart(next)`
  starts its next generation.
- A normal return from `loop` starts the next generation; `loop.break(result)`
  completes the region.
- `restart` and `break` return `Nothing` and use library control-flow
  exceptions internally.
- `RestartableScope` and `LoopScope` are dual public views over the same
  internal region scope and expose the same `restartCount` metadata.

Conceptual API shapes:

```scala
def restartable[S: Cacheable, R](id: String, initialState: => S)(
  body: (S, RestartableScope[S]) => R
): R

def loop[S: Cacheable, R](id: String, initialState: => S)(
  body: (S, LoopScope[R]) => S
): R

trait RestartableScope[S]:
  def restartCount: Long
  def restart(nextState: S): Nothing

trait LoopScope[R]:
  def restartCount: Long
  def break(result: R): Nothing
```

The implementation uses internal control-flow exceptions to implement the `restart`/`break` (which _must_ be considered in the library's `NonFatal`-like extractor!). The two functions share the same implementation by internally converting one into the other.

## Region semantics

- Every region has a mandatory explicit ID, stable within its enclosing
  workflow or scope.
- A restartable or loop region introduces a workflow subscope, like
  `Workflow.scoped`. It should reuse `Workflow.scoped`'s scoping mechanism where
  possible, even calling `Workflow.scoped` internally.
- The initial state is a by-name, pure seed. It is evaluated and persisted only
  when the region is first created, then ignored on later replays.
- Region state may be any cacheable value and is the only user state carried
  between generations.
- `restartCount` starts at zero and counts committed restart transitions. It
  does not count body executions caused by crashes or outer replay.
- Restart atomically replaces the region's Step row with its next state and
  discards nested Step rows and subscriptions owned by the previous looping.
- Execution re-enters the region locally after restart; outer workflow code is
  not replayed merely to begin the next looping.
- Only the current state and current-generation execution records are retained,
  so thousands of iterations do not create unbounded workflow state.
- Normal completion keeps the final looping. On outer replay, the body runs
  again and its durable operations reuse their cached records; the region does
  not cache its whole return value like a Step.
- Reusing a Step or sleep ID in the successor generation creates fresh work,
  which makes polling intervals repeat without explicit invalidation.

```scala
Workflow.loop("poll-job", PollState.initial) { (state, loop) =>
  val status = Step.atLeastOnce("read-status") { jobs.status(state.jobId) }

  if status.isFinished then loop.break(status.result)
  else
    Workflow.sleep("poll-interval", 5.seconds)
    state.nextAttempt
}
```

## Signals and children

- A nested region restart preserves the workflow's visibility into the global
  immutable event sequence. Exact-key signal cursors survive across loopings,
  while cached await Step rows in the region are discarded. Discarding an await
  result therefore does not make an event behind its signal cursor consumable
  again.
- Children created in a discarded generation are handled according to their
  `ParentClosePolicy`. Successor generations use distinct child identities.
- Restartable regions have the same problems as `continueAsNew`: While Step and Await IDs can be reused after deletion, child IDs cannot, since the child may continue to live independently for a short time (there ID may then collide with the newly started child with the same ID). Thus, restartable regions must also include the loop count in the child's `WorkflowInstanceId` derivation (in the `scope` field), just like `continueAsNew` does for the generation. Each enclosing region's key and `restartCount` therefore
  contributes to the child's `scope`. The complete scope derivation rules are defined in `sub-workflows-iteration.md`.

## Parallel branches

`restart` and `break` use library control-flow exceptions. If either crosses a
`Workflow.parallel` boundary, `Workflow.parallel` handles cleanup of the other branches
internally, just as it does for other control-flow exceptions.

## Why restartable regions cannot completely replace continue-as-new:

- `continueAsNew` also deletes events addressed to the workflow instance, while
  retaining its exact-key cursors. A restartable region cannot delete those
  events because signal visibility belongs to the workflow instance as a whole,
  whereas Step rows are owned by specific code sections and can be reset
  independently.
- `continueAsNew` is more appropriate for recursion.
