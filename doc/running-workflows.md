# Running Workflows

Guideline for the API to create, run, and observe workflow instances. Companion to `design.md` (which stays as the original draft).

## API layers

Single-instance operations exist on up to three levels; only `WorkflowRuntime` is implemented per backend, everything else is a one-line forwarder written once.

| Layer | Role |
|---|---|
| `WorkflowRuntime` (trait) | Canonical home of all operations. Implemented per backend (in-memory, Postgres, ...). Convenience methods are `final`, built from a small set of abstract primitives. |
| `Workflow[In, Out]` methods | Pre-instance sugar (`create`, `run`, `createAndRun`, ...). Forward to a contextual `(using WorkflowRuntime)`. |
| `WorkflowInstance[In, Out]` | Post-instance typed handle. Captures the workflow definition and runtime it came from, so no `using` clause is needed. |

- Why hybrid: `myWorkflow.createAndRun(...)` reads naturally for the common case; the runtime stays the single implementation point, so backends and tests only deal with one interface.
- Rule of thumb: `Workflow` = "I don't have an instance yet", `WorkflowInstance` = "I do".
- Contextual access: `WorkflowRuntime()` summons the given runtime (`WorkflowRuntime.apply(using rt)`).

## Core operations

```scala
given WorkflowRuntime = ... // backend implementation

// Register key + input only, don't run (idempotent)
myWorkflow.create(instanceKey, input): WorkflowInstance[In, Out]

// Run a previously created instance (no input parameter!)
myWorkflow.run(instanceKey): WorkflowRunResult[Out]

// Atomic create-if-absent + run
myWorkflow.createAndRun(instanceKey, input): WorkflowRunResult[Out]
```

- `create` and `run` are separate because `run` takes no input: the input is already persisted. `createAndRun` covers the common atomic case. All three are needed.
- `create` exists for deferred runs.
- Idempotency: `create`/`createAndRun` succeed if the instance already exists with equal input (`createAndRun` re-runs/returns the result); they throw `WorkflowInputConflictException` if the input differs. No separate `createIfNotExists` needed. `createWorkflowInstanceDiscardExisting` exists for explicitly replacing an instance.
- Input is a **single parameter** (`In`); multiple values are passed as a case class or tuple. Scala 3 has no auto-tupling, so multi-param call sites would require type-level machinery (`TupledFunction` / match types) — can be added later as sugar without breaking the core API.

`WorkflowRunResult` is the public synchronous execution outcome:

```scala
enum WorkflowRunResult[+A]:
  case WorkflowStopped
  case WorkflowCancelled
  case Result(value: A)
```

- `WorkflowStopped` means execution durably suspended on an await.
- `WorkflowCancelled` means cooperative cancellation escaped the workflow body.
- `Result(value)` means the workflow completed successfully. Failures still propagate as exceptions.

## Run semantics

- `run` always works the same regardless of instance state: it takes a lease (lock) on the instance, executes the workflow function from the top, and replays previously executed steps from the `StepCache` instead of re-executing them.
- The lease prevents concurrent executions of the same instance.
- Already-completed instances are a terminal state: `run` returns the stored result immediately without executing the body.
- Completed instances must eventually be deletable automatically (retention policy) — TODO

## Suspension and results

- A workflow that suspends yields `WorkflowStopped`; successful completion yields `Result(out)`.
- No machine-readable suspension reason — a debug string / stack trace is enough. A structured reason model would be complex to implement and has no driving use case.
- Failures propagate as exceptions (direct style), not as error-encoding return values.
- Suspension, reset, and continue-as-new each use a distinct internal control-flow exception. The library catches them at its workflow boundary; they must never escape outside the workflow body. Broad catches and resource wrappers inside workflow code must rethrow library control-flow exceptions, so the library provides a `NonFatal`-like extractor that excludes them.
- Planned: `awaitResult(timeout)` on `WorkflowRuntime`, `Workflow`, and `WorkflowInstance` returns `WorkflowRunResult[Out]` and blocks through intermediate suspensions until the instance reaches a terminal state, with a **mandatory timeout** as footgun guard. Mainly for tests and short-lived request-scoped workflows. Implementation strategy still open.

## Multi-instance operations

Always scoped to one workflow definition — you cannot run instances without knowing their type anyway, and this keeps the API type-safe.

```scala
runtime.getWorkflowInstancesByPrefix(workflowId, keyPrefix)
runtime.getUnfinishedWorkflowInstances(workflowId, includeWaiting, limit)
runtime.deleteWorkflowInstancesByPrefix(workflowId, keyPrefix)
```

- Method names are expressive (`getWorkflowInstancesByPrefix`, not `list`).
- Broadcast signals (setting a signal on many instances) are part of the Signals chapter.

## WorkflowInstance handle (planned)

- Obtained **only** from the runtime (returned by `create`, queries, ...) — never constructed freely — so a handle always refers to an existing instance.
- Use cases: await result, query info, send signals to one instance, manual intervention (restart at step, abandon), reattach from another process.
- `WorkflowInstance[In, Out]`: captures the workflow definition (code), instance key, and runtime. See `core-types.md` for the full taxonomy (identity vs metadata vs persisted info vs handle).
- Queries parameterized by a `Workflow[In, Out]` return typed handles; key-based queries without the definition return `WorkflowInstance.Info` records instead (they cannot produce something runnable).

## Cancellation and termination

Two operations stop a running or suspended workflow instance; they differ fundamentally.

### `runtime.cancel(instanceId)` — cooperative stop

1. Persist a cancellation flag on the instance.
2. If the instance is currently **suspended** (no thread): schedule a resume. On that resume the workflow replays normally (cached steps are not re-executed). When execution reaches the next checkpoint — a pending await or a new step — the runtime throws `InterruptedException` inside whatever `try`/`catch` the workflow has reconstructed at that point. User code may catch this to run compensating `Step`s and then re-throw (or let it propagate).
3. If the instance is currently **running**: set the interrupt flag on the workflow thread. The runtime already checks this flag around every Step and Await boundary; execution surfaces `InterruptedException` at the next checkpoint.
4. **Timeout escalation.** After the runtime-configured `cancelTimeout`, if the instance has not reached a terminal state, cancel automatically escalates to `terminate`. This timeout is a global runtime option — not a per-instance field — to avoid extra DB columns.
5. **Best-effort under non-determinism.** If the workflow body is non-deterministic, replay may diverge and take a checkpoint-free path, completing before cancellation is ever observed. Workflow bodies should be deterministic (all non-determinism wrapped in Steps); this is the only boundary at which the cancel guarantee holds.

### `runtime.terminate(instanceId)` — force stop

- Atomically flip DB state to `Terminated` and revoke the lease.
- If currently running: also set the interrupt flag on the workflow thread. The orphan thread may continue briefly but can never commit — all lease-checked writes will be rejected. It self-terminates at its next checkpoint or when it observes the interrupt.
- **JVM limitation**: there is no safe way to forcibly stop a JVM thread. `terminate` guarantees the instance is *durably stopped and lease-revoked*; it cannot guarantee the orphan thread stops immediately. A thread stuck in uninterruptible blocking I/O or a tight loop that ignores interrupts will run until it next checks the flag or the process ends. Document this as an inherent JVM constraint rather than hiding it.
- Positioned as a **last-resort / manual-intervention** tool (matching the manual-intervention design goal). Prefer `cancel` for all lifecycle management; use `terminate` for stuck, buggy, or non-cooperative workflows and ops tooling.
- Intentionally absent from `ParentClosePolicy`; not a lifecycle primitive.

### Semantics and threading

- **User code inside the workflow body sees `InterruptedException`**, the same exception Ox and other concurrency libraries use. No wrapping or translation. Code can handle it with ordinary `try`/`catch`.
- The `run`/`createAndRun` functions are the outermost boundary. Any `InterruptedException` that escapes the workflow body is caught there and returned as `WorkflowCancelled`.
- **Suspension is a normal public outcome**, even though an internal exception performs the non-local control transfer. `WorkflowStopped` means the instance suspended waiting on a signal, timer, or other workflow. Cancellation during a pending await propagates when the instance resumes and the interrupt flag causes the await to throw.
- The question "how does the cancelling thread obtain a handle to the executing thread?" is answered by the runtime: it tracks which thread holds the lease for each instance internally and sets the flag directly. No thread handles are ever exposed to external callers.

### Rationale

- Two distinct operations (`cancel` vs `terminate`) are necessary because cancel's delivery is path-dependent under non-determinism — it can miss entirely. `terminate` provides the unconditional stop that `cancel` cannot guarantee.
- Re-running a workflow to deliver cancellation (rather than injecting it without replay) is consistent with the replay model and ensures `InterruptedException` surfaces inside the user's live `try`/`catch` block, where compensation code has access to local scope variables. Without replay, there is no scope to deliver into.
- User code sees `InterruptedException` (not a wrapper) so it integrates seamlessly with Ox, concurrent libraries, and standard Java cancel semantics. This avoids the fragmentation of having multiple cancellation exception types.
- Fewer execution modes is better; two (cooperative/forceful) is the minimum needed to cover the JVM and non-determinism constraints.

## `continueAsNew` — bounded-history tail loops and tail recursion

For workflows that run indefinitely (event-loop style) or recurse deeply, the instance history (step cache rows, signal log, decision records) grows without bound. `continueAsNew` resets it by atomically replacing the current execution state in place, incrementing its generation, and installing new input under the same key.

```scala
// inside a workflow body — never returns (return type Nothing)
Workflow.continueAsNew(newInput: In): Nothing
```

The public operation is available only inside an executing workflow through the `Workflow` companion forwarder. It is not an externally callable runtime operation because it relies on the current workflow context.

### Semantics

1. `continueAsNew` is implemented as a special control-flow exception, caught at the outermost runtime boundary — the same mechanism as suspension. User code that calls it experiences it as a tail call: execution stops immediately, all local state is abandoned, and the runtime takes over.
2. In one atomic transaction: erase the current execution state, increment the generation counter in place, and persist `newInput`. Older generations are not retained as separate instances or history.
3. The new generation starts with a **completely empty** step cache and signal log. No history carries over.
4. The instance key is stable across generations. External signals addressed to that key are routed to the current (latest) generation automatically.

### Unconsumed signals at the reset boundary

Before transition, the workflow's `onUnconsumedSignals` handler receives all unacknowledged events. After the handler finishes, all signal events are discarded with the rest of the old generation's execution state. Workflows that need information in the next generation must include it in `newInput`.

### Typical usage

```scala
// Eternal event loop: history resets on every iteration
val processorWf = Workflow("processor") { (state: State) =>
  val event = eventSignal.await("next-event")
  val newState = processEvent(state, event)
  Workflow.continueAsNew(newState)
}

// Paginated work: tail-recurse through cursor pages
val paginatedWf = Workflow("paginated-fetch") { (cursor: Cursor) =>
  val page = Step.atLeastOnce("fetch") { fetchPage(cursor) }
  Step.atLeastOnce("process") { process(page.items) }
  if page.hasMore then Workflow.continueAsNew(page.nextCursor)
  else page.finalResult
}
```

### Relationship to `fork`

`fork` (planned, deferred) shares some backend operations but differs in intent: it seeds a new instance from an **existing step-prefix** (for recovery or branching at a specific point in history), assigns a **new key**, and optionally keeps the source running. The two are separate named functions rather than one parameterized operation; this keeps the common case (`continueAsNew`) simple and the rare case (`fork`) explicit.

### Rationale

- A function of the runtime (with a `Workflow` forwarder) rather than a helper like `Workflow.loop`: the recursive nature is the point. Users write the recursion themselves; `continueAsNew` is the tail call. A `loop` wrapper would hide the history-reset boundary behind a callback, making the semantics less visible.
- Return type `Nothing`: `continueAsNew` is a transfer of control, not a value-producing expression. This makes it impossible to accidentally use its "return value."
- No automatic history carry-over: keeping history reset semantics simple means the implementation stays correct in edge cases (e.g., concurrent signal delivery at the reset boundary). Users who need state across generations pass it as `newInput`.

## Open points

- `awaitResult` implementation approach (polling vs backend notification).
- Retention / auto-deletion of completed instances.
- **Cancellation mechanism** — Interface and guarantee model are specified above. Implementation details still open: flag persistence, resume scheduling, timeout watchdog, lease revocation coordination between signal delivery, timeout, and manual abort.
- **`fork` / `forkFromFailure`** — Deferred. `continueAsNew` is the first use case of the shared substrate. `fork` adds branching from a step-prefix with a new key; design deferred until a concrete recovery use case drives it.
