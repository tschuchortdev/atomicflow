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
  case WorkflowSuspended
  case WorkflowCancelled
  case WorkflowTerminated
  case Result(value: A)
```

- `WorkflowSuspended` means execution durably suspended on an await.
- `WorkflowCancelled` means cooperative cancellation escaped the workflow body.
- `WorkflowTerminated` means the instance was force-stopped — by `runtime.terminate` or by the cancellation-timeout escalation.
- `Result(value)` means the workflow completed successfully. Failures still propagate as exceptions.
- Calling `run` again on a terminal instance returns the same outcome as the run that completed it — see "Re-run determinism".

## Run semantics

- `run` always works the same regardless of instance state: it takes a lease (lock) on the instance, executes the workflow function from the top, and replays previously executed steps from the `StepCache` instead of re-executing them.
- The lease prevents concurrent executions of the same instance.
- Terminal instances never execute the body again: `run` decodes the stored terminal outcome and returns or throws it — see "Terminal outcome storage" and "Re-run determinism".
- Completed instances must eventually be deletable automatically (retention policy) — TODO

## Instance lifecycle and terminal states

Every instance row carries one status plus two lifecycle columns:

- `status`: one of `CREATED`, `RUNNING`, `SUSPENDED`, `COMPLETED`, `FAILED`, `CANCELLED`, `TERMINATED` (stored uppercase; the Scala enum `WorkflowStatus` in `core-types.md` mirrors these values).
- `cancel_requested_at` (nullable timestamp): set when cancellation is requested. It is set once and never reset. It doubles as the start of the escalation clock for the cancellation timeout.
- `terminal_outcome` (nullable): the serialized terminal outcome, written by the terminal transition (see "Terminal outcome storage").

`CANCELLING` is not a stored status. It is the derived predicate
`cancel_requested_at IS NOT NULL AND status IN ('RUNNING', 'SUSPENDED')`.
Monitoring and the cancellation-timeout sweep query this predicate; storing it
would add one more transition that must be kept atomic without adding
information.

### Transitions

| From | Event | To |
|---|---|---|
| — | `create` | `CREATED` |
| `CREATED` | first `run` takes the lease | `RUNNING` |
| `RUNNING` | parked on an await, timer, or child completion | `SUSPENDED` |
| `SUSPENDED` | resume reaches the suspension point | `RUNNING` |
| `CREATED` | cancel before first start | `CANCELLED` — finalized immediately; the body never runs |
| `RUNNING` | uncaught non-control-flow exception | `FAILED` |
| any non-terminal | terminal transition (guarded) | `COMPLETED` / `FAILED` / `CANCELLED` / `TERMINATED` |

Rules:

- **Exactly one terminal transition.** The terminal status update is conditional (`WHERE status NOT IN (<terminal states>)`) and runs in the same transaction that appends the `WorkflowCompleted` event. Concurrent attempts are serialized by the guarded update: exactly one wins; the loser re-reads and adopts the winner's outcome. **The first terminal event wins.** In particular, a workflow that catches a delivered cancellation and finishes normally is `COMPLETED` (with `cancel_requested_at` set) if its completion event lands first.
- **Every resume path re-checks `status` before running user code** and discards the execution if the instance is terminal. This is what makes late timer firings, signal deliveries, child completions, and duplicate resume schedules harmless no-ops — including after cancellation and termination.
- `run` on a terminal instance never executes the body.

## Terminal outcome storage

The `WorkflowCompleted` event is the authoritative record of a terminal
outcome. It is appended exactly once, for *every* terminal transition, whatever
the reason — success, failure, cancellation, and termination alike. Its payload
is the serialized `WorkflowCompletionResult[Out]` (below).

- The `terminal_outcome` column is a **projection** of that event: the same
  serialized payload, written in the same transaction as the event and the
  guarded status update. The event is truth; the column is a read cache for the
  hot path (`run` on a terminal instance, child-completion awaits, monitoring
  queries). If the column is ever lost, replaying the `WorkflowCompleted` event
  reconstructs it.
- Invariant: `status` and the outcome case agree — `COMPLETED` ↔ `Completed`,
  `FAILED` ↔ `Failed`, `CANCELLED` ↔ `Cancelled`, `TERMINATED` ↔ `Terminated`.

### `WorkflowCompletionResult[R]`

```scala
enum WorkflowCompletionResult[+R]:
  case Completed[R](result: R)    extends WorkflowCompletionResult[R]
  case Failed(failure: Throwable) extends WorkflowCompletionResult[Nothing]
  case Cancelled                  extends WorkflowCompletionResult[Nothing]
  case Terminated                 extends WorkflowCompletionResult[Nothing]
```

- `TERMINATED` is a real terminal state, not a flavor of cancellation: only
  `cancel` gives the body a chance to react; `terminate` and the escalation
  sweep stop the instance without running user code.
- Decoding requires both codecs. The library provides
  `given Cacheable[WorkflowCompletionResult[R]]` built from
  `(Cacheable[R], Cacheable[Throwable])`; it records the selected member codec
  IDs per the `Cacheable` composition scheme (see `steps.md`), so a stored
  payload states how it was written.
- Every operation that surfaces the completion takes the codecs contextually.
  `run`/`awaitResult` already have `Cacheable[Out]` from the definition and
  receive `Cacheable[Throwable]` — the application-global throwable codec that
  every application must select explicitly (see `steps.md`). `Step.await` on a
  completion awaitable resolves the composite codec at the call site, matching
  the existing rule that the awaited result's codec is required there.
- For direct-style call sites, `completionResult.get` returns the `Completed`
  value, rethrows the `Failed` failure, and throws a runtime-owned
  `WorkflowCancelledException` / `WorkflowTerminatedException` for the other
  cases. These are ordinary exceptions user code may catch — not library
  control-flow exceptions.
- Considered and rejected: keeping the enum in serialized form (payload
  strings plus `unapply` extractor methods taking the implicit codecs).
  Rejected because every consumer would unwrap payloads by hand and the codec
  plumbing would leak into all call sites; contextual givens keep decoded
  values in memory while persistence stays self-describing. The one spot
  without a static type — a bare `WorkflowInstanceId.completion` awaitable —
  pins the child's output type at the call site (`completion[R]`) instead of
  carrying serialized payloads.

### Re-run determinism

`run` on a terminal instance is a pure function of durable state: it returns
the same outcome as the run that completed the instance.

- success → `Result(decoded value)`, decoded with the definition's
  `Cacheable[Out]`.
- failure → throws the decoded failure, decoded with the recorded throwable
  codec. Catch behavior therefore depends on the selected
  `Cacheable[Throwable]` exactly as it does for Step failures. If decoding the
  stored payload fails, the runtime throws a runtime-owned deterministic
  exception mirroring `StepSerializationFailed` instead of recursing into a
  broken codec.
- cancelled → `WorkflowCancelled`.
- terminated → `WorkflowTerminated`.

## Suspension and results

- A workflow that suspends yields `WorkflowSuspended`; successful completion yields `Result(out)`.
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
- `WorkflowInstance[In, Out]`: captures the workflow definition (code), instance id, and runtime. See `core-types.md` for the full taxonomy (identity vs definition objects vs runtime contexts vs persisted info vs handle).
- Queries parameterized by a `Workflow[In, Out]` return typed handles; key-based queries without the definition return `WorkflowInstance.Info` records instead (they cannot produce something runnable).

## Cancellation and termination

Two operations stop a running or suspended workflow instance; they differ fundamentally.

### `runtime.cancel(instanceId)` — cooperative stop

### `runtime.cancel(instanceId)` — cooperative stop

Cancellation is a **level, not an edge**: `cancel_requested_at` stays set forever, and delivery repeats at every checkpoint until the instance reaches a terminal state.

1. **Request.** In one transaction, set `cancel_requested_at` and ensure
   delivery: schedule a resume if the instance has no live owner; a live owner
   observes the flag at its next checkpoint. If the instance is already
   terminal, `cancel` is a no-op (the guarded terminal transition keeps this
   idempotent). If the instance is still `CREATED`, finalize it as `CANCELLED`
   immediately — there is no execution state to deliver into and the body never
   runs.
2. If the instance is currently **suspended** (no thread): the scheduled resume replays normally (cached steps are not re-executed). When execution reaches the next checkpoint — a pending await or a new step — the runtime throws `InterruptedException` inside whatever `try`/`catch` the workflow has reconstructed at that point. User code may catch this to run compensating `Step`s and then re-throw (or let it propagate).
3. If the instance is currently **running**: the owner re-reads the durable flag around every Step and Await boundary and, when set, throws the same synthetic `InterruptedException` at that checkpoint. No thread is ever interrupted; delivery is a runtime throw at a checkpoint, uniform with the suspended path.
4. **Sticky redelivery.** If user code catches the `InterruptedException` and continues, the flag remains set and every subsequent checkpoint throws again. Catching it is for compensation, not for declining the cancel. The only way past it is to reach a terminal state — and if the workflow completes normally despite the pending cancel, the first-terminal-event rule applies: its `WorkflowCompleted` event wins and the instance is `COMPLETED`.
5. **Timeout escalation.** When an instance in the derived `CANCELLING` state (see "Instance lifecycle") stays non-terminal longer than the runtime-configured `cancelTimeout` — measured from `cancel_requested_at` — a background sweep escalates it to `terminate`. The timeout is a global runtime/job-runner option, not a per-instance field: a cancelling parent never waits for the children it is cancelling, so no child-side timeout declaration exists. Escalation produces `TERMINATED` (never scheduled again); an orphan thread still executing a Step is handled like any terminated instance (below).
6. **Best-effort under non-determinism.** If the workflow body is non-deterministic, replay may diverge and take a checkpoint-free path, completing before cancellation is ever observed. The first-terminal-event rule then makes the instance `COMPLETED`. Workflow bodies should be deterministic (all non-determinism wrapped in Steps); this is the only boundary at which the cancel guarantee holds.

### `runtime.terminate(instanceId)` — force stop

- Atomically flip DB state to `Terminated` via the guarded terminal transition
  (which also appends the `WorkflowCompleted` event with a `Terminated`
  outcome), revoke the lease, and never schedule the instance again. No resume
  is scheduled and no `InterruptedException` is delivered — the body gets no
  chance to run.
- If currently running: the orphan thread may continue briefly but can never
  commit — all lease-checked writes are rejected, and its next checkpoint
  re-check observes the terminal status and discards the execution.
- **JVM limitation**: there is no safe way to forcibly stop a JVM thread. `terminate` guarantees the instance is *durably stopped and lease-revoked*; it cannot guarantee the orphan thread stops immediately. A thread stuck in uninterruptible blocking I/O or a tight loop will run until it next reaches a checkpoint or the process ends. Document this as an inherent JVM constraint rather than hiding it.
- Positioned as a **last-resort / manual-intervention** tool (matching the manual-intervention design goal) and as the escalation target of `cancelTimeout`. Prefer `cancel` for all lifecycle management; use `terminate` for stuck, buggy, or non-cooperative workflows and ops tooling.
- Intentionally absent from `ParentClosePolicy`; not a lifecycle primitive.

### Delivery mechanics

- **User code inside the workflow body sees `InterruptedException`** — the same exception Ox and other concurrency libraries use, and the exception `scala.util.control.NonFatal` deliberately does *not* catch. Broad `catch { case NonFatal(e) => }` cleanup blocks therefore never swallow a cancellation. No wrapping or translation.
- The synthetic `InterruptedException` is thrown with the thread's interrupt flag **cleared** (the canonical JDK shape: blocking methods clear-then-throw). Stickiness comes solely from re-checking the durable `cancel_requested_at` at every checkpoint — the thread's interrupt bit is never an input to any runtime decision. The runtime guarantees an un-set interrupt flag whenever it hands control to user code.
- `Thread.interrupt` is not part of the mechanism. A stray or user-set interrupt on a worker thread has no contractual meaning — an interrupt targets a thread, but the unit of semantics here is the durable workflow — and the runtime clears interrupt status it observes to protect its own I/O. The supported channel is `cancel` plus the durable flag.
- Known limitation: a synthetic throw cannot unblock a library call that is already in progress. Cancellation is observed at the next checkpoint; a long-running Step delays it until the Step returns. The `cancelTimeout` escalation bounds the wait.
- The `run`/`createAndRun` functions are the outermost boundary. Any `InterruptedException` that escapes the workflow body is caught there and returned as `WorkflowCancelled`.
- **Suspension is a normal public outcome**, even though an internal exception performs the non-local control transfer. `WorkflowSuspended` means the instance suspended waiting on a signal, timer, or other workflow. Cancellation during a pending await propagates when the instance resumes and the checkpoint throws.
- No thread handles are ever exposed to external callers: the runtime needs none, because delivery goes through the durable flag and checkpoint checks, not through cross-thread signaling.

### Rationale

- Two distinct operations (`cancel` vs `terminate`) are necessary because cancel's delivery is path-dependent under non-determinism — it can miss entirely. `terminate` provides the unconditional stop that `cancel` cannot guarantee.
- Re-running a workflow to deliver cancellation (rather than injecting it without replay) is consistent with the replay model and ensures `InterruptedException` surfaces inside the user's live `try`/`catch` block, where compensation code has access to local scope variables. Without replay, there is no scope to deliver into.
- User code sees `InterruptedException` (not a wrapper) so it integrates seamlessly with Ox, concurrent libraries, and standard Java cancel semantics, and so `NonFatal`-guarded cleanup does not intercept it. This avoids the fragmentation of having multiple cancellation exception types.
- Cancellation as a durable level — a never-reset timestamp with redelivery at every checkpoint — instead of a one-shot signal: a workflow that catches and continues can never outrun its cancellation, and the timestamp doubles as the escalation clock without extra columns.
- Fewer execution modes is better; two (cooperative/forceful) is the minimum needed to cover the JVM and non-determinism constraints.

## `continueAsNew` — bounded-history tail loops and tail recursion

For workflows that run indefinitely (event-loop style) or recurse deeply, the instance history (Step rows, directly addressed events, and subscriptions) grows without bound. `continueAsNew` resets it by atomically replacing the current execution state in place, incrementing its generation, and installing new input under the same key.

```scala
// inside a workflow body — never returns (return type Nothing)
Workflow.continueAsNew(newInput: In): Nothing
```

The public operation is available only inside an executing workflow through the `Workflow` companion forwarder. It is not an externally callable runtime operation because it relies on the current workflow context.

### Semantics

1. `continueAsNew` is implemented as a special control-flow exception, caught at the outermost runtime boundary — the same mechanism as suspension. User code that calls it experiences it as a tail call: execution stops immediately, all local state is abandoned, and the runtime takes over.
2. After the unconsumed-signal handler finishes, one atomic transaction closes
   old child relationships before deleting directly addressed `Signal` events
   addressed to this instance, erases the remaining current execution state
   (including old subscriptions and wakeups but not cursors), increments the generation counter,
   and persists `newInput`. Older generations are not retained separately.
3. The new generation therefore starts with a **completely empty** set of Step
   rows, subscriptions, and directly addressed event rows. Exact-key signal
   cursors survive the transition.
4. The instance key is stable across generations. External signals addressed to that key are routed to the current (latest) generation automatically.
5. The global event sequence continues independently of lifecycle deletion.
   Deleted `sequenceId`s are never reused, so exact-key signal cursors may point
   into gaps; this is expected.
6. If this workflow is itself a child, it remains attached to its parent with
   the same signal-inheritance configuration. It retains its exact-key signal
   cursors, so inherited parent signals already consumed are not consumed again.

### Unconsumed signals at the reset boundary

Before transition, the workflow's `onUnconsumedSignals` handler receives all
currently visible signals after the old generation's exact-key cursors. After
the handler finishes and old child relationships are closed, directly addressed
`Signal` events are physically deleted. Cached child Step results remain
replayable without their source event. Events belonging to other workflow
instances are not owned by this transition and are not deleted. A continuing
child remains attached to its own parent and can inherit retained parent events
according to `inheritSignals` and `inheritPastEvents`. State the successor must
own independently belongs in `newInput`.

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
- No automatic history carry-over: deleting the old generation's directly
  addressed signal events bounds event retention. Closing its child
  relationships first makes those events unnecessary for unresolved child
  awaits, while cached child Step results remain replayable. Users who need
  state across generations pass it as `newInput`.

## Open points

- `awaitResult` implementation approach (polling vs backend notification).
- Retention / auto-deletion of completed instances.
- **Cancellation mechanism** — Interface and guarantee model are specified above, including the durable `cancel_requested_at` flag, same-transaction resume scheduling, sticky redelivery, and the `cancelTimeout` escalation sweep. Implementation details still open: lease revocation coordination between signal delivery, timeout sweep, and manual abort.
- **`fork` / `forkFromFailure`** — Deferred. `continueAsNew` is the first use case of the shared substrate. `fork` adds branching from a step-prefix with a new key; design deferred until a concrete recovery use case drives it.
