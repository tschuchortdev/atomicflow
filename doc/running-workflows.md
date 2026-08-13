# Running Workflows

Guideline for the API to create, run, and observe workflow instances. Companion to `design.md` (which stays as the original draft).

## API layers

Single-instance operations exist on up to three levels; only `WorkflowRuntime` is implemented per backend, everything else is a one-line forwarder written once.

| Layer | Role |
|---|---|
| `WorkflowRuntime` (trait) | Canonical home of all operations. Implemented per backend (in-memory, Postgres, ...). Convenience methods are `final`, built from a small set of abstract primitives. |
| `Workflow[In, Out]` methods | Pre-instance sugar (`create`, `run`, `createAndRun`, ...). Forward to a contextual `(using WorkflowRuntime)`. |
| `WorkflowInstance[Out]` | Post-instance typed handle. Captures the runtime it came from, so no `using` clause needed. |

- Why hybrid: `myWorkflow.createAndRun(...)` reads naturally for the common case; the runtime stays the single implementation point, so backends and tests only deal with one interface.
- Rule of thumb: `Workflow` = "I don't have an instance yet", `WorkflowInstance` = "I do".
- Contextual access: `WorkflowRuntime()` summons the given runtime (`WorkflowRuntime.apply(using rt)`).

## Core operations

```scala
given WorkflowRuntime = ... // backend implementation

// Register key + input only, don't run (idempotent)
myWorkflow.create(instanceKey, input): Unit

// Run a previously created instance (no input parameter!)
myWorkflow.run(instanceKey): Either[StoppedWorkflow[Out], Out]

// Atomic create-if-absent + run
myWorkflow.createAndRun(instanceKey, input): Either[StoppedWorkflow[Out], Out]
```

- `create` and `run` are separate because `run` takes no input: the input is already persisted. `createAndRun` covers the common atomic case. All three are needed.
- `create` exists for deferred runs, e.g. registering a workflow in the same DB transaction as business data, or preparing a batch before running it.
- Idempotency: `create`/`createAndRun` succeed if the instance already exists with equal input (`createAndRun` re-runs/returns the result); they throw `WorkflowInputConflictException` if the input differs. No separate `createIfNotExists` needed. `createWorkflowInstanceDiscardExisting` exists for explicitly replacing an instance.
- Input is a **single parameter** (`In`); multiple values are passed as a case class or tuple. Scala 3 has no auto-tupling, so multi-param call sites would require type-level machinery (`TupledFunction` / match types) — can be added later as sugar without breaking the core API.

## Run semantics

- `run` always works the same regardless of instance state: it takes a lease (lock) on the instance, executes the workflow function from the top, and replays previously executed steps from the `StepCache` instead of re-executing them.
- The lease prevents concurrent executions of the same instance.
- Already-completed instances are a terminal state: `run` returns the stored result immediately without executing the body.
- Completed instances must eventually be deletable automatically (retention policy) — TODO

## Suspension and results

- A workflow that stops itself (waiting on a signal/timer/other workflow) yields `Left(StoppedWorkflow[Out])`; successful completion yields `Right(out)`.
- No machine-readable suspension reason — a debug string / stack trace is enough. A structured reason model would be complex to implement and has no driving use case.
- Failures propagate as exceptions (direct style), not as error-encoding return values.
- The suspension of a workflow is implemented as a special exception, which is then caught in the outermost layer by the runtime. User code must be able to distinguish this exception from genuine errors. Provide an extractor (similar to `NonFatal`) for this and accompanying documentation. Document that resource finalizers must catch the exception, finalize and then rethrow. The exception must never be swallowed or logged.
- `StoppedWorkflow` offers `addContinueListener(...)` and `inefficientBlockUntilFinished()`; the name warns that blocking a thread for a long-running workflow is wasteful.
- Planned: `awaitResult(timeout)` on `WorkflowRuntime`, `Workflow`, and `WorkflowInstance` — blocks through suspensions until the instance reaches a terminal state, with a **mandatory timeout** as footgun guard. Mainly for tests and short-lived request-scoped workflows. Implementation strategy still open.

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

## Preemption: Thread interruption

Workflows run on JVM threads. Cancellation and preemption are signaled via the JVM's interrupt flag:

- **Runtime checks** the interrupted flag before and after every `Step` execution. If set, the step is aborted and the workflow suspends.
- **User code** may check the flag explicitly at any point via `Thread.currentThread().isInterrupted()` or catch `InterruptedException`.
- **Setting the flag** is how external code (via signal delivery, timeout, or manual cancellation) requests the workflow to stop. Whether the workflow honors it cooperatively depends on where execution is (mid-step vs at a checkpoint).
- Integration with signals: TBD — design API for racing code (Activities, computation) against signals via Ox or similar concurrency library.

## Open points

- `awaitResult` implementation approach (polling vs backend notification).
- Retention / auto-deletion of completed instances.
- **Cancellation mechanism with Thread.interrupt** — **TODO**: Design and implement cancellation coordination between signal delivery, timeout, and manual abort. How are interrupt flags propagated? What happens to running steps? Document the expected workflow behavior under cancellation.
- **Workflow.fork / forkFromFailure** — **TODO**: Implement DBOS-style fork primitives as an alternative to manual step invalidation. `fork(cond)` branches to a different recovery workflow on failure. How does this interact with step/await invalidation?
