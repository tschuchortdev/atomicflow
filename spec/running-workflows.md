# Running Workflows

Guideline for the API to create, run, and observe workflow instances. Companion to `design.md` (which stays as the original draft).

## API layers

Single-instance operations exist on up to three levels; only `WorkflowRuntime` is implemented per backend, everything else is a one-line forwarder written once.

| Layer | Role |
|---|---|
| `WorkflowRuntime` (trait) | Canonical home of all operations. Implemented per backend (in-memory, Postgres, ...). Convenience methods are `final`, built from a small set of abstract primitives. |
| `Workflow[In, Out]` methods | Pre-instance sugar (`create`, `run`, `createAndRun`, ...). Forward to a contextual `(using WorkflowRuntime)`. |
| `WorkflowInstance[In, Out]` | Post-instance typed handle. Captures the workflow definition it came from; its runtime methods take a contextual `(using WorkflowRuntime)`. |

- Why hybrid: `myWorkflow.createAndRun(...)` reads naturally for the common case; the runtime stays the single implementation point, so backends and tests only deal with one interface.
- Rule of thumb: `Workflow` = "I don't have an instance yet", `WorkflowInstance` = "I do".
- Contextual access: `WorkflowRuntime()` summons the given runtime (`WorkflowRuntime.apply(using rt)`).

## Core operations

```scala
given WorkflowRuntime = ... // backend implementation

// Register key + input only; started later by an explicit run (idempotent)
myWorkflow.create(instanceKey, input): WorkflowInstance[In, Out]

// Register + schedule: a job runner starts it (idempotent)
myWorkflow.createAndSchedule(instanceKey, input): WorkflowInstance[In, Out]

// Run a previously created instance on the caller thread (no input parameter!)
myWorkflow.run(instanceKey): WorkflowRunResult[Out]

// Atomic create-if-absent + run on the caller thread
myWorkflow.createAndRun(instanceKey, input): WorkflowRunResult[Out]
```

- `createAndSchedule` is the production fire-and-forget start: it registers the instance and upserts its first wakeup so a job runner picks it up ("Job runner and scheduling"). `createAndRun` executes inline on the caller thread until suspension or completion and leaves no wakeup behind — its suspension is covered by subscriptions, and events arriving during the run upsert a wakeup for the runner.
- `create` without scheduling exists for pre-registration: instances that are only started by an explicit later action — conditionally triggered flows, ops/migration tooling, tests. Such instances are visible via `getUnfinishedWorkflowInstances`.
- `create` and `run` are separate because `run` takes no input: the input is already persisted. All four are needed.
- Idempotency: all create variants succeed if the instance already exists with equal input; they throw `WorkflowInputConflictException` if the input differs. `createAndSchedule` on an existing instance re-upserts the wakeup — at most one redundant no-op run. No separate `createIfNotExists` needed. `createWorkflowInstanceDiscardExisting` exists for explicitly replacing an instance.
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

- `run` always works the same regardless of instance state: it takes the execution lease on the instance, executes the workflow function from the top, and replays previously executed steps from the `StepCache` instead of re-executing them.
- **A run is self-sufficient.** Every await-site it reaches evaluates from durable state alone — events, cached Step rows, and its own timer subscriptions with their stored deadlines, whose due timers it materializes itself — and resolves if the condition is satisfied, regardless of whether the scheduler ever noticed. Sweeps and wakeups are purely a latency mechanism for *unattended* instances; a run advances whether or not scheduling happened. (The timer *subscription* is not scheduling state: it is durable semantic state — see `signals-timers.md`, "Why timer awaits need a subscription".) This is what lets a caller thread drive a workflow to completion with `run` alone (see "Testing"); how awaits evaluate and race is specified in `signals-timers.md`.
- Terminal instances never execute the body again: `run` decodes the stored terminal outcome and returns or throws it — see "Terminal outcome storage" and "Re-run determinism".
- Completed instances must eventually be deletable automatically (retention policy) — TODO

## Job runner and scheduling

Workflow execution runs on two kinds of threads: **caller threads**, which execute the blocking `run`/`createAndRun` API, and **runner threads**, which belong to a started **JobRunner**. The JobRunner is a **driver loop** plus an **executor**: each cycle it invokes the runtime's sweep operations — fire due timers, escalate overlong cancellations, recover expired leases — on their cadences, and claims due wakeups to execute instances. The sweep operations are *definition-agnostic operations of the runtime* (like `sendSignal`), not sub-components of the runner: any runner services every workflow in the shared tables, including other applications'. The executor is the only part that knows workflow code — it resolves definitions from the runner's registry.

**The runner and its runtime are one implementation family.** The driver loop executes backend-internal operations — wakeup claiming, conditional lease acquisition, fenced writes, the timer-firing primitive, the sweeps — that are deliberately absent from the public `WorkflowRuntime` trait. A generic runner parameterized by the public trait is therefore impossible: each backend implements its own runner bound to its runtime, and pairings cannot be mixed and matched. `startJobRunner` lives on the runtime precisely so that a runner without its runtime — or with the wrong one — is unrepresentable.

Every background path — signals, timers, child completions, cancellation, inheritance changes, `continueAsNew` — reduces to the same mechanism: **upsert one coalesced row in `workflow_wakeups`; an executor claims it.**

```scala
trait WorkflowRuntime {
  /** Creates and starts this process's job runner. Implemented per backend:
    * the runner executes backend-internal operations and is bound to this
    * runtime — runners and runtimes cannot be mixed and matched. */
  def startJobRunner(
      definitions: Seq[Workflow[?, ?]],
      settings: JobRunnerSettings = JobRunnerSettings.default
  ): JobRunner
}

trait JobRunner {
  def stop(gracePeriod: FiniteDuration): Unit
}
```

```scala
val runner = runtime.startJobRunner(
  definitions = Seq(orderWf, paymentWf, auditWf),  // the registry — only the runner needs it
  settings = JobRunnerSettings.default
)

runner.stop(gracePeriod = 30.seconds)   // stop claiming, drain in-flight runs
```

- **One `JobRunner` type, created started; no separate handle type.** `startJobRunner` constructs *and* starts, so the returned runner needs only `stop` — a separate handle type would still be one-method ceremony. Lifecycle rules: calling `startJobRunner` while this runtime's runner is still active throws (two driver loops in one process double every sweep — a guard, not a correctness need; multi-process cooperation is unaffected); `stop` is idempotent; after `stop`, the factory may be called again. Runners of all processes sharing the storage cooperate through the database alone; no leader election, no external broker.
- The runner holds the definition registry because it is the only component that ever resolves code from an id (below).
- **Production always runs a runner; tests drive manually.** The runner is the default way workflows progress, and regular applications always start one. Tests of user workflows deliberately do not: they drive execution deterministically on caller threads via `run`/`createAndRun` (see "Testing"). Signals never need background machinery at all — senders append events and upsert wakeups on their own caller threads.
- **No sweep is public API.** Timer firing, cancellation escalation, and lease recovery are internal steps of the runner's driver loop. Await evaluation is self-sufficient (see "Run semantics"), so no on-demand sweep operation exists: escalation and recovery are never needed by caller threads (an external `run` acquires an expired lease directly through the conditional update), and timer awaits fire their own due timers.
- There is deliberately no way to run sweeps without the executor as a component: a signal-sending gateway needs no background machinery, and production needs everything together — no scenario wants services and execution as separately-managed components.
- `stop` stops claiming and waits up to `gracePeriod` for in-flight runs. Runs abandoned at process exit are recovered after lease expiry (below).
- Without any started runner nothing is lost; progress waits for an external `run` or a runner in another process.

### Definitions, registration, and execution resources

The executor holds only a `workflowInstanceId` from the wakeup row. The code it must execute is resolved from the **runner's definition registry** — the `definitions` list passed to `runtime.startJobRunner`:

- The registry maps `workflowId` to the **one current** `Workflow[?, ?]` definition. It is validated once at start (duplicate `workflowId` → error) and immutable for the runner's lifetime. The runtime stores no registry — the runner object returned by `startJobRunner` is the only holder.
- The registry is an *execution prerequisite*, not a metadata catalog — core-types.md's "no catalog" rule is about introspection and stays intact. The job runner is the one legitimate consumer that starts from an id alone.
- **Resolution path**: claim a wakeup → load the instance row → registry lookup by `workflowId` → decode the persisted input with the definition's `Cacheable[In]` → execute the body on a worker thread. `workflowVersionAtCreation` is read from the instance row into `WorkflowContext`; the current body branches on it internally (see `workflow-evolution.md`). There is no multi-version registry.
- **Claim filtering**: the executor only selects wakeups whose instance's `workflowId` is in its registry — the claim query joins `workflow_instances` and filters by the known ids (see the claim query below). A wakeup of an unknown workflow is invisible to this runner, never claimed, never requeued. This is what allows **different applications with different code to share the same tables**: each application's runners claim only their own workflows.
- A workflow known to no runner anywhere is not an error anywhere — its wakeups simply age. That failure mode is detected by wakeup-age alerting (see "Sources of work"), not by any runner failing.
- **Permissive call sites**: `create`, `createAndSchedule`, `createAndRun`, `run`, and `startAsChild` do not consult the registry. The in-hand `Workflow` object carries everything a direct call needs — identity, codecs, code — including inline execution until suspension.
- **Concurrency resources.** By default the runner owns a daemon worker pool sized by `workerThreads`; `executor = Some(...)` overrides it with a user-supplied `Executor` (e.g. a virtual-thread-per-task executor). `maxConcurrentInstances` caps concurrently executing instances per `workflowId` (default unlimited).
- **Cap enforcement at claim time.** Each runner process keeps an in-process permit count per `workflowId`. If a claimed instance's workflow is at capacity, the wakeup is deferred *in place* (`scheduled_at = now + capacityRetryDelay`, no lease taken) so a hot workflow can neither starve the FIFO batch nor pin a worker. Caps are per-process and multiply across processes; the lease still prevents concurrent execution of one instance. Caller-thread `run` bypasses caps — they are runner-side resource control, not a cross-process guarantee.

### Sources of wakeups

`workflow_wakeups (workflowInstanceId PRIMARY KEY, createdAt, scheduledAt, attempts)` is the single scheduling queue. It holds **one row per instance** — many pending events coalesce — and every row means "this instance may have durable work". The queue is **instance-addressed only**: there are no rows meaning meta-work such as "run a thread which checks if a timer is due" — timer firing is time-driven, and only its *results* (a fired timer's wakeup for the owning instance) enter the queue. A sender's upsert uses `ON CONFLICT DO NOTHING` and never resets an existing row's timestamps. Sources:

| Source | Upserts a wakeup when |
|---|---|
| Signal sender | the appended event matches a pending subscription (exact key, or inherited from an allowed ancestor) |
| Timer firing | the sweep appended a `TimerFired` event for a due, not-yet-fired timer subscription |
| Terminal transition | it appends `WorkflowCompleted` while completion subscribers exist (their wakeups — not the completing instance's) |
| `cancel` | cancellation is requested while the instance has no live lease owner |
| Inheritance broadening | the policy change may unblock awaits in the affected descendant subtree |
| `createAndSchedule` / `startAsChild` | schedules the first execution of a new instance |
| Lease recovery sweep | a non-terminal instance's lease expired while held (worker crash) |
| Executor requeue | a claimed run failed with a transient infrastructure error (delayed `scheduledAt`, `attempts + 1`) |

- `createdAt` records when the row was first inserted and is never modified — not by requeue, backoff, or capacity defer. `scheduledAt` is when the instance becomes runnable again. **Wakeup age** is measured from `createdAt` (how long the work has been pending); **overdue time** from `scheduledAt` (how long it could have run). Both are exported as metrics; the oldest due wakeup per workflow is the alert that flags work no runner claims — an unknown definition, all runners down, or anything else that prevents claiming.

- The sender never evaluates payload filters and never touches Step results; it appends the event and upserts the wakeup (see `signals-timers.md`).
- An event for a key nobody awaits creates no wakeup. The event stays in the log and the instance's next run finds it behind its cursor — wakeups accelerate subscribed awaits; the event log, not the queue, is the source of truth.
- An event arriving while the instance is running is either observed by that run directly (the run evaluates awaits against events after the current `sequenceId`) or produces a wakeup row that the run re-checks before parking ("drain", below). Together with the existing recheck-before-commit rule on suspension, no event can be missed.
- Invariant: **a durably suspended instance always has at least one pending subscription row or one wakeup row.** Suspension commits together with its subscriptions; every operation that retires a subscription (await resolution, terminal cleanup) either produces the successor wakeup or is itself part of a run that re-evaluates the instance. Firing never retires a subscription (see `signals-timers.md`, "Why timer awaits need a subscription").

### The execution lease

Ownership of an instance's execution is a **lease on the instance row, not a row lock and not an in-process mutex** (see `child-signal-inheritance.md`). Three columns on `workflow_instances`:

```text
lease_owner (nullable)   -- worker identity, e.g. "processUuid:workerId"
fencing_token (bigint)   -- stale-writer fence, incremented on every lease acquisition
lease_expires_at (nullable timestamp)
```

- **Acquire** is one conditional update — the sole concurrency arbiter between any two executors (runner threads, external `run` calls, other processes):
  ```sql
  UPDATE workflow_instances
  SET lease_owner = :worker, fencing_token = fencing_token + 1, lease_expires_at = :now + :leaseDuration
  WHERE workflow_instance_id = :id
    AND terminal_state IS NULL
    AND (lease_owner IS NULL OR lease_expires_at <= :now)
  ```
  Zero rows updated means a live owner or a terminal instance; the acquirer backs off.
- **Renewal — `Workflow.heartbeat`.** There is no background heartbeater thread; the lease is renewed on the workflow's own thread. The runtime invokes the heartbeat at every checkpoint (Step invocation, await evaluation, suspension), and long-running Step bodies call it explicitly through the public API below. `leaseDuration` must therefore exceed the longest gap between heartbeat opportunities. A lease that expires mid-Step invites takeover and at-least-once re-execution of that Step; fenced writes still prevent state corruption, but the side effect may duplicate.

**`Workflow.heartbeat()` — public lease renewal**

```scala
object Workflow {
  /** Renews the execution lease of the instance executing on the current thread,
    * extending its `lease_expires_at` by the runtime's `leaseDuration`. The
    * runtime calls this automatically at every checkpoint; call it explicitly
    * inside long-running Step bodies, between checkpoints.
    *
    * @throws LeaseLostException when the lease was taken over or the instance is terminal
    */
  def heartbeat()(using WorkflowContext): Unit
}
```

- Renewal is a **fenced write that does not bump the fencing token**:
  ```sql
  UPDATE workflow_instances
  SET lease_expires_at = :now + :leaseDuration
  WHERE workflow_instance_id = :id
    AND lease_owner = :worker
    AND fencing_token = :token
    AND terminal_state IS NULL
  ```
  Zero rows updated → the runtime raises `LeaseLostException` and aborts the run without durable effect, identical to any other fenced-write loss.
- **Not a cancellation checkpoint.** Heartbeat never delivers cancellation; delivery remains bound to checkpoints about to perform new work.
- **`Workflow.uncancellable` does not suppress renewal.** The region disables cancellation delivery only; automatic and explicit heartbeats continue inside it, so a long Saga compensation keeps its lease while the `cancelTimeout` escalation still bounds it.
- Available only inside an executing workflow: the `(using WorkflowContext)` requirement makes external or off-thread calls unrepresentable.
- **Fenced writes.** Every write that mutates execution state — Step rows, subscription rows, cursor movements, and the guarded terminal transition (on top of its `WHERE terminal_state IS NULL` guard) — carries `AND fencing_token = :token`. After a takeover, a stale run's next write affects zero rows; the runtime raises an internal `LeaseLostException` and aborts the run without durable effect.
- **Release.** A run releases the lease when it ends (suspension, terminal state, abort). Release is an optimization for prompt takeover; correctness relies only on expiry plus fencing.
- **Expiry is the crash signal**: `lease_owner IS NOT NULL AND lease_expires_at <= now()` means the worker died or stalled; the recovery sweep re-enqueues the instance.
- External `run` on a leased instance waits for the lease up to `leaseAcquireTimeout` (bounded poll of the conditional update) and then throws a runtime-owned lease-unavailable exception. It never steals a live lease.

### Claiming wakeups and the run loop

Every runner process runs this loop; processes cooperate only through storage:

```text
loop until stopped:
  BEGIN
    due = WITH ranked AS (
            SELECT w.workflow_instance_id, w.scheduled_at, w.attempts,
                   ROW_NUMBER() OVER (PARTITION BY i.workflow_id
                                      ORDER BY w.scheduled_at) AS share
            FROM workflow_wakeups w
            JOIN workflow_instances i USING (workflow_instance_id)
            WHERE w.scheduled_at <= now()
              AND i.terminal_state IS NULL
              AND i.workflow_id IN (:knownWorkflowIds)     -- this runner's registry
          )
          SELECT r.* FROM ranked r
          JOIN workflow_wakeups w USING (workflow_instance_id)
          WHERE r.share <= :perWorkflowBatchShare
          ORDER BY r.scheduled_at LIMIT :wakeupBatchSize
          FOR UPDATE OF w SKIP LOCKED
    claimed = for each selected row:
      lease acquire (conditional update above)   -- 0 rows → skip, row stays
      DELETE its wakeup row                      -- removal = accepted responsibility
  COMMIT
  if claimed.isEmpty: sleep(pollInterval with per-process jitter); continue
  for id <- claimed, dispatched onto a bounded worker pool:
    outcome = run(id)      // resolves the Workflow definition from the registry
                           // and decodes persisted input first; then the same
                           // code path as the public run API
    on suspension: if a wakeup row exists again, loop again immediately (drain)
    on terminal: done (the terminal transition already cleaned wakeups/subscriptions)
    on abort:
      lease still ours and terminal unset → requeue (transient failure, below)
      lease lost → do nothing; the new owner is responsible
    release the lease if still ours
```

- **`FOR UPDATE SKIP LOCKED` on the wakeup rows** distributes disjoint batches to concurrent runners. Without it, every process polls the same FIFO head each cycle, races on the same lease acquires, and burns poll cycles on conflicts — correct, but wasteful and convoy-prone. SKIP LOCKED is an efficiency mechanism only: the conditional lease update remains the correctness arbiter against executors that do not select through this path (external `run`, lease takeover after expiry).
- **Fairness.** The selection ranks due wakeups per `workflowId` and takes at most `perWorkflowBatchShare` of each per batch, ordered by `scheduledAt`. A burst from one workflow cannot monopolize the batch, and an instance queued behind N same-workflow instances waits at most ⌈N / share⌉ polls. Within one workflow it is FIFO by `scheduledAt`. Together with the per-workflow concurrency caps this bounds cross-workflow starvation; there is no cross-workflow ordering guarantee — the durable event order (`sequenceId`) remains the only ordering contract.
- **Registry filter.** The join with `workflow_instances` and the `IN (:knownWorkflowIds)` predicate implement claim filtering (see "Definitions, registration, and execution resources"): unknown workflows are invisible to this runner, which is what allows several applications to share one set of tables.
- Claim is atomic with lease acquisition: **the wakeup row is removed only when the worker has obtained the lease and accepted responsibility** (existing invariant). The delete matches the row's seen `scheduled_at`, so a row requeued with a future time by a previous failed attempt survives. On lease conflict the row stays and a later poll retries; the competing owner's own drain removes it.
- **Classify outcomes by durable state, not by exception type.** After the run boundary, the worker checks `terminal_state`: if set, the run is done — including `FAILED` workflows, whose decoded failure is logged and metered, never rescheduled, and left inspectable via `terminal_outcome`. If unset and the lease was lost, the new owner is responsible. Only otherwise is the failure transient.
- **Transient failures** (connection loss, deadlock, serialization failure): requeue with `created_at` unchanged, `scheduled_at = GREATEST(existing, now + backoff(attempts))`, and `attempts = attempts + 1`; capped exponential backoff, unbounded retries, `attempts` exported for alerting. A parked row delays only its own instance; other work proceeds.

### Background sweeps

Timer firing, cancellation escalation, and lease recovery are internal operations of the runner's driver loop — no public on-demand API. They run on **every** runner process (leaderless); guarded, idempotent transitions make concurrent execution harmless. Because they are definition-agnostic, every runner services every workflow in the shared tables — including other applications' — regardless of its registry. All are bounded batches:

| Sweep | Predicate | Action |
|---|---|---|
| Timer firing | due, not-yet-fired timer subscriptions | fire + upsert wakeup (mechanics: `signals-timers.md`, "Timer firing: two paths, one primitive") |
| Cancellation escalation | `cancel_requested_at IS NOT NULL AND terminal_state IS NULL AND cancel_requested_at <= now() - cancelTimeout` | guarded `terminate` transition: `TERMINATED` outcome, lease revoked (fencing-token bump), wakeups deleted |
| Lease recovery | `lease_owner IS NOT NULL AND lease_expires_at <= now() AND terminal_state IS NULL` | clear `lease_owner`, upsert wakeup |

- The timer sweep processes due timers in deadline order within bounded batches (`timerBatchSize`), so timer catch-up (after downtime or mass registration) cannot starve signal and completion events.
- **Timers progress without the sweep.** A manual run — no runner, no sweep — fires the await's own due timers during evaluation, so timer waits resolve correctly in manual mode too; the sweep exists only so *unattended* instances are noticed promptly. Mechanics in `signals-timers.md` ("Run semantics" above; "Timer firing: two paths, one primitive" there).
- The escalation sweep implements `cancelTimeout` ("Cancellation and termination") and is the only path that turns a stuck `CANCELLING` instance into `TERMINATED`.
- Lease recovery bounds worker crashes: the crashed run is fenced out by the next fencing-token bump, and the instance is replayed from its Step cache.
- The same sweep mechanism is the designated hook for future retention (auto-deletion of terminal instances).

### Children, created instances, and continue-as-new

- `startAsChild` creates the child instance and upserts the child's first wakeup; children are **never executed inline on the parent's thread**. Inline execution would hold two leases per thread and serialize large fan-outs; the parent normally suspends on `child.completion` immediately anyway.
- `CREATED` instances are started by `createAndSchedule`, `createAndRun`, an external `run`, or a bulk start over a key prefix (an upsert of one wakeup per instance). Plain `create` deliberately schedules nothing — see "Core operations".
- `continueAsNew` and restartable-region restarts re-enter the body inline on the same lease and thread. If the worker crashes after the transition commits, lease expiry plus recovery re-enqueue the successor generation.
- Hazard: a generation containing no durable suspension (no await) inside a `continueAsNew`/loop cycle pins the thread across generations. Keep at least one suspension within a bounded number of generations. A checkpoint-free spin can only be stopped by `terminate` and the `cancelTimeout` escalation — and the pinned thread still persists until its next checkpoint (JVM limitation, "Cancellation and termination").

### Transient failures and observability

- Runner-level metrics: due-wakeup queue depth, wakeup age (per workflow: `now - created_at` of the oldest due row — the alert for work no runner claims), overdue time (`now - scheduled_at`), claim-to-start latency, timer firing lag, run duration, transient requeues (`attempts`), lease takeovers, cancellations escalated to `TERMINATED`, terminal `FAILED` outcomes.
- A runner whose storage is unavailable sleeps with backoff and keeps looping; its leases expire and other processes (or itself, later) recover the instances.
- `workerThreads` (or the user-supplied `executor`) and `maxConcurrentInstances` bound concurrently *executing instances*; a run's internal parallelism (`Workflow.parallel`) is layered on top by the execution engine.

### Settings and backends

```scala
case class JobRunnerSettings(
  workerThreads: Int,                   // runtime-owned daemon pool; ignored when executor is set
  executor: Option[Executor] = None,    // user-supplied pool override (e.g. virtual threads)
  maxConcurrentInstances: WorkflowId => Int = { _ => Int.MaxValue }, // per-workflow cap, per process
  capacityRetryDelay: FiniteDuration = 1.second, // in-place wakeup defer when a cap is reached
  perWorkflowBatchShare: Int,           // fairness: max claimed wakeups per workflowId per batch
  pollInterval: FiniteDuration,         // wakeup poll; jittered per process
  wakeupBatchSize: Int,
  timerSweepInterval: FiniteDuration,
  timerBatchSize: Int,
  sweepInterval: FiniteDuration,        // escalation + recovery sweeps
  leaseDuration: FiniteDuration,        // must exceed the longest gap between Workflow.heartbeat calls
  leaseAcquireTimeout: FiniteDuration,  // wait bound for external run on a leased instance
  cancelTimeout: FiniteDuration,
)
```

- PostgreSQL is the reference implementation: the database is the queue, the lease, and the coordination point. `LISTEN/NOTIFY` can reduce claim latency; **polling remains the correctness contract** (NOTIFY is lossy).
- The in-memory backend implements the same behavior with a concurrent queue, a timer scheduler, and CAS-based leases; coalescing, exclusivity, and fencing semantics are identical, nothing is durable.
- **The runtime takes a `clock: Clock` parameter** (default `Clock.systemUTC()`) — the single time source for timer due-ness, retry thresholds, sweep predicates, and all `:now` parameters. The database server's clock is never consulted for logic. Workflow code reaches it contextually, so `Awaitable.Timer` computes deadlines from it (see `signals-timers.md`). Tests inject a small mutable `TestClock` (public utility, shipped with the in-memory backend) and advance it between runs.
- `JobRunnerSettings.forTests` bundles test-friendly defaults: tiny `pollInterval`/`timerSweepInterval`, one worker thread for deterministic child ordering. Only used when a test explicitly starts a runner (see "Testing").

### Why this design

- **One queue, instance-addressed only.** Signals, timers, completions, cancellation, inheritance changes, and crash recovery all reduce to coalesced wakeup rows, and every row means "this instance may have work" — the queue never carries meta-work like "check whether timers are due"; timer firing is time-driven and only its results enter the queue. The executor has a single claim path, and coalescing keeps queue cost O(1) per instance per burst.
- **The database is the coordinator.** No leader election, no external broker, no distributed scheduler state. Multi-process cooperation is conditional row updates — exactly the guarantee the storage backends already provide. Horizontal scaling is "start another process with a runner". The registry-filtered claim even allows different applications with different code to share the same tables.
- **Fencing instead of mutual exclusion.** Leases must survive process crashes, so exclusivity is eventually consistent. The `fencing_token` makes stale writers harmless instead of assuming they are gone, which is what makes crash recovery a bounded, self-healing event.
- **Sweeps are the liveness mechanism.** Every kind of durable pending state — wakeup row, due timer, overlong cancellation, expired lease — is discovered by a bounded, idempotent, leaderless sweep on a fixed cadence. Correctness needs no lost-wakeup reasoning beyond the existing recheck-before-commit rule.
- **Evaluation is self-sufficient; the scheduler is only latency.** Every await a run reaches resolves from durable state alone — including its own due timers, which the evaluation materializes itself. Sweeps and wakeups never affect correctness, only how quickly an unattended instance is noticed; the timer *subscription* is the deliberate exception — durable semantic state, not scheduling (`signals-timers.md`). That is what lets tests drive a workflow with `run` alone and what keeps production progress independent of any single mechanism.
- **Sweeps are runtime operations, not sub-components.** Timer firing, cancellation escalation, and lease recovery are definition-agnostic operations of the runtime, driven on a cadence by the runner's loop. None is public API — await evaluation's self-sufficiency removes the need for on-demand firing, and escalation/recovery are never needed by caller threads. There is deliberately no way to manage sweeps as a separate component: a gateway needs no background machinery, and production always needs everything together.
- **The sweep pre-fires timers for race predictability** — why, and how firing stays exactly-once, is specified in `signals-timers.md` ("Timer firing: two paths, one primitive"); this document only states the scheduling consequence above: manual mode progresses timers without any sweep.
- **Runner and runtime are one implementation family.** The runner executes backend-internal operations (claiming, lease acquisition, sweeps, timer firing) that the public `WorkflowRuntime` trait deliberately omits; a generic runner parameterized by the public trait could not exist. Each backend implements its own runner, created by its runtime via `startJobRunner`, and pairings cannot be mixed.
- **Registration on the runner, permissive call sites.** The executor is the only consumer that starts from an id alone, so the id→definition registry is passed to `startJobRunner`, lives on the runner object, and is validated once at start. Direct API calls keep working with any in-hand definition object; a workflow known to no runner anywhere surfaces through wakeup-age alerting instead of failing anywhere.

## Testing

Workflow tests deliberately do **not** start a JobRunner. The test is the driver: `createAndRun`/`run` execute inline on the test's thread until suspension, and nothing progresses until the test delivers the next event and calls `run` again. Single-threaded, fully deterministic, no background interference. This is possible because runs are self-sufficient (see "Run semantics") — the scheduler is never required for progress, only for latency of unattended instances.

```scala
// 1. Happy path with signal + timer — the virtual clock makes timers deterministic
val rt = InMemoryWorkflowRuntime(clock = testClock)
val instanceId = WorkflowInstanceId(orderWf.id, "order-1")

val r1 = orderWf.createAndRun("order-1", order)
assert(r1 == WorkflowSuspended)                      // waiting for approval
assert(steps.charged == 0)

approvalSignal.send(instanceId, Approved())
val r2 = orderWf.run("order-1")                      // consumes the signal, proceeds
assert(r2 == WorkflowSuspended)                      // now waiting on the SLA timer

testClock.advanceBy(31.minutes)
val r3 = orderWf.run("order-1")                      // timer due → evaluation fires it inline
assert(r3 == Result(Escalated))                      //   → escalation branch runs
assert(steps.chargeCount == 1)

// 2. Failure + durable retry
provider.failNext(2)
assert(wf.createAndRun("k", in) == WorkflowSuspended)   // step throws → retry scheduled
testClock.advanceBy(2.minutes); wf.run("k")             // retry 1 (still failing)
testClock.advanceBy(4.minutes)
assert(wf.run("k") == Result(ok))                       // retry 2 succeeds

// 3. Child workflows (fan-out) — children have derived scopes, so query by parent
val parentId = WorkflowInstanceId(parentWf.id, "p-1")
assert(parentWf.createAndRun("p-1", batch) == WorkflowSuspended)
val kids = rt.getChildWorkflowInstances(parentId)
assert(kids.size == 3)
kids.foreach(k => rt.getWorkflowInstance(workerWf, k.id).run())
assert(parentWf.run("p-1") == Result(allDone))
```

- **The manual-mode contract**: no runner, no threads. Events delivered before a `run` always precede anything that run materializes — so `advanceBy` + `run` decides timer races deterministically. In runner mode the sweep pre-fires timers earlier; occurrence windows and race ordering rules are specified in `signals-timers.md` ("Timer firing: two paths, one primitive", "Racing signals/timers").
- **Determinism guidance**: assert terminal states (`Result`, `WorkflowCancelled`, ...) and intermediate state via `Step.getExecutionState`, `getInfo`, and stub counters — never mid-flight wall-clock timing. With no runner running, `WorkflowSuspended` between runs is itself a valid assertion.
- **Recipe**: `InMemoryWorkflowRuntime(clock = testClock)` + retry threshold set low (workflow settings) so step retries become *durable* suspensions (driven by `advanceBy` + `run`) instead of inline `Thread.sleep`s. Start a real runner only for tests that specifically exercise scheduling behavior (coalescing, fairness, lease takeover): `rt.startJobRunner(definitions, JobRunnerSettings.forTests)`.
- **Footguns**: `createAndSchedule` is inert without a runner — tests use `createAndRun`/`run` (or `create` + explicit runs). `awaitResult` is a passive waiter: without a runner or further `run` calls a suspended instance times out; it is for asserting terminal states, not for driving. Cancellation delivery and compensation (`Workflow.uncancellable`) are tested the same way: `runtime.cancel` + the next `run` delivers at the frontier checkpoint.

### Runtime-internal test hooks

Testing the runtime itself needs finer-grained control than the public API. These hooks exist **package-private** (`private[atomicflow]`); the visibility rule is: *public is what user workflows need, everything else is package-private*.

- `JobRunner.runDriverCycle()` — one driver-loop iteration (sweeps + claim + dispatch) for stepping the scheduler deterministically.
- The timer-firing primitive (shared by sweep and evaluation) — invocable directly.
- Lease-expiry / takeover simulation (in-memory: an operation; Postgres tests may also manipulate rows via SQL directly).
- Instance creation with an explicit `versionAtCreation` — for workflow-evolution tests.
- Internal-state inspection: pending subscriptions, cursors, wakeup rows, step rows (in-memory: direct access; Postgres: SQL in tests).
- Metrics capture (in-memory collector).

## Instance lifecycle and terminal states

Conceptually, an instance moves through `CREATED` (registered, never started),
`RUNNING` (executing under a lease), and `SUSPENDED` (parked on an await,
timer, or child completion) until it reaches exactly one terminal state:
`COMPLETED`, `FAILED`, `CANCELLED`, or `TERMINATED`.

| From | Event | To |
|---|---|---|
| — | `create` | `CREATED` |
| `CREATED` | first `run` takes the lease | `RUNNING` |
| `RUNNING` | parked on an await, timer, or child completion | `SUSPENDED` |
| `SUSPENDED` | resume reaches the suspension point | `RUNNING` |
| `CREATED` | cancel before first start | `CANCELLED` — finalized immediately; the body never runs |
| `RUNNING` | uncaught non-control-flow exception | `FAILED` |
| any non-terminal | terminal transition (guarded) | `COMPLETED` / `FAILED` / `CANCELLED` / `TERMINATED` |

Only the terminal states are durable, user-visible state. The instance row
stores them as a nullable column plus its payload:

- `terminal_state` (nullable): `COMPLETED`, `FAILED`, `CANCELLED`, or
  `TERMINATED` once the instance is terminal; `NULL` before. (The Scala type
  is `Option[WorkflowTerminalState]`, see `core-types.md`.)
- `terminal_outcome` (nullable): the serialized terminal outcome, written by
  the terminal transition (see "Terminal outcome storage").
- `cancel_requested_at` (nullable timestamp): set when cancellation is
  requested. It is set once and never reset. It doubles as the start of the
  escalation clock for the cancellation timeout.

`CREATED`, `RUNNING`, and `SUSPENDED` are deliberately not stored statuses —
they are transient bookkeeping, not user-facing state. "Running" is lease
ownership: short-lived, rarely observable, and decided by the lease under
concurrency. "Suspended" is derivable from pending subscriptions and wakeup
rows. "Created" is simply an instance with no execution state yet. Exposing
them as stored state would suggest a precision the runtime cannot guarantee
and the user does not need. `CANCELLING` is likewise derived:
`cancel_requested_at IS NOT NULL AND terminal_state IS NULL`. Monitoring and
the cancellation-timeout sweep query this predicate.

Rules:

- **Exactly one terminal transition.** The terminal update is conditional
  (`WHERE terminal_state IS NULL`) and runs in the same transaction that
  appends the `WorkflowCompleted` event, setting `terminal_state` and
  `terminal_outcome` together. Concurrent attempts are serialized by the
  guarded update: exactly one wins; the loser re-reads and adopts the winner's
  outcome. **The first terminal event wins.** In particular, a workflow that
  catches a delivered cancellation and finishes normally is `COMPLETED` (with
  `cancel_requested_at` set) if its completion event lands first.
- **Every resume path re-checks `terminal_state` before running user code** and
  discards the execution if it is set. This is what makes late timer firings,
  signal deliveries, child completions, and duplicate resume schedules harmless
  no-ops — including after cancellation and termination.
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
- Invariant: `terminal_state` and the outcome case agree — `COMPLETED` ↔ `Completed`,
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
  values in memory while persistence stays self-describing. There is no untyped
  completion awaitable that would need serialized payloads: a bare id carries
  no output type, so it is upgraded to a typed `WorkflowInstance` handle from
  the workflow definition (`runtime.getWorkflowInstance(workflow,
  instanceId)`, which validates the workflowId) and the handle's `Out` supplies
  the type and codecs.

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
- Suspension, reset, and continue-as-new each use a distinct internal control-flow exception — suspension's is `WorkflowSuspendedException`. The library catches them at its workflow boundary; they must never escape outside the workflow body. Broad catches and resource wrappers inside workflow code must rethrow library control-flow exceptions, so the library provides a `NonFatal`-like extractor that excludes them.
- Planned: `awaitResult(timeout)` on `WorkflowRuntime`, `Workflow`, and `WorkflowInstance` returns `WorkflowRunResult[Out]` and blocks through intermediate suspensions until the instance reaches a terminal state, with a **mandatory timeout** as footgun guard. For request-scoped workflows (production, next to a runner) and for asserting on terminal states. It is a **passive waiter**: it never executes the workflow itself and never fires timers — it only waits for the terminal outcome. Progress comes from a runner or from caller-thread `run` calls; a suspended instance with no runner and no further `run` calls simply times out. Workflows that never suspend need no runner at all — `createAndRun` executes them inline to completion and `awaitResult` returns immediately. Implementation: in-memory registers a completion listener; Postgres polls the terminal projection, optionally optimized with `LISTEN/NOTIFY`. Tests drive progress deterministically with `run` and assert with `awaitResult` (see "Testing").

## Multi-instance operations

Always scoped to one workflow definition — you cannot run instances without knowing their type anyway, and this keeps the API type-safe.

```scala
runtime.getWorkflowInstancesByPrefix(workflowId, keyPrefix)
runtime.getUnfinishedWorkflowInstances(workflowId, includeWaiting, limit)
runtime.getChildWorkflowInstances(parentId: WorkflowInstanceId)
runtime.deleteWorkflowInstancesByPrefix(workflowId, keyPrefix)
```

- Method names are expressive (`getWorkflowInstancesByPrefix`, not `list`).
- `getChildWorkflowInstances` queries by the active-parent relationship. It is both an observability query ("show this instance's children") and the test driver for fan-out: children have derived scopes (`sub-workflows-iteration.md`), so `childWf.run(childKey)` cannot address them — the test upgrades each child's `Info` to a typed handle via `runtime.getWorkflowInstance(workflow, id)` and runs it.
- Broadcast signals (setting a signal on many instances) are part of the Signals chapter.

## WorkflowInstance handle (planned)

- Obtained **only** from the runtime (returned by `create`, queries, ...) — never constructed freely — so a handle always refers to an existing instance.
- Use cases: await result, query info, send signals to one instance, manual intervention (restart at step, abandon), reattach from another process.
- `WorkflowInstance[In, Out]`: captures the workflow definition (code) and instance id; its runtime methods take `(using WorkflowRuntime)`. See `core-types.md` for the full taxonomy (identity vs definition objects vs runtime contexts vs persisted info vs handle).
- Queries parameterized by a `Workflow[In, Out]` return typed handles; key-based queries without the definition return `WorkflowInstance.Info` records instead (they cannot produce something runnable).

## Cancellation and termination

Two operations stop a running or suspended workflow instance; they differ fundamentally.

### `runtime.cancel(instanceId)` — cooperative stop

Cancellation is a **level, not an edge**: `cancel_requested_at` stays set forever, and delivery repeats at every checkpoint that is about to perform new work until the instance reaches a terminal state. A **checkpoint** is any point where execution is about to touch durable state — invoking a Step (before its body runs), evaluating or resolving an await, or parking on and resuming from a suspension.

1. **Request.** In one transaction, set `cancel_requested_at` and ensure
   delivery: schedule a resume if the instance has no live owner; a live owner
   observes the flag at its next checkpoint. If the instance is already
   terminal, `cancel` is a no-op (the guarded terminal transition keeps this
   idempotent). If the instance has never started and has no execution state to
   deliver into, finalize it as `CANCELLED` immediately — the body never runs.
2. If the instance is currently **suspended** (no thread): Instance is scheduled for execution. When execution reaches the next checkpoint — a pending await or a new step — the runtime throws `WorkflowCancelledException`. User code may catch this to run compensating code and then re-throw (or let it propagate).
3. If the instance is currently **running**: Instance is scheduled for execution. The owner re-reads the durable flag around every Step and Await boundary and, when set, throws the same `WorkflowCancelledException` at that checkpoint. If the currently executing run already notices the cancellation and finishes the workflow, the job runner will notice in the future that the scheduled workflow is already finished and skip it. No thread is ever interrupted; delivery is a runtime throw at a checkpoint, uniform with the suspended path.
4. **Sticky redelivery.** If user code catches the `WorkflowCancelledException` and continues, the flag remains set and every subsequent cached checkpoint throws again. Catching it is for compensation, not for declining the cancel. The only way past it is to reach a terminal state — and if the workflow completes normally despite the pending cancel, the first-terminal-event rule applies: its `WorkflowCompleted` event wins and the instance is `COMPLETED`.
5. **Timeout escalation.** When an instance in the derived `CANCELLING` state (see "Instance lifecycle") stays non-terminal longer than the runtime-configured `cancelTimeout` — measured from `cancel_requested_at` — a background sweep escalates it to `terminate`. The timeout is a global runtime/job-runner option, not a per-instance field: a cancelling parent never waits for the children it is cancelling, so no child-side timeout declaration exists. It is a last line of defense and must be sized to allow long-running `uncancellable` Saga compensations to finish. Escalation produces `TERMINATED` (never scheduled again); an orphan thread still executing a Step is handled like any terminated instance (below).
6. **Best-effort under non-determinism.** If the workflow body is non-deterministic, replay may diverge and take a checkpoint-free path, completing before cancellation is ever observed. The first-terminal-event rule then makes the instance `COMPLETED`. Workflow bodies should be deterministic (all non-determinism wrapped in Steps); this is the only boundary at which the cancel guarantee holds.

### `runtime.terminate(instanceId)` — force stop

- Atomically flip DB state to `Terminated` via the guarded terminal transition
  (which also appends the `WorkflowCompleted` event with a `Terminated`
  outcome), revoke the lease, and never schedule the instance again. No resume
  is scheduled and no `WorkflowCancelledException` is delivered — the body gets no
  chance to run.
- If currently running: the orphan thread may continue briefly but can never
  commit — all lease-checked writes are rejected, and its next checkpoint
  re-check observes the stored terminal state and discards the execution.
- **JVM limitation**: there is no safe way to forcibly stop a JVM thread. `terminate` guarantees the instance is *durably stopped and lease-revoked*; it cannot guarantee the orphan thread stops immediately. A thread stuck in uninterruptible blocking I/O or a tight loop will run until it next reaches a checkpoint or the process ends. Document this as an inherent JVM constraint rather than hiding it.
- Positioned as a **last-resort / manual-intervention** tool (matching the manual-intervention design goal) and as the escalation target of `cancelTimeout`. Prefer `cancel` for all lifecycle management; use `terminate` for stuck, buggy, or non-cooperative workflows and ops tooling.
- Intentionally absent from `ParentClosePolicy`; not a lifecycle primitive.

### Delivery mechanics

- **Delivery points are frontier checkpoints.** While `cancel_requested_at` is set, the runtime throws `WorkflowCancelledException` at every checkpoint that is about to perform *new* work (not cached) — a Step body about to execute, or an await about to be evaluated or resolved. Replay of cached Step results never delivers. On every resume, delivery therefore happens at the same place: the first checkpoint without a cached result — exactly the pending await or the next not-yet-run Step, where the user's reconstructed `try`/`catch` and its local scope live. The Step cache is what tells the runtime where that point is; later not-yet-run Steps are never reached, because the frontier throws first.
- **`WorkflowCancelledException` is a public, runtime-owned `RuntimeException`.** Ordinary cleanup works: `scala.util.control.NonFatal` catches it, so `catch { case NonFatal(e) => ... }` blocks run on cancellation. It is deliberately *not* one of the internal control-flow exceptions (suspension, reset, continue-as-new) that must be rethrown and are excluded by the library's `NonFatal`-like extractor — catching cancellation is legitimate user behavior.
- **`Thread.interrupt` plays no role.** There is no JDK interrupt semantics to mimic: no interrupt flag is set, cleared, or read by the runtime. A stray or user-set interrupt on a worker thread has no contractual meaning — an interrupt targets a thread, but the unit of semantics here is the durable workflow — and the runtime clears interrupt status it observes to protect its own I/O. The supported channel is `cancel` plus the durable flag.
- **Level, with one escape hatch.** Redelivery repeats at every new-work checkpoint until the instance is terminal; the only suppression is `Workflow.uncancellable` (below). A workflow that catches and continues can never outrun its cancellation.
- Known limitation: a throw at a checkpoint cannot unblock a library call that is already in progress inside a Step body. Cancellation is observed at the next checkpoint; a long-running Step delays it until the Step returns. The `cancelTimeout` escalation bounds the wait.
- The `run`/`createAndRun` functions are the outermost boundary. Any `WorkflowCancelledException` that escapes the workflow body is caught there and returned as `WorkflowCancelled`.
- **Suspension is a normal public outcome**, even though an internal exception (`WorkflowSuspendedException`) performs the non-local control transfer. `WorkflowSuspended` means the instance suspended waiting on a signal, timer, or other workflow. Cancellation during a pending await propagates when the instance resumes and the frontier checkpoint throws.
- No thread handles are ever exposed to external callers: the runtime needs none, because delivery goes through the durable flag and checkpoint checks, not through cross-thread signaling.

### `Workflow.uncancellable[R](f: WorkflowCtx ?=> R)` — the compensation region

Temporarily disables cancellation delivery inside a lexical region. This is the
mechanism for durable compensation (Saga pattern) and for finishing a unit of
work before stopping:

```scala
try {
  bookFlight()   // Steps
  bookHotel()    // Steps
} catch {
  case e: WorkflowCancelledException =>
    Workflow.uncancellable {
      refundFlight()   // ordinary durable Steps: cached, replayable
      refundHotel()
    }
    throw e
}
```

- Inside the region, checkpoints do not deliver cancellation — Step bodies
  execute and awaits resolve normally. Pending awaits inside the region still
  wake up from their events.
- The region does not clear `cancel_requested_at`. After it exits, the next
  new-work checkpoint throws again.
- Lexical and re-entrant. Entry is replayed user code, so the region is
  replay-deterministic: if a worker crashes mid-compensation, the resumed run
  re-delivers at the same frontier checkpoint, re-enters the catch block, and
  already-completed compensation Steps are returned from the cache.
- If the workflow swallows the cancellation and finishes normally, the
  first-terminal-event rule applies and the instance is `COMPLETED`.
- The `cancelTimeout` escalation still applies to an instance stuck inside —
  or looping in — `uncancellable` regions; size the timeout to the longest
  legitimate compensation.

### Rationale

- Two distinct operations (`cancel` vs `terminate`) are necessary because cancel's delivery is path-dependent under non-determinism — it can miss entirely. `terminate` provides the unconditional stop that `cancel` cannot guarantee.
- Re-running a workflow to deliver cancellation (rather than injecting it without replay) is consistent with the replay model and ensures `WorkflowCancelledException` surfaces inside the user's live `try`/`catch` block, where compensation code has access to local scope variables. Without replay, there is no scope to deliver into.
- `WorkflowCancelledException` is a plain public `RuntimeException` rather than `InterruptedException` so that cleanup is reachable from ordinary catch handlers — including broad `NonFatal` blocks, which `InterruptedException` would silently bypass. Catching it is legitimate user behavior; only the internal control-flow exceptions (suspension, reset, continue-as-new) are must-rethrow.
- Cancellation as a durable level — a never-reset timestamp with redelivery at every new-work checkpoint, plus `Workflow.uncancellable` as the scoped escape for compensation — instead of a one-shot signal: a workflow that catches and continues can never outrun its cancellation, and the timestamp doubles as the escalation clock without extra columns. This matches Temporal's proven shape (level delivery + detached cancellation scopes); DBOS and Inngest instead make external cancellation uncatchable and push cleanup to out-of-band handlers.
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
  val event = Step.await("next-event", Awaitable.SignalEvent(eventSignal))
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

`fork` shares some backend operations but differs in intent: it seeds a new instance from an **existing step-prefix** (for recovery or branching at a specific point in history), assigns a **new key**, and optionally keeps the source running (specified in `continue-as-new-fork-reset.md`). The two are separate named functions rather than one parameterized operation; this keeps the common case (`continueAsNew`) simple and the rare case (`fork`) explicit.

### Rationale

- A function of the runtime (with a `Workflow` forwarder) rather than a helper like `Workflow.loop`: the recursive nature is the point. Users write the recursion themselves; `continueAsNew` is the tail call. A `loop` wrapper would hide the history-reset boundary behind a callback, making the semantics less visible.
- Return type `Nothing`: `continueAsNew` is a transfer of control, not a value-producing expression. This makes it impossible to accidentally use its "return value."
- No automatic history carry-over: deleting the old generation's directly
  addressed signal events bounds event retention. Closing its child
  relationships first makes those events unnecessary for unresolved child
  awaits, while cached child Step results remain replayable. Users who need
  state across generations pass it as `newInput`.

## Open points

- `awaitResult` — **Resolved**: passive waiter for the terminal outcome (see "Suspension and results"); tests drive progress with `run` and assert with `awaitResult`. `LISTEN/NOTIFY` is an optional latency optimization.
- Retention / auto-deletion of completed instances. The job runner's sweep mechanism is the designated hook (see "Job runner and scheduling").
- **Cancellation mechanism** — **Resolved at design level**: delivery is scheduled through the wakeup queue, the `cancelTimeout` escalation is the background sweep, and `terminate` revokes the lease via the fencing-token bump (see "Job runner and scheduling").
- **`Workflow.heartbeat`** — **Resolved**: public API on `Workflow` for explicit lease renewal (see "The execution lease"). The runtime invokes it automatically at every checkpoint; long-running Step bodies call it explicitly. Renewal is a fenced, token-preserving write; `Workflow.uncancellable` does not suppress it.
- **Scheduler tuning** — Priority/fairness classes beyond `perWorkflowBatchShare` and per-workflow caps, and Postgres claim-latency optimization (`LISTEN/NOTIFY` vs short poll) — see "Job runner and scheduling".
- **`fork` / `forkFromFailure`** — `forkWorkflow` is specified in `continue-as-new-fork-reset.md`; its one open point there is the causal boundary for parallel-branch step prefixes. `forkFromFailure` is not yet specified.
