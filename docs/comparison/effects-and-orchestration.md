# Effects & Orchestration

Steps and their guarantees, caching, retries, idempotency, parallelism,
subworkflows, child workflows, continue-as-new/fork/reset, scheduling, and
cancellation — with the **implementation mechanism** and guarantee for each.

Terminology follows [`README.md`](README.md) §1. Companion documents:
[`execution-and-durability.md`](execution-and-durability.md),
[`events-signals-timers-evolution.md`](events-signals-timers-evolution.md).

---

## 1. Steps: behaviour and identity

**AtomicFlow**
```scala
Step.atLeastOnce("charge", version = 1,
  ensureUnchanged = Seq("orderId" -> orderId),
  invalidateOn    = Seq("amount"  -> amount),
  invalidateAfter = 12.hours
) { payment.charge(orderId, amount) }

Step.atMostOnce("send-email") { mailer.send(to, body) }: Option[Receipt]
```
**Temporal**
```python
await workflow.execute_activity(
    charge, order, start_to_close_timeout=timedelta(seconds=30),
    retry_policy=RetryPolicy(initial_interval=..., maximum_attempts=5))
```
**DBOS**
```python
@DBOS.step(retries_allowed=True, max_attempts=5, interval_seconds=1, backoff_rate=2)
def charge(order): ...
```

| | AtomicFlow | Temporal | DBOS |
|---|---|---|---|
| Step identity | `StepId` + `version` | step type at command position | ordinal `function_id` |
| Reorder-safe identity | ✅ | ❌ | ❌ |
| Metadata affects lookup | no (`name`/`description` are cosmetic) | type name does | function name does |
| Cached-result store | `workflow_steps` row | Event History entry | `dbos.operation_outputs` row |

**Implementation.** AtomicFlow persists a `Started` row *before* the body runs,
then replaces it with `Succeeded(payload)` or `Failed(payload)`; the natural key
is `(instance, stepId, stepVersion, scope)`. Identity is a developer-chosen
string, so metadata edits never orphan a cached result — something a positional
scheme cannot offer. Temporal records an `ActivityTaskScheduled` event at a
command position and an `ActivityTaskCompleted` event with the result; replay
matches by position. DBOS inserts `(workflow_uuid, function_id, output)` where
`function_id` is a monotonically increasing counter of the call order within the
workflow. Both positional schemes are why determinism is mandatory.

---

## 2. Execution guarantees (the central divergence)

AtomicFlow exposes two orthogonal axes (`spec/steps.md`):

- **Axis 1 — guarantee:** `atLeastOnce` vs `atMostOnce`.
- **Axis 2 — cache-drift policy:** `ensureUnchanged`, `invalidateOn`, `invalidateAfter` (TTL).

Both guarantees persist a `Started` row before the body and differ only for an
unresolved `Started` row on replay:

```scala
Step.atLeastOnce("fetch")  { fetch() }        // unresolved Started → re-execute
Step.atMostOnce("charge")  { charge() }       // unresolved Started → return None
```

| Replay of an unresolved `Started` row | AtomicFlow | Temporal | DBOS |
|---|---|---|---|
| Re-execute | `atLeastOnce` | step attempt retried | step retried |
| Do **not** re-execute | `atMostOnce` → `None` | ❌ | ❌ |

Four patterns:

| | `ensureUnchanged` | `invalidateOn` |
|---|---|---|
| **at-least-once** | "must never change" + auto-retry on crash | "expected to change" + auto-retry |
| **at-most-once** | "must never change" + no crash-retry | "intentional re-run" + no crash-retry |

| Guarantee | AtomicFlow | Temporal | DBOS |
|---|---|---|---|
| at-least-once step | `Step.atLeastOnce` | `execute_activity` | `@DBOS.step` |
| at-most-once step | `Step.atMostOnce` | — | — |
| exactly-once DB write | — (compensate with `uncancellable` Saga) | — (idempotency keys) | `@DBOS.transaction` |
| never re-execute once completed | cached row | recorded result | checkpointed output |

**Implementation.**
- *AtomicFlow:* "commit before observation" — the step's value or exception is
  serialized, deserialized, **persisted**, and only then returned/thrown, so the
  first run and replay take the same downstream branch. Object identity is never
  preserved. `atMostOnce` persists the same `Started` marker but its replay policy
  returns `None` instead of re-running. Fatal JVM errors and the library's
  control-flow exceptions are never cached as step outcomes.
- *Temporal:* the activity result lives in Event History; the platform provides an
  "effectively once" experience across attempts via the recorded
  `ActivityTaskCompleted`, but a step that never recorded completion is retried
  (at-least-once). There is no per-step knob to *refuse* retry after a crash.
- *DBOS:* the step output is checkpointed; once checkpointed it is never
  re-executed (at-least-once). For database writes, a `transaction` step
  co-commits the application write with the DBOS checkpoint in one Postgres
  transaction, giving exactly-once for that write.

---

## 3. Caching, fingerprints, and invalidation

**AtomicFlow**
```scala
Step.atLeastOnce("quote", version = 2,
  ensureUnchanged = Seq("customer" -> id),
  invalidateOn    = Seq("market" -> market),
  invalidateAfter = 12.hours
) { quoteApi(id, market) }
```
**Temporal** — cache reuse is positional; changing inputs means a different
command, not a cache miss (no snippet needed).
**DBOS** — reuse is positional; no per-step invalidation API.

| | AtomicFlow | Temporal | DBOS |
|---|---|---|---|
| Reuse key | `(instance, stepId, version, scope)` | command position | `(workflow_uuid, function_id)` |
| Input drift by named keys | ✅ | ❌ | ❌ |
| TTL on cached result | ✅ | ❌ | ❌ |
| Concurrent re-run after invalidation | new row replaces old atomically, fenced | n/a | n/a |

**Implementation.** AtomicFlow stores `input_fingerprints` (SHA-256 over named
`"key" -> value` pairs, sorted by name for order-insensitivity) **separately from
the natural key**, so a replay *finds* the row and then judges it:

- `ensureUnchanged` difference → `StepInputConflictException`, workflow blocked
  for manual intervention (no re-execution);
- `invalidateOn` difference → the existing row is replaced and the body re-runs;
- `invalidateAfter` → the row expires and re-runs; it also invalidates ongoing
  retries and terminal failures ("as if the step never executed").

Temporal and DBOS have no equivalent policy: a recorded result is effectively
permanent, and "changing" a step means patching/versioning so the replay
bookkeeping still lines up.

Data-format evolution is separate from control-flow evolution: AtomicFlow's
`Cacheable.withFallback` tries the current codec then ordered fallbacks (see
[`events-signals-timers-evolution.md`](events-signals-timers-evolution.md) §7).

---

## 4. Retries

**AtomicFlow**
```scala
Step.atLeastOnce("check", retry = Step.RetryPolicy.exponentialBackoff(
  initialDelay = 1.minute, maxCumulativeDelay = 3.days)) { check() }
```
**Temporal**
```python
RetryPolicy(initial_interval=timedelta(seconds=1), backoff_coefficient=2.0, maximum_attempts=5)
```
**DBOS**
```python
@DBOS.step(retries_allowed=True, max_attempts=5, interval_seconds=1.0, backoff_rate=2.0)
```

| | AtomicFlow | Temporal | DBOS |
|---|---|---|---|
| Scope | per at-least-once step | per step | per step |
| Policy | `never` / `fixedDelay` / `exponentialBackoff` | interval/coefficient/attempts/non-retryable | attempts/interval/backoff/`should_retry` |
| Long delay | **durable suspension** above `durableRetryThreshold`, inline sleep below | durable timer, server-driven | durable, DB wakeup |
| Crash during retry | "as if the retry was never begun" | server redelivers | checkpoint-driven |
| Separate from at-least/at-most-once | ✅ documented as orthogonal | step-level | step-level |

**Implementation.** AtomicFlow makes the delay itself durable: a delay longer
than `durableRetryThreshold` becomes an ordinary **timer subscription** (the run
suspends and releases the lease/thread); below the threshold it is an inline
sleep. The retry bookkeeping is stored in the `Started` row's payload, and
`invalidateAfter` also invalidates an in-flight retry. Temporal schedules the
retry on the server side (durable timer Event) and redelivers the activity task.
DBOS retries inside the step invocation between checkpoints; a crash mid-retry
re-executes from the last checkpoint.

---

## 5. Idempotency and external effects

| | AtomicFlow | Temporal | DBOS |
|---|---|---|---|
| Instance-level | create-if-absent by identity; `WorkflowInputConflictException` on differing input | Workflow Id + reuse/conflict policy | Workflow Id is an idempotency key (one execution per id) |
| Step-level dedup | cached row + `atMostOnce` | recorded activity result | checkpointed output |
| External provider dedup | **deferred** (external idempotency tokens) | manual idempotency keys | manual idempotency keys |
| DB-write exactly-once | — | — | `@DBOS.transaction` |

**Implementation.** DBOS's `workflow_status` row makes a workflow id a single
execution: re-invoking with the same id returns the recorded result; a
`creator_xid` transaction-id field prevents duplicate starts racing. Temporal
separates Workflow Id from Run Id, so an id can have multiple runs (continue-as-
new, retries, cron), and deduplication at start is governed by the reuse/conflict
policies. AtomicFlow uses `(workflowId, instanceKey, scope)` and compares
persisted input by byte-equality, throwing on conflict.

---

## 6. Parallelism

**AtomicFlow**
```scala
val results: Seq[R] = Workflow.parallel(items.map(i => () => Workflow.scoped(i) { process(i) }))
val winner = Step.awaitRace("approval-or-deadline")(
  "approval" -> Awaitable.SignalEvent(approval),
  "deadline" -> Awaitable.Timer(1.hour))
```
**Temporal** — deterministic concurrency via futures/selectors (no snippet).
**DBOS**
```python
results = await asyncio.gather(step1(a), step2(b), return_exceptions=True)  # deterministic start order
```

| | AtomicFlow | Temporal | DBOS |
|---|---|---|---|
| Fan-out | `Workflow.parallel` (Ox) | futures, selectors | `gather` (deterministic start order), child workflows |
| Race | `awaitRace`, `firstToRunWithoutSuspension` | `Selector` | `asyncio_wait` |
| Partial collection | `Workflow.runToSuspension` per branch | futures + selectors | `gather(..., return_exceptions=True)` |

**Implementation.** AtomicFlow's `Workflow.parallel` uses **join-then-propagate**
(DEVIATIONS #40): every branch outcome is collected as a *value*, all branches are
joined to completion, and only then is a control-flow exception rethrown. The
reason is concrete: interrupting a sibling thread mid-JDBC-write was unsafe (an
interrupt-swallowing dead backend held a row lock indefinitely). If some branches
complete and others suspend, one combined `WorkflowSuspendedException` carries
each branch's suspension. Branch bodies are context functions, so scope and
`uncancellable` depth travel with the context onto other threads.

DBOS requires steps to *start* in a deterministic order; interleaving two step
sequences with `gather` is disallowed (the ordinals would race), and the
recommended substitute is child workflows. Temporal's concurrency is
deterministic by construction (the SDK event loop).

---

## 7. Inline subworkflow scopes (AtomicFlow-only)

**AtomicFlow**
```scala
Workflow.scoped(order) { process(order) }        // key derived from fingerprint
Workflow.scoped("approval-check") { ... }         // explicit key
```
Temporal/DBOS: no equivalent; factor code or use child workflows.

**Implementation.** A subworkflow scope is an **ID namespace**, not a separate
instance. Step and wait-site rows are keyed partly by
`step_scope_within_instance`, a `TEXT[]` segment array; nested `scoped` blocks and
restartable/loop regions extend the array. The scope path is part of the primary
key, so the same step definition executes independently per element while
sharing the parent's history, lease, and event cursors. Cost is
**O(events × elements)**: the parent re-runs on every event touching any element.
No escaping/shortening is performed on segments (structural injectivity), and an
overlong derived scope is rejected rather than truncated.

---

## 8. Child workflows and parent-close

**AtomicFlow**
```scala
val child = workerWf.startAsChild(childKey, input,
  parentClosePolicy = ParentClosePolicy.Cancel,           // or .Abandon
  inheritSignals = SignalInheritance.some("approval/"),
  inheritPastEvents = false)
val result = Step.await("child-result", child.completion)
```
**Temporal**
```python
result = await workflow.execute_child_workflow(Child.run, arg,
    parent_close_policy=ParentClosePolicy.ABANDON)
```
**DBOS**
```python
handle = DBOS.start_workflow(child, arg)
result = handle.get_result()
```

| | AtomicFlow | Temporal | DBOS |
|---|---|---|---|
| Start | `startAsChild` (create-if-absent, idempotent on replay) | `ExecuteChildWorkflow` | nested `start_workflow` |
| Child identity | derived scope from parent identity + generation + enclosing scopes | child Workflow Id/Run Id | child workflow id; `parent_workflow_id` |
| Inline execution | never (scheduled via wakeups) | server-mediated | inline or background |
| Awaiting | `child.completion` → `WorkflowCompletionResult` | await child result | `handle.get_result()` |
| Parent-close policies | `Cancel`, `Abandon` | `Terminate`, `Abandon`, `RequestCancel` | ◑ via cancellation |
| Child signal inheritance | ✅ | ❌ | ❌ |
| Across continue-as-new | old generation's children closed async | children not carried over | n/a |

**Implementation.** AtomicFlow's parent-close delivery is **asynchronous**: the
parent's terminal/continue-as-new/reset/region-restart transition writes a row to
`child_close_requests` (keyed by the parent's identity + a `target_prefix`) and
never touches child rows. Delivery happens at the child's own run start or via a
dedicated sweep, one child per short transaction, so **no transaction ever locks
rows of more than one instance**. Consequences: the active parent pointer can
linger briefly; a child can run (even complete) before its cancellation is
delivered; `cancelTimeout` is measured from delivery. A cancelled/abandoned child
replays already-consumed inherited signals from its cached step results, so the
source event rows are not required.

Temporal propagates cancellation/termination from parent to child per the
parent-close policy when the parent closes; children have their own histories and
are not carried over continue-as-new. DBOS starts a child workflow and records
`parent_workflow_id`; cancellation/timeout propagates to the child tree, but
there is no explicit parent-close policy enum.

---

## 9. Continue-as-new

**AtomicFlow**
```scala
Workflow.continueAsNew(nextState)   // recursive tail transition, returns Nothing
```
**Temporal**
```python
workflow.continue_as_new(new_state)
```
**DBOS** — no primitive; the migration guide recommends storing long-lived objects
in the database and using shorter workflows + queues.

| | AtomicFlow | Temporal | DBOS |
|---|---|---|---|
| Identity after | same instance, `generation++` | same Workflow Id, **new Run Id** | — |
| History | step rows/subscriptions/wakeups erased; cursors kept; directly-addressed signal events deleted | fresh Event History | — |
| Children | closed asynchronously per policy | not carried over | — |
| Run outcome | `WorkflowRunResult.ContinueAsNew`; successor scheduled | new Run starts | — |

**Implementation.** AtomicFlow performs an atomic in-place transition: increment
`generation`, erase the old generation's step/subscription/wakeup rows while
preserving exact-key signal cursors, delete directly-addressed `Signal` events,
and write a `child_close_requests` row for the closing generation's children. The
marker `key@generation` makes old and new children structurally non-colliding.
Temporal's continue-as-new creates a new Run (same Workflow Id) with a fresh
Event History; from the parent's perspective the chain is one execution.

AtomicFlow additionally offers **restartable regions** (`Workflow.restartable` /
`Workflow.loop`): a bounded-history loop *inside* one instance where each
iteration replaces the region row's state and discards the previous generation's
nested records. This is finer-grained than continue-as-new and has no direct
analogue elsewhere.

---

## 10. Fork and reset

**AtomicFlow**
```scala
runtime.forkWorkflow(sourceId, newInstanceKey, restartFromStep = stepId)   // new identity
runtime.resetWorkflow(sourceId, restartFromStep = stepId)                  // in place
```
**DBOS**
```python
DBOS.fork_workflow(workflow_id, start_step)   # programmatic + UI
```
**Temporal** — Reset-to-event via server/CLI (no arbitrary fork).

| | AtomicFlow | Temporal | DBOS |
|---|---|---|---|
| Fork to new instance | ✅ | ❌ | ✅ |
| In-place reset | ✅ | ✅ (new Run from an event) | ❌ |
| Boundary | exclusive `restartFromStep`, ordered by row `updated_at` | target event id | start step |
| Purpose | recover from a buggy step; branch a corrected copy | replay from earlier point | restart many failed workflows from a step after an outage |

**Implementation.** AtomicFlow's fork copies cached rows **strictly before** the
boundary (ordered by `updated_at`, rewritten to the new identity) and omits the
selected step and everything after, so execution replays the copied prefix and
re-executes from the boundary. Reset does the in-place analogue: `generation++`,
erases from the boundary onward, and closes old-generation children. Both carry a
documented limitation: a single step id is an imperfect causal cut through
parallel branches with no total order; boundary-tied rows are conservatively
re-executed/erased. DBOS's fork creates a new workflow id, copying the first
`start_step` checkpointed outputs (tracked via `forked_from` / `was_forked_from`).

---

## 11. Scheduling, queues, cron

**AtomicFlow**
```scala
wf.createAndSchedule("order-1", order)   // registrar + upsert a wakeup
runtime.startJobRunner(Seq(orderWf, payWf), JobRunnerSettings(...))
```
**Temporal**
```python
await client.create_schedule("daily", Schedule(action=..., spec=ScheduleSpec(cron_expressions=["0 9 * * *"])))
```
**DBOS**
```python
DBOS.register_queue("orders", global_concurrency=10)
DBOS.enqueue_workflow("orders", process_order, order)
DBOS.create_schedule(schedule_name="daily", workflow_fn=daily_report, schedule="0 9 * * *")
```

| | AtomicFlow | Temporal | DBOS |
|---|---|---|---|
| Scheduling queue | `workflow_wakeups` (one coalesced row per instance) | server Task Queue (Matching Service) | durable queues (`dbos.queues`) |
| Flow control | per-workflow `maxConcurrentInstances` (per process) + fairness batch share | task-queue routing; server rate limits | global/worker/partition concurrency, rate limits, priority, dedup, debounce |
| Cron | ❌ not first-class | ✅ Schedules | ✅ `create_schedule` (backfill, pause, trigger) |
| Delayed start | wakeup `scheduled_at` | start delay / cron | `delay_until`, debounce |

**Implementation.** AtomicFlow's wakeup queue is **instance-addressed only**:
every state-affecting event upserts one row meaning "this instance may have work"
(`ON CONFLICT DO NOTHING` so an existing row's timestamps are never reset).
Runners claim with `FOR UPDATE SKIP LOCKED`, rank per workflow for fairness, and
delete the row only after acquiring the lease. Unknown workflows (not in a
runner's registry) are invisible to that runner, so several applications can share
the tables. Temporal's Matching Service hosts task queues and matches workers to
tasks; the History Service's transfer queue dispatches them transactionally.
DBOS queues are the richest: they are SQL rows claimed with concurrency/rate/
partition limits, priority, dedup, and debounce, and can be enqueued from another
application or from a PL/pgSQL function.

---

## 12. Cancellation and termination

**AtomicFlow**
```scala
runtime.cancel(instanceId)                         // durable flag, delivered at checkpoints
Workflow.uncancellable { compensate() }            // suppress delivery during compensation
runtime.terminate(instanceId)                      // force stop + fencing revoke
```
**Temporal** — `handle.cancel()` (propagates through scopes); `handle.terminate()`.
**DBOS** — `DBOS.cancel_workflow(id)` (preempts at the next step boundary).

| | AtomicFlow | Temporal | DBOS |
|---|---|---|---|
| Cooperative cancel | durable `cancel_requested_at` flag | cancellation request through scopes | `cancel_workflow`, preempt at step boundary |
| Force stop | `terminate` (guarded transition + lease revoke + fenced orphan) | Terminate | cancel/timeout |
| Suppress cancellation | `Workflow.uncancellable` (Saga) | disconnected cancellation scope | mark step preemptible |
| Escalation | `cancelTimeout` sweep → `TERMINATED` | server-driven | workflow timeout cancels tree |

**Implementation.** AtomicFlow never interrupts a thread. `cancel` durably sets
`cancel_requested_at` (once, never reset); the flag is delivered at **new-work
checkpoints** — a step body about to execute, or a wait site about to be evaluated
— as `WorkflowCancelledException`; cached replays never deliver. A `CREATED`
instance with no execution state is finalized `CANCELLED` immediately. `terminate`
runs one transaction: guarded terminal transition to `TERMINATED` (+ completion
event), a **fencing-token bump that revokes the lease** (fencing out any orphan
run), and terminal cleanup of wakeups/subscriptions. `uncancellable` is a lexical,
re-entrant depth on the context that suppresses *delivery* (not heartbeats), so a
long Saga compensation keeps its lease while the `cancelTimeout` escalation still
bounds it; on replay, completed compensation steps are served from cache. DBOS
preempts at the start of the next step (or immediately for a step marked
preemptible); Temporal delivers cancellation through cancellation scopes.

---

## 13. Feature summary

| Capability | AtomicFlow | Temporal | DBOS |
|---|---|---|---|
| Per-step at-least-once | ✅ | ✅ | ✅ |
| Per-step at-most-once | ✅ | ❌ | ❌ |
| Exactly-once DB write | ◑ (Saga) | ◑ (keys) | ✅ (transaction) |
| Input-drift invalidation | ✅ | ❌ | ❌ |
| TTL on cached result | ✅ | ❌ | ❌ |
| Reorder-safe step identity | ✅ | ❌ | ❌ |
| Inline subworkflow scope | ✅ | ❌ | ❌ |
| Child workflows | ✅ | ✅ | ✅ |
| Parent-close policy | ✅ | ✅ | ◑ |
| Child signal inheritance | ✅ | ❌ | ❌ |
| Continue-as-new | ✅ | ✅ | ❌ |
| Restartable regions | ✅ | ❌ | ◑ |
| Fork from step | ✅ | ◑ | ✅ |
| Durable queues with flow control | ◑ | ◑ | ✅ |
| Cron schedules | ❌ | ✅ | ✅ |
| Durable cancellation + Saga region | ✅ | ◑ | ◑ |
