# Execution & Durability Model

Instance lifecycle, replay/recovery, determinism, **concurrency control**, storage
schema, and failure handling.

Terminology follows [`README.md`](README.md) §1: *step*, *cached result*,
*execution lease*, *fencing token*, *event log*, *wakeup*, *instance*.

Companion documents: [`effects-and-orchestration.md`](effects-and-orchestration.md),
[`events-signals-timers-evolution.md`](events-signals-timers-evolution.md).

---

## 1. Programming model

**AtomicFlow** (Scala 3, direct style):
```scala
val wf = Workflow[Order, Receipt]("order") { order =>
  val ok = Step.atLeastOnce("validate") { validate(order) }
  Step.atMostOnce("charge")      { charge(order, ok) }
}
```

**Temporal** (Python):
```python
@workflow.defn
class Order:
    @workflow.run
    async def run(self, order):
        ok = await workflow.execute_activity(validate, order, start_to_close_timeout=...)
        return await workflow.execute_activity(charge, order, ...)
```

**DBOS** (Python):
```python
@DBOS.workflow()
def order(order):
    ok = validate(order)          # @DBOS.step
    return charge(order, ok)      # @DBOS.step
```

| Unified concept | AtomicFlow | Temporal | DBOS |
|---|---|---|---|
| Workflow body style | direct style, no effect monad | SDK async/decorators | plain functions |
| Waiting | `Step.await` throws an internal suspension exception | SDK `Await`/receive | blocking call / `await` |
| Step call | inline lambda `Step.atLeastOnce { ... }` | `execute_activity(...)` | call a step-decorated function |
| Non-determinism allowed in body | yes | no (must be in a step) | no (must be in a step) |

AtomicFlow suspends by throwing a control-flow exception that the run boundary
catches; there is no scheduler callback into user code. Temporal and DBOS suspend
by resolving SDK awaitables while the SDK runtime replays the body.

---

## 2. Instance identity and lifecycle

```scala
// AtomicFlow identity
WorkflowInstanceId(workflowId, instanceKey, scope)   // + generation on the row
```
```python
# Temporal identity
handle = await client.start_workflow(Order.run, order, id="order-123")   # + run_id
```
```python
# DBOS identity (one execution per ID)
with SetWorkflowID("order-123"):
    handle = DBOS.start_workflow(order, order)
```

| | AtomicFlow | Temporal | DBOS |
|---|---|---|---|
| Identity | `(workflowId, instanceKey, scope)` | `(namespace, workflowId, runId)` | `workflowId` |
| Multiple runs under one public id | only via `generation` (in place) | yes (execution chain: continue-as-new/retry/cron) | no (one execution per id) |
| Durable terminal outcomes | `COMPLETED` / `FAILED` / `CANCELLED` / `TERMINATED` | Completed/Failed/Cancelled/Terminated/TimedOut/ContinuedAsNew | `SUCCESS` / `ERROR` / `CANCELLED` / `MAX_RECOVERY_ATTEMPTS_EXCEEDED` |
| Non-terminal status stored? | no (derived from lease + subscriptions) | Open (Running/Paused) | `PENDING` / `ENQUEUED` / `DELAYED` |

AtomicFlow deliberately stores **only terminal** states; `CREATED`, `RUNNING`,
and `SUSPENDED` are derived (lease ownership, pending subscriptions, wakeups).
Temporal and DBOS store non-terminal status because their schedulers list and
dispatch by it.

Implementation consequence: AtomicFlow's "is it suspended?" is a query over
subscription rows, and "is it running?" is lease ownership — both transient and
under concurrency. Temporal/DBOS keep an explicit status column that their
scheduler scans.

---

## 3. Replay and recovery (behaviour + implementation)

All three re-run the workflow body and skip completed steps. They differ in the
**replay key** and therefore in what a code change or branch does.

```scala
// AtomicFlow: which cached result is used depends only on stable IDs
val a = Step.atLeastOnce("fetch") { fetch(x) }   // keyed by StepId "fetch"
if flag then Step.atLeastOnce("b") { ... }        // branch is free to differ across runs
```
```python
# Temporal: the n-th command must match the n-th recorded event
await workflow.execute_activity(do_something, ...)   # positional
```
```python
# DBOS: the n-th step maps to function_id n
do_something()   # recorded as function_id = <call order>
```

| Replay mechanism | AtomicFlow | Temporal | DBOS |
|---|---|---|---|
| Replay key | `StepId` (+ `version`, + fingerprint) | command position | ordinal `function_id` |
| Body re-run from top | yes | yes | yes |
| Completed step on replay | returns cached result | returns recorded result | returns checkpointed output |
| Divergent branch | allowed (unreached step is just unused) | non-determinism error | `DBOSStepNondeterminismError` |
| Recovery trigger | lease expiry sweep or next `run` | server reschedules workflow task | startup scan (or Conductor) |

### Temporal implementation
- The **History Service** owns each instance's mutable state and its append-only
  Event History. A worker receives a **Workflow Task** (the history + new
  events), replays the body, and returns **Commands**.
- The server appends the commands as new events atomically to the persistence
  store (`execution` table = mutable state, `history` table = append-only log)
  and schedules the next Workflow Task.
- A worker's in-memory **workflow cache** (sticky queue) avoids full replay on
  the next task; if evicted, the worker replays from scratch. Correctness never
  depends on the cache — only latency.

### DBOS implementation
- The workflow body is invoked with the checkpointed input. Before each step,
  DBOS reads `dbos.operation_outputs` for `(workflow_uuid, function_id)`; a hit
  returns the recorded output, a miss executes the step and writes a checkpoint.
- The first missing ordinal is where the original run died.

### AtomicFlow implementation
- A `run` acquires the **execution lease**, executes the body from the top, and
  at each step site either returns the cached `workflow_steps` row or executes
  and persists it (`Started` → `Succeeded`/`Failed`).
- **Wait sites are runtime-computed steps**: reaching one evaluates durable facts
  (signal events after the cursor, fired timers, child completions). Satisfied →
  persist the result as the step row; unsatisfied → register subscriptions and
  suspend via a thrown exception.
- **Evaluation is self-sufficient**: a run resolves every wait site from durable
  state alone, even firing its own due timers, so progress never depends on the
  scheduler. The scheduler only reduces latency for unattended instances. This is
  why AtomicFlow tests can drive a workflow to completion with `run` and no
  background runner.

---

## 4. Determinism

| | AtomicFlow | Temporal | DBOS |
|---|---|---|---|
| Deterministic body required | **no** | **yes** | **yes** |
| Non-determinism belongs in | a step (or anywhere) | a step | a step |
| What breaks on code change | nothing structural (IDs are stable) | command/history mismatch | ordinal/result mismatch |
| Tooling to manage change | step version, creation version, invalidation | worker versioning, `patched`/`GetVersion` | `DBOS.patch`, app versioning |

AtomicFlow relaxes determinism because the durable identity of a step is its
explicit ID, not its position. A re-run that takes a different branch simply
does not consume the now-unreached cached result, and a new branch executes
fresh. This is the reason AtomicFlow rejects positional patch semantics
(`spec/workflow-evolution.md`).

Caveat: AtomicFlow still re-executes the body, so to *reuse* a cached result the
run must reach that same step ID; it relaxes command-sequence matching, not the
fact that replay runs code from the top.

---

## 5. Concurrency control (the deep section)

This is where the three designs diverge most sharply. The question every durable
engine must answer is: *what prevents two processes from corrupting one
instance's state, and what happens to a writer that has been superseded?*

### 5.1 AtomicFlow — leases + fencing tokens + targeted row locks

AtomicFlow never holds a database lock while user code runs. Exclusivity is a
**timestamp-based lease** acquired by one conditional update, and safety is
preserved by a **fencing token** on every subsequent state write.

**Acquire the execution lease** (sole concurrency arbiter):
```sql
UPDATE workflow_instances
SET fencing_token = nextval('workflow_fencing_token_seq'),
    lease_expires_at = :now + :leaseDuration
WHERE workflow_id = :wf AND ...
  AND terminal_state IS NULL
  AND (lease_expires_at IS NULL OR lease_expires_at <= :now)
```
Zero rows updated → a live owner or a terminal instance; the acquirer backs off
(an external `run` polls up to `leaseAcquireTimeout`). The fencing token doubles
as the ownership identity: it is unique per claim, and every ownership change —
each acquisition, and each revocation by lease recovery or termination — advances
it, so the token-only guard below always detects a superseded writer. The token
is drawn from a database sequence rather than incremented in place, so it never
repeats across an instance's deletion and recreation.

**Every state write is fenced** — step rows, subscription rows, cursor moves, and
the guarded terminal transition all carry:
```sql
... WHERE workflow_instance_id = :id AND fencing_token = :token
```
After a takeover, the stale run's next write affects zero rows; the runtime
raises `LeaseLostException` and aborts the run with **no durable effect**. This is
the mechanism that makes crash recovery self-healing rather than
mutual-exclusion-based.

**Lease renewal** is manual — `Workflow.heartbeat()` inside long-running steps,
also a fenced write (without bumping the token). There is no background
heartbeater, so `leaseDuration` must exceed the longest gap between heartbeats;
a lease expiring mid-step invites takeover and at-least-once re-execution of that
step (fenced writes still prevent corruption).

**Targeted row locks** are used only for short critical sections:
- *Signal acceptance*: `sendSignal` briefly `SELECT ... FOR UPDATE`s the instance
  row and checks that it is not already terminal (`terminal_state IS NULL`), so a
  signal can never be appended after the instance reaches its terminal
  transition.
- *Event append*: every appender takes a **global transaction-scoped advisory
  lock** (`pg_advisory_xact_lock`) around `nextval` + `INSERT` + commit, so
  `sequenceId`s are commit-ordered across all workflows. This is a deliberate
  global bottleneck whose only purpose is to make cross-tree `awaitRace`
  comparison valid.
- *Timer firing*: lock the `workflow_timer_subscriptions` row, re-check no
  `TimerFired` event exists, append it — a partial unique index on
  `(instance, eventKey) WHERE eventKind='TimerFired'` is the exactly-once net.
- *Wakeup claim*: select due wakeups with `FOR UPDATE SKIP LOCKED`, acquire the
  lease, then `DELETE` the wakeup row (removal = accepted responsibility).
  Concurrent runners get disjoint batches.
- *Wait-site resolution*: `evaluateWait` runs the caller's `decide` callback
  **inside the transaction that holds the instance row lock**, so registration +
  candidate read + resolution are atomic (no lost wakeup). `decide` must be pure
  and fast.

**Idempotent upserts**: wakeups, subscriptions, etc. use
`ON CONFLICT DO NOTHING` so retries never reset state or duplicate rows.

**Sweeps are leaderless and idempotent**: timer firing, cancellation escalation,
and lease recovery run on *every* runner; guarded transitions make concurrent
execution harmless. Liveness does not depend on any single mechanism because
evaluation is self-sufficient.

**Guarantees / failure modes**

| Property | Result |
|---|---|
| Mutual exclusion of execution | at most one holder of a live lease; enforced by one conditional UPDATE |
| Stale writer safety | fenced writes become no-ops; run aborts before durable effect |
| Lost wakeup | prevented by the full subscription scan + atomic registration/resolution |
| Exactly-once timer firing | row lock + `NO event exists` re-check + partial unique index |
| Cross-tree event ordering | global advisory lock + `CACHE 1` sequence |
| Failure mode | a step whose body spans `leaseDuration` without heartbeat can be re-executed (at-least-once) |
| Cost | the global event-append lock is a throughput bottleneck |

### 5.2 Temporal — no distributed locks; server-owned state on a shard

Temporal does **not** expose database row locks or fencing tokens to
applications. Instead it removes the multi-writer problem entirely: the
**History Service** is the sole owner of an instance's mutable state.

- Executions are assigned to a **History Shard** by hashing the workflow id (+
  namespace). A shard maps to a single persistence partition, and the shard
  assumes **only one concurrent operation per partition at a time** — the shard
  is the serialization point.
- Progress is a **State Transition**: the server builds a Workflow Task, the
  worker replays and returns Commands, and the server appends the resulting
  Events to the persistence store in one transaction against that execution's
  mutable-state row (the `execution` table) and history log (`history` table).
  There is exactly one writer per execution because only the owning shard writes.
- A **Workflow Task** is effectively a server-issued lease: the server dispatches
  a task, and if the worker doesn't complete it within the workflow-task timeout
  (or returns a failure), the server retries the task with backoff while the
  execution stays Open. No application-level lock is held while user code runs;
  workflow tasks are millisecond-scale replay + decision steps.
- Activities run on workers via the **Matching Service** task queues and are
  independent of the workflow's shard lock. Their completions become events
  appended by the history service.
- Workers hold an LRU **workflow cache** shared via sticky queues; a miss just
  means a full replay. Correctness does not depend on caching.

**Guarantees / failure modes**

| Property | Result |
|---|---|
| Mutual exclusion of execution | guaranteed by single-shard ownership, not by row locks |
| Stale writer safety | no concurrent writer exists; obsolete tasks are superseded by the server |
| Lost wakeup | server schedules a Workflow Task on every state-affecting event |
| Failure mode | workflow-task failure (non-determinism, bad binary, timeout) retries with backoff; execution stays Open |
| Failure mode | a "hot" shard is a throughput ceiling; shard count is fixed for the service's lifetime |
| Failure mode | Event History size limits force continue-as-new or child workflows |

### 5.3 DBOS — ownership token re-checked on every checkpoint

DBOS keeps state in Postgres and prevents corruption with an **ownership token**
that plays the same role as AtomicFlow's fencing token.

- When an executor starts/resumes/dequeues/recovers a workflow, it generates a
  unique ownership token, writes it to the workflow's `workflow_status` row, and
  keeps it in memory.
- **Every checkpoint write, in the same database transaction, first verifies the
  recorded token still matches the executor's.** On mismatch the checkpoint is
  not written; the execution stops and **parks**, waiting for the workflow's
  recorded outcome to become visible, then returns that outcome through its
  handle.
- Duplicate workflow starts are prevented by a `creator_xid` transaction-id
  field (same id → reuse existing execution).
- Concurrent executions of the *same* workflow on the *same* executor are
  detected and prevented in-process.
- Steps get at-least-once guarantees; workflow outcomes are persisted
  exactly-once. A single execution that records two different results for the
  same step raises a step non-determinism error.

Because there is no central orchestrator, DBOS can *itself* experience a "zombie"
executor (e.g. during a rollout that Conductor misjudges as dead). The ownership
token is what keeps that safe: the zombie's next checkpoint fails the check and
it parks, so no divergent state is written.

**Guarantees / failure modes**

| Property | Result |
|---|---|
| Mutual exclusion of execution | intended single owner; token mismatch makes a losing writer harmless |
| Stale writer safety | token re-checked in the checkpoint transaction → losing writer parks |
| Lost work | none (steps at-least-once; outcomes exactly-once) |
| Failure mode | a zombie can run until its next checkpoint before parking |
| Distributed recovery | needs Conductor or manual coordination to find interrupted `PENDING` workflows |

### 5.4 Side-by-side

| Concern | AtomicFlow | Temporal | DBOS |
|---|---|---|---|
| Serialization primitive | conditional row UPDATE (lease) | shard ownership | row UPDATE + token check |
| Stale-writer safety | `fencing_token` in every WHERE | no second writer | `owner_xid` check per checkpoint |
| Lock held during user code? | no | no | no |
| Global ordering primitive | advisory lock + sequence | per-shard serial processing | none needed (per-instance ordinals) |
| Batch dispatch | `FOR UPDATE SKIP LOCKED` | matching service | queue polling + `SKIP LOCKED`-style claims |
| Self-healing after crash | lease-expiry sweep + fencing | server timeout + reschedule | startup scan / Conductor + token |
| Main contention risk | global event-append lock | hot shard | Postgres write throughput |

The conceptual equivalence worth remembering: **AtomicFlow `fencing_token` ≈ DBOS
`owner_xid`**, while **Temporal has no analogue because it never lets a second
writer exist.**

---

## 6. Storage schema (implementation)

### AtomicFlow (PostgreSQL, `db/src/main/resources/db/migration/V001__init.sql`)

```text
workflow_instances        -- identity + input + generation + terminal_* + lease_* / fencing_token + inheritance
workflow_steps            -- user steps, wait sites, first-to-run, restartable regions (one table)
workflow_events           -- global append-only log: Signal | TimerFired | WorkflowCompleted
signal_cursor             -- per (instance, signal key) cursor into the global sequence
workflow_signal_subscriptions / _timer_subscriptions / _completion_subscriptions
workflow_wakeups          -- per-instance coalesced scheduling row
workflow_event_dispatch   -- single-row watermark for async wakeup materialization
child_close_requests      -- asynchronous parent-close delivery
```

Design notes: one `workflow_steps` table for **all** durable step kinds
(`step_kind` records which step kind wrote the row and selects interpretation);
`input_fingerprints` is comparison data,
not part of the key; the event log uses a `SEQUENCE ... CACHE 1` plus a global
advisory lock; every execution-state write is fenced.

### DBOS (system database)

```text
dbos.workflow_status      -- status, app version, queue, owner_xid, parent_workflow_id, forked_from, ...
dbos.workflow_input       -- serialized input
dbos.workflow_output      -- serialized output/error
dbos.operation_outputs    -- per-step outputs keyed by (workflow_uuid, function_id)
dbos.notifications        -- messages; consumed flag
dbos.workflow_events / _history / streams
dbos.application_versions / workflow_schedules / queues
```
Plus PL/pgSQL functions (`dbos.enqueue_workflow`, `dbos.send_message`) so work can
be produced from inside the database, and co-committed checkpoints for
`transaction` steps.

### Temporal

No user-facing schema. The server owns an `execution` table (mutable state), an
append-only `history` table, tasks, namespace metadata, and a separate
**visibility** store (typically Elasticsearch) for list/search. You interact
through the Client API / Web UI / CLI, never SQL — the opposite of DBOS and
AtomicFlow, where the database *is* the surface.

---

## 7. Failure handling & recovery triggers

| Failure | AtomicFlow | Temporal | DBOS |
|---|---|---|---|
| Process crash mid-run | lease expires → recovery sweep re-enqueues; next run replays from cached results | workflow-task timeout → server reschedules | startup scan (or Conductor) re-invokes `PENDING` |
| Superseded executor | fenced writes no-op; run aborts | not applicable (single writer) | ownership mismatch → parks, returns recorded outcome |
| Transient infra error | wakeup requeued with capped backoff (`attempts`) | server retries the task/activity | workflow/step recovered |
| Uncaught workflow exception | `FAILED` terminal (unless caught); recoverable via fork/reset | Failed; retried only if a retry policy is set | `ERROR`, **not** recovered |
| Double external effect | `atMostOnce` prevents local re-run; `atLeastOnce` may re-run | step may re-run | step may re-run |
| Non-determinism | not produced by design | workflow-task failure (retried, stays Open) | step non-determinism error |

Notable behavioural difference: DBOS does **not** recover a workflow that threw
an uncaught exception; AtomicFlow persists a terminal `FAILED` but can be
forked/reset and inspected; Temporal distinguishes workflow-task failures
(infrastructure, retried, execution stays Open) from workflow-execution failures
(business logic, closes Failed).

---

## 8. Summary

- **Temporal** = deterministic state machine owned by a sharded server. No
  distributed locks because there is a single writer; scaling and failure
  handling live in the cluster.
- **DBOS** = checkpointed step outputs in your Postgres, guarded by an ownership
  token re-checked per checkpoint. No orchestrator; a zombie executor is fenced
  off at its next write.
- **AtomicFlow** = ID-addressed cached results + runtime-computed wait sites in
  Postgres, guarded by a lease and a fencing token on every write, with targeted
  row locks and one deliberate global append lock.

Continue with [`effects-and-orchestration.md`](effects-and-orchestration.md).
