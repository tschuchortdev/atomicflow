# AtomicFlow vs. Temporal vs. DBOS

A comparison of workflow behavior, storage, concurrency control, and execution
guarantees. The glossary maps shared concepts to each library's API terms.

> **Scope & freshness.** Written against the AtomicFlow spec in `spec/` and the
> public Temporal/DBOS documentation as of 2026-10. External products evolve;
> verify version-sensitive details. AtomicFlow is an early `0.1.0-SNAPSHOT`
> rewrite with some spec-only features and deliberately out-of-scope areas, noted
> inline. Sources: <https://docs.temporal.io/workflows>,
> <https://docs.temporal.io/workflow-definition>,
> <https://docs.temporal.io/workflow-execution>,
> <https://docs.temporal.io/tasks>,
> <https://docs.temporal.io/temporal-service>,
> <https://docs.temporal.io/temporal-service/temporal-server>,
> <https://docs.temporal.io/temporal-service/persistence>,
> <https://docs.temporal.io/sending-messages>,
> <https://docs.temporal.io/workflow-execution/continue-as-new>,
> <https://docs.dbos.dev/architecture>,
> <https://docs.dbos.dev/explanations/system-tables>,
> <https://docs.dbos.dev/explanations/concurrent-executions>,
> <https://docs.dbos.dev/explanations/sharing-a-system-database>,
> <https://docs.dbos.dev/explanations/comparing-temporal>,
> <https://docs.dbos.dev/explanations/migrating-from-temporal>.

Companion documents:

| Document | Covers |
|---|---|
| [`execution-and-durability.md`](execution-and-durability.md) | Instance lifecycle, replay/recovery, determinism, **concurrency control (locks/leases/ownership/shards)**, storage schema, failure handling |
| [`effects-and-orchestration.md`](effects-and-orchestration.md) | Steps, execution guarantees, caching/fingerprints, retries, idempotency, parallelism, subworkflows, child workflows, continue-as-new/fork/reset, scheduling, cancellation |
| [`events-signals-timers-evolution.md`](events-signals-timers-evolution.md) | Signal events, wait sites, timers, updates, queries, child signal inheritance, code evolution, serialization, observability, testing |

---

## 1. Unified vocabulary

Use these terms throughout; the third column lists each library's local name.

| Unified term | Meaning | AtomicFlow | Temporal | DBOS |
|---|---|---|---|---|
| **workflow definition** | The code that defines a workflow | `Workflow[In, Out]` | Workflow Definition / Type | workflow function |
| **workflow instance** | One durable execution of a definition | `WorkflowInstance` | Workflow Execution | workflow execution |
| **instance identity** | What uniquely names an instance | `(workflowId, instanceKey, scope)` | `(namespace, workflowId, runId)` | `workflowId` |
| **generation** | Counter incremented when an instance's history is reset | `generation` | Run Id (in the execution chain) | — (no continue-as-new) |
| **workflow body** | The user function that is re-run on resume | `body` | Workflow Function | workflow function body |
| **step** | A durable, individually recoverable unit of side effect | `Step` | Activity | step |
| **at-most-once step** | A step that never re-runs after a crash | `Step.atMostOnce` | — | — |
| **cached result** | The persisted outcome of a step | `workflow_steps` row | activity result in Event History | `dbos.operation_outputs` row |
| **step identity** | What addresses a step's cached result | `StepId` + `version` | type at command position | ordinal `function_id` |
| **input fingerprint** | Hash of a step's inputs used for drift detection | `input_fingerprints` | — (positional) | — (positional) |
| **execution lease / ownership** | Exclusive right to run an instance | `fencing_token` + `lease_expires_at` | server-owned workflow task | `owner_xid` token |
| **fencing token** | Monotonic value that invalidates stale writers | `fencing_token` | mutable-state version (server) | ownership token |
| **event log / history** | Durable ordered record of facts | `workflow_events` (global) | Event History (per execution) | `dbos.workflow_events` |
| **signal event** | Async message to an instance | `Signal` | Signal | message (`send`/`recv`) |
| **wait site / await** | A durable point where an instance waits | `Step.await` | Await / receive | `recv` / `get_event` |
| **timer subscription** | Durable registration of a deadline | `workflow_timer_subscriptions` | timer Event | sleep wakeup |
| **update** | Synchronous request/response to an instance | `Update` (unimplemented; see `spec/signals-timers.md`) | Update | (message/event pattern) |
| **query** | Synchronous read of instance state | (out of scope) | Query | event read (`get_event`) |
| **child workflow** | An instance started by another instance | `startAsChild` | Child Workflow | child workflow |
| **subworkflow scope** | An inline ID namespace inside one instance | `Workflow.scoped` | — | — |
| **continue-as-new** | Reset an instance's history in place | `Workflow.continueAsNew` | Continue-As-New | — (not provided) |
| **fork / reset** | Restart from a chosen step | `forkWorkflow` / `resetWorkflow` | Reset-to-event | `fork_workflow` |
| **wakeup** | A coalesced "this instance may have work" row | `workflow_wakeups` | Task on a Task Queue | queued workflow row |
| **scheduler / runner** | The component that claims and executes wakeups | JobRunner | Worker + Matching Service | executor / queue poller |

In all tables below, cells state the **unified concept in each library's
implementation**, not the library's marketing name for it.

---

## 2. Consolidated feature matrix

Legend: ✅ supported · ◑ partial or different behavior · ❌ absent or out of scope.

| Feature | AtomicFlow | Temporal | DBOS |
|---|---|---|---|
| Language / API | Scala 3, direct style | 8 SDKs, async/decorators | 4 languages, plain functions |
| Orchestration location | in-process; DB coordinates | central server cluster | in-process; DB coordinates |
| Durability store | Postgres | Temporal persistence store (+ visibility store) | Postgres |
| Replay keyed by | step identity (ID) | step identity (command position) | step identity (ordinal) |
| Determinism of workflow body | relaxed | strict | strict |
| Step guarantee | at-least-once **and** at-most-once | at-least-once | at-least-once (`transaction`: exactly-once) |
| Cached-result invalidation | fingerprint drift + TTL | none (positional) | none (positional) |
| Retries | per at-least-once step; delay durable above threshold | per step | per step |
| Signal events | global event log + per-key cursor | per-instance history | per-instance messages |
| Wait sites | `await`, `awaitRace` (global order), first-to-run | `Await`, selectors | `recv`, `get_event` |
| Timers | durable subscription, two firing paths | durable timer Event | durable sleep |
| Updates (sync) | await-site scoped (unimplemented; signals today) | always-on handlers | event/message pattern |
| Queries | ❌ | ✅ (+ stack trace) | ✅ events |
| Parallelism | `Workflow.parallel` (Ox) | futures/selectors | `gather` / child workflows |
| Inline subworkflow scope | ✅ | ❌ | ❌ |
| Child workflows | ✅ + signal inheritance | ✅ | ✅ |
| Parent-close policy | `Cancel` / `Abandon` | `Terminate` / `Abandon` / `RequestCancel` | ◑ via cancellation |
| Continue-as-new | ✅ | ✅ | ❌ |
| Fork / reset from step | ✅ / ✅ | ◑ reset | ✅ |
| Durable queues with flow control | ◑ caps + fairness | ◑ task-queue routing | ✅ concurrency/rate/partitions/priority/dedup |
| Cron schedules | ❌ | ✅ | ✅ |
| Cooperative cancellation | ✅ + `uncancellable` region | ✅ scopes | ✅ (preempt at step boundary) |
| Code evolution | creation version + step version + codec fallbacks | worker versioning / patching | patching + app versioning |
| Observability / UI | ❌ (SLF4J only) | ✅ UI/CLI/metrics | ✅ SQL/Conductor/metrics |
| Maturity | early | production | production |

---

## 3. Architecture at a glance (and where concurrency is arbitrated)

Where each system serializes access to one instance:

| | AtomicFlow | Temporal | DBOS |
|---|---|---|---|
| Owner of an instance's mutable state | the database row (`workflow_instances`) | the History Service shard that hashes the instance | the database row (`dbos.workflow_status`) |
| Exclusive execution enforced by | **lease** = conditional `UPDATE ... WHERE lease free`, plus a **fencing token** on every write | **server-side single workflow task** per instance | **ownership token** (`owner_xid`) re-checked on every checkpoint |
| Stale-writer handling | fencing token → stale writes affect 0 rows | server rejects/ignores obsolete tasks; workers replay | token mismatch → execution parks, reads recorded outcome |
| Cross-instance coordination | DB conditional updates + `FOR UPDATE SKIP LOCKED` | shard assignment (hash of workflow id) | DB rows + `application_name` ownership |
| Long-lived DB locks while user code runs? | no (lease is a timestamp, not a held lock) | no (server state transitions are short) | no (checkpoints are short transactions) |
| Scaling unit | runner processes | History shards | workers + Postgres |

```text
AtomicFlow one-run lifecycle              Temporal one-step lifecycle            DBOS one-checkpoint lifecycle
──────────────────────────────           ──────────────────────────────         ──────────────────────────────
BEGIN                                     Worker polls frontend/history          BEGIN
  UPDATE instance SET                     History builds Workflow Task           write step output
    fencing_token=nextval(seq),           (new events since last task)           check owner_xid still == me
    lease_expires_at=now+lease   ◄─┐      ─────────────────────────────          COMMIT
  WHERE lease free / expired    │         Worker replays full history,           (or: co-commit app DB write
COMMIT                           │         emits Commands                        for a transaction step)
run user code (lease held by     │      History appends new Events atomically
  timestamp, not by a lock)      │      and schedules the next Task
every state write:               │
  ... WHERE fencing_token=token ─┘
```

**DBOS's ownership token is functionally the same idea as AtomicFlow's fencing
token** — a per-execution counter/identity checked in the same transaction as
each durable write, so a superseded executor cannot corrupt state. Temporal
avoids the problem instead of fencing it: the server *is* the single owner of the
instance's mutable state, so there is never a second writer to fence.

The detailed mechanisms are in
[`execution-and-durability.md`](execution-and-durability.md) §5.

---

## 4. Main differences

1. **Positional vs. ID-addressed steps.** Temporal matches emitted commands to
   Event History *positionally*; DBOS numbers steps by call order (`function_id`).
   Both therefore require deterministic workflow bodies and dedicated
   patch/version machinery. AtomicFlow addresses each step by an explicit
   `StepId` (+ version + input fingerprint), so the body need not be reproducible
   command-by-command and evolution uses versions + invalidation rather than
   patches.

2. **Central orchestrator vs. library.** Temporal puts a server (and its
   persistence + visibility stores) on the critical path and owns each
   instance's state on a shard. DBOS and AtomicFlow put only the database there
   and arbitrate concurrency with database rows (ownership token / lease +
   fencing).

3. **Guarantee model.** Temporal exposes at-least-once steps (exactly-once only
   with external idempotency keys); DBOS exposes at-least-once steps and
   exactly-once *transactions*. AtomicFlow exposes `atLeastOnce` and `atMostOnce`
   separately from input-change and cache-expiry policies. `atMostOnce` skips
   unresolved attempts after a crash, returning `None`.

---

## 5. When to choose which

- **Temporal** — polyglot, managed, mature operational tooling, large ecosystem;
  accept operating a cluster and strict deterministic workflow code.
- **DBOS** — durable execution *inside* an existing application, data stays in
  your Postgres, SQL-native introspection, rich queue flow control, step-level
  fork; accept deterministic workflow code.
- **AtomicFlow** — Scala 3, freedom from the positional-determinism discipline,
  explicit at-most-once steps, scoped/parallel subworkflows, child signal
  inheritance, durable signal/timer/race waits; accept an early-stage library
  with no UI/metrics/introspection.

Next: [`execution-and-durability.md`](execution-and-durability.md).
