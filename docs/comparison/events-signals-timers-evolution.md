# Signal Events, Wait Sites, Timers, Updates & Evolution

The interactive surface (signal events, wait sites, timers, updates, queries) and
how each system evolves workflow code — with the **implementation mechanism** and
guarantee for each.

Terminology follows [`README.md`](README.md) §1. Companion documents:
[`execution-and-durability.md`](execution-and-durability.md),
[`effects-and-orchestration.md`](effects-and-orchestration.md).

---

## 1. Signal events (asynchronous messages)

**AtomicFlow**
```scala
val approval = Signal[String]("approval")
approval.send(instanceId, "alice")                                 // append-only event
val who = Step.await("wait-approval", Awaitable.SignalEvent(approval)) { _ => true }
```
**Temporal**
```python
@workflow.signal
async def payment_received(self, status): ...
await handle.signal(Order.payment_received, "paid")
```
**DBOS**
```python
DBOS.send("order-123", "paid", topic="payment_status")
status = DBOS.recv("payment_status", timeout_seconds=3600)
```

| | AtomicFlow | Temporal | DBOS |
|---|---|---|---|
| Stored as | immutable row in global `workflow_events` | event appended to the instance's Event History | `dbos.notifications` row |
| Ordering | one global `sequenceId` across all instances | per-instance history order | per-topic order |
| Consumption tracked by | per `(instance, signalKey)` cursor | history position / handler | `consumed` flag + `consumed_by_function_id` |
| Sender blocked by busy receiver | no (append only, no lease) | no (server buffers) | no (durable insert) |
| Receiver completed | `InstanceAlreadyCompleted` | per conflict/reuse policy | message persists after completion |
| Send dedup | cursor (exactly-once consumption) | signal id (newer versions) | `message_id` (exactly-once send) |
| Returns a value | no | no | no |

**Implementation.**
- *AtomicFlow:* `sendSignal` briefly `SELECT ... FOR UPDATE`s the instance row,
  checks that it is not already terminal (`terminal_state IS NULL`), acquires the
  **global event-append advisory lock**, calls `nextval`, and inserts one
  immutable `Signal` row — no lease, no step mutation, no wakeup on the sender.
  Wakeups are materialized
  **asynchronously** by the runner's watermark dispatcher and periodic full
  subscription scan (the full scan is the liveness guarantee; the dispatcher is
  only latency). Consumption is a single cursor per `(instance, exact key)`;
  rejected-by-filter events advance the cursor on a suspending evaluation.
- *Temporal:* a signal is appended to the instance's Event History by the server,
  which schedules a new **Workflow Task**; the SDK then runs signal handlers.
  Signals are buffered and ordered per execution.
- *DBOS:* `send` inserts a `notifications` row (dedup by `message_id`); `recv`
  marks it consumed. Messages are durable and remain readable after completion.

---

## 2. Wait sites and selection

**AtomicFlow**
```scala
val x = Step.await("one", Awaitable.SignalEvent(sig))               // single wait site
val w = Step.awaitRace("race")(
  "approval" -> Awaitable.SignalEvent(approval),
  "deadline" -> Awaitable.Timer(1.hour))                            // earliest durable event wins
```
**Temporal**
```python
await workflow.wait_condition(lambda: self.approved)
# or a Selector over futures
```
**DBOS**
```python
status = DBOS.recv("payment_status", timeout_seconds=3600)
progress = DBOS.get_event("order-123", "progress", timeout_seconds=30)
```

| | AtomicFlow | Temporal | DBOS |
|---|---|---|---|
| Single wait | `Step.await` | `Await` / channel receive | `recv` / `get_event` |
| Select/race | `Step.awaitRace` | `Selector` | `asyncio_wait` |
| Cross-source ordering | one global `sequenceId` | history order | per-mechanism |
| Race arbitrary code | `Step.firstToRunWithoutSuspension` (documented pitfalls) | selectors over futures | — |

**Implementation.** Every AtomicFlow wait site is a **runtime-computed step**
evaluated by one protocol, `evaluateWait`:

1. In one committed transaction, fire the site's own due **timer subscriptions**
   (append `TimerFired`).
2. In a second transaction, register every subscriber idempotently
   (`ON CONFLICT DO NOTHING`), read all candidates, merge them sorted by global
   `sequenceId`, and run the caller's `decide`. If `decide` resolves, persist the
   step row, advance the winning signal cursor, and delete the site's
   subscriptions — **all atomically with registration and the candidate read**.
   If `decide` returns none, the registrations commit and the run suspends.

`decide` runs while the instance row lock is held, so it must be pure and fast.
This registration + read + resolution atomicity is what makes wait sites
lost-wakeup-free without relying on the sender. Since all candidates share one
global `sequenceId`, the earliest durable signal, timer, or child completion wins
even across unrelated workflow trees.

Temporal's selection is over SDK futures within one deterministic execution
(the history defines the order). DBOS reads its notification/event tables and
waits via durable sleep; there is no global cross-source ordering primitive.

---

## 3. Timers

**AtomicFlow**
```scala
Step.await("cooldown", Awaitable.Timer(30.minutes))
```
**Temporal**
```python
await workflow.sleep(timedelta(minutes=30))
```
**DBOS**
```python
DBOS.sleep(1800)   # seconds
```

| | AtomicFlow | Temporal | DBOS |
|---|---|---|---|
| Durable | ✅ | ✅ | ✅ |
| Clock source | `runtime.clock` (single injectable source) | SDK replay-safe clock | system/DB |
| Deadline stored | absolute, in `workflow_timer_subscriptions` | Event | DB wakeup time |
| Firing | two paths, one primitive | server timer → event | DB-backed |
| Exactly-once firing | row lock + `no event` re-check + partial unique index | server-managed | DB-managed |
| Fires early? | no, late only (monitored lag) | late | late |

**Implementation.** An AtomicFlow timer subscription is **semantic state, not a
wakeup hint**: it stores the absolute deadline (so replays never push a timeout
forward), schedules unattended instances, and versions the registration
incarnation via `timer_id`. A due timer becomes durable through two paths sharing
one primitive — the runner sweep and inline wait-site evaluation both *lock the
subscription row, re-check that no `TimerFired` event exists, append it*. Firing
does **not** delete the subscription; only the wait site's resolution does. This
matters for invalidation: re-registering after `invalidateOn` mints a fresh
`timer_id`, so the previous incarnation's `TimerFired` event is structurally
inert. Temporal records a `StartTimer` event and a `TimerFired` event fired by the
server. DBOS persists a wakeup time in the workflow row.

---

## 4. Updates (synchronous request/response)

> Updates are **not implemented** in AtomicFlow (see `spec/signals-timers.md`,
> "Updates (unimplemented)"): the synchronous-record design shown here and a
> signal-based alternative are kept as unimplemented designs. Use signals for
> request/response today.

**AtomicFlow (design, unimplemented)**
```scala
val nameUpdate = Update[String, Validation]("validate-name")
val outcome: UpdateSendResult[Validation] =
  instanceHandle.sendUpdate(nameUpdate, "alice", idempotencyKey = "req-1")
```
**Temporal**
```python
result = await handle.execute_update(Order.set_name, "alice", update_id="req-1")
```
**DBOS** — no update primitive; modelled with `set_event`/`recv` or a
`handle.get_result()` request/response.

| | AtomicFlow | Temporal | DBOS |
|---|---|---|---|
| Sync result | `Success` / `Unhandled` / `InstanceAlreadyCompleted` | Update (Accepted/Completed stages) | via event/message patterns |
| Validator / early reject | no | ✅ validators | no |
| Idempotency | `idempotencyKey` dedup | Update Id | message/workflow Ids |
| Always-on handler | ❌ (tied to an `awaitUpdate` site) | ✅ | ❌ |
| Inherited by children | ❌ (ambiguous response) | n/a | n/a |

**Design of the unimplemented variant.** The sender would run the workflow **on
its own thread only if a subscription currently exists** for that update key; if
the instance is busy it would wait for the lease, then return `Success(result)`
if the record was handled or `Unhandled` otherwise. Records would live in
`workflow_updates`, with pending awaits in `workflow_update_subscriptions` using
the same subscription/wakeup machinery as signal events. `persistUnhandledUpdates`
controls whether an unhandled record survives for a later `awaitUpdate`; handled
records are retained only when they carry an idempotency key (dedup). A documented
narrow race (DEVIATIONS #50): a runner wakeup can consume the update so the sender
sees `Unhandled` even though it was handled durably.

Temporal Updates have always-on handlers and validators, with explicitly modeled
Accepted/Completed stages and Update IDs for exactly-once processing. DBOS
generally recommends events/messages instead of synchronous updates.

---

## 5. Queries and reading state

| | AtomicFlow | Temporal | DBOS |
|---|---|---|---|
| External state read | ❌ **out of scope** | ✅ Queries (incl. `__stack_trace`) | ✅ events (`get_event`) + SQL |
| Historical values | ❌ | ✅ Event History | ✅ `workflow_events_history` |
| Streaming output | ❌ | ◑ | ✅ `dbos.streams` (append-only) |
| Introspection discovery | ❌ explicitly out of scope | ✅ Web UI / CLI / search | ✅ SQL + Conductor UI |

**Implementation.** AtomicFlow's rationale: all state lives in local variables
inside the body and there is no ambient object to read, so a query API has no
substrate. It offers only `Step.peekSignal` (a durable read of visible signal
events without advancing the cursor) and `Step.getExecutionState` (a durable read
of a step row's state). Temporal runs query handlers against reconstructed
workflow state (with a built-in stack-trace query). DBOS reads its own tables:
`set_event`/`get_event` for named values (with history) and `streams` for
append-only sequences readers can tail in real time.

This is the largest **feature gap** on AtomicFlow's side, together with the
absence of any UI/metrics.

---

## 6. Child signal inheritance (AtomicFlow-only)

**AtomicFlow**
```scala
val child = workerWf.startAsChild(childKey, input,
  inheritSignals = SignalInheritance.some("approval/"),
  inheritPastEvents = true)
```

| | AtomicFlow | Temporal | DBOS |
|---|---|---|---|
| Children see ancestors' signals | ✅ read-time policy | ❌ (children addressed directly) | ❌ (messages per instance) |
| Visibility scoped by | every edge on the path must permit the key | n/a | n/a |
| Past events | `inheritPastEvents` + `inheritedEventsStartSequenceId` | n/a | n/a |
| Delivery | none (one append is visible to all eligible descendants) | n/a | n/a |

**Implementation.** Inheritance is a **read-time projection** over the global
event log, not a copy. A child's unresolved wait site runs a recursive ancestor
query over `workflow_instances` and selects the earliest visible event for its
exact key, subject to every edge's selector. `inheritPastEvents = false` limits
visibility to `sequenceId` greater than the relationship's
`inheritedEventsStartSequenceId`. Broadening the policy durably wakes the affected
subtree (which re-checks eligibility and may re-suspend); narrowing schedules
conservative no-op wakeups. Visibility cuts off at the closure *event*, not at
delivery: the walk treats a terminal parent — or one holding a matching
`child_close_requests` row — as detached, so no transitive ancestor event leaks
through a closed parent. Sending creates no child copies or wakeups, so one
commit is atomically visible to all eligible descendants.

Temporal children have their own identity and are signalled directly; DBOS
messages are addressed to a single instance.

---

## 7. Code evolution and versioning

**AtomicFlow**
```scala
if Workflow.versionAtCreation >= 2 then
  Step.atLeastOnce("fraud-check", version = 1) { checkFraud(order) }

Step.atLeastOnce("quote", version = 2) { newQuoteCall(input) }   // discard v1 cache
```
**Temporal**
```python
if workflow.patched("add-fraud-check"):
    await workflow.execute_activity(check_fraud, order)
```
**DBOS**
```python
if DBOS.patch("add-fraud-check"):
    check_fraud(order)
```

| | AtomicFlow | Temporal | DBOS |
|---|---|---|---|
| Mechanism | creation version + step version + invalidation | worker versioning or `patched`/`GetVersion` | `DBOS.patch` + application versioning |
| Positional patches | ❌ deliberately not used | ✅ core | ✅ core |
| Limit to new instances | `Workflow.versionAtCreation` branch | worker versioning / `GetVersion` | application version routing |
| Change a step's behavior | keep `version` (hotfix) or bump it (new retryable work) | step change + replay safety | patch/version |
| Keep serving old builds | ❌ (runtime does not retain old builds) | ✅ old workers can | ✅ version routing via Conductor |
| Data-format evolution | `Cacheable.withFallback` (ordered fallbacks) | converter/codec versioning | serializers + versioning |

**Implementation.** AtomicFlow stores `workflow_version_at_creation` on the
instance row and exposes `Workflow.versionAtCreation` to the body, so a breaking
change can be gated to newly created instances. Step rows carry a `version`; an
`atLeastOnce` version bump discards reuse of the old cached result, while
`atMostOnce` deliberately has no version (a bump could execute a new operation
after the old one possibly produced its effect — a genuinely new operation uses a
new step ID and a creation-version gate). There are no positional patches because
AtomicFlow "cannot reliably determine whether an instance is 'past' a patch" given
ID-addressed caching, invalidation, and parallelism. `Step.getExecutionState`
gives an exact view (`NeverStarted`/`Started`/`Failed`/`Completed`).

Temporal requires determinism, so evolution uses Worker Versioning (keep workers
on a build id) or `patched`/`GetVersion`, with replay tests. DBOS uses
`DBOS.patch` and application versioning, routing recovery only to
version-compatible code.

---

## 8. Serialization and data formats

| | AtomicFlow | Temporal | DBOS |
|---|---|---|---|
| Abstraction | `Cacheable[A]` (`stableSerializedTypeId`, `write: A => String`, `read`) | data converter + payload codec | serializer (per field / per app) |
| Default | upickle JSON | JSON / Protobuf converters | portable JSON; pickle / Jackson |
| Cross-language | ❌ | ✅ | ✅ |
| Encryption hook | custom `Cacheable` | payload codec | custom serializer |
| Failure encoding | `Cacheable[Throwable]` (portable message or Java serialization; `unionMostSpecific`) | framework failure types | Java serialization / recorded error |

**Implementation.** `Cacheable`'s `stableSerializedTypeId` identifies a durable
serialized representation (not a JVM class name), so generic codecs compose inner
codecs while treating their strings as opaque payloads; the framing is
`<length>:<content>`. Two details matter:

- **Commit before observation** — a step's value/exception is serialized,
  deserialized, persisted, and only then returned/thrown, so the initial run and
  replay take the same branch; object identity is never preserved.
- **The throwable codec is an explicit application choice** — a portable
  `genericStringMessageSerializer` (replays a library `StepFailed`) or
  `javaSerializable` (preserves concrete exception classes, DBOS-style);
  `unionMostSpecific` composes concrete catches with a fallback. The spec notes
  Temporal follows the portable-wrapper approach and DBOS Java uses Java
  serialization, so both styles are supported.

---

## 9. Observability, tooling, testing

| | AtomicFlow | Temporal | DBOS |
|---|---|---|---|
| Web UI / CLI | ❌ / ❌ | ✅ / ✅ | ✅ Conductor / ✅ `dbosctl` |
| Metrics | ❌ out of scope (SLF4J only) | ✅ Prometheus etc. | ✅ Prometheus via Conductor |
| List/search instances | ◑ prefix queries only | ✅ search attributes | ✅ SQL + rich search |
| Pause/cancel/resume | ◑ cancel/terminate; fork/reset | ✅ | ✅ |
| Retention | ❌ (TODO) | ✅ per-namespace retention | ✅ Conductor-managed |
| Deterministic testing | ✅ `TestClock` + manual `run`, no runner | ✅ time-skipping test server | ✅ library test helpers |
| Internal test hooks | ✅ package-private | ✅ | ✅ |

**Implementation.** AtomicFlow's testing model is a direct payoff of
self-sufficient evaluation: tests drive instances with `createAndRun`/`run` on the
caller thread and advance a `TestClock`, with **no background runner**; timer
races are decided deterministically by `advanceBy` + `run`. A runner is started
only to test scheduling behaviour itself (coalescing, fairness, lease takeover).
The reconstruction gap is observability: metrics and wakeup-age alerting are out
of scope in the rewrite (DEVIATIONS #64), and there is no UI — AtomicFlow's
largest production-adoption gap. Temporal ships a Web UI, CLI, metrics, search
attributes, and a stack-trace query; DBOS exposes both SQL introspection and the
Conductor dashboard.

---

## 10. Maturity and scope

| | AtomicFlow | Temporal | DBOS |
|---|---|---|---|
| Status | `0.1.0-SNAPSHOT`, spec-driven rewrite | GA, widely deployed | production |
| Backends | Postgres (reference); in-memory disabled in this rewrite | Cassandra/MySQL/Postgres + visibility store | Postgres (SQLite in dev) |
| Languages | Scala 3 only | 8 SDKs | 4 languages |
| Explicitly out of scope | queries, introspection/diagrams, metrics, prefix bulk ops, external idempotency tokens | — | — |

AtomicFlow is best read as a design exploration of durable workflows without the
two assumptions shared by Temporal and DBOS — positional replay and a
central/ambient orchestrator. See repository `DEVIATIONS.md` for where the
implementation currently diverges from the spec.

---

## 11. Summary: who does what best

- **Signal events:** Temporal (always-on handlers, tooling); AtomicFlow (global
  ordering + cursors + inheritance); DBOS (messages + events + streams, SQL-visible).
- **Wait sites / race:** AtomicFlow's global-`sequenceId` `awaitRace` gives the
  strongest cross-source ordering guarantee; Temporal selectors are the most
  ergonomic.
- **Timers:** all durable; AtomicFlow's subscription-as-semantic-state design is
  the most explicit and supports invalidation.
- **Updates:** Temporal is the most complete; AtomicFlow's design is simpler and
  await-site-scoped but unimplemented (signals cover the use case today); DBOS
  models it with events/messages.
- **Code evolution:** AtomicFlow (creation version + ID-addressed invalidation)
  vs. Temporal (worker versioning/patching) vs. DBOS (patching/app versions).
- **Operations:** Temporal and DBOS are far ahead of AtomicFlow on UI, metrics,
  retention, and introspection.
