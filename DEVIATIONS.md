# Implementation notes and design differences

This document describes the current implementation where it differs from, or
adds detail to, the documents in `spec/`.

## Build and runtimes

- PostgreSQL is the implemented backend. The commented in-memory prototype does
  not implement the current `WorkflowRuntime` API.
- Flyway applies the initial schema at runtime construction. `V001__init.sql`
  has been edited during pre-release development; it is not an upgrade migration
  for an existing installation of the prototype.
- The database runtime uses doobie and plain Flyway. Core code does not depend
  on cats-core or circe; JSON derivation uses upickle.
- `TestClock` is in `core` and can be used independently of a storage backend.

## Serialization

- `Cacheable` stores strings. Derived codecs use JSON, replacing the prototype's
  MessagePack format.
- Composite codecs store member codec ids and payloads in length-prefixed
  frames: `<UTF-16 character count>:<content>`.
- `unionMostSpecific` validates ambiguous runtime classes when the codec is
  constructed, rather than during macro expansion.
- Step input hashes are stored as text sorted by input name. Input order does
  not affect comparison between runs.
- A step serializes and decodes its result before storing it, then returns the
  decoded value after the write commits. The first caller and replay callers
  therefore observe the same serialized representation.
- Failure encoding uses the configured throwable codec. If it cannot encode a
  step failure, the step attempts to encode `StepSerializationFailed` with the
  same codec. If that also fails, the original exception propagates without a
  stored step failure.
- `WorkflowNonFatal` includes `InterruptedException`, unlike Scala's `NonFatal`.
  Both exclude `ControlThrowable`, which is the base of internal suspension,
  restart, break, and continue-as-new exceptions.

## Runtime API and execution context

- The runtime is exposed as two views of one object. `WorkflowRuntime` is the
  application view (create, run, cancel, query); `WorkflowRuntimeInternals` is
  the library-core view (storage and execution primitives). A backend implements
  both. Core reaches the engine view through `WorkflowContext.internals`.
- Backends define `Lease`, an opaque per-run handle created when the run's lease
  is acquired and taken implicitly by engine operations. A backend returns it
  through `withLease`, which guarantees the lease is released.
- The handle's identity is stable during a run. A backend that mutates its
  contents must support concurrent access from parallel branches.
- `WorkflowContext` holds the enclosing step scope and cancellation-suppression
  depth. Nested scopes pass an immutable copy to their bodies, so a captured
  context works on another thread without thread-local state.
- Runtime settings include `leaseDuration` (5 minutes), `leaseAcquireTimeout`
  (30 seconds), and `durableRetryThreshold` (30 seconds). Caller-thread runs
  need these settings as well as job-runner executions.
- `Step.getExecutionState` requires `Cacheable[Throwable]` to decode a stored
  failure, and reports records even when their cache expiry has passed.

## Steps and waits

- Input-change and expiry checks apply to pending `Started` records as well as
  completed records.
- A durable step retry is represented on the step record itself: the row's
  `state_kind` is `RetryPending` and carries the retry bookkeeping in
  `state_payload` plus the deadline in `retry_deadline`. Core compares that
  deadline against its clock to decide due-ness; the retry's timer subscription
  is kept only to wake the instance at the deadline, and a fired retry
  `TimerFired` event is never read by core.
- `RetryPolicy.exponentialBackoff` is one method with both retry-count and
  cumulative-delay limits. Scala does not allow both proposed overloads to
  define default arguments.
- All supported waits use one evaluation implementation. A single wait has the
  empty subscriber key; race branches use distinct, non-empty keys. Keys
  beginning with `__` are reserved for internal subscribers, including retries.
- `Awaitable` does not require a result codec at construction. `Step.await` or
  `Step.awaitRace` supplies the codec used to store the result.
- Mapped awaitables work in `Step.awaitRace`; direct `Step.await` currently
  rejects them. A workflow-completion awaitable captures the codecs needed to
  decode its source outcome before mapping.
- An empty race is rejected. When no event passes a signal filter, evaluation
  advances the signal cursor past the examined events before suspending.
- `deleteSteps` deletes step records and all their subscriptions in one
  ownership-checked transaction. Re-evaluation is a separate transaction. A
  replacement timer gets a fresh id, so its old firing event cannot satisfy it.
- Timer firing commits before wait evaluation. Its event remains stored if
  registration, filtering, mapping, or result storage subsequently fails.
- `firstToRunWithoutSuspension` waits for its concurrent branches to settle.
  If several complete, the first declared completed branch wins. Branch
  subscriptions are deleted before storing the winner; a crash before that
  write can cause branches to run again and choose a different winner.

## Scheduling and cancellation

- A runner processes a claimed batch before claiming another. Per-workflow
  limits control batch share and concurrent execution.
- Transient execution failures are rescheduled with unbounded retries and
  exponential delay: 1 second initially, capped at 5 minutes.
- Signal sends and completion transitions append events without scheduling
  subscribers in their transactions. Event dispatch handles recent events;
  the periodic full subscription scan also finds subscriptions that committed
  concurrently with those events. Both passes retry database deadlocks.
- Cancellation, lease recovery, and child-close sweeps use batches of 128.
- Cancellation immediately finalizes an instance only when it has never run
  and has no step, subscription, or wakeup records. Otherwise it is delivered
  before the next uncached step or wait.
- Internal test helpers allow manual driver cycles, sweeps, and injected run
  failures without starting a background loop.
- Unconsumed directly addressed signals are deleted in the same
  transaction as completion or continue-as-new. There is no user-facing
  handler for them; they are ignored (matching Temporal and DBOS).
- `sendSignal` rejects terminal instances by checking `terminal_state` under
  the same instance-row-lock used by completion. There is no separate
  `is_accepting_signals` flag.

## Child workflows and history changes

- Scopes are `VARCHAR[]` segment arrays, with an empty array for the top level.
  Derived child scopes longer than 1800 characters in joined form are rejected
  rather than shortened.
- Child-close requests are stored separately and processed in short per-child
  transactions. Generation and region-iteration scope prefixes prevent a close
  request for old work from matching newly created children.
- Signal inheritance is fixed at child creation. Replaying `startAsChild` with
  a changed policy retains the stored policy and logs a warning.
- `inherited_signal_prefixes` stores the complete selector: an empty array
  inherits nothing, `[""]` inherits every key, and other arrays permit matching
  prefixes. There is no separate mode column. `inherit_past_events` and
  `inherited_events_start_sequence_id` store the past-event selection.
- Signal reads, event dispatch, and the full subscription scan share the same
  recursive ancestor query. Every parent-child link must permit the key; a
  terminal parent or pending child-close request ends that part of the walk.
- Parallel execution joins all branches before propagating control flow,
  failures, or suspension. It does not interrupt a sibling during a JDBC write.
- `continueAsNew` returns `WorkflowRunResult.ContinueAsNew` and schedules the
  next generation. It retains the instance identity and signal cursors.
- Reset retains directly addressed events and cursors. It requires a
  non-terminal instance; fork can use a source in any state.
- Fork and reset order stored steps by their last-updated timestamp. Rows with
  the selected step's timestamp or a later timestamp are rerun. This boundary
  cannot represent an arbitrary cut across parallel branches.
- Reset closes children from the old generation, including those created
  before the selected step. A child started inside a retained cached step is
  not recreated, because that step's body is not executed on replay.
- Synchronous updates are not implemented. The two proposed designs in
  `spec/signals-timers.md` are reference designs, not supported APIs.
