# Steps

Guideline for the caching, execution guarantees, and APIs of individual workflow steps.

## Execution and cache policies

Steps have separate policies for crash recovery and cached-result reuse:

### Execution guarantee

- **at-least-once (default)**: Step body executes; result is persisted after completion. If a crash occurs between execution and persistence, the step re-executes on the next workflow run (because no cached result is found). Safe for idempotent effects (queries, read-only operations); unsafe for non-idempotent side effects without external deduplication.
- **at-most-once**: If a started-but-no-result marker exists on replay, the step returns `None` without re-executing (a lost effect is assumed over a double-execution). The caller must handle the `Option[R]` return and decide how to proceed. Recommended for effects that cannot be safely retried (resource creation, payments without external idempotency keys).

Both guarantees persist a `Started` record before the body executes, then replace it with a durable success or failure outcome. This gives both guarantees the same durable record shape and lets their public functions delegate to one implementation. Their replay policies differ only for an unresolved `Started` record: at-least-once re-executes, while at-most-once returns `None`.

### Named-input and expiry policies

When a step has been cached and is replayed:

- **ensureUnchanged**: The values provided for these keys must not change between runs. If a key's value differs, `StepInputConflictException` is thrown, blocking the workflow and requiring manual intervention. Used for inputs that *must* be invariant (e.g., "which customer?" is locked, but "how much?" can be retried safely).
- **invalidateOn**: If any of these keys' values change, the previous result is discarded and the step body re-executes. Used for inputs that may legitimately change (e.g., retry with different parameters).
- **invalidateAfter**: A TTL duration. The cached result expires after this time and re-executes automatically. Applied to the entire step, not per-key.

The policies can be combined:

| | ensureUnchanged | invalidateOn |
|---|---|---|
| **at-least-once** | "must never change" + auto-retry on crash | "expected to change" + auto-retry |
| **at-most-once** | "must never change" + no crash-retry | "intentional re-run" + no crash-retry |

## API

```scala
import atomicflow.Step.*

// at-least-once, drift policies optional; empty = instance id is implicit key
Step.atLeastOnce("fetch-rates", version = 1, 
  ensureUnchanged = Seq("date" -> date),
  invalidateOn = Seq("market" -> market),
  invalidateAfter = 12.hours
) {
  fetchRatesFromProvider(date, market)
}

// at-most-once, some keys frozen
Step.atMostOnce("charge-payment",
  ensureUnchanged = Seq("orderId" -> orderId, "amount" -> amount)
) {
  paymentProvider.charge(orderId, amount)
}: Option[Receipt]

// No cache keys (uses instance id) and no TTL
Step.atLeastOnce("send-welcome", version = 1) {
  sendMail(user)
}

// uses built-in retry functionality
Step.atLeastOnce("check-results", retry = Step.RetryPolicy.exponentialBackoff(initialDelay = 1.minute, maxCumulativeDelay = 3.days)) {
  checkResults()
}
```

- Choose `Step.atLeastOnce` or `Step.atMostOnce` to set the crash-recovery policy.
- `version` exists only on `Step.atLeastOnce`. Increasing it creates new retryable work and stops reusing the previous version's cached result. `Step.atMostOnce` deliberately has no version because a new version could execute after the old operation possibly produced its effect; use a new step ID and explicit workflow-version gating for a genuinely new operation.
- Named parameters: `ensureUnchanged` and `invalidateOn` are lists of key-value pairs; both optional and can be empty. Each `"key" -> value` pair is converted to a `StepInput` by an implicit conversion; see the `StepInput` Scaladoc.
- **Named parameters are mandatory** — if you write `ensureUnchanged = Seq(...)`, the reader sees immediately what the policy is; positional ambiguity is impossible.
- `invalidateAfter`: optional TTL applied to the entire cached result (independent of drift policy).
- **No implicit key list**: if neither `ensureUnchanged` nor `invalidateOn` is provided, the instance id is the sole cache key (equivalent to `.atLeastOnce(..., ensureUnchanged = Seq("instance" -> instanceId)) { ... }`).
- Return type for `at-most-once`: `Option[R]`; `None` means "started but not completed, unsafe to retry." Caller must handle.

### Built-in retries

`Step.atLeastOnce` provides a built-in retry functionality (`Step.atMostOnce` does not) via the `retry` parameter (default `Step.RetryPolicy.never`). It accepts a `Step.RetryPolicy`:
```scala
trait RetryPolicy {
  def nextDelay(failure: Throwable, failedAttempts: Int, cumulativeDelay: FiniteDuration, lastDelay: Option[FiniteDuration]): Option[FiniteDuration] 
}
```
The library provides a few basic retry policies:
```scala
object RetryPolicy {
  def never: RetryPolicy = ...
  
  def fixedDelay(
      maxRetries: Long, 
      delay: FiniteDuration, 
      isRetriable: Throwable => Boolean = { _ => true }
  ): RetryPolicy = ...
  
  def exponentialBackoff(
      initialDelay: FiniteDuration,
      maxRetries: Long = Long.MaxValue,
      maxCumulativeDelay: Duration = Duration.Inf,
      multiplier: Float = 2, 
      isRetriable: Throwable => Boolean = { _ => true }
  ): RetryPolicy = ...
}
```

- Delays at or below `WorkflowRuntime.durableRetryThreshold` block the executing thread. Longer delays persist a `RetryPending` step row carrying the retry bookkeeping and the deadline at which the body may run again (replacing the site's retry timer subscription with one at that deadline; the fired `TimerFired` event only wakes the instance). Set the threshold when constructing `PostgresWorkflowRuntime`.
- The `failure` is the exact exception thrown by the Step body. If the policy decides to retry, it will not be serialized and cached.
- `invalidateAfter` also invalidates ongoing retries or terminal failures. It is as if the step never executed before.
- If the application crashes during a retry, it is as if the retry was never begun (scheduled retry in the database is not touched until the Step body has returned/thrown).

Retry policy and crash recovery are separate. The execution guarantee decides what to do after a crash when the step's outcome is unknown. `RetryPolicy` decides what to do after an attempt throws an exception.

## Durable results and failures

Every Step has two cacheable outcomes:

```text
NeverStarted | Started | Succeeded(value) | Failed(exception)
```

### API alternatives

The alternatives below explain why failures use one contextual codec:

```scala
// Rejected: union of declared failure types
Step.atLeastOnce[Result, PaymentDeclined | ProviderUnavailable](...) { ... }

// Rejected: tuple retaining every declared failure type
Step.atLeastOnce[Result, (PaymentDeclined, ProviderUnavailable)](...) { ... }

// Chosen: one contextual codec for all thrown application failures
given cacheableThrowable: Cacheable[Throwable] = ... 
Step.atLeastOnce[Result](...)(using cacheableThrowable) { ... }
```

- A union is misleading: Scala absorbs a subtype into its supertype, and overlapping alternatives do not guarantee that every explicitly written type is reconstructed and catchable.
- A tuple preserves all alternatives and permits most-specific dispatch, but makes the common single-error and no-error cases wordy and complicates every Step declaration.
- A contextual `Cacheable[Throwable]` keeps the direct-style API small. Applications that care about concrete exception classes can compose a codec once with `unionMostSpecific`, rather than restating an error list on every Step.
- The tradeoff is explicit: catch behavior depends on the globally selected throwable codec. This is unavoidable because durable replay can only throw what that codec reconstructs.

### Chosen API

Steps take no failure type parameter. Their result serialization and throwable serialization are contextual:

```scala
def atLeastOnce[A: Cacheable](...)(body: => A)(using Cacheable[Throwable]): A
def atMostOnce[A: Cacheable](...)(body: => A)(using Cacheable[Throwable]): Option[A]
```

An application must define a contextual `Cacheable[Throwable]`:

```scala
given Cacheable[Throwable] = Cacheable.forThrowable.genericStringMessageSerializer

// Alternatively, preserve concrete exception classes with Java serialization:
// given Cacheable[Throwable] = Cacheable.forThrowable.javaSerializable
```

- `genericStringMessageSerializer` stores portable diagnostics (class name, message, cause chain) and presents step failures as the library `StepFailed` exception, so catches can address the failing step (see "Step selectors and key rules" below). It is the recommended default for new workflows.
- `javaSerializable` uses Java serialization and can preserve concrete exception classes, but persisted data is coupled to those classes and their serializable object graphs; step failures are rethrown unchanged and cannot be matched by step selector.
- Custom codecs decide which concrete exception types remain catchable after replay. Workflow code must not assume more than the selected codec promises.
- Neither codec is an automatic given. Choosing the throwable codec is part of workflow behavior and must be explicit at application setup.

### Commit before observation

Workflow code must never observe an outcome that the runtime cannot reproduce. Successful values and thrown exceptions therefore cross the serialization boundary before they are returned or thrown:

```text
body returns  -> serialize -> deserialize -> persist Succeeded -> return decoded value
body throws   -> serialize -> deserialize -> persist Failed    -> throw decoded exception
```

- Replay returns or throws the persisted, decoded outcome without executing the Step body.
- The initial run also exposes the decoded representation, so it takes the same downstream pattern-match or `catch` branch as replay.
- A Step result's object identity is not preserved, even during its initial execution. Code after a Step may rely on its value, but not on reference equality with an object created inside the Step.
- Fatal JVM errors and the internal library control-flow exceptions for suspension, reset, and continue-as-new are never cached as Step failures. Lease loss raised inside a Step body — for example by an explicit `Workflow.heartbeat` call — likewise aborts the run without durable effect and is never a Step outcome. The public `WorkflowCancelledException` delivered at checkpoints is likewise never a Step outcome: delivery happens before a Step body executes, so a cancellation surfacing through a Step boundary cannot poison the Step cache.
- If result or exception serialization fails, the step attempts to store `StepSerializationFailed` with the configured throwable codec. If that codec also fails, the original failure propagates without a stored step failure. `StepSerializationFailed` is independent of `StepFailed`.
- A `Cacheable[Throwable]` is expected to handle every application `Throwable`; falling back to a generic portable representation is preferable to failing serialization.

### StepFailed and step selectors

With `genericStringMessageSerializer`, a step failure is thrown as
`StepFailed(step, cause)` — on the run that persisted the failure and on every
replay, so catches see the same type both times. `cause` is the decoded original
failure in its portable form (rendering as the original exception, e.g.
`Caused by: java.lang.RuntimeException: boom`); the retry policy still sees the
raw body exception. Failures decoded by other codecs are rethrown unchanged.

`StepFailed` is matched with the `stepId` string interpolator, whose pattern
addresses the failing step by its scope path and key — the same path
`Workflow.scoped`, `Workflow.loop`, and `firstToRun` branches build:

```scala
try {
  Workflow.scoped("orders") { Step.atLeastOnce("charge")(...) }
} catch {
  case StepFailed(stepId"charge", msg)              => // key "charge", any depth
  case StepFailed(stepId"orders/charge", msg)       => // suffix match: charge directly inside orders
  case StepFailed(stepId"/orders/charge", msg)      => // anchored: the full path from the instance root
  case StepFailed(stepId"poll/**/fetch", msg)       => // ** matches zero or more scope segments
  case StepFailed(stepId"item-*/fetch", msg)        => // * matches text within one segment
  case StepFailed(stepId"orders/$id/charge", msg)   => // $id binds the matched segment text
}
```

- Matching is a pure function of the pattern and the step id: the first run and every replay agree.
- Iteration markers (`name@3`, written by `Workflow.loop`) are ignored on both sides, so `orders` also matches `orders@3`; iteration counts cannot be pinned.
- The same interpolator builds `StepSelector` values for `resetWorkflow`/`forkWorkflow` (`restartFrom = stepId"orders/charge"`) and is inspectable via `StepSelector.parse`/`render` for tooling.
- In expression position, an interpolated `String` hole is a literal segment and any other `Fingerprintable` value becomes its fingerprint segment — the same segment `Workflow.scoped(elem)` derives.

### Step keys and scope keys

Keys that become scope-path segments or step keys — step keys, `Workflow.scoped`
keys, region ids (`loop`/`restartable`), and `firstToRun` branch keys — must not
be empty, must not start with `_`, and must not contain `/`, `*`, `@`, `[`, `]`,
`{`, `}`. These characters are the selector pattern syntax, so a key containing
them could never be addressed by a selector. Violations throw
`IllegalArgumentException` when workflow execution reaches the operation.
Fingerprint segments (derived by `scoped(elem)`) are encoded as unpadded
Base64url and never contain the pattern-syntax characters.

### Workflow-level failures use the same codec

The terminal outcome of a failed workflow is serialized with the same
application-global `Cacheable[Throwable]` (see `running-workflows.md`).

- The global given is in scope at the workflow boundary: `run`/`blockUntilCompleted`
  and the completion awaitable take it contextually, just as every Step call
  does.
- The stored outcome payload records the selected codec IDs per the
  `Cacheable` composition scheme, so decoding later is self-describing even if
  the application switches its global throwable codec.
- Catch behavior for a re-run failed workflow depends on the selected codec
  exactly as it does for Step replay; workflow code must not assume more than
  the codec promises.

### Cacheable identity and composition

```scala
trait Cacheable[A]:
  def stableSerializedTypeId: String
  def write(value: A): String
  def read(serialized: String): A
```

- `stableSerializedTypeId` identifies a durable serialized representation, not a Scala/JVM class name. It must remain stable across class and package renames.
- The ID lets generic codecs compose other `Cacheable` instances while treating their serialized strings as opaque payloads. The outer codec records the selected ID and a self-delimiting payload; it does not assume JSON, Protobuf, or any other inner format.
- IDs used within one composite codec must be unique. Changing an ID is a persisted-format compatibility change.
- `Cacheable` remains invariant because it both consumes and produces `A`.

For applications that want concrete catches for selected exceptions and a broad fallback, the library provides a tuple-based codec builder:

```scala
given Cacheable[PaymentDeclined] = ...
given Cacheable[ProviderUnavailable] = ...

given Cacheable[Throwable] =
  Cacheable.unionMostSpecific[
    (PaymentDeclined, ProviderUnavailable)
  ](fallback = Cacheable.forThrowable.genericStringMessageSerializer)
```

- Every tuple member must be a subtype of `Throwable` and have `Cacheable` and runtime type-test evidence.
- On write, all matching members are considered and the unique most-specific member codec is selected. When none matches, the required `Cacheable[Throwable]` fallback is used.
- On read, the persisted `stableSerializedTypeId` selects the same member directly; decoder trial order is never used.
- Codec construction rejects runtime classes where two potentially matching members are incomparable, such as overlapping exception traits, and reports that an explicit `Cacheable[Throwable]` is required.
- The explicit fallback avoids a recursive `Cacheable[Throwable]` lookup while constructing the global `Cacheable[Throwable]`, and makes the resulting codec total.

Temporal follows the portable-wrapper approach: terminal Activity failures are recorded and replayed as framework failure types rather than arbitrary original exceptions. DBOS Java records Step failures using Java serialization and rethrows reconstructed concrete exceptions. The two provided codecs cover both styles.

## Internal storage model

One `workflow_steps` table stores the durable runtime state for ordinary user
Steps, runtime-computed awaits, `Step.firstToRunWithoutSuspension`, and
`Workflow.restartable` / `Workflow.loop` regions:

```text
workflow_steps (
  workflowInstanceId, stepId, stepVersion, stepKind, stateKind, statePayload,
  retryDeadline, inputFingerprints, expiresAt, createdAt, updatedAt
)
```

- The natural key is `(workflowInstanceId, stepId, stepVersion, stepScopeWithinInstance)`. Unversioned
  step kinds use internal `stepVersion = 0`; public at-least-once versions are
  positive. A step key belongs to exactly one step kind: declaring the same key
  for two different step kinds — e.g. an await and a step — is a definition
  error; the runtime fails the run with a `StepKeyConflictException` instead of
  overwriting the stored row.
- `stepKind` records which step kind wrote the row: `Step` (both
  `atLeastOnce` and `atMostOnce` rows), `Await`, `FirstToRun` (a
  `firstToRunWithoutSuspension` winner row), or `RestartableRegion` (a
  `Workflow.restartable`/`Workflow.loop` region row).
- `stateKind` is legal only for the step kinds that actually write it, and
  a state kind that is not legal for its row's kind is a parse error:
  - kind `Step`: `Started`, `RetryPending`, `Succeeded`, `Failed`
  - kind `Await`: `Succeeded` only (a suspended await persists no row at all;
    its subscriptions carry the waiting)
  - kind `FirstToRun`: `Succeeded` only (the winner marker)
  - kind `RestartableRegion`: `RestartableRegion` only
- `RestartableRegion` state rows carry the region's committed `restartCount`
  and its encoded user state; the runtime replaces them in place on each
  generation boundary.
- `retryDeadline` is non-null only for `RetryPending`: the clock instant at which
  the step body may run again. Core compares it against its clock to decide
  due-ness; the retry's timer subscription is kept only to wake the instance at
  that deadline, and a fired retry `TimerFired` event is never read by core.
- `statePayload` is a runtime-owned encoded value, so it
  can carry a returned value, failure, retry bookkeeping, or a region's
  `(restartCount, currentState)` without kind-specific nullable columns.
- Awaits and `firstToRunWithoutSuspension` persist only their public returned
  value. Their selected event and winning branch are used while committing the
  result, cursor movement, and subscription cleanup but are not retained.
- A restartable region replaces its own row on each generation boundary
  (`Workflow.restartable`/`Workflow.loop`), and discards the previous
  generation's nested Step rows and subscriptions idempotently after the new
  row is committed, closing that generation's children per their
  `ParentClosePolicy`. It never caches the region's final return value.
- `inputFingerprints` is comparison data, not part of the natural key. An
  `ensureUnchanged` difference finds the existing row and raises
  `StepInputConflictException`; an `invalidateOn` difference replaces the
  existing state.
- Manual intervention directly resets, deletes, or replaces the row addressed
  by `(workflowInstanceId, stepId, stepVersion)`. No idempotency-ID indirection
  table or override map is needed.

## Deferred: external idempotency tokens

Idea: expose a deterministic, stable idempotency token to the step body so it can be passed to external systems (Stripe `idempotency_key`, AWS `ClientToken`, etc.) for provider-side deduplication, giving effectively-once guarantees even across crashes.

Deferred because:
- At-most-once (above) already covers the "don't re-execute locally" case.
- External token exposure requires design of its derivation, manual-override semantics, and lifecycle — not blocking for MVP.
- Can be added as an opt-in capability later without breaking the core caching API.

## Cache key format: named pairs

Cache keys are written as `"key" -> value` pairs. Each pair is converted to a
`StepInput` by an implicit conversion. `StepInput` retains the value's
`Fingerprintable` instance, so inputs of different types can
be fingerprinted without requiring a `Fingerprintable[Any]`; it is never
constructed directly by callers.

**Rationale:**
- **Self-documenting**: `ensureUnchanged("orderId" -> orderId)` is instantly clear; bare values require inference from context.
- **Evolution-resilient**: When workflow definitions change and keys are reordered, named pairs remain clear (supports your design goal "maintainable workflow definitions over time"); bare positional lists become ambiguous.
- **Diff clarity**: Changes to keyed lists are obvious in version control; positional changes are harder to track.
- **Future tooling**: Per-key overrides, debugging, and documentation generation are easier with names attached to each value.

Note: This decision is tightly coupled to workflow evolution semantics; revisit and refine during that design chapter if needed.
