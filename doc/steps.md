# Steps

Guideline for the caching, execution guarantees, and APIs of individual workflow steps.

## Two orthogonal axes

Steps have independent guarantees on two axes:

### Axis 1: Execution guarantee (crash/persistence)

- **at-least-once (default)**: Step body executes; result is persisted after completion. If a crash occurs between execution and persistence, the step re-executes on the next workflow run (because no cached result is found). Safe for idempotent effects (queries, read-only operations); unsafe for non-idempotent side effects without external deduplication.
- **at-most-once**: If a started-but-no-result marker exists on replay, the step returns `None` without re-executing (a lost effect is assumed over a double-execution). The caller must handle the `Option[R]` return and decide how to proceed. Recommended for effects that cannot be safely retried (resource creation, payments without external idempotency keys).

Both guarantees persist a `Started` record before the body executes, then replace it with a durable success or failure outcome. This gives both guarantees the same durable record shape and lets their public functions delegate to one implementation. Their replay policies differ only for an unresolved `Started` record: at-least-once re-executes, while at-most-once returns `None`.

### Axis 2: Cache key drift policy (per-key)

When a step has been cached and is replayed:

- **ensureUnchanged**: The values provided for these keys must not change between runs. If a key's value differs, `StepInputConflictException` is thrown, blocking the workflow and requiring manual intervention. Used for inputs that *must* be invariant (e.g., "which customer?" is locked, but "how much?" can be retried safely).
- **invalidateOn**: If any of these keys' values change, the previous result is discarded and the step body re-executes. Used for inputs that may legitimately change (e.g., retry with different parameters).
- **invalidateAfter**: A TTL duration. The cached result expires after this time and re-executes automatically. Applied to the entire step, not per-key.

The combination of both axes gives four primary patterns:

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
Step.atLeastOnce("check-results", retry = Step.RetryPolicy.exponentialBackoff(start = 1.minute, max = 3.days)) {
  checkResults()
}
```

- Constructor: `Step.atLeastOnce` or `Step.atMostOnce` is required; it sets Axis 1 (the guarantee).
- `version` exists only on `Step.atLeastOnce`. Increasing it creates new retryable work and stops reusing the previous version's cached result. `Step.atMostOnce` deliberately has no version because a new version could execute after the old operation possibly produced its effect; use a new step ID and explicit workflow-version gating for a genuinely new operation.
- Named parameters: `ensureUnchanged` and `invalidateOn` are lists of key-value pairs; both optional and can be empty.
- **Named parameters are mandatory** — if you write `ensureUnchanged = Seq(...)`, the reader sees immediately what the policy is; positional ambiguity is impossible.
- `invalidateAfter`: optional TTL applied to the entire cached result (independent of drift policy).
- **No implicit key list**: if neither `ensureUnchanged` nor `invalidateOn` is provided, the instance id is the sole cache key (equivalent to `.atLeastOnce(..., ensureUnchanged = Seq(instanceId -> "instance")) { ... }`).
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
      maxCumulativeDelay: FiniteDuration, 
      multiplier: Float = 2, 
      isRetriable: Throwable => Boolean = { _ => true }
  ): RetryPolicy = ...
  
  def exponentialBackoff(
      maxRetries: Long, 
      initialDelay: FiniteDuration, 
      multiplier: Float = 2, 
      isRetriable: Throwable => Boolean = { _ => true }
  ): RetryPolicy = ...
}
```

- Delays that are long should be durable suspensions while short delays can be implemented with a simple `Thread.sleep`. The threshold between the two is configurable through the `WorkflowRunSettings`.
- The `failure` is the exact exception thrown by the Step body. If the policy decides to retry, it will not be serialized and cached.
- `invalidateAfter` also invalidates ongoing retries or terminal failures. It is as if the step never executed before.
- If the application crashes during a retry, it is as if the retry was never begun (scheduled retry in the database is not touched until the Step body has returned/thrown).

Retry policies are orthogonal to "atLeastOnce" semantics: `Step.atLeastOnce` vs `Step.atMostOnce` is about how the workflow recovers from application crashes, when it is unclear whether the step body completed.  `RetryPolicy` retries when the body completed with an exception. This must be documented clearly for the user!

## Durable results and failures

Every Step has two cacheable outcomes:

```text
NeverStarted | Started | Succeeded(value) | Failed(exception)
```

### API alternatives

Three API shapes were considered:

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

An application must explicitly import or define one global `Cacheable[Throwable]`:

```scala
import Cacheable.forThrowable.genericStringMessageSerializer

// Or, for DBOS-like concrete Java exception serialization:
import Cacheable.forThrowable.javaSerializable
```

- `genericStringMessageSerializer` stores portable diagnostics and replays a library `StepFailed` exception. It is the recommended default for new workflows.
- `javaSerializable` uses Java serialization and can preserve concrete exception classes, but persisted data is coupled to those classes and their serializable object graphs.
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
- Fatal JVM errors and library control-flow exceptions for suspension, cancellation, reset, and continue-as-new are never cached as Step failures.
- If result or exception serialization/deserialization fails, the runtime persists and throws a runtime-owned `StepSerializationFailed`, a subtype of `StepFailed`. Its minimal encoding does not use the failing user codec, avoiding recursive failure.
- A `Cacheable[Throwable]` is expected to handle every application `Throwable`; falling back to a generic `StepFailed` representation is preferable to failing serialization.

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
- The derivation macro rejects declarations where two potentially matching members are incomparable, such as overlapping exception traits, and reports that an explicit `Cacheable[Throwable]` is required.
- The explicit fallback avoids a recursive `Cacheable[Throwable]` lookup while constructing the global `Cacheable[Throwable]`, and makes the resulting codec total.

Temporal follows the portable-wrapper approach: terminal Activity failures are recorded and replayed as framework failure types rather than arbitrary original exceptions. DBOS Java records Step failures using Java serialization and rethrows reconstructed concrete exceptions. The two provided codecs cover both styles.

## Internal storage model

Steps do not use a separate idempotency-id indirection table. Both guarantees use the same `Started` / completed-outcome record, keyed by the natural composite key:
- At-least-once: `(workflowId, workflowInstanceKey, [subworkflowScope], stepId, stepVersion, cache-key-hash)`
- At-most-once: `(workflowId, workflowInstanceKey, [subworkflowScope], stepId, cache-key-hash)`

Manual intervention / override of a step (e.g., "forget this step was started, retry it"):
- Direct by `(workflowId, workflowInstanceKey, stepId)`: reset/delete the row, or replace the "started" marker and cached result.
- No separate idempotency-id table or override map needed.

This removes `StepIdempotencyId` and `StepIdempotencyStore` as public concepts; they become internal implementation details of the backend if needed at all.

## Deferred: external idempotency tokens

Idea: expose a deterministic, stable idempotency token to the step body so it can be passed to external systems (Stripe `idempotency_key`, AWS `ClientToken`, etc.) for provider-side deduplication, giving effectively-once guarantees even across crashes.

Deferred because:
- At-most-once (above) already covers the "don't re-execute locally" case.
- External token exposure requires design of its derivation, manual-override semantics, and lifecycle — not blocking for MVP.
- Can be added as an opt-in capability later without breaking the core caching API.

## Cache key format: named pairs

Cache keys are specified as `"key" -> value` pairs, not bare values.

**Rationale:**
- **Self-documenting**: `ensureUnchanged("orderId" -> orderId)` is instantly clear; bare values require inference from context.
- **Evolution-resilient**: When workflow definitions change and keys are reordered, named pairs remain clear (supports your design goal "maintainable workflow definitions over time"); bare positional lists become ambiguous.
- **Diff clarity**: Changes to keyed lists are obvious in version control; positional changes are harder to track.
- **Future tooling**: Per-key overrides, debugging, and documentation generation are easier with names attached to each value.

Note: This decision is tightly coupled to workflow evolution semantics; revisit and refine during that design chapter if needed.
