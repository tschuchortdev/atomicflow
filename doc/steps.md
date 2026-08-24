# Steps

Guideline for the caching, execution guarantees, and APIs of individual workflow steps.

## Two orthogonal axes

Steps have independent guarantees on two axes:

### Axis 1: Execution guarantee (crash/persistence)

- **at-least-once (default)**: Step body executes; result is persisted after completion. If a crash occurs between execution and persistence, the step re-executes on the next workflow run (because no cached result is found). Safe for idempotent effects (queries, read-only operations); unsafe for non-idempotent side effects without external deduplication.
- **at-most-once**: If a started-but-no-result marker exists on replay, the step returns `None` without re-executing (a lost effect is assumed over a double-execution). The caller must handle the `Option[R]` return and decide how to proceed. Recommended for effects that cannot be safely retried (resource creation, payments without external idempotency keys).

Both guarantees persist a `Started` record before the body executes, then replace it with a completed result after successful execution. This gives both guarantees the same durable record shape and lets their public functions delegate to one implementation. Their replay policies differ: at-least-once re-executes after a `Started` record, while at-most-once returns `None`.

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
```

- Constructor: `Step.atLeastOnce` or `Step.atMostOnce` is required; it sets Axis 1 (the guarantee).
- `version` exists only on `Step.atLeastOnce`. Increasing it creates new retryable work and stops reusing the previous version's cached result. `Step.atMostOnce` deliberately has no version because a new version could execute after the old operation possibly produced its effect; use a new step ID and explicit workflow-version gating for a genuinely new operation.
- Named parameters: `ensureUnchanged` and `invalidateOn` are lists of key-value pairs; both optional and can be empty.
- **Named parameters are mandatory** — if you write `ensureUnchanged = Seq(...)`, the reader sees immediately what the policy is; positional ambiguity is impossible.
- `invalidateAfter`: optional TTL applied to the entire cached result (independent of drift policy).
- **No implicit key list**: if neither `ensureUnchanged` nor `invalidateOn` is provided, the instance id is the sole cache key (equivalent to `.atLeastOnce(..., ensureUnchanged = Seq(instanceId -> "instance")) { ... }`).
- Return type for `at-most-once`: `Option[R]`; `None` means "started but not completed, unsafe to retry." Caller must handle.

## Internal storage model

Steps do not use a separate idempotency-id indirection table. Both guarantees use the same `Started` / completed-result record, keyed by the natural composite key:
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

## Exceptions in Steps

Each `Step` definition declares its result type as a type argument with a `Cacheable` typeclass instance so that the result can be serialized by the runtime. However, that is not the only possible result: A step body may throw exceptions. Thus, the exceptions must also be cached by the runtime and rethrown on replay so that failed steps will not be silently retried on replay. 