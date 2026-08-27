# Workflow Evolution

Guideline for changing workflow and step code while instances are unfinished.
Workflow evolution is based on stable creation versions and explicit step
identities, not positional replay patches.

## Workflow code

Each instance keeps the workflow version with which it was created. Workflow
code can access it directly from the current context:

```scala
if Workflow.versionAtCreation >= 2 then
  Step.atLeastOnce("fraud-check", version = 1) { checkFraud(order) }
```

- An unconditional code change applies to every unfinished instance on its next
  run.
- A `versionAtCreation` branch limits a breaking change to instances created
  with the new workflow version.
- Old behavior remains available only while its branch remains in the current
  application build. The runtime does not retain or route to old builds.
- Temporal/DBOS-style positional patches are not used. Atomicflow has
  ID-addressed cached operations, cache invalidation, and parallel execution,
  so it cannot reliably determine whether an instance is "past" a patch.
- `WorkflowInstance.Info.workflowVersionAtCreation` remains available for
  external inspection; `Workflow.versionAtCreation` is the workflow-code API.

## Step code

Changing code inside a step is distinct from changing workflow control flow.

```scala
// Compatible hotfix: completed results remain cached; unfinished work uses the fix.
Step.atLeastOnce("quote", version = 1) { fixedQuoteCall(input) }

// New retryable operation: the previous cached result is not reused.
Step.atLeastOnce("quote", version = 2) { newQuoteCall(input) }

// At-most-once steps deliberately have no version.
Step.atMostOnce("charge") { charge(order) }
```

- Keeping an at-least-once step version promises that retries and cached-result
  decoding remain compatible with the previous body.
- Increasing an at-least-once step version creates new retryable work and
  discards reuse of the old version. This is consistent with its duplicate
  execution risk and must still be deliberate.
- `atMostOnce` has no version because a version change could execute a new
  operation after the old operation possibly produced its effect, making the
  API misleadingly unsafe.
- A genuinely new at-most-once operation uses a new step ID and, where needed,
  a `Workflow.versionAtCreation` branch. The new identity makes the possibility
  of another effect visible in code.

## Inspecting old steps

One API exposes the durable state known for either guarantee:

```scala
enum StepExecutionState[+A]:
  case NeverStarted
  case Started
  case Failed(failure: Throwable)
  case Completed(value: A)

Step.getExecutionState[QuoteV1]("quote", stepVersion = 1)
Step.getExecutionState[Receipt]("charge")
```

- Both overloads return the same states because both guarantees persist a
  `Started` record before executing their bodies. `NeverStarted` means no such
  record exists; `Started` means the body began but no completed result was
  persisted.
- The replay policy remains guarantee-specific: an at-least-once step may retry
  from `Started`, while an at-most-once step returns `None` rather than retry.
- `Completed` is returned only after the persisted result is decoded using the
  required `Cacheable[A]` instance.
- Step versions evolve independently of workflow versions. Lookup is by one
  exact step version and does not mix the two version dimensions.
- Steps that are currently retrying with the built-in step retry functionality are considered `Started`.

## Evolving cached formats

`Cacheable` supports one current serializer and ordered deserialization
fallbacks:

```scala
given Cacheable[Receipt] =
  currentReceiptCacheable
    .withFallback(receiptV2Cacheable.imap(_.toCurrent)(_.toV2))
    .withFallback(receiptV1Cacheable.imap(_.toCurrent)(_.toV1))
```

- Serialization always uses the current `Cacheable`.
- Deserialization tries the current format first, followed by fallbacks in
  declaration order. A fallback must ultimately construct the same current
  type.
- `Cacheable` is invariant because it both serializes and deserializes its type.
  Adapting `Cacheable[A]` to `Cacheable[B]` therefore requires both directions
  through `imap`; `map` or `contramap` alone cannot produce a complete
  `Cacheable[B]`.
- `withFallback` combines `Cacheable` instances of the same type. When a legacy
  format represents another type, it must first be adapted with `imap`.
- Fallbacks are composable so support for an older format can be packaged and
  reused without changing the current serializer.
- `Fingerprintable` remains unchanged: it computes cache-key fingerprints and
  does not deserialize cached values.

## Why this design

- Creation version is immutable and meaningful despite invalidation,
  concurrency, and non-deterministic workflow code.
- Compatible step fixes can reach unfinished work without invalidating already
  completed effects.
- Different APIs for versioned at-least-once and unversioned at-most-once steps
  make unsafe re-execution visible rather than hiding it behind a version bump.
- A shared `Started` record gives both guarantees one storage shape and makes
  the three-state inspection result an exact view of durable facts.
- Deserialization fallbacks address data evolution independently from workflow
  control-flow evolution.
