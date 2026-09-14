# Deviations from the spec

This document tracks every place where the implementation deviates from the
spec in `spec/`, with the reason for the deviation. It is updated after every
implementation phase.

## General / build

1. **Legacy prototype code removed instead of ported.** The old prototype API
   (`WorkflowMeta`/`StepMeta` hierarchy, `SignalStore`/`StepCache`/
   `StepIdempotencyStore` SPI, `DbWorkflowRuntime`, `WorkflowRunSettings`) was
   deleted rather than migrated: the spec amounts to a rewrite of that surface,
   and git history (branch `before_impl`) preserves the prototype for
   reference. `InMemoryWorkflowRuntime` is kept in the tree but commented out
   (per explicit instruction); it will be re-implemented against the new
   `WorkflowRuntime` API later.
2. **`de.lhns/doobie-flyway` dependency dropped.** Plain Flyway
   (`Flyway.configure().dataSource(ds).load().migrate()`) is used instead of
   the baseline-migrate wrapper. The wrapper only matters for adopting
   pre-existing databases with legacy schema state, which the rewrite does not
   need.
3. **`doobie-postgres-circe` dependency dropped.** The new schema stores
   step-input fingerprints as TEXT (see `Fingerprintable.Fingerprint`'s
   Base64 form), so no `jsonb` codecs are needed.
4. **neotype / cats-core / circe removed from `core`** (no longer referenced).
   `Fingerprintable.contramap` replaces the cats `Contravariant` instance.
5. **Flyway migration `V001__init.sql` replaced instead of appended to.** The
   new schema is the initial schema of the rewritten library; the prototype
   schema was never deployed anywhere. Test databases are ephemeral
   (testcontainers), so checksum history is irrelevant.

## Phase 1 (types & codecs)

6. **`Cacheable.MsgPack` replaced by `Cacheable.Json`.** The spec's
   `Cacheable` is String-based (`write: A => String`). The built-in derived
   codec therefore uses upickle JSON (human-readable in DB rows) instead of
   binary MessagePack. The old prototype's binary `Cacheable` is gone.
7. **Envelope/framing format is an implementation detail.** `withFallback`,
   `unionMostSpecific` and the `WorkflowCompletionResult` codec must record
   which member codec wrote a payload ("the outer codec records the selected
   ID and a self-delimiting payload"). The concrete framing used is
   `<length>:<content>` frames (see `atomicflow.internal.Framing`); the spec
   leaves the exact format open.
8. **`unionMostSpecific` performs its "incomparable members" check at codec
   construction time (runtime), not in a compile-time macro.** The spec says
   "the derivation macro rejects declarations where two potentially matching
   members are incomparable". The check (reject a pair where neither runtime
   class is assignable from the other and at least one is an interface) runs
   when the union codec is constructed. This is safe (fail-fast on first use)
   and much simpler than compile-time reflection over `Class` relationships,
   which Scala 3 cannot express.
9. **`WorkflowNonFatal` matches `InterruptedException`** (unlike
   `scala.util.control.NonFatal`). Interrupts carry no workflow semantics
   ("Thread.interrupt plays no role", `running-workflows.md`), so user cleanup
   code may handle them; the extractor only excludes fatal JVM errors and
   `ControlThrowable` (which covers all library control-flow exceptions).
   This gives broad catches one place to re-express "everything except library
   control flow".

## Phase 2 (Postgres run engine)

10. **Runtime-level lease settings.** `PostgresWorkflowRuntime` takes
    constructor parameters `leaseDuration` (default 5 minutes) and
    `leaseAcquireTimeout` (default 30 seconds) for caller-thread runs. The
    spec pins these only on `JobRunnerSettings` (the runner's runs); external
    `run` calls need durations too, so the runtime carries its own defaults.
11. **`TestClock` will ship in `core`**, not "with the in-memory backend" as
    the spec suggests — the in-memory backend is disabled for this rewrite,
    and a mutable clock is backend-independent.
12. **`getExecutionState` takes `(using Cacheable[Throwable])`.** The spec's
    inspection examples omit it, but `Failed(failure: Throwable)` cannot
    decode without the application-global throwable codec, which the spec
    requires at every failure-decode point.
13. **Drift policies also apply to unresolved `Started` rows.** The spec's
    "an `ensureUnchanged` difference finds the existing row" is applied to any
    existing row (including `Started`), which is more conservative than
    re-executing with changed inputs.
14. **`StepSerializationFailed` failure payloads are encoded through the
    application's throwable codec** (a plain `RuntimeException` usually encodes
    even when the original failure did not; the last resort propagates the
    original failure unpersisted). The spec's "minimal encoding does not use
    the failing user codec" is satisfied in outcome (no recursive failure),
    but the payload is not written with a separate runtime-owned encoding —
    that would break decode symmetry with the contextual codec on replay.
    Only manifests with codecs that cannot encode any `RuntimeException`.
15. **Step input fingerprints are stored as deterministic TEXT sorted by
    name** (order-insensitive across runs); the spec is silent on ordering.
16. **Event-kind values are CamelCase** (`Signal`, `TimerFired`,
    `WorkflowCompleted`) per the spec's envelope table; the plan's original
    snake_case index predicate was corrected before any data existed.
17. **`WorkflowSuspendedException`'s constructor is `private[atomicflow]`**;
    the boundary-catch behavior is tested from a package-`atomicflow` test
    (users fabricating suspensions would violate the durable-suspension
    invariant).
