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
