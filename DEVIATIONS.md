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

## Phase 3 (signals, timers, awaits)

18. **`RetryPolicy.exponentialBackoff` exists as one method, not the spec's two
    overloads.** Scala 3 forbids two overloaded alternatives that both define
    default arguments (verified empirically); the merged method serves both
    spec signature-block call shapes (`initialDelay = ..., maxCumulativeDelay =
    ...` and `maxRetries = ..., initialDelay = ...`). `steps.md`'s own example
    (`start =`, `max =`) contradicts its signature block and is resolved in
    favor of the latter.
19. **`onUnconsumedSignals` receives `Map[SignalKey, Seq[String]]`** (raw
    serialized payloads, in sequenceId order, after cursors) instead of the
    spec's `Seq[Any]`: decoding would require a per-key codec registry the
    spec does not provide. Payload strings serve the stated purpose
    ("mainly for logging").
20. **The spec's open TODO "completion delivery boundary" is resolved**: a
    workflow flips `is_accepting_signals := false` BEFORE running the
    unconsumed-signals handler, so a signal arriving during the handler is
    rejected as `InstanceAlreadyCompleted` (never reported as delivered and
    omitted).
21. **`Awaitable` carries no `Cacheable` context bound.** The spec is
    self-contradictory (it puts `R : Cacheable` on the enum but also states
    `map` "does not require a `Cacheable[B]`" and that the result codec is
    "required at the `Step.await` call site"). The call-site requirement wins.
22. **`WorkflowCompletion` carries its composite codec, captured at
    `.completion` call time** (which now takes `Cacheable[Throwable]`
    contextually, per "every operation that surfaces the completion takes the
    codecs contextually"). This makes mapped completion leaves decodable —
    the spec's named use case for `map` — by decoding via the source's own
    codec and persisting with the call-site codec.
23. **Timer subscriptions are backed by a UNIQUE (site, leaf) constraint**, and
    each durable retry attempt mints a fresh subscription (new id, new
    deadline) atomically with its started-row write — the spec is silent on
    both; idempotency otherwise rested on the lease fence alone.
24. **The durable-retry delay threshold is a `PostgresWorkflowRuntime`
    constructor parameter (`durableRetryThreshold`, default 30 seconds).**
    The spec attributes it to a `WorkflowRunSettings` type that does not
    exist in the new API.
25. **An empty `awaitRace` fails fast** (`require(awaits.nonEmpty)`): a
    zero-leaf race can never satisfy and would violate the
    suspended-instance invariant.
26. **The unconsumed-signals handler also runs on the failure path** (the spec
    ties it to "before the instance is marked complete"); a throwing handler
    on the failure path masks the original exception (unspecified edge,
    documented behavior).

## Phases 4-5 (cancellation, job runner)

27. **CREATED-cancel predicate.** "Cancel before first start" finalizes CANCELLED
    when the instance has no execution state at all (`times_executed = 0` and
    no rows in workflow_steps/subscriptions/wakeups) — the spec says "has
    never started and has no execution state to deliver into" without
    pinning the predicate.
28. **`JobRunnerSettings` gains a `throwableCacheable` field** (default
    `forThrowable.genericStringMessageSerializer`). Runner-driven failure
    terminal outcomes need an explicit codec choice; the spec's settings field
    list omits one, and hard-coding it silently would violate "choosing the
    throwable codec is part of workflow behavior and must be explicit".
29. **Requeue backoff constants** (base 1 second, cap 5 minutes, factor 2) are
    internal; the spec pins only "capped exponential backoff, unbounded
    retries". The escalation/recovery sweep batch size (128) is likewise
    internal (the spec pins only `timerBatchSize`).
30. **Package-private test hooks**: `JobRunner.runDriverCycle`,
    `runTimerSweep`/`runEscalationSweep`/`runRecoverySweep`, a loop-less
    runner constructor, and a `testRunWrapper` fault-injection point. All
    `private[atomicflow]` — the spec's "Runtime-internal test hooks" category.
31. **The driver loop dispatches a whole claimed batch to the bounded pool and
    processes outcomes before claiming again** (bounded pool = backpressure).
    The spec's pseudocode leaves the await semantics of the dispatch loop
    open.

## Phase 6 (children, inheritance, parallelism)

32. **`step_scope_path` is `NOT NULL DEFAULT ''` rather than nullable.** The
    spec says step/await rows carry a "nullable scope-path column"; Postgres
    primary-key columns cannot be NULL, and the column participates in the
    natural keys of `workflow_steps` and all three subscription tables. The
    empty string is the top-level sentinel.
33. **Scope-path shortening (`shortened[sha256:...]`) is not implemented.**
    The spec's derivation section describes truncating long scope paths; the
    current implementation stores full escaped paths. Deferred as a
    performance concern (no behavioral difference).
34. **`firstToRunWithoutSuspension` defaults live on the vararg overload
    only** (Seq form delegates without defaults) — Scala 3 forbids default
    arguments on two overloaded apply methods (same class of limitation as
    entry 18).
35. **`firstToRunWithoutSuspension` records the lowest completed branch index
    when multiple branches complete in the same execution** (deterministic
    tie-break); the spec leaves the winner "whichever the implementation
    observes first".
36. **`firstToRunWithoutSuspension` accepts an unused `Cacheable[Throwable]`
    parameter for parity** with the other `Step` constructs (uniform
    signature); failures are carried by the step machinery, not encoded.
37. **Signal inheritance selectors persist as text tokens** (`none`, `all`,
    `some:` + JSON array of prefixes via upickle) — the storage format is an
    implementation choice the spec leaves open.
38. **Narrowing an inheritance policy schedules conservative wakeups** (the
    affected subtree is woken and re-checks eligibility, re-suspending if
    newly ineligible). The spec requires wakeups only on broadening; the
    conservative extra wakeups are no-ops.

## Phase 7 (continueAsNew, restartable regions, fork/reset)

39. **`WorkflowRunResult.ContinueAsNew` outcome.** The spec requires the run
    to end when `continueAsNew` fires but does not name the run outcome; the
    outcome set gains this case (and the JobRunner treats it as terminal for
    the claiming run).
40. **Parallel control-flow crossing is join-then-propagate.** The spec says
    parallel "handles cleanup of the other branches internally" without
    pinning the mechanism. Interrupting sibling threads mid-DB-write is
    unsafe under JDBC (interrupt-swallowing on a dead backend held a row
    lock indefinitely), so `Workflow.parallel` collects every branch outcome
    as a value, joins all branches to completion, then propagates: control-flow
    exception first, then first non-suspension failure, then the combined
    suspension.
41. **Enclosing scope and `uncancellable` depth propagate into parallel
    branch threads.** The spec is silent on branch-thread context; without
    propagation, steps inside parallel would persist at the empty scope,
    defeating region restarts and scoped isolation.
42. **Reset keeps directly-addressed signal events and cursors** (it is a
    history-erase operation only; only continueAsNew deletes events).
43. **Fork is allowed from any source state; reset requires a non-terminal
    instance.** The spec is silent on source states; these are the pinned
    and documented semantics.
44. **Fork/reset boundary ties count as "after"** (rows with `updated_at`
    equal to the selected step's are re-executed — conservative).
45. **Reset leaves subscriptions of erased steps in place when they have no
    step row** (benign: re-registration reuses them via ON CONFLICT and
    consumption is cursor-based).

## Phase 8 (Updates)

46. **`sendUpdate` and its forwarders take a leading `workflow` definition
    argument** (the sender-thread run needs the definition; the runtime has no
    registry). The spec's shapes omit it.
47. **`Step.awaitUpdate` requires `O: Cacheable`** (the await persists its
    output like every other step construct); the spec's shape omits the bound.
48. **A `workflow_update_subscriptions` table backs suspended
    `awaitUpdate`s**, in addition to the spec's update records — the same
    subscription/wakeup machinery signals use, so a send wakes a suspended
    awaiter.
49. **Handled update records are retained only when they carry an
    idempotency key** (dedup requires them); empty-key handled records are
    deleted. The spec's "deleted after workflow completion" phrasing is
    resolved in favor of idempotency.
50. **Known edge (deferred):** a runner-mode wakeup scheduled for the same
    instant as a sender-thread run can, in a narrow race, consume the update
    on the runner's thread and leave the sender reading `Unhandled` — the
    update IS handled durably; only the sender's synchronous view is stale.

## Phase 9 (final conformance pass)

64. **Metrics export and wakeup-age alerting are out of scope** (project
    constraint). The spec's observability paragraphs (wakeup-age monitoring,
    timer-firing lag, "workflow known to no runner" detection) are not
    implemented; only SLF4J logging exists.
65. **No lease renewal at await checkpoints.** Renewals happen at step
    checkpoints; a run spending longer than `leaseDuration` between step
    checkpoints risks lease loss (bounded, recovers via sweeps).
66. **`StepContext` as a separate public type is folded into
    `WorkflowContext`.**
67. **No `getWorkflowResult` accessor**; `awaitResult` covers terminal-outcome
    reading.
68. **Reusing a step id with a different `stepKind` is not a conflict.** Rows
    are keyed by identity; a kind change follows the same-version
    compatible-hotfix semantics.
69. **`Step.await` of a raw `Mapped` awaitable throws.** Composition happens
    through `Awaitable.map`; awaiting the raw mapped case is a programming
    error.
70. **Race rows persist `stepKind = 'AwaitRace'`**, distinct from `'Await'`.
71. **Draft `design.md` bullets (prefix bulk-start, broadcast send, signal
    streams) are not implemented** — non-binding draft material.
72. **`SignalInheritance.some` takes `Seq[SignalKey]`** (Scala 3 enum
    parameters cannot be varargs; same class as entries 18/34). Scaladoc
    shows the usage.
73. **The inheritance ancestor walk is an iterative in-Scala walk, not a
    recursive CTE** (semantics equivalent; the spec's PostgreSQL notes are
    non-binding).
74. **`StepSerializationFailed`'s payload is encoded via the user codec**
    rather than a runtime-owned minimal encoding with a payload marker (only
    manifests with broken user codecs).
75. **Started rows persist an empty `state_payload` sentinel** (arbitrary
    placeholder for the pre-body state).

## Phase 10 (context-carried scope state)

76. **`WorkflowContext` carries the transient scope/uncancellable state;
    `PostgresExecution` no longer uses ThreadLocals.** The spec does not
    detail how `Workflow.scoped`/`Workflow.uncancellable` state reaches Step
    registrations; the implementation used per-thread ThreadLocals in
    `PostgresExecution` plus a snapshot/restore protocol for parallel-branch
    threads. Per the design intent (the `local` of a reader monad), the state
    now lives immutably on `WorkflowContext` (`scopePath`,
    `uncancellableDepth`, both `private[atomicflow]`), and every region
    function passes a derived context to its body. Consequence: the branch
    parameters of `Workflow.parallel` and `Step.firstToRunWithoutSuspension`
    and the bodies of `Workflow.restartable`/`Workflow.loop` are context
    functions (`WorkflowContext ?=> R`) — a public signature change from the
    spec's `Seq[() => R]` (sub-workflows-iteration.md,
    restartable-regions-loops.md): call sites drop the `() =>` (behavior
    is otherwise identical; branch bodies are still evaluated at application
    time). Benefit: Step IDs and cancellation suppression travel with the
    context value, so code that hops threads and carries the context (e.g.
    applies a captured `WorkflowContext ?=> R` on another thread) records
    the correct scope instead of silently corrupting Step IDs; branch fork
    sites no longer need the snapshot/restore choreography.
