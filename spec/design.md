# Durable workflow design

AtomicFlow provides Scala 3 direct-style workflows whose progress survives
process crashes. A workflow body is ordinary code. Operations that need stored
results are wrapped in steps; waits store subscriptions and return control to
the runtime until an event is available.

## Goals

- Resume long-running workflows after crashes or waits without a user-defined
  state machine.
- Give side effects an explicit at-least-once or at-most-once execution policy.
- Support signals, timers, child workflows, and parallel work.
- Permit definition and serialized-format changes while instances remain active.
- Allow manual runs, cancellation, forks, and resets for operational recovery.
- Keep storage behind `WorkflowRuntime`. PostgreSQL is the implemented backend.

## Workflow and step identities

A definition has a stable workflow id, version, body, and input/output codecs.
An instance is identified by the workflow id, user-chosen instance key, and
derived scope. Top-level instances have an empty scope.

Each step has a stable key within its enclosing scope. An at-least-once step
also has a positive version. A stored result is reused unless an input policy
or expiry invalidates it. Changing a step key creates a different operation;
keys should describe the business operation and remain stable across releases.

```scala
import atomicflow.*
import atomicflow.given
import atomicflow.Cacheable.Simple.given

given Cacheable[Throwable] = Cacheable.forThrowable.genericStringMessageSerializer

val normalizeCustomer = Workflow[String, String](id = "normalize-customer") { name =>
  Step.atLeastOnce[String]("normalize-name", ensureUnchanged = Seq("name" -> name)) {
    name.trim
  }
}
```

## Recovery and side effects

The runtime calls the workflow body from the beginning on each run. Completed
steps return stored results. An at-least-once step with an unresolved attempt
runs again; an at-most-once step returns `None` without running again.

These guarantees apply to individual steps. The workflow is not a transaction
over all external effects. At-least-once operations need external idempotency
when duplicate effects are unacceptable; at-most-once operations can lose an
effect if a crash occurs after recording the attempt but before the effect.

## Waiting and scheduling

An unresolved wait stores its subscriptions and suspends the workflow. A job
runner schedules another run when a matching signal, timer, or completion is
available. Explicit `run` calls can also resolve waits. `awaitResult` only
observes the stored terminal outcome.

`Workflow.scoped` gives repeated work separate step namespaces. Ordinary Scala
loops handle sequential collections; `Workflow.parallel` runs branches
concurrently. Child workflows have separate instance records and scheduling.

## Detailed documents

- [`core-types.md`](core-types.md): identities, definitions, contexts, and codecs.
- [`steps.md`](steps.md): step guarantees, input policies, retries, and storage.
- [`running-workflows.md`](running-workflows.md): execution, leases, scheduling, and cancellation.
- [`signals-timers.md`](signals-timers.md): event waits and timer subscriptions.
- [`sub-workflows-iteration.md`](sub-workflows-iteration.md): scopes, branches, and children.
- [`child-signal-inheritance.md`](child-signal-inheritance.md): inherited signal visibility.
- [`restartable-regions-loops.md`](restartable-regions-loops.md): stored loop state.
- [`continue-as-new-fork-reset.md`](continue-as-new-fork-reset.md): history changes.
- [`workflow-evolution.md`](workflow-evolution.md): definition and format changes.
