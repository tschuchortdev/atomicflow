# AtomicFlow

AtomicFlow is a Scala 3 library for durable workflows. It stores step results,
signals, timers, and execution state in PostgreSQL. After a crash or a suspended
wait, the workflow body runs again and completed steps return their stored results.

## Execution guarantees

- `Step.atLeastOnce` runs again after a crash if its result was not stored. Use
  idempotent operations or pass an idempotency key to the external service.
- `Step.atMostOnce` does not run again when an earlier attempt has no stored
  outcome. It returns `None` in that case: the external operation may or may not
  have completed.
- Completed steps replay their stored success or failure. Named input hashes
  and cache expiry control when results can be reused.
- A workflow is not one database transaction. Completed external effects are
  not rolled back when a later step fails.

PostgreSQL is the implemented runtime. The in-memory runtime file contains
disabled prototype code.

## Dependencies

The project uses Scala 3.7.3 and sbt. The `core` module contains the workflow API;
the `db` module contains the PostgreSQL runtime and depends on `core`. The
current development version is `0.1.0-SNAPSHOT`.

## Define and run a workflow

```scala
import atomicflow.*
import atomicflow.given
import atomicflow.Cacheable.Simple.given
import atomicflow.impl.db.PostgresWorkflowRuntime
import org.postgresql.ds.PGSimpleDataSource
import scala.concurrent.ExecutionContext
import scala.concurrent.duration.*

val dataSource = new PGSimpleDataSource()
dataSource.setUrl("jdbc:postgresql://localhost:5432/atomicflow")
dataSource.setUser("postgres")
dataSource.setPassword(sys.env("ATOMICFLOW_DB_PASSWORD"))

given ExecutionContext = ExecutionContext.global
given WorkflowRuntime = PostgresWorkflowRuntime(dataSource)
given Cacheable[Throwable] = Cacheable.forThrowable.genericStringMessageSerializer

val normalizeAddress = Workflow[String, String](id = "normalize-address") { address =>
  Step.atLeastOnce[String]("normalize", ensureUnchanged = Seq("address" -> address)) {
    address.trim.toLowerCase(java.util.Locale.ROOT)
  }
}

val result = normalizeAddress.createAndRun("customer-123", " Alice@Example.com ")
assert(result == WorkflowRunResult.Result("alice@example.com"))
```

Runtime construction applies the Flyway migrations. Creating an instance is
idempotent when its serialized input matches the existing input; different
input raises `WorkflowInputConflictException`.

Select a throwable codec explicitly. The diagnostic codec above stores the
original exception's class name, message, and cause chain, then decodes it as
`StepFailed`. Workflow failures are thrown to the caller; suspension,
cancellation, termination, and continue-as-new are `WorkflowRunResult` cases.

## Wait for a signal

```scala
val approval = Signal[String]("approval")

val approveOrder = Workflow[String, String](id = "approve-order") { orderId =>
  val approver = Step.await("wait-for-approval", Awaitable.SignalEvent(approval))
  s"Order $orderId approved by $approver"
}

val order = approveOrder.create("order-123", "123")
assert(order.run() == WorkflowRunResult.WorkflowSuspended)
assert(order.sendSignal(approval, "manager") == SignalSendResult.Success)
assert(order.run() == WorkflowRunResult.Result("Order 123 approved by manager"))
```

Each signal key has a cursor shared by all waits in an instance. A filtered
wait advances past rejected events, so later waits cannot receive them.
`Step.peekSignal` reads visible events without advancing the cursor.

Use `Awaitable.Timer(15.minutes)` inside a workflow to wait for a duration, or
`Awaitable.Timer(deadline)` for an absolute `java.time.Instant`. `Step.awaitRace`
selects the matching signal, timer, or workflow completion event with the lowest
stored sequence number.

## Run workflows in the background

```scala
import scala.concurrent.duration.*

val runner = WorkflowRuntime.startJobRunner(Seq(normalizeAddress, approveOrder))
try {
  val order = approveOrder.createAndScheduleForAsyncRun("order-456", "456")
  order.sendSignal(approval, "manager")
  val result = order.blockUntilCompleted(30.seconds)
} finally runner.stop(5.seconds)
```

The runner executes registered workflow definitions, fires due timers, finds
signal and completion events, and recovers expired execution leases. Manual
`run` calls can also resolve waits without a runner. `blockUntilCompleted` only polls
for completion; it does not execute the workflow.

## Other operations

- **Input changes:** `ensureUnchanged` rejects changed named inputs;
  `invalidateOn` discards the cached result when a named input changes;
  `invalidateAfter` sets cache expiry.
- **Retries:** `Step.RetryPolicy` retries failures in at-least-once steps.
  Short delays block the executing thread; delays above the runtime's
  `durableRetryThreshold` suspend until a stored timer fires. A stored terminal
  failure is replayed, not retried by calling `run` again.
- **Scopes and parallel work:** `Workflow.scoped` gives steps a nested
  namespace. `Workflow.parallel` runs branches concurrently and waits for all
  branches to finish or suspend.
- **Child workflows:** `startAsChild` creates a separately scheduled instance.
  The parent can await `child.completion`. Parent-close policy and signal
  inheritance are fixed at child creation.
- **Loops:** `Workflow.restartable` and `Workflow.loop` store loop state and
  remove the previous iteration's nested execution records on restart.
- **History reset:** `Workflow.continueAsNew` replaces input and execution
  history in place. Runtime `forkWorkflow` and `resetWorkflow` operations restart
  from a selected stored step.
- **Cancellation:** `cancel` requests cancellation at the next uncached step or
  wait. `Workflow.uncancellable` permits cleanup before stopping. `terminate`
  stops further durable execution but cannot interrupt an external operation
  already running on a JVM thread.

## Build and test

Use JDK 21 or later. Database and example tests require Docker for PostgreSQL
Testcontainers.

```sh
sbt test
```

The `example` module contains equivalent document-processing workflows using
AtomicFlow and workflows4s. API and storage details are in [`spec/`](spec/).
Runtime design notes are in [`docs/design/`](docs/design/).
[`DEVIATIONS.md`](DEVIATIONS.md) records implementation decisions and differences
from the design. System comparisons are in [`docs/comparison/`](docs/comparison/).

## License

Apache License 2.0. See [`LICENSE`](LICENSE).
