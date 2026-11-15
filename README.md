# AtomicFlow

AtomicFlow is a Scala 3 library for durable workflows. Workflows are multistep programs that are characterized by being failure proof and very long-running. For example, in an e-commerce purchasing workflow, days or weeks can pass between order, payment and shipping. The workflow must be able to handle failures like unavailable third-party systems, application crashes, etc. and resume execution in reaction to external events. A durable workflow library allows you to write a workflow just like regular procedural code without worrying about state persistence, retries, event handling and so on. 

AtomicFlow achieves durability by wrapping side effects in `Step` functions which persist their results in a database. If the application crashes between steps, the workflow runtime will re-execute the code of unfinished workflows from the beginning and immediately return the cached results for steps that have already happened instead of re-executing side effects:

```scala
Workflow[Unit, Int](id = "example-wf") {
  val x = Step.atLeastOnce("step-1") {
    Random.nextInt()
  }
  
  // if crash here -> x result will be loaded from cache. Side-effect does not re-execute.

  val y = Step.atLeastOnce("step-1") {
    Random.nextInt()
  }

  x + y
}
```

To enable workflows to wait or react on external events, some step functions can register a _subscription_ and then _suspend_ the workflow by throwing an internal `WorkflowSuspendedException`. The code execution will stop at that point and the runtime will replay the workflow code when the subscribed event occurs.

```scala
val resumeSignal = Signal[Unit]("resume")

Workflow[Unit, Int](id = "example-wf") {
  Step.await(Awaitable.Timer(7.days)) // Wait for 7 days to pass

  Step.await(Awaitable.SignalEvent(resumeSignal)) // Wait until user sends the resume signal
}
```

## Comparison to other durable workflow libraries



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
  Step.atLeastOnce[String]("normalize", assertUnchanged = Seq("address" -> address)) {
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

- **Input changes:** `assertUnchanged` rejects changed named inputs;
  `invalidateOn` discards the cached result when a named input changes;
  `invalidateAfter` sets cache expiry.
- **Retries:** `Step.RetryPolicy` retries failures in at-least-once steps.
  Short delays block the executing thread; delays above the runtime's
  `durableRetryThreshold` durably suspend the step until its stored retry
  deadline is due (a timer wakes the instance). A stored terminal
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


## License

Apache License 2.0. See [`LICENSE`](LICENSE).
