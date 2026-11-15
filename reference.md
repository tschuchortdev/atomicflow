# AtomicFlow user reference

## Setup

Examples use the following imports throughout:

```scala
import atomicflow.*
import atomicflow.given // named step-input conversions
import atomicflow.Cacheable.Simple.given
import atomicflow.impl.db.PostgresWorkflowRuntime
import org.postgresql.ds.PGSimpleDataSource
import java.time.Instant
import scala.annotation.experimental
import scala.concurrent.ExecutionContext
import scala.concurrent.duration.*
```

The `WorkflowRuntime` interface acts as an adapter between the workflow library and different database technologies. Currently, PostgreSQL is the only available runtime. It needs a `DataSource` and an `ExecutionContext`:
```scala
val dataSource = new PGSimpleDataSource()
dataSource.setUrl("jdbc:postgresql://localhost:5432/atomicflow")
dataSource.setUser("postgres")
dataSource.setPassword(sys.env("ATOMICFLOW_DB_PASSWORD"))

given ExecutionContext = ExecutionContext.global
given WorkflowRuntime = PostgresWorkflowRuntime(dataSource)
```
Flyway migrations are executed when the constructor is called.

## Declaring a workflow

```scala
given Cacheable[Throwable] = Cacheable.forThrowable.genericStringMessageSerializer

val exampleWorkflow = Workflow[String, String](id = "example-wf", version = 1L) { inputStr =>
  // Workflow steps go here
  Step.atLeastOnce("print-input") {
    println(s"I'm the input $inputStr")
  }
  "I'm the result"
}
```

`Workflow[In, Out]` is a reusable definition. Its ID is a stable storage key;
the instance key identifies one execution of that definition. Input is stored
at creation and output at completion. Both types need `Cacheable` codecs.
`Unit` is useful when there is no input or result. The body receives a
`WorkflowContext`, used by operations inside the workflow.

### Declaring helper functions

Pass the context to helpers that call `Step` or workflow operations:

```scala
def printInput(input: String)(using WorkflowContext): Unit =
  Step.atLeastOnce("print-input") { println(s"I'm the input $input") }

Workflow[String, String]("with-helper") { input =>
  printInput(input)
  "done"
}
```

The helper runs on replay; its step returns the stored result. Call it in a
different `Workflow.scoped` scope when each invocation needs a separate step.

### Serialization with `Cacheable`

`Cacheable[A]` converts values to and from strings. The runtime uses it to store
workflow inputs and outputs, step results, signals, and loop state. Keep stored
formats readable across deployments as long as old versions of the workflow code are unfinished. `Cacheable.Simple.given` provides codecs
for `Unit`, `String`, `Boolean`, `Int`, `Long`, `Float`, `Double`, `Option`,
`Seq`, and `Map` when their members have codecs.

```scala
case class Order(id: String, quantity: Int) derives Cacheable

// For a custom format, choose a stable ID and implement both directions.
case class CustomerId(value: String)
given Cacheable[CustomerId] with {
  def stableSerializedTypeId: String = "customer-id"
  def write(value: CustomerId): String = value.value
  def read(serialized: String): CustomerId = CustomerId(serialized)
}
```

`stableSerializedTypeId` identifies the stored representation, not the Scala
type. Composite codecs store and check member IDs when reading. Keep the ID
stable for compatible formats; change it for an incompatible format, and plan
how to read or migrate existing data. Changing the ID alone does not migrate
stored values. `derives Cacheable` uses JSON and an ID based on the class name,
so a rename can break decoding of unions or products involving that type. See [workflow evolution](#workflow-evolution).

Step failures and failed workflow outcomes are stored too, so select a
`Cacheable[Throwable]` explicitly:

- `genericStringMessageSerializer` stores the exception class name, message,
  and cause chain. It reads them as diagnostic exceptions and presents step
  failures as `StepFailed`; it does not restore the original exception class.
- `javaSerializable` uses Java serialization and restores exception classes.
  The exception and its object graph must be serializable, and old data remains
  dependent on those classes and their serialization compatibility.

Use the generic codec when diagnostics are enough; use a compatible codec that
preserves exception types if workflow code needs to catch those types on replay.

## Running a workflow

Running a workflow needs two things: 
1. You need to create the workflow instance and supply input arguments
2. Someone needs to execute the workflow instance. Workflow instances can be run synchronously but more common is to run them asynchronously with a job runner that comes with the workflow runtime.

Creating an instance is idempotent for the same workflow ID, instance key,
and **serialized input**. A different input raises `WorkflowInputConflictException`.

```scala
val instance = exampleWorkflow.create("first", "hello")
val outcome = instance.run() // runs synchronously on the calling thread
assert(outcome == WorkflowRunResult.Result("I'm the result"))

// Equivalent create-and-run operation:
val another = exampleWorkflow.createAndRun("second", "hello")
```

`run`, `createAndRun`, and `blockUntilCompleted` return the outcome as a value; they never rethrow a workflow's own failure:

```scala
exampleWorkflow.createAndRun("third", "hello") match {
  case WorkflowRunResult.Result(value)      => s"completed with $value"
  case WorkflowRunResult.Failed(cause)      => s"failed: ${cause.getMessage}"
  case WorkflowRunResult.WorkflowSuspended  => "suspended while waiting for an event"
  case WorkflowRunResult.WorkflowCancelled  => "cancelled"
  case WorkflowRunResult.WorkflowTerminated => "terminated"
  case WorkflowRunResult.ContinueAsNew      => "restarted with a new input"
}
```

A terminal instance that is re-run returns its stored outcome without executing its body again.

### Background runner

Synchronously running workflows is mainly used in tests. It is more common to use a background job runner that automatically executes all workflow instances that are ready (not waiting for an event):

```scala
val runner = summon[WorkflowRuntime].startJobRunner(
  definitions = Seq(exampleWorkflow),
  JobRunnerSettings(workerThreads = 4, pollInterval = 250.millis,
    throwableCacheable = summon[Cacheable[Throwable]])
)
try {
  val job = exampleWorkflow.createAndScheduleForAsyncRun("third", "hello")
  val result = job.blockUntilCompleted(30.seconds)
} finally runner.stop(5.seconds)
```

Every job runner needs to be supplied with a list of workflow `definitions` so it can know what code to execute.

The runner handles wakeups, timers, event dispatch, cancellation, and lease
recovery. Runners can share a database. `blockUntilCompleted` only polls; it
does not run the workflow and throws `TimeoutException` on timeout.

## Declaring steps

`Step`s wrap side effects to control their durable execution. Functions found under the `Step` object typically cache their results, whereas functions found under `Workflow` typically do not.

```scala
val exampleStep = Workflow[String, String]("step-example") { input =>
  val normalized = Step.atLeastOnce("read", retry = Step.RetryPolicy.fixedDelay(3, 5.seconds)) {
    input.trim
  }
  val recorded = Step.atMostOnce("record") { normalized.length }
  s"$normalized: $recorded"
}
```

### Execution guarantees

There are two types of simple `Step`s:

| Operation | After a crash before the result is stored |
| --- | --- |
| `Step.atLeastOnce` | Runs the body again. The external effect may happen more than once. |
| `Step.atMostOnce` | Does not run the body again; returns `None` because the outcome is unknown. A stored success returns `Some(value)`. |

Neither guarantee makes an external effect atomic with the step record. Use an
idempotency key at the external service when duplicate effects matter. Handle
`None` as an unknown outcome, not proof that the effect did not happen.

### Step ID

Give every step and await a stable key unique within its scope. Step keys,
scope keys, and region IDs must be non-empty, cannot start with `_`, and cannot
contain `/`, `*`, `@`, `[`, `]`, `{`, or `}`. Step keys may not be shared among steps of different kinds. That means if you change the kind of an existing step, you also need a new key.

### `invalidateOn`, `invalidateAfter` and `assertUnchanged`

Many step functions have parameters `invalidateOn`, `invalidateAfter` and `assertUnchanged` that act like cache keys. 

```scala
val certificate = Workflow[String, String]("certificate") { owner =>
  Step.atLeastOnce("issue",
    assertUnchanged = Seq("owner" -> owner),
    invalidateOn = Seq("issuer" -> "issuer-v1"),
    invalidateAfter = 24.hours
  ) { s"certificate for $owner" }
}
```

In contrast to other workflow libraries like Temporal and DBOS, AtomicFlow does not require every workflow replay to execute steps in the same order as before. That allows us to use cache keys for domain logic or for easier manual intervention in running workflows (see [reset & fork](#manual-intervention-in-running-workflows-reset--fork)). For example, we can imagine a use case where a certificate is only valid for 24 hours. With `invalidateAfter` we can easily re-run the certificate step after it becomes outdated. Other steps that depend on the certificate should then use `invalidateOn` to also invalidate themselves whenever the certificate changes.

Of course, re-running some steps and not others can lead to out-of-order execution and insidious bugs; the feature should be used very carefully. `assertUnchanged` is intended as a safety feature to easily assert that inputs to a step haven't changed unexpectedly because a previous step was invalidated.

**It is best practice to put all local variables that are captured by the `Step` body either in `invalidateOn` or `assertUnchanged` to prevent unnoticed bugs.**

Note that `Step` invalidation is only checked when the workflow instance is replayed and loads the cached result of a `Step`. No timer subscriptions are created that would automatically wake up the instance when `invalidateAfter` is reached.

### Automatic retry of failed steps

When an exception is thrown inside a `Step`, it can be automatically retried with a given `RetryPolicy`. The default is `Step.RetryPolicy.never`: the first body failure is persisted as the step's outcome. `Step.RetryPolicy.fixedDelay` and `Step.RetryPolicy.exponentialBackoff` retry a failed body up to a limit; both take an `isRetriable` predicate to retry only selected exceptions. A custom policy implements `nextDelay`, which receives the thrown exception, the number of failed attempts, the cumulative delay so far, and the last delay; returning `None` stops retrying and persists the failure.

Delays up to the runtime's `durableRetryThreshold` are applied as inline sleeps inside the run. Longer delays suspend the workflow durably: the retry is persisted with a deadline and the instance is woken when it is due, so a pending retry survives a crash or redeploy.

```scala
val payment = Workflow[String, String]("payment") { orderId =>
  Step.atLeastOnce("charge",
    retry = Step.RetryPolicy.exponentialBackoff(
      initialDelay = 1.second,
      maxRetries = 5,
      isRetriable = { case _: java.io.IOException => true; case _ => false }
    )
  ) { chargeCard(orderId) }
}
```

### Handling exceptions that escape the step body

After retries are exhausted, an uncaught exception inside a `Step` body becomes that `Step`s persisted outcome and is rethrown whenever the `Step` executes. It can be handled with a plain `try`-`catch` but the type of exception depends on the serializer that was used:

```scala
// With Cacheable.forThrowable.javaSerializable, the original exception class is
// restored, so catch the exception type:
try Step.atLeastOnce("charge") { chargeCard(orderId) }
catch { case e: CardDeclined => s"declined: ${e.getMessage}" }

// With the generic string-message codec, failures surface as StepFailed, and
// the stepId interpolator addresses the failing step (the message holds the
// original class name and message):
try Step.atLeastOnce("charge") { chargeCard(orderId) }
catch { case StepFailed(stepId"charge", message) => s"charge failed: $message" }
```

Using `Cacheable.forThrowable.javaSerializable` can be dangerous because not every `Exception` is serializable. 

### Long-running Steps - `Workflow.heartbeat`

Workflow runtimes typically lock workflows with an execution lease for a limited time to avoid liveness-problems. If your `Step` body is long-running, or you have long-running pure code outside of `Step` bodies, you should make sure to call `Workflow.heartbeat()` frequently enough to renew the lease before it runs out. Furthermore, `Workflow.heartbeat()` also checks for cancellation of the workflow (may throw `WorkflowCancelledException`), thus ensuring that cooperative cancellation happens in a timely manner.  

## Workflow suspension: waiting on time or events

Durable workflows need to wait for time or external events and they need to do so without blocking application threads for a long time. AtomicFlow provides a `Step.await` function to do this. `Step.await` can wait for a `SignalEvent`, a `Timer`, or another workflow instance's
completion:

```scala
val approver     = Step.await("wait-approval", Awaitable.SignalEvent(approvalSignal)) // next signal event
Step.await("sla", Awaitable.Timer(1.hour))                                           // until a deadline
val childOutcome = Step.await("wait-child", child.completion)                         // another instance's outcome
```

When `Step.await` is called, it will check whether there is a matching event in the event log. If not, it will register a *subscription* with the workflow runtime and abort execution by throwing `WorkflowSuspendedException`. The runtime keeps track of the subscriptions and schedules a new execution of the workflow instance when a matching event arrives that could unblock the workflow. Once a `Step.await` finds a satisfying event, it stores it as its own result just like any other `Step` and returns the cached result on every replay.

### Signals

Communication with a workflow instance from the outside or between independent workflows happens by sending events to a workflow instance. A *signal* is a channel of events with a specific key and type. It is not necessary to declare signals upfront; events can be sent with any arbitrary signal key and all end up in the same event log. However, it is often more convenient to declare a reusable `Signal[A]` definition:

```scala 
val formInputSignal = Signal[String](key = "form-input-signal")/*(using summon[Cacheable[String]])*/
```

This `formInputSignal: Signal[String]` definition bundles the stable key identifying the signal with the expected type of the event payload and the `Cacheable` that should be used to serialize/deserialize the payload. Reusable `Signal` definitions thus make sure that sender and receiver agree on the types.

To send an event to a workflow instance, you only need the `WorkflowInstanceId` or `WorkflowInstance` handle and `Signal` definition:
```scala
val approvalSignal = Signal[String]("approval")
val approvalWf = Workflow[String, String]("approve") { orderId =>
  val approver = Step.await("wait-approval", Awaitable.SignalEvent(approvalSignal))
  s"$orderId approved by $approver"
}

val order123 = approvalWf.create("order-123", "123")
order123.run()
order123.sendSignal(approvalSignal, "manager")
```
Sending appends the event in one transaction and does not wait for the workflow to run; wakeups are produced asynchronously by the job runner. The send can briefly block on database row locks (the instance row is locked to reject terminal instances), but never on the instance's execution lease. Terminal instances return `InstanceAlreadyCompleted`.

#### Cursors, filters, and `lookBack`

To make it easy to consume a sequence of signal events, every `Step.await(SignalEvent("x"))` also persists _cursor_ for the signal key `"x"` in the database, if it doesn't already exist. This cursor records a monotonically increasing event ID watermark to remember the latest processed event from the signal `"x"`. The cursor for `"x"` is shared between all `Step.await(SignalEvent("x"))` that listen on `"x"`:

```scala 
// Assume signal "x" contains the events "x1" and "x2"
val signalX = Signal[String](key = "x")

Workflow[Unit, Unit]("example-wf") { _ =>
  val ev1 = Step.await(Awaitable.SignalEvent(signalX)) // will consume the first matching event "x1"
  val ev2 = Step.await(Awaitable.SignalEvent(signalX)) // will see that the cursor has already advanced past "x1" and instead consume "x2"
}
```

A filter miss does not stop the cursor: when a signal await uses a filter, the cursor advances past rejected events too, to preserve monotonicity. Earlier rejected events are therefore permanently unavailable to later awaits on the same signal key:

```scala
// Assume signal "x" contains the events "x1", "x2", "x3"
val signalX = Signal[String](key = "x")

Workflow[Unit, Unit]("example-wf") { _ =>
  val ev1 = Step.await(Awaitable.SignalEvent(signalX, filter = { s => s == "x2" })) // will reject "x1" and consume "x2"
  val ev2 = Step.await(Awaitable.SignalEvent(signalX)) // will consume "x3" since the cursor is past "x2" and thus also past "x1", even though "x1" was never consumed
}
```

Using the `lookBack` parameter, it is possible to accept only those events that arrived recently. `lookBack` is measured against the event's original acceptance time. An event older than five minutes is rejected like a filter miss.

```scala
val freshApproval = Step.await("wait-approval", Awaitable.SignalEvent(approvalSignal, lookBack = 5.minutes))
```

### Racing between events

AtomicFlow provides a special primitive function `Step.awaitRace` to select the earliest of several awaitable events:
```scala
val approveWithTimeout = Workflow[String, String]("approve-with-timeout") { _ =>
  Step.awaitRace[String]("approval-or-timeout")(
    "approval" -> Awaitable.SignalEvent(approval).map(name => s"approved by $name"),
    "timeout" -> Awaitable.Timer(1.hour).map(_ => "timed out")
  )
}
```

How the "earliest" event is selected depends on the Workflow runtime implementation and its ordering guarantees. In the PostgreSQL implementation, all events share a single event log and so a reliable ordering between all events, including completions of other workflows, is guaranteed.

To race arbitrary branch code (for example, sub-workflows with multiple steps) against each other instead of single events, use `Step.firstCompletedWithoutSuspension` (see [parallel execution](#parallel-execution)).

## Parallel Execution

AtomicFlow provides two primitive functions for parallel execution: `Workflow.parallel` and `Step.firstCompletedWithoutSuspension`. You must use these functions to implement parallelism instead of using something like `Future`, cats-effect or Ox because third-party libraries do not handle workflow suspension, cancellation and cleanup correctly.

### `Workflow.parallel`

`Workflow.parallel` executes branches concurrently and returns a tuple of all branch results once all branches have finished execution. If one branch suspends, the `WorkflowSuspendedException` is automatically held back until the other branches also suspend or finish execution. 

```scala
val report = Workflow[OrderId, Report]("report") { orderId =>
  val (profile, history) = Workflow.parallel(
    Step.atLeastOnce("profile") { loadProfile(orderId) },
    Step.atLeastOnce("history") { loadHistory(orderId) }
  )
  Report(profile, history)
}
```

Since `Workflow.parallel` is under the `Workflow` object, it does not save its result itself and instead replays all branches every time it is called.

### `Step.firstCompletedWithoutSuspension`

`Step.firstCompletedWithoutSuspension` executes branches concurrently and returns only the result of the branch that finishes first. All other branches are cooperatively canceled as soon as one branch has finished. Therefore, this function can be used to race sub-workflows containing multiple workflow steps against each other, whereas `Step.awaitRace` can only do so for events and not arbitrary code (see [racing between events](#racing-between-events)).

```scala 
Step.firstCompletedWithoutSuspension[Int]("race")(
    "first" -> { () => 
      Step.atLeastOnce("a") { ... } 
      Step.atLeastOnce("b") { ... } 
    },
    "second" -> { () => 
      Step.atLeastOnce("c") { ... } 
      Step.atLeastOnce("d") { ... } 
    }
)
```

Just like `Workflow.parallel`, `Step.firstCompletedWithoutSuspension` only returns once all branches have finished execution to ensure that cleanup is done. Losing branches are canceled cooperatively, which means that they will abort execution at the next `Step` or `heartbeat` but may include custom cleanup logic that can suspend the workflow. Once all branches have finished, `firstCompletedWithoutSuspension` will return its persisted result without replaying the branches.

## Workflow cancellation

AtomicFlow workflows rely on cooperative cancellation. Cancellation can be requested for a whole workflow instance using `summon[WorkflowRuntime].cancel(instance.id)` or just for parts of a workflow instance through certain library functions.

- Cancellation is cooperative: running workflow instances are not forcefully aborted by interrupting a thread. Instead, a `WorkflowCancelledException` is thrown the next time that a `Step` or `Workflow.heartbeat` function is called, which should promptly finish the workflow since no more side effects can be executed.
- The exception cannot be ignored: if the code catches it and attempts to continue executing `Step`s anyhow, those `Step` function calls continually rethrow it, preventing a workflow instance from simply ignoring the cancellation request.
- Cancellation can be temporarily disabled inside of a `Workflow.uncancellable` body to run cleanup or compensation steps with `Step`s that may suspend.
- Because of this, you should never assume that canceled workflows will finish immediately.

```scala
val booking = Workflow[String, String]("booking") { bookingId =>
  try {
    Step.atLeastOnce("book-flight") { bookFlight(bookingId) }
    Step.atLeastOnce("book-hotel") { bookHotel(bookingId) }
    "booked"
  } catch {
    case e: WorkflowCancelledException =>
      // Compensation steps run to completion even though the workflow is cancelled.
      // They are ordinary durable Steps: cached, replayable, and resumable after a crash.
      Workflow.uncancellable {
        Step.atLeastOnce("refund-flight") { refundFlight(bookingId) }
        Step.atLeastOnce("refund-hotel") { refundHotel(bookingId) }
      }
      throw e // rethrow so the instance ends up cancelled, not completed
  }
}
```

## Child workflows

Child workflows are separate workflow instances that are started from within a workflow and then run independently, receive and process events independently and fail independently, but are namespaced by the parent workflow and bound to its lifecycle. 

```scala
val worker = Workflow[String, String]("worker") { input =>
  Step.atLeastOnce("process") { input.toUpperCase(java.util.Locale.ROOT) }
}
val parent = Workflow[String, String]("parent") { input =>
  val child = worker.startAsChild("worker-1", input,
    parentClosePolicy = ParentClosePolicy.Cancel,
    inheritSignals = SignalInheritance.some(Seq("approval/")),
    inheritPastEvents = false)
  Step.await("wait-child", child.completion) match {
    case WorkflowCompletionResult.Completed(value) => value
    case WorkflowCompletionResult.Failed(cause)    => throw cause
    case WorkflowCompletionResult.Cancelled        => "cancelled"
    case WorkflowCompletionResult.Terminated       => "terminated"
  }
}
```

Children are separate instances, scheduled rather than run inline. Start is
idempotent for the same child identity and serialized input. A runner must
register the child definition, or the child must be run separately. Use children
for independently scheduled work, separate signal handling, or separate
history. Awaiting completion returns an outcome; it does not throw the child's
failure automatically.

When the parent closes, `Cancel` requests child cancellation; `Abandon`
detaches the child so it can continue. Closing does not wait for cancellation.
You can also cancel a child explicitly through its ID. Child identity includes
the parent, generation, and enclosing scope.

### Signal inheritance

Signal inheritance defaults to `none`; `all` or `some(Seq(prefixes...))`
makes matching parent signals visible to the child. Every edge in an ancestor
chain must permit the key. Each child has its own cursor. By default only
events after child creation are inherited; `inheritPastEvents = true` also
allows retained earlier events. Inheritance settings are fixed at creation.
Detachment ends inherited visibility; deleted parent events cannot be recovered.

## Iteration

Iteration can be implemented in three different ways:

### With a plain loop and `Workflow.scoped`

Any plain Scala loop and `map` or `fold` function can be used to iterate over collections in a workflow. The key to make this work is to prefix all `Step` IDs with a fingerprint of the respective collection element, so that each collection element has unique `Step` IDs. 

`Workflow.scoped(x)` is an easy way to do this. It prefixes all `Step`s inside its body with the hash of its argument:  

```scala
val processOrders = Workflow[Seq[String], Unit]("process-orders-wf") { orderIds =>
  orderIds.foreach { orderId =>
    Workflow.scoped(orderId) {
      Step.atLeastOnce("process-order") { processOrder(orderId) }
    }
  }
}
```

Note that `Workflow.scoped` calls can be nested arbitrarily.

In contrast to other workflow libraries, the collection need not necessarily have the same order or elements across workflow replays. The workflow will still replay without error, though it is good practice to make all workflows deterministic.

### With `Workflow.loop` or `Workflow.restartable`

For polling and similar use cases where you need to loop with changing state, use `Workflow.loop` or `Workflow.restartable`:

```scala
val poll = Workflow[Unit, Int]("poll") { _ =>
  Workflow.loop[Int, Int]("poll-loop", 0) { (attempt, loop) =>
    val ready = Step.atLeastOnce("check") { attempt >= 3 }
    if (ready) loop.break(attempt)
    Step.await("pause", Awaitable.Timer(1.minute))
    attempt + 1
  }
}
```

`loop` and `restartable` each take an initial state and a next state that is persisted after every iteration. Both functions are equivalent to each other. The only difference is that `loop` will iterate until `break(result)` is called, whereas `restartable` will return after one iteration unless `restart(nextState)` is called:

The same poller written both ways:

```scala
// loop: iterates until break(result)
val withLoop = Workflow[Unit, Int]("with-loop") { _ =>
  Workflow.loop[Int, Int]("poll", 0) { (attempt, loop) =>
    Step.atLeastOnce("ready") { check(attempt) }
    if (isReady) {
      loop.break(attempt)
    }
    
    Step.await("pause", Awaitable.Timer(1.minute))
    attempt + 1
  }
}

// restartable: equivalent; returns after one iteration unless restart is called
val withRestartable = Workflow[Unit, Int]("with-restartable") { _ =>
  Workflow.restartable[Int, Int]("poll", 0) { (attempt, scope) =>
    val isReady = Step.atLeastOnce("ready") { check(attempt) }
    if (isReady) {
      attempt
    } else {
      Step.await("pause", Awaitable.Timer(1.minute))
      scope.restart(attempt + 1)
    }
  }
}
```

`Workflow.restartable` can also be useful in situations where your workflow needs to `goto` an earlier step. 

### With Continue-as-new

Workflows that loop can accumulate a long history of events and step caches which may lead to performance problems. To solve this problem, workflow libraries typically implement a `continueAsNew` function which is also present in AtomicFlow: 

`Workflow.continueAsNew(nextInput)` stops the current run, clears execution history and subscriptions, and
schedules the same instance to start again from a clean slate with a new input. Thus, all execution state that should persist across `continueAsNew` must be part of the workflow's input type. `nextInput` must use the workflow's
input type and compatible codec. Signal cursors remain,
directly addressed signal events are deleted, and children close according to
their policies. A child remains attached to its own parent: if this workflow is
itself a child, it stays attached with unchanged signal inheritance, and its
parent's completion await keeps tracking it across the generation change.

## Workflow evolution

Over time, it becomes necessary to alter workflow code and, inevitably, there will be unfinished workflow instances that were started with the old code when the new version is deployed. New code must be able to deal with old workflow data. To handle code changes, `Workflow` and `Step.atLeastOnce` have a version parameter:

```scala 
Workflow[Unit, Unit]("example-wf", version = 1) { _ => 
  Step.atLeastOnce("example-step", version = 1)
}
```

You can change the workflow version when incompatible code changes are made and then write migration code using `Workflow.versionAtCreation`: 

```scala
val greeter = Workflow[String, String]("greeter", version = 2) { name =>
  if (Workflow.versionAtCreation == 1L)
    Step.atLeastOnce("greet", version = 1) { s"Hello $name" } // instances created by v1 replay this branch
  else
    Step.atLeastOnce("greet", version = 2) { s"Hi $name!" }   // instances created by v2 run this branch
}
```

When the version number of a `Step.atLeastOnce` is incremented, the workflow will simply re-run the step with the new code as if the step key had changed, which should always be safe for at-least-once semantics. Alternatively, you can also query the step's state and write your own migration code:

```scala
val quote = Workflow[String, String]("quote", version = 2) { input =>
  // Instances created before the version bump reuse the stored v1 result
  // instead of re-running the side effect with v2 code:
  Step.getStepState[String]("quote", stepVersion = 1) match {
    case StepExecutionState.Completed(oldQuote) => oldQuote
    case _                                      => Step.atLeastOnce("quote", version = 2) { computeQuote(input) }
  }
}
```

`Step.atMostOnce` and `Step.await` deliberately have no `version` parameter, since it would be unsafe to simply re-run them. If you wish to do so, you must change the step key yourself. 

For codec changes, keep old payloads readable. `newCodec.withFallback(oldCodec)`
writes the new format and tries the old codec on read; `imap` can adapt a
legacy codec to a new type. `withFallback` changes the codec's type ID, and
composite codecs check member type IDs before reading payloads. A fallback alone
may therefore not read old values nested inside a composite. Test compatibility
with actual old data, including nested values.

## Manual intervention in running workflows (reset & fork)

It may become necessary to manually intervene in a running workflow and re-run certain parts of the workflow due to bugs or unforeseen behavior of third-party systems. AtomicFlow provides two functions to do this: `resetWorkflow` and `forkWorkflow`. `resetWorkflow` restarts a non-terminal workflow instance in place. `forkWorkflow` copies all state of the source workflow instance to create a new, independent workflow instance. Both replay
from the beginning with selected step results removed; all other cached steps remain.

```scala
val runtime = summon[WorkflowRuntime]
given Workflow[String, String] = exampleWorkflow
val sourceId = instance.id
val selected = Seq(stepId"print-input")
val matches = runtime.listSteps(sourceId, selected.head)
val fork = runtime.forkWorkflow(sourceId, "customer-retry",
  invalidateIndividualSteps = selected, invalidateAllExecutedAfter = Seq.empty)
```

At least one selector list must be non-empty, and at least one selector must
match a stored step. `invalidateIndividualSteps` removes exactly the matches;
`invalidateAllExecutedAfter` removes the earliest matched row and all rows with
an equal or later last-update timestamp. This timestamp boundary is not a precise
causal cut: When parallel execution is involved or steps were invalidated due to a `invalidateAfter` parameter, then steps which appear earlier in the code can be executed after steps that appear later, so it is preferable to use `invalidateIndividualSteps` in combination with `Step`s `invalidateOn`.

#### `StepIdSelector`

To select which steps to invalidate in `resetWorkflow` or `forkWorkflow` you have to know the steps' IDs, which are path-like strings composed of the user-given step `key` parameter as well as all nested `Workflow.scoped` keys, iteration counters of `Workflow.loop`, etc. (Example: `"/foo@1/bar@2/bob"`) In other words: the exact step ID is an implementation detail and difficult to know. AtomicFlow provides a `stepId"..."` string interpolator to easily pattern match on step IDs without knowing the exact ID: 

`stepId"..."` builds a `StepIdSelector`: 

- `/` anchors at the instance root, otherwise the pattern matches any path suffix. 
- `*` matches any single path segment
- `**` matches zero or more segments. Loop iteration markers are ignored.
- Interpolated string values match literal scope segments
- other `Fingerprintable` values match segments with the value's fingerprint. (When you call `Workflow.scoped(x: UserId)` then the step ID scope path will contain `x`s `Fingerprintable[UserId]` value)

```scala
val bulkCharge = Workflow[Seq[String], Unit]("bulk-charge") { orderIds =>
  orderIds.foreach { orderId =>
    Workflow.scoped(orderId) { // scope segment: the literal order ID
      Workflow.loop[Int, Unit]("attempts", 0) { (attempt, loop) => // scope segment: attempts@<n>
        Step.atLeastOnce("charge") { charge(orderId) }
        loop.break(())
      }
    }
  }
}

// Matches `charge` for order "42" in every loop iteration; the runtime-generated
// iteration marker `attempts@<n>` is ignored by the pattern:
val selector = stepId"${42}/attempts/charge"
```

The same `stepId"..."` selectors can also be used to easily pattern match `StepFailed` exceptions:

```scala
try Step.atLeastOnce("charge") { chargeCard(orderId) }
catch {
  // Matches the failing step's ID; `message` holds the exception message:
  case StepFailed(stepId"charge", message) => s"charge failed: $message"

  // Patterns can also address a step inside scopes, with wildcards and bindings:
  case StepFailed(stepId"orders/$id/charge", message) => s"order $id: charge failed: $message"
}
```

### Using `invalidateOn` to re-run specific parts

To propagate a targeted reset or fork, declare downstream dependencies with
`invalidateOn`. A changed upstream result then invalidates the dependent step
when replay reaches it; unchanged results remain cached.

## Testing workflows

Tests drive workflow instances manually instead of starting a job runner. `createAndRun` and `run` execute inline on the test's thread until the workflow suspends; nothing progresses until the test delivers the next event or advances the clock. This gives single-threaded, deterministic tests without background interference.

```scala
val testClock = TestClock(Instant.parse("2026-01-01T00:00:00Z")) // any mutable java.time.Clock
val runtime = PostgresWorkflowRuntime(dataSource, testClock)(using ExecutionContext.global)
given WorkflowRuntime = runtime

val approvalSignal = Signal[String]("approval")
val approvalWf = Workflow[String, String]("approve") { orderId =>
  val approver = Step.await("wait-approval", Awaitable.SignalEvent(approvalSignal))
  s"$orderId approved by $approver"
}

val instance = approvalWf.create("order-1", "123")
assert(instance.run() == WorkflowRunResult.WorkflowSuspended) // waiting for the signal

instance.sendSignal(approvalSignal, "manager")
assert(instance.run() == WorkflowRunResult.WorkflowSuspended) // now waiting on the SLA timer

testClock.advanceBy(2.hours)
assert(instance.run() == WorkflowRunResult.Result("123 timed out"))
```

- The clock passed to the runtime constructor controls timers, `invalidateAfter`, and lease timing. Advance it and call `run` again; a run fires its own due timers inline, so timer races are decided deterministically by `advanceBy` + `run`.
- Set a low `durableRetryThreshold` in the runtime constructor so step retries become durable suspensions (driven by clock advances and `run`) instead of inline `Thread.sleep`s.
- `createAndScheduleForAsyncRun` and `blockUntilCompleted` need a runner. Manual tests use `createAndRun`, or `create` followed by explicit `run` calls.
- Assert terminal outcomes, and inspect intermediate state with `Step.getStepState`, `instance.getInfo()`, and counters stubbed into step bodies. A `WorkflowSuspended` between runs is itself a valid assertion.
- Start a runner only for tests that exercise scheduling behavior (lease takeover, wakeup ordering): `runtime.startJobRunner(definitions, JobRunnerSettings.forTests)`.