# **AtomicFlow - A Workflow Framework with Idempotent Steps and Atomic Replay**

**AtomicFlow** is a workflow framework designed to help you manage and execute workflows in an atomic, idempotent, and repeatable manner. With the ability to handle side effects, deduplication, and easy retries, this framework ensures that business processes are executed reliably while maintaining consistency.

---

## Features

- **Atomic Workflows**: Your workflows are guaranteed to run atomically, meaning either all steps complete successfully, or none of them do.
- **Idempotent Execution**: Steps are idempotent, so re-executing a step with the same inputs and context will not result in side effects being performed multiple times.
- **Only-Once Effects**: Critical steps that have side effects (e.g., sending data to a customer) are protected by **only-once execution** to ensure they are only executed once, unless explicitly retried with a new idempotency ID.
- **Step Caching**: Every step's output is cached, enabling you to skip already executed steps during retries, providing an efficient means to recover from failures.
- **Replayable Workflows**: If a failure occurs, you can replay the workflow from the beginning, skipping already successfully completed steps.
- **Time-limited Steps**: Some steps (e.g., certificate validation) are only valid for a certain period and must be re-evaluated after expiration via TTL-based cache expiry.
- **Signals**: Workflows can stop and wait for external signals (e.g., user approval) or timers, then be automatically restarted when the condition is met.
- **Subworkflows**: Run parallel subworkflows within a parent workflow, with scoped step idempotency.

---

## Out of scope
- Introspection / Diagram generation

---

## How it Works

AtomicFlow focuses on **workflow steps** that can be executed safely and deterministically. Each step can be classified into two types:

1. **Cached Steps** – These steps are computationally simple or have side effects that are safe to repeat. Their output is cached by input fingerprint, so re-executing with the same inputs returns the cached result.
2. **Only-Once Steps** – These steps may trigger important, irreversible changes (e.g., sending data to an external service). AtomicFlow ensures these steps are executed **exactly once** using a combination of **idempotency IDs** and **input hashing**.

The framework also supports **retrying workflows** when exceptions occur, ensuring **atomicity** even in case of transient errors.

---

## Installation

Add the following to your `build.sbt` (for Scala projects using sbt):

```scala
libraryDependencies += "de.lhns" %% "atomicflow-core" % "0.1.0-SNAPSHOT"

// Optional: PostgreSQL-backed runtime
libraryDependencies += "de.lhns" %% "atomicflow-db" % "0.1.0-SNAPSHOT"
```

---

## Example Usage

### Creating a Workflow Runtime

Before running any workflows, you need a `WorkflowRuntime`. AtomicFlow provides an in-memory runtime (for testing/development) and a PostgreSQL-backed runtime (for production).

```scala
import atomicflow.*
import atomicflow.impl.memory.InMemoryWorkflowRuntime
import atomicflow.Cacheable.MsgPack.given

// Create an in-memory runtime
given WorkflowRuntime = InMemoryWorkflowRuntime()
given WorkflowRunSettings = WorkflowRunSettings()
```

### Defining a Workflow

A workflow is defined using the `Workflow` builder, which takes a `WorkflowId`, a name, and a body function:

```scala
val myWorkflow = Workflow[String, Int](
  WorkflowId("d9ab1884-6e83-48d7-82c4-cbc89fb32ffc"),
  name = "enrich and send"
) { (input: String) =>
  // A cached step: output is cached by input fingerprint
  val enriched = Step(StepId("d27142b9-e7db-4b8e-b341-6dd3009655c7"), version = 0, name = "enrich") {
    Step.cache("input" -> input)
    input.trim.toUpperCase
  }

  // An only-once step: executed exactly once per workflow instance
  Step(StepId("f4a18269-83a8-4fcf-a62b-3cbb6216ddee"), version = 0, name = "send email") {
    Step.onlyOnce("email" -> enriched)
    println(s"Sending email to $enriched")
  }

  42
}
```

### Executing the Workflow

```scala
val instanceId = "my-instance-1"
val result = myWorkflow.createAndRun(instanceId, "alice@example.com")
// result: Either[StoppedWorkflow[Int], Int] = Right(42)
```

### Retrying Workflows

If an exception occurs during execution, you can retry the workflow. Steps that were previously completed (cached or only-once) will be skipped:

```scala
val instanceId = "my-instance-1"
try
  myWorkflow.createAndRun(instanceId, "alice@example.com")
catch
  case e: Exception =>
    println(s"Workflow failed, retrying: ${e.getMessage}")
    myWorkflow.run(instanceId)  // Retry — cached steps are skipped
```

### Using the PostgreSQL Runtime

For production use, the `DbWorkflowRuntime` persists workflow state, step caches, signals, and schedules in PostgreSQL:

```scala
import atomicflow.impl.db.DbWorkflowRuntime
import org.postgresql.ds.PGSimpleDataSource
import scala.concurrent.ExecutionContext

val ds = new PGSimpleDataSource()
ds.setUrl("jdbc:postgresql://localhost:5432/atomicflow")
ds.setUser("postgres")
ds.setPassword("postgres")

given WorkflowRuntime = DbWorkflowRuntime(ds)(using ExecutionContext.global)
```

The database schema is managed by Flyway migrations and is created automatically when the runtime is initialized.

---

## Concepts and Terminology

- **Workflow**: A definition with a unique `WorkflowId`, an input type `In`, and an output type `Out`. Workflows are parameterized by `Cacheable[In]` and `Cacheable[Out]` for serialization.
- **Workflow Instance**: A specific run of a workflow, identified by a `(WorkflowId, WorkflowInstanceKey)` pair. The instance key is a user-provided string.
- **Step**: A unit of work within a workflow, identified by a `StepId` (UUID) and a version number. Steps have access to a `StepContext` for caching and idempotency.
- **Cached Step**: A step whose output is cached by input fingerprint. Re-running with the same inputs returns the cached result without re-executing the step body.
- **Only-Once Step**: A step that is executed exactly once per workflow instance, regardless of input changes. Useful for irreversible side effects like sending emails or making payments.
- **Step Idempotency ID**: A UUID assigned to each step execution, used as the cache key. For only-once steps, this ID can be overridden to force re-execution.
- **Fingerprint**: A SHA-256 hash of a step's inputs, used to detect when a step's inputs have changed between runs.
- **Signal**: A typed value that a workflow can wait for. Workflows can stop themselves and be restarted when a signal is set.
- **StoppedWorkflow**: When a workflow stops to wait for a timer, signal, or another workflow, it returns `Left(StoppedWorkflow)`. The runtime schedules an automatic restart when the condition is met.

---

## Advanced Features

### Expiring Steps (TTL)

Steps can have a time-to-live (TTL) after which their cached output expires and the step must be re-executed:

```scala
Step(StepId("..."), version = 0) {
  Step.cacheFor(Some(30.minutes))("input" -> inputValue)
  // This step's cache expires after 30 minutes
  computeExpensiveResult(inputValue)
}
```

### Signals

Workflows can wait for external signals:

```scala
val approvalSignal = Signal[String](SignalId("..."), name = "approval")

val workflow = Workflow[Unit, String](...) { _ =>
  val approval = Workflow.stopAndAwaitSignal(approvalSignal)
  // Workflow stops here if signal is not set, and resumes when it is
  s"Approved by: $approval"
}

// Set the signal externally
workflow.setSignal("instance-1", approvalSignal, "manager", 30.days)
```

### Subworkflows

Run parallel subworkflows within a parent workflow:

```scala
val workflow = Workflow[Seq[String], Unit](...) { (items: Seq[String]) =>
  Workflow.subworkflowForEach(items)(parallelism = 4)(
    subworkflowKey = identity
  ) { item =>
    processItem(item)
  }
}
```

### Awaiting Other Workflows

A workflow can wait for another workflow instance to complete:

```scala
val result = Workflow.stopAndAwaitWorkflow[Int](otherWorkflow, "other-instance-id")
// If the other workflow hasn't finished, this workflow stops and resumes later
```

---

## Contributing

We welcome contributions to **AtomicFlow**! If you have ideas for new features, improvements, or bug fixes, feel free to open an issue or submit a pull request.

1. Fork the repo.
2. Create your feature branch (`git checkout -b feature-name`).
3. Commit your changes (`git commit -am 'Add new feature'`).
4. Push to the branch (`git push origin feature-name`).
5. Open a pull request.

---

## License

This project uses the Apache 2.0 License. See the file called LICENSE.
