# **AtomicFlow - A Workflow Framework with Idempotent Steps and Atomic Replay**

**AtomicFlow** is a workflow framework designed to help you manage and execute workflows in an atomic, idempotent, and repeatable manner. With the ability to handle side effects, deduplication, and easy retries, this framework ensures that business processes are executed reliably while maintaining consistency.

---

## 🚀 **Features**

- **Atomic Workflows**: Your workflows are guaranteed to run atomically, meaning either all steps complete successfully, or none of them do.
- **Idempotent Execution**: Steps are idempotent, so re-executing a step with the same inputs and context will not result in side effects being performed multiple times.
- **Fixed Point Effects**: Critical steps that have side effects (e.g., sending data to a customer) are protected by **fixed-point execution** to ensure they are only executed once, unless explicitly retried with a new UUID.
- **Event Logging**: Every step is logged for traceability, enabling you to skip already executed steps during retries, providing an efficient means to recover from failures.
- **Replayable Workflows**: If a failure occurs, you can replay the workflow from the beginning, skipping already successfully completed steps.
- **Time-limited Steps**: Some steps (e.g., certificate validation) are only valid for a certain period and must be re-evaluated after expiration.

---

## Out of scope
- Introspection / Diagram generation
- 

---

## ⚙️ **How it Works**

AtomicFlow focuses on **workflow steps** that can be executed safely and deterministically. Each step can be classified into two types:

1. **Pure Steps** – These steps are computationally simple and are guaranteed to have no side effects. They can be re-executed as needed.
2. **Side-Effecting Steps** – These steps may trigger important, irreversible changes (e.g., sending data to an external service). AtomicFlow ensures these steps are executed **exactly once** using a combination of **UUIDs** and **input hashing**.

The framework also supports **retrying workflows** when exceptions occur, ensuring **atomicity** even in case of transient errors.

---

## 📦 **Installation**

1. Add the following to your `build.sbt` (for Scala projects using sbt):

   ```scala
   libraryDependencies += "de.lhns" %% "atomicflow" % "0.0.1"
   ```

2. Alternatively, download and include the jar in your project.

---

## 🧰 **Example Usage**

### **Creating a Workflow Context**

Before running any workflows, you need to set up a context that includes an event log.

```scala
import java.util.UUID
import java.time.Instant

// Create an in-memory event log (can be replaced by a DB or external service)
class InMemoryEventLog extends EventLog {
  private val store = scala.collection.mutable.Map.empty[(String, String, Option[String]), StepEvent]
  
  def load(name: String, inputHash: String, uuid: Option[String] = None): Option[StepEvent] =
    store.get((name, inputHash, uuid))
  
  def save(event: StepEvent): Unit = 
    store((event.name, event.inputHash, event.uuid)) = event
}

// Initialize the workflow context with an event log
val ctx = new WorkflowContext(new InMemoryEventLog())
```

### **Defining a Step**

A typical step in your workflow might look like this:

```scala
def sendEmail(email: String): String = {
  println(s"📤 Sending email to $email")
  s"sent-to:$email"
}

// Define a workflow that uses the fixed-point effect
def runWorkflow(input: String)(using ctx: WorkflowContext): String = {
  val enriched = step("Enrich")(_.trim.toUpperCase)(input)  // Pure step: Trims and capitalizes the email
  val _ = fixedPointEffect("SendEmail", enriched) { email => sendEmail(email) }  // Side-effecting step: Sends email
  "done"
}
```

### **Executing the Workflow**

Once the workflow is defined, you can execute it:

```scala
val input = "alice@example.com"
runWorkflow(input)(using ctx)  // Runs the workflow
```

The framework ensures that if the `fixedPointEffect` is called with the same UUID and data again, the email will not be sent again, preserving the **exactly-once** execution semantics.

---

## 🔄 **Retrying Workflows**

If an exception occurs during the execution of the workflow, you can retry the workflow. The steps that were previously executed successfully will be skipped, ensuring that the workflow is atomic.

```scala
try {
  runWorkflow(input)(using ctx)
} catch {
  case ex: Exception =>
    println(s"Workflow failed, retrying: ${ex.getMessage}")
    runWorkflow(input)(using ctx)  // Retry the workflow
}
```

---

## 📝 **Concepts and Terminology**

- **Atomic Workflow**: A workflow is guaranteed to run atomically — either all steps are successful, or none are.
- **Idempotent Steps**: Steps are guaranteed to be idempotent, meaning running them multiple times with the same input won’t cause side effects.
- **Fixed Point Effect**: Critical steps that must only run once, even if the workflow is retried. These steps use a **UUID** and **input hash** for deduplication.
- **Event Log**: Each step is logged, and you can inspect the log for details about execution, retries, and step outputs.

---

## 🛠️ **Advanced Features**

- **Expiring Steps**: Some steps (e.g., certificate validation) may expire after a certain period. You can configure the framework to ensure these steps are re-executed after expiration.
- **Custom Event Logging**: You can implement custom logging strategies (e.g., to a database or remote logging service) by extending the `EventLog` trait.

---

## 📚 **Contributing**

We welcome contributions to **AtomicFlow**! If you have ideas for new features, improvements, or bug fixes, feel free to open an issue or submit a pull request.

1. Fork the repo.
2. Create your feature branch (`git checkout -b feature-name`).
3. Commit your changes (`git commit -am 'Add new feature'`).
4. Push to the branch (`git push origin feature-name`).
5. Open a pull request.

---

## 📄 **License**

TODO
