# Core Types and Taxonomy

Guideline for identity, metadata, and handle types. Companion to `design.md` and `running-workflows.md`.

## Four categories

| Category | Types | Contents |
|---|---|---|
| Identity | `WorkflowId`, `WorkflowInstanceKey`, `WorkflowInstanceId`, `StepId`, `SignalId` | ids only |
| Definition metadata | `WorkflowMeta`, `SignalMeta` | id + version + name + description |
| Execution-scoped metadata | `StepMeta` | step definition fields + the instance it runs in |
| Persisted instance state | `WorkflowInstance.Info` | bookkeeping fields stored per instance |
| Handle | `WorkflowInstance[In, Out]` | workflow definition (code) + instance key + runtime |

```scala
type WorkflowId = String
type WorkflowInstanceKey = String

// Identity: the address of an instance; used for queries, signals, logging
case class WorkflowInstanceId(
  workflowId: WorkflowId,
  workflowInstanceKey: WorkflowInstanceKey,
  scope: String = ""
)

case class StepId(key: String, scope: String = "")


// Definition metadata: describes a *definition*, never instance state (pattern: existing SignalMeta)
case class WorkflowMeta(workflowId: WorkflowId, version: Long, name: String, description: Option[String])
case class SignalMeta(name: String, description: Option[String])

// Execution-scoped: steps are inline lambdas, so their metadata only materializes while a
// workflow instance executes — hence StepMeta carries the instance id (composition, not inheritance)
case class StepMeta(stepId: StepId, stepVersion: Option[Long], stepName: Option[String],
                    stepDescription: Option[String], workflowInstanceId: WorkflowInstanceId)

// Handle: capability object, obtained only from the runtime
final class WorkflowInstance[In, Out](workflow: Workflow[In, Out], instanceKey: WorkflowInstanceKey, instanceScope: String, runtime: WorkflowRuntime):
  def id: WorkflowInstanceId
  def run(): WorkflowRunResult[Out]
  def awaitResult(timeout: FiniteDuration): WorkflowRunResult[Out]
  def getInfo(): WorkflowInstance.Info   // fresh from DB

object WorkflowInstance:
  // Persisted instance state: the data view of an instance ("a row"), returned by
  // key-based queries that don't have the workflow code, and by instance.getInfo()
  case class Info(
    id: WorkflowInstanceId,
    parentId: Option[WorkflowInstanceId],
    generation: Long,
    workflowVersionAtCreation: Long,
    createdAt: Instant,
    lastRunAt: Option[Instant],
    timesExecuted: Int,
  )
```

## Rationale

- "Meta" means *descriptive* metadata. Id-only bundles are named `...Id` (`WorkflowInstanceId`), not `...Meta`.
- **`scope` field**: Child workflows need some sort of prefix to prevent collision between themselves. This is not a prefix of the regular key, but a separate field, to prevent collision between this derived key and the arbitrary user-specified key of top-level instances. Creation
  APIs never accept a scope; top-level instances always have `scope = ""`, while
  `startAsChild` derives a non-empty scope. Query APIs have a scope parameter with the same empty default, so omitting it means an exact top-level lookup rather than a
  lookup across all scopes. The database uniqueness key is `(workflowId,
  workflowInstanceKey, scope)`. See `sub-workflows-iteration.md` for derivation.
- **`parentId` field**: tracks the active parent relationship for child workflows (empty for top-level or detached workflows). It belongs to `WorkflowInstance.Info`, not `WorkflowInstanceId`, because the relationship may be cleared when the parent closes while identity must remain stable.
- The workflow-instance storage record also carries runtime-only child signal
  inheritance fields: the selector, `inheritPastEvents`, and
  `inheritedEventsStartSequenceId`. Runtime signal state also retains exact-key
  cursors independently of deletable event rows. These are not identity and are
  omitted from `WorkflowInstance.Info` until an external inspection use case
  requires them. See `child-signal-inheritance.md`.
- Capability vs data: `WorkflowInstance` requires the workflow code and can act; `WorkflowInstance.Info` requires nothing and just reports. Key-based queries without the code can only return `Info`; queries parameterized by a `Workflow[In, Out]` return typed handles. The two meet in exactly one place: `instance.getInfo()`. Nesting `Info` inside `WorkflowInstance` expresses that it is the data view of the same concept, not a second concept.
- The handle retains both `In` and `Out` because it captures a `Workflow[In, Out]`. Every place that can construct a typed handle knows both types; operations that know only an ID return `Info` instead.
- `StepMeta` stays a single class combining step definition fields and the instance id: steps are inline lambdas, so step metadata cannot exist outside an execution — a definition-only `StepMeta` would have no producer or consumer. **`StepMeta` is never used as a lookup key.** Lookups use `WorkflowInstanceId` + `StepId`; since this pair appears only rarely in internal implementation code, a plain tuple suffices — no named type unless it surfaces in a public/SPI signature. Consequently, descriptive fields (`stepName`, `stepDescription`) never affect any lookup, so editing a description never orphans cached results.
- `StepMeta.stepVersion` is present for versioned at-least-once steps and empty for unversioned at-most-once steps. See `workflow-evolution.md` for why version bumps are intentionally unavailable to at-most-once operations.
- Bulk data (serialized inputs, result, step cache) lives in **neither** class: `run()` loads it eagerly but internally; targeted accessors (`getWorkflowResult`) serve the rest. Keeps list queries cheap and both types small.
- No stored status enum for now; completed/unfinished are derived via queries (matches existing implementation).
- `Workflow` carries `WorkflowMeta` (including version and description). `Info` reports only the persisted bookkeeping fields needed by the current API.

## Composition over inheritance

The previous design had `StepMeta extends WorkflowInstanceMeta extends WorkflowMeta` with hand-rolled `equals`/`hashCode`. Decision: flat case classes, composition only.

- Case classes give correct `equals`/`hashCode`/`copy` for free; the hand-rolled inheritance `equals` was asymmetric (violating the equals contract), illustrating the risk.
- Semantically has-a, not is-a: an instance address is not a kind of definition metadata, and a step is not a kind of instance address.
- Subtyping plus contextual abstractions is risky: a given of the subtype would silently satisfy a `using` clause of the supertype.
- Ergonomics are preserved where wanted via `export` clauses instead of inheritance.
- Migration note: code inside steps that relied on `StepMeta` *being* a `WorkflowInstanceMeta` (e.g. `setSignal(using WorkflowInstanceMeta)`) now gets the `WorkflowInstanceId` from `StepMeta.workflowInstanceId` or from the enclosing `WorkflowContext`.

## Considered and rejected: type-level workflow ids

Idea: lift the workflow id to the type level (`WorkflowInstance[WfId]`), resolve the definition via `given Workflow[WfId]`, enabling `run` only when the definition is in scope.

- Rejected because untyped/dynamic handling (ids from config, ops tooling over heterogeneous instances) fights the encoding; recovering the type from a runtime string still needs a runtime check, so it degenerates into the same id-equality check the plain design does at the single untyped-to-typed upgrade point (`runtime.getWorkflowInstance(workflow, key)` validates the workflowId and throws on mismatch).
- Singleton-type inference and error messages get complicated for little added safety.
- Can be revisited later as opt-in sugar without breaking this model.
