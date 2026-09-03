# Core Types and Taxonomy

Guideline for identity, definition, context, and handle types. Companion to `design.md` and `running-workflows.md`.

> **Status:** This document describes the target design. The current source still
> contains the legacy `WorkflowMeta` / `WorkflowInstanceMeta` / `StepMeta` /
> `SignalMeta` hierarchy; it will be refactored to match this model.

## Five categories

| Category | Types | Contents |
|---|---|---|
| Identity | `WorkflowId`, `WorkflowInstanceKey`, `WorkflowInstanceId`, `StepId`, `SignalKey` | ids only |
| Definition objects | `Workflow[In, Out]`, `Signal[A]` | code + id/key + version + name + description |
| Runtime contexts | `WorkflowContext`, `StepContext` | instance identity + step identity + runtime services; materialized only during execution |
| Persisted instance state | `WorkflowInstance.Info` | bookkeeping fields stored per instance |
| Handle | `WorkflowInstance[In, Out]` | workflow definition (code) + instance id + runtime |

```scala
type WorkflowId = String
type WorkflowInstanceKey = String
type SignalKey = String

// Identity: the address of an instance; used for queries, signals, logging
case class WorkflowInstanceId(
  workflowId: WorkflowId,
  workflowInstanceKey: WorkflowInstanceKey,
  scope: String = ""
)

case class StepId(key: String, scope: String = "")


// Definition objects: the code + its descriptive fields. No separate Meta wrapper.
final class Workflow[In, Out](
  val id: WorkflowId,
  val version: Long,
  val name: String,
  val description: Option[String],
  // body, codecs
)

final class Signal[A](
  val key: SignalKey,
  val name: Option[String] = None,
  val description: Option[String] = None
)(using val cacheable: Cacheable[A])


// Runtime contexts: materialized only during execution; compose stable ids, not Meta wrappers.
trait WorkflowContext:
  def instanceId: WorkflowInstanceId
  def versionAtCreation: Long
  def runtime: WorkflowRuntime

trait StepContext[Out]:
  def stepId: StepId
  def stepVersion: Option[Long]
  def stepName: Option[String]
  def stepDescription: Option[String]
  def workflowCtx: WorkflowContext


// Handle: capability object, obtained only from the runtime
final class WorkflowInstance[In, Out](workflow: Workflow[In, Out], instanceId: WorkflowInstanceId, runtime: WorkflowRuntime):
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

- **Definition objects own their fields directly.** `Workflow` and `Signal[A]` are the definition objects; they carry `id`/`key`, `version`, `name`, and `description` as direct fields. No separate `*Meta` wrapper is needed because there is no metadata-only catalog or introspection API: every consumer that needs the definition also has the executable `Workflow` or typed `Signal` object. A separate descriptor would only introduce indirection (`workflow.meta.workflowId` instead of `workflow.id`).
- **Runtime contexts, not execution-scoped metadata.** `WorkflowContext` and `StepContext` are runtime contexts materialized only during execution. They compose stable IDs (`WorkflowInstanceId`, `StepId`) rather than extending or wrapping metadata classes. Steps are inline lambdas, so step identity fields only exist while a workflow instance executes — `StepContext` carries `stepId`, `stepVersion`, and descriptive fields for logging and diagnostics. **Step identity is never used as a lookup key.** Lookups use `WorkflowInstanceId` + `StepId` + `stepVersion`; since this triple appears only rarely in internal implementation code, a plain tuple suffices. Descriptive fields (`stepName`, `stepDescription`) never affect any lookup, so editing a description never orphans cached results.
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
- `StepContext.stepVersion` is present for versioned at-least-once steps and empty for unversioned at-most-once steps. See `workflow-evolution.md` for why version bumps are intentionally unavailable to at-most-once operations.
- Bulk data (serialized inputs, result, step cache) lives in **neither** the handle nor `Info`: `run()` loads it eagerly but internally; targeted accessors (`getWorkflowResult`) serve the rest. Keeps list queries cheap and both types small.
- No general status enum for now; completion and suspension are derived via queries. `terminate` persists a terminal `Terminated` flag that `run` checks before executing (see `running-workflows.md`).
- `Workflow` carries `version`, `name`, and `description` as direct fields. `Info` reports only the persisted bookkeeping fields needed by the current API.

## Composition over inheritance

The previous design had `StepMeta extends WorkflowInstanceMeta extends WorkflowMeta` with hand-rolled `equals`/`hashCode`. Decision: no metadata wrapper classes; definition objects own their fields, and runtime contexts compose stable IDs.

- Case classes give correct `equals`/`hashCode`/`copy` for free; the hand-rolled inheritance `equals` was asymmetric (violating the equals contract), illustrating the risk.
- Semantically has-a, not is-a: a runtime context has-a stable ID, a step context has-a step ID; neither is a kind of definition metadata.
- Subtyping plus contextual abstractions is risky: a given of the subtype would silently satisfy a `using` clause of the supertype.
- Migration note: code inside steps that relied on `StepMeta` *being* a `WorkflowInstanceMeta` (e.g. `setSignal(using WorkflowInstanceMeta)`) now gets the `WorkflowInstanceId` from `StepContext.workflowCtx.instanceId` or from the enclosing `WorkflowContext`.

## Considered and rejected: type-level workflow ids

Idea: lift the workflow id to the type level (`WorkflowInstance[WfId]`), resolve the definition via `given Workflow[WfId]`, enabling `run` only when the definition is in scope.

- Rejected because untyped/dynamic handling (ids from config, ops tooling over heterogeneous instances) fights the encoding; recovering the type from a runtime string still needs a runtime check, so it degenerates into the same id-equality check the plain design does at the single untyped-to-typed upgrade point (`runtime.getWorkflowInstance(workflow, key)` validates the workflowId and throws on mismatch).
- Singleton-type inference and error messages get complicated for little added safety.
- Can be revisited later as opt-in sugar without breaking this model.
