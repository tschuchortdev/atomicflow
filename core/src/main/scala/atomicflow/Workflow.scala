package atomicflow

/** The typed handle of a workflow instance, obtained only from the runtime. It
  * captures the [[Workflow]] definition it came from plus the instance's stable
  * identity. Methods requiring execution take a contextual `(using
  * WorkflowRuntime)`; further methods arrive in later tasks.
  */
final class WorkflowInstance[In, Out] private[atomicflow] (
    val workflow: Workflow[In, Out],
    val id: WorkflowInstanceId
)

/** A workflow definition: the identifying/descriptive fields plus the executable
  * `body` and its codecs. There is deliberately no separate meta wrapper; every
  * consumer that needs the definition has the executable object (see
  * `spec/core-types.md`).
  *
  * `Cacheable[In]` and `Cacheable[Out]` are captured at construction (the
  * companion's `[In: Cacheable, Out: Cacheable]` context bounds) and are the
  * definition's codecs; only `Cacheable[Throwable]` is resolved at run call
  * sites.
  */
final class Workflow[In, Out] private[atomicflow] (
    val id: WorkflowId,
    val version: Long,
    val name: String,
    val description: Option[String],
    val body: In => WorkflowContext ?=> Out,
    val onUnconsumedSignals: Map[SignalKey, Seq[Any]] => Unit,
    val inputCacheable: Cacheable[In],
    val outputCacheable: Cacheable[Out]
) {

  /** Register the instance for later explicit runs (idempotent). */
  def create(instanceKey: WorkflowInstanceKey, in: In)(using
      runtime: WorkflowRuntime
  ): WorkflowInstance[In, Out] =
    runtime.createWorkflowInstance(this, instanceKey, in)(using inputCacheable)

  /** Register the instance and schedule it for a job runner. */
  def createAndSchedule(instanceKey: WorkflowInstanceKey, in: In)(using
      runtime: WorkflowRuntime
  ): WorkflowInstance[In, Out] =
    runtime.createAndSchedule(this, instanceKey, in)

  /** Run a previously created instance on the caller thread (no input param). */
  def run(instanceKey: WorkflowInstanceKey)(using
      runtime: WorkflowRuntime,
      cacheableThrowable: Cacheable[Throwable]
  ): WorkflowRunResult[Out] =
    runtime.runWorkflowInstance(this, WorkflowInstanceId(id, instanceKey))

  /** Atomically create-if-absent and run on the caller thread. */
  def createAndRun(instanceKey: WorkflowInstanceKey, in: In)(using
      runtime: WorkflowRuntime,
      cacheableThrowable: Cacheable[Throwable]
  ): WorkflowRunResult[Out] =
    runtime.createAndRun(this, instanceKey, in)
}

object Workflow {
  def apply[In: Cacheable, Out: Cacheable](
      id: WorkflowId,
      version: Long = 1L,
      name: String = "",
      description: Option[String] = None
  )(
      body: In => WorkflowContext ?=> Out,
      onUnconsumedSignals: Map[SignalKey, Seq[Any]] => Unit = _ => ()
  ): Workflow[In, Out] =
    new Workflow[In, Out](
      id,
      version,
      name,
      description,
      body,
      onUnconsumedSignals,
      summon[Cacheable[In]],
      summon[Cacheable[Out]]
    )
}
