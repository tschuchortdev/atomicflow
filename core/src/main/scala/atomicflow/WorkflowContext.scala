package atomicflow

/** The runtime context materialized only during workflow execution. It carries the
  * instance identity and runtime services.
  *
  * NOTE: This is a minimal placeholder created so the [[Workflow]] body type
  * (`In => WorkflowContext ?=> Out`) resolves. The full context (instanceId,
  * versionAtCreation, runtime, signal/step services) is completed by a later
  * task.
  */
trait WorkflowContext
