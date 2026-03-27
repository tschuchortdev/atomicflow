package atomicflow

import java.util.Objects

class StepMeta(val stepId: StepId,
               val stepVersion: Long,
               val stepName: Option[String],
               val stepDescription: Option[String],
               private val workflowInstanceMeta: WorkflowInstanceMeta)
    extends WorkflowInstanceMeta(workflowInstanceMeta) {

  override def hashCode(): Int = 
    Objects.hash(stepId, stepVersion, stepName, stepDescription, workflowInstanceMeta)

  override def equals(obj: Any): Boolean = obj match
    case other: StepMeta if
      other.stepId == this.stepId
        && other.stepVersion == this.stepVersion
        && other.stepName == this.stepName
        && other.stepDescription == this.stepDescription
        && super.equals(other) => true
    case _ => false

  override def toString: String = s"${super.toString}/$stepId"
}
