package atomicflow

import io.github.iltotore.iron.*
import io.github.iltotore.iron.constraint.string.*

case class StepIdempotencyId(id: String :| ValidUUID) {
  override def toString: String = id
}

object StepIdempotencyId {
  def generate(using runtime: WorkflowRuntime): StepIdempotencyId =
    runtime.generateStepIdempotencyId
}