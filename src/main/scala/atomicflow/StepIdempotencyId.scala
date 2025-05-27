package atomicflow

import neotype.*

type StepIdempotencyId = StepIdempotencyId.Type

object StepIdempotencyId extends Newtype[String] {
  override inline def validate(input: String): Boolean | String =
    if (input.matches("[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}")) true
    else "Should be an UUID"

  def generate(using runtime: WorkflowRuntime): StepIdempotencyId =
    runtime.generateStepIdempotencyId
}
