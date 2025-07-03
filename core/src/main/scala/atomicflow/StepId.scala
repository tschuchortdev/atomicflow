package atomicflow

import neotype.*

type StepId = StepId.Type

object StepId extends Newtype[String] {
  override inline def validate(input: String): Boolean | String =
    if (input.matches("[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}")) true
    else "Should be an UUID"
}
