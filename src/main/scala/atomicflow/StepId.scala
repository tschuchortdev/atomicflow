package atomicflow

import io.github.iltotore.iron.*
import io.github.iltotore.iron.constraint.string.*

case class StepId(id: String :| ValidUUID) {
  override def toString: String = id
}
