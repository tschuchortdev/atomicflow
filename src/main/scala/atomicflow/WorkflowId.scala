package atomicflow

import io.github.iltotore.iron.*
import io.github.iltotore.iron.constraint.string.*

case class WorkflowId(id: String :| ValidUUID)
