package atomicflow

import scala.collection.immutable.ListMap
import scala.concurrent.duration.FiniteDuration

case class WorkflowDefaultSettings(
    defaultCacheTtl: FiniteDuration = Constants.defaultCacheTtl,
    defaultSignalTtl: FiniteDuration = Constants.defaultSignalTtl,
    stepIdempotencyIdOverrides: Map[StepId, StepIdempotencyId] = ListMap.empty
)
