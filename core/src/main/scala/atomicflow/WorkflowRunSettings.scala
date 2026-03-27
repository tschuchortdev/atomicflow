package atomicflow

import scala.collection.immutable.ListMap
import scala.concurrent.duration.{Duration, FiniteDuration}

case class WorkflowRunSettings(
    defaultCacheTtl: Option[FiniteDuration] = Some(Constants.defaultCacheTtl),
    defaultSignalTtl: Option[FiniteDuration] = Some(Constants.defaultSignalTtl),
    stepIdempotencyIdOverrides: Map[StepId, StepIdempotencyId] = ListMap.empty
)
