package atomicflow

import scala.concurrent.duration.*

private[atomicflow] object Constants {
  val libraryVersion: Long = 0

  val defaultCacheTtl: FiniteDuration = 30.days
}
