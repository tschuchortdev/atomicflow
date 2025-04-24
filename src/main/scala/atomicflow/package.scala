import scala.concurrent.duration.*

package object atomicflow {
  val libraryVersion: Long = 0

  val defaultCacheTtl: FiniteDuration = 30.days
}
