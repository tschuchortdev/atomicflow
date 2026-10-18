package atomicflow.impl.db

import atomicflow.ScopePath
import doobie.Meta
import doobie.postgres.implicits.*

/** doobie codec mapping [[ScopePath]] to the same `VARCHAR[]` representation
  * already used for scope columns. */
given Meta[ScopePath] =
  Meta[Array[String]].imap(arr => ScopePath.fromVector(arr.toVector))(scope => scope.segments.toArray)
